import os
import logging
import json
from datetime import datetime
import threading

log = logging.getLogger("calrissian.report")

SECONDS_PER_HOUR = 60.0 * 60.0


class TimedReport(object):
    """
    Report on operations with a specific start time and finish time.
    """

    def __init__(self, name=None, start_time=None, finish_time=None):
        self.name = name
        self.start_time = start_time
        self.finish_time = finish_time

    def start(self, start_time=None):
        self.start_time = start_time if start_time else datetime.now()

    def finish(self, finish_time=None):
        self.finish_time = finish_time if finish_time else datetime.now()

    def elapsed_seconds(self):
        if self.start_time is None or self.finish_time is None:
            # one of the times is None, cannot report
            return None
        delta = self.finish_time - self.start_time
        total_seconds = delta.total_seconds()
        if total_seconds < 0:
            raise ValueError('Negative time is not allowed: {}'.format(total_seconds))
        else:
            return total_seconds

    def elapsed_hours(self):
        elapsed_seconds = self.elapsed_seconds()
        if elapsed_seconds:
            return elapsed_seconds / SECONDS_PER_HOUR
        else:
            return None

    def to_dict(self):
        # Create a dict of our variables, filtering out None
        result = dict((k,v) for k,v in vars(self).items() if v is not None)
        result['elapsed_hours'] = self.elapsed_hours()
        result['elapsed_seconds'] = self.elapsed_seconds()
        return result


class ResourceParser(object):
    """
    Base class for converting Kubernetes resources (memory/CPU) from strings to reportable numbers
    """
    kind = None
    url = None
    suffixes = None

    @classmethod
    def parse(cls, value):
        try:
            for suffix, factor in cls.suffixes.items():
                if value.endswith(suffix):
                    return float(value.replace(suffix, '')) * factor
            # No suffix, assume raw number
            return float(value)
        except Exception:
            raise ValueError('Unable to parse \'{}\' as {}. See {}'.format(value, cls.kind, cls.url))


class MemoryParser(ResourceParser):
    """
    Converts Kubernetes memory resource strings (e.g. 1Mi, 1G)to byte quantities
    """
    kind = 'memory'
    url = 'https://kubernetes.io/docs/concepts/configuration/manage-compute-resources-container/#meaning-of-memory'
    suffixes = {
        'E': 1e18,
        'P': 1e15,
        'T': 1e12,
        'G': 1e9,
        'M': 1e6,
        'K': 1e3,
        'Ei': 2**60,
        'Pi': 2**50,
        'Ti': 2**40,
        'Gi': 2**30,
        'Mi': 2**20,
        'Ki': 2**10,
    }

    @classmethod
    def parse_to_megabytes(cls, value):
        return (cls.parse(value) / cls.suffixes['M'])


class CPUParser(ResourceParser):
    """
    Converts Kubernetes CPU resource strings (e.g. 2, 200m) to floating point CPU quantities.
    """

    kind = 'cpu'
    url = 'https://kubernetes.io/docs/concepts/configuration/manage-compute-resources-container/#meaning-of-cpu'
    suffixes = {
        'm': 0.001
    }


class TimedResourceReport(TimedReport):
    """
    Adds CPU, memory, and disk values to TimedReport, in order to calculate resource usage over the
    duration of the timed report. These values, by convention, are the kubernetes **requested**
    resources (not limits or actual).
    """
    def __init__(self, cpus=0, ram_megabytes=0, disk_megabytes=0, exit_code=0, *args, **kwargs):
        self.cpus = cpus
        self.ram_megabytes = ram_megabytes
        self.disk_megabytes = disk_megabytes
        self.exit_code = exit_code
        super(TimedResourceReport, self).__init__(*args, **kwargs)

    def ram_megabyte_hours(self):
        elapsed_hours = self.elapsed_hours()
        if elapsed_hours:
            return self.ram_megabytes * elapsed_hours
        else:
            return None

    def cpu_hours(self):
        elapsed_hours = self.elapsed_hours()
        if elapsed_hours:
            return self.cpus * elapsed_hours
        else:
            return None

    def to_dict(self):
        result = super(TimedResourceReport, self).to_dict()
        result['ram_megabyte_hours'] = self.ram_megabyte_hours()
        result['cpu_hours'] = self.cpu_hours()
        result['exit_code'] = self.exit_code
        return result

    @classmethod
    def create(cls, name, completion_result, disk_bytes):
        cpus = CPUParser.parse(completion_result.cpus)
        ram_megabytes = MemoryParser.parse_to_megabytes(completion_result.memory)
        disk_megabytes = MemoryParser.parse_to_megabytes(str(disk_bytes))

        return cls(name=name, start_time=completion_result.start_time, finish_time=completion_result.finish_time, cpus=cpus,
                   ram_megabytes=ram_megabytes, disk_megabytes=disk_megabytes,
                   exit_code=completion_result.exit_code)


class Event(object):
    """
    Represents a start or finish event in a report, associated with its time.
    Event objects are intended to be sorted into a list and processed
    """

    # These numeric values are used to sort finish events before start events
    # if the Event.time is identical
    START = 1
    FINISH = -1

    def __init__(self, time, type, report):
        self.time = time
        self.type = type
        self.report = report

    @classmethod
    def start_event(cls, report):
        """
        Generate a start event for the provided report at its start time
        :param report:
        :return:
        """
        return Event(report.start_time, Event.START, report)

    @classmethod
    def finish_event(cls, report):
        """
        Generate a finish event for the provided report at its finish time
        :param report: a TimedResourceReport
        :return: an Event
        """
        return Event(report.finish_time, Event.FINISH, report)

    def process(self, processor):
        """
        Call the processor's process method with this Event's report and type
        :param processor: Object that implements .process(report, type)
        :return: None
        """
        processor.process(self.report, self.type)


class MaxParallelCountProcessor(object):
    """
    Simple processor to track the maximum parallel reports
    The process() method add count_unit (1) to self.count
    for each START and subtracts it for each FINISH, and recomputes self.max on each iteration
    """

    def __init__(self):
        self.count = 0
        self.max = 0

    def count_unit(self, report):
        """
        The unit to count when processing an event. Override this based on the report to calculate different
        max parallel metrics
        :param report: Report for context (not used in base class)
        :return: The value to count (here, 1)
        """
        return 1

    def process(self, report, event_type):
        """
        Examine the event type, add/subtract the count unit, and recompute max
        :param report: The report to consider
        :param event_type: Event.START or Event.FINISH
        :return: None
        """
        if event_type == Event.START:
            self.count += self.count_unit(report)
        elif event_type == Event.FINISH:
            self.count -= self.count_unit(report)
        self.max = max(self.max, self.count)

    def result(self):
        return self.max


class MaxParallelCPUsProcessor(MaxParallelCountProcessor):
    """
    Subclass of MaxParallelCountProcessor that counts cpus
    """

    def count_unit(self, report):
        return report.cpus


class MaxParallelRAMProcessor(MaxParallelCountProcessor):
    """
    Subclass of MaxParallelCountProcessor that counts ram_megabytes
    """

    def count_unit(self, report):
        return report.ram_megabytes


def sum_ignore_none(iterable):
    """
    Sum function that skips None values
    :param iterable: An iterable to sum, may contain None or False values
    :return: the sum of the numeric values
    """
    return sum([x for x in iterable if x])


class TimelineReport(TimedReport):
    """
    A TimedReport that contains children.
    Can calculate totals and parallel statistics
    Automatically computes start_time and finish_time based on earliest/latest child reports
    """

    def __init__(self, cores_allowed=0, ram_mb_allowed=0, *args, **kwargs):
        self.cores_allowed = cores_allowed
        self.ram_mb_allowed = ram_mb_allowed
        self.children = []
        super(TimelineReport, self).__init__(*args, **kwargs)

    def add_report(self, report):
        self.children.append(report)
        self._recalculate_times()

    def total_cpu_hours(self):
        return sum_ignore_none([child.cpu_hours() for child in self.children])

    def total_ram_megabyte_hours(self):
        return sum_ignore_none([child.ram_megabyte_hours() for child in self.children])

    def total_disk_megabytes(self):
        return sum_ignore_none([child.disk_megabytes for child in self.children])

    def total_tasks(self):
        return len(self.children)

    def max_parallel_cpus(self):
        processor = MaxParallelCPUsProcessor()
        self._walk(processor)
        return processor.result()

    def max_parallel_ram_megabytes(self):
        processor = MaxParallelRAMProcessor()
        self._walk(processor)
        return processor.result()

    def max_parallel_tasks(self):
        processor = MaxParallelCountProcessor()
        self._walk(processor)
        return processor.result()

    def _recalculate_times(self):
        start_times = [c.start_time for c in self.children if c.start_time]
        if start_times:
            self.start_time = sorted(start_times)[0]
        finish_times = [c.finish_time for c in self.children if c.finish_time]
        if finish_times:
            self.finish_time = sorted(finish_times)[-1]

    def _walk(self, processor):
        events = []
        for report in self.children:
            events.append(Event.start_event(report))
            events.append(Event.finish_event(report))
        # Sort the events by their time and type, putting finishes ahead of starts when simultaneous.
        for event in sorted(events, key=lambda x: (x.time, x.type,)):
            event.process(processor)
        return processor.result()

    def to_dict(self):
        result = super(TimelineReport, self).to_dict()
        result['total_cpu_hours'] = self.total_cpu_hours()
        result['total_ram_megabyte_hours'] = self.total_ram_megabyte_hours()
        result['total_disk_megabytes'] = self.total_disk_megabytes()
        result['total_tasks'] = self.total_tasks()
        result['max_parallel_cpus'] = self.max_parallel_cpus()
        result['max_parallel_ram_megabytes'] = self.max_parallel_ram_megabytes()
        result['max_parallel_tasks'] = self.max_parallel_tasks()
        result['children'] = [x.to_dict() for x in self.children]
        return result


class Reporter(object):
    """
    Singleton thread-safe reporting class
    """
    # Initially None to force initaliziation
    timeline_report = None
    lock = threading.Lock()

    @staticmethod
    def initialize(cores_allowed=0, ram_mb_allowed=0):
        with Reporter.lock:
            Reporter.timeline_report = TimelineReport(cores_allowed, ram_mb_allowed)

    @staticmethod
    def add_report(report):
        with Reporter.lock:
            Reporter.timeline_report.add_report(report)

    @staticmethod
    def get_report():
        with Reporter.lock:
            return Reporter.timeline_report



def default_serializer(obj):
    """
    Function to handle JSON serialization of objects that cannot be natively serialized
    :param obj: object to seralize
    :return: JSON-compatible representation of object
    """
    if isinstance(obj, (datetime)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))


def initialize_reporter(max_ram_mb, max_cores):
    Reporter.initialize(cores_allowed=max_cores, ram_mb_allowed=max_ram_mb)


def write_report(filename):
    with open(filename, 'w') as f:
        json.dump(Reporter.get_report().to_dict(), f, indent=4, default=default_serializer)
