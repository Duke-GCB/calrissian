import logging
from datetime import datetime

log = logging.getLogger("calrissian.report")

SECONDS_PER_HOUR = 60.0 * 60.0

class TimedReport(object):

    def __init__(self, start_time=None, finish_time=None):
        self.start_time = start_time
        self.finish_time = finish_time

    def start(self, start_time=None):
        self.start_time = start_time if start_time else datetime.now()

    def finish(self, finish_time=None):
        self.finish_time = finish_time if finish_time else datetime.now()

    def elapsed_seconds(self):
        delta = self.finish_time - self.start_time
        total_seconds = delta.total_seconds()
        if total_seconds < 0:
            raise ValueError('Negative time is not allowed: {}'.format(total_seconds))
        else:
            return total_seconds

    def elapsed_hours(self):
        return self.elapsed_seconds() / SECONDS_PER_HOUR


class TimedResourceReport(TimedReport):
    """
    Tracks RAM (in megabytes) and CPU (in cores) usage
    """

    def __init__(self, cpus=0, ram_megabytes=0, *args, **kwargs):
        self.cpus = cpus
        self.ram_megabytes = ram_megabytes
        super(TimedResourceReport, self).__init__(*args, **kwargs)

    def ram_megabyte_hours(self):
        return self.ram_megabytes * self.elapsed_hours()

    def cpu_hours(self):
        return self.cpus * self.elapsed_hours()


class Event(object):

    # These are used to sort finish events before start events if occurring at the same time
    START = 1
    FINISH = -1

    def __init__(self, time, type, report):
        self.time = time
        self.type = type
        self.report = report

    @classmethod
    def start_event(cls, report):
        return Event(report.start_time, Event.START, report)

    @classmethod
    def finish_event(cls, report):
        return Event(report.finish_time, Event.FINISH, report)

    def process(self, processor):
        processor.process(self.report, self.type)


class MaxParallelCountProcessor(object):

    def __init__(self):
        self.count = 0
        self.max = 0

    def increment(self, report):
        self.count += 1

    def decrement(self, report):
        self.count -= 1

    def process(self, report, event_type):
        if event_type == Event.START:
            self.increment(report)
        elif event_type == Event.FINISH:
            self.decrement(report)
        self.max = max(self.max, self.count)

    def result(self):
        return self.max


class MaxParallelCPUsProcessor(MaxParallelCountProcessor):

    def increment(self, report):
        self.count += report.cpus

    def decrement(self, report):
        self.count -= report.cpus


class MaxParallelRAMProcessor(MaxParallelCountProcessor):

    def increment(self, report):
        self.count += report.ram_megabytes

    def decrement(self, report):
        self.count -= report.ram_megabytes


class TimelineReport(TimedReport):

    def __init__(self, *args, **kwargs):
        self.children = []
        super(TimelineReport, self).__init__(*args, **kwargs)

    def add_report(self, report):
        self.children.append(report)

    def total_cpu_hours(self):
        return sum([child.cpu_hours() for child in self.children])

    def total_ram_megabyte_hours(self):
        return sum([child.ram_megabyte_hours() for child in self.children])

    def total_tasks(self):
        return len(self.children)

    def _walk(self, processor):
        events = []
        for report in self.children:
            events.append(Event.start_event(report))
            events.append(Event.finish_event(report))
        # Sort the events by their time and type, putting finishes ahead of starts when simultaneous.
            events = sorted(events , key=lambda x: (x.time, x.type,))
        for event in events :
            event.process(processor)
        return processor.result()

    def max_parallel_tasks(self):
        processor = MaxParallelCountProcessor()
        self._walk(processor)
        return processor.result()

    def max_parallel_cpus(self):
        processor = MaxParallelCPUsProcessor()
        self._walk(processor)
        return processor.result()

    def max_parallel_ram_megabytes(self):
        processor = MaxParallelRAMProcessor()
        self._walk(processor)
        return processor.result()
