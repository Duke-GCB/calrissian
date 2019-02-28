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


class ParentReport(TimedReport):

    def __init__(self, *args, **kwargs):
        self.children = []
        super(ParentReport, self).__init__(*args, **kwargs)

    def add_report(self, report):
        self.children.append(report)

    def total_cpu_hours(self):
        return sum([child.cpu_hours() for child in self.children])

    def total_ram_megabyte_hours(self):
        return sum([child.ram_megabyte_hours() for child in self.children])

    def total_tasks(self):
        return len(self.children)

    def max_parallel_tasks(self):
        return None

    def max_parallel_cpus(self):
        return None

    def max_parallel_ram_megabytes(self):
        return None
