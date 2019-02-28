from unittest import TestCase
from calrissian.report import TimedReport, TimedResourceReport, TimelineReport
from freezegun import freeze_time
import datetime

TIME_1000 = datetime.datetime(2000, 1, 1, 10, 0, 0)
TIME_1015 = datetime.datetime(2000, 1, 1, 10, 15, 0)
TIME_1030 = datetime.datetime(2000, 1, 1, 10, 30, 0)
TIME_1045 = datetime.datetime(2000, 1, 1, 10, 45, 0)
TIME_1100 = datetime.datetime(2000, 1, 1, 11, 0, 0)


class TimedReportTestCase(TestCase):

    def setUp(self):
        self.report = TimedReport()

    @freeze_time(TIME_1000)
    def test_start_defaults_to_now(self):
        self.report.start()
        self.assertEqual(self.report.start_time, TIME_1000)

    @freeze_time(TIME_1000)
    def test_start_uses_provided_time(self):
        self.report.start(start_time=TIME_1100)
        self.assertEqual(self.report.start_time, TIME_1100)

    @freeze_time(TIME_1100)
    def test_finish_defaults_to_now(self):
        self.report.finish()
        self.assertEqual(self.report.finish_time, TIME_1100)

    @freeze_time(TIME_1100)
    def test_finish_uses_provided_time(self):
        self.report.finish(finish_time=TIME_1000)
        self.assertEqual(self.report.finish_time, TIME_1000)

    def test_elapsed_seconds(self):
        self.report.start_time = TIME_1000
        self.report.finish_time = TIME_1100
        self.assertEqual(3600.0, self.report.elapsed_seconds())

    def test_elapsed_hours(self):
        self.report.start_time = TIME_1000
        self.report.finish_time = TIME_1100
        self.assertEqual(1.0, self.report.elapsed_hours())

    def test_elapsed_fails_if_not_started(self):
        self.assertEqual(self.report.start_time, None)
        with self.assertRaises(TypeError):
            self.report.elapsed_seconds()

    def test_elapsed_raises_if_not_finished(self):
        self.report.start()
        self.assertEqual(self.report.finish_time, None)
        with self.assertRaises(TypeError):
            self.report.elapsed_seconds()

    def test_elapsed_raises_if_negative(self):
        self.report.start_time = TIME_1100
        self.report.finish_time = TIME_1000
        with self.assertRaises(ValueError):
            self.report.elapsed_seconds()


class TimedResourceReportTestCase(TestCase):

    def setUp(self):
        self.report = TimedResourceReport(start_time=TIME_1000, finish_time=TIME_1015)

    def test_calculates_ram_hours(self):
        # 1024MB for 15 minutes is 256 MB-hours
        self.report.ram_megabytes = 1024
        self.assertEqual(self.report.ram_megabyte_hours(), 256)

    def test_calculates_cpu_hours(self):
        # 8 CPUs for 15 minutes is 2 CPU-hours
        self.report.cpus = 8
        self.assertEqual(self.report.cpu_hours(), 2)

    def test_resources_default_zero(self):
        self.assertEqual(self.report.ram_megabytes, 0)
        self.assertEqual(self.report.cpus, 0)


class TimelineReportTestCase(TestCase):

    def setUp(self):
        self.report = TimelineReport()

    def test_add_report(self):
        child = TimedResourceReport()
        self.report.add_report(child)
        self.assertIn(child, self.report.children)

    def test_total_cpu_hours(self):
        # 1 hour at 1 CPU and 15 minutes at 4 cpu should total 2 CPU hours
        self.report.add_report(TimedResourceReport(start_time=TIME_1000, finish_time=TIME_1100, cpus=1))
        self.report.add_report(TimedResourceReport(start_time=TIME_1000, finish_time=TIME_1015, cpus=4))
        self.assertEqual(self.report.total_cpu_hours(), 2)

    def test_total_ram_megabyte_hours(self):
        # 1 hour at 1024MB and 15 minutes at 8192MB cpu should total 3072 MB/hours
        self.report.add_report(TimedResourceReport(start_time=TIME_1000, finish_time=TIME_1100, ram_megabytes=1024))
        self.report.add_report(TimedResourceReport(start_time=TIME_1000, finish_time=TIME_1015, ram_megabytes=8192))
        self.assertEqual(self.report.total_tasks(), 2)
        self.assertEqual(self.report.total_ram_megabyte_hours(), 3072)

    def test_total_tasks(self):
        self.report.add_report(TimedResourceReport())
        self.report.add_report(TimedResourceReport())
        self.assertEqual(self.report.total_tasks(), 2)

    def test_max_parallel_tasks(self):
        # Count task parallelism. 3 total tasks, but only 2 at a given time
        self.report.add_report(TimedResourceReport(start_time=TIME_1000, finish_time=TIME_1015))
        self.report.add_report(TimedResourceReport(start_time=TIME_1030, finish_time=TIME_1100))
        self.report.add_report(TimedResourceReport(start_time=TIME_1000, finish_time=TIME_1100))
        self.assertEqual(self.report.total_tasks(), 3)
        self.assertEqual(self.report.max_parallel_tasks(), 2)

    def test_max_parallel_tasks_handles_start_finish_bounds(self):
        # If a task finishes at the same time another starts, that is 1 parallel task and not 2
        task_1000_1015 = TimedResourceReport(start_time=TIME_1000, finish_time=TIME_1015)
        task_1015_1030 = TimedResourceReport(start_time=TIME_1015, finish_time=TIME_1030)
        self.assertEqual(task_1000_1015.finish_time, task_1015_1030.start_time)
        self.report.add_report(task_1000_1015)
        self.report.add_report(task_1015_1030)
        self.assertEqual(self.report.total_tasks(), 2)
        self.assertEqual(self.report.max_parallel_tasks(), 1)

    def test_max_parallel_cpus_discrete(self):
        # 4 discrete 15 minute intervals of 1 cpu
        self.report.add_report(TimedResourceReport(start_time=TIME_1000, finish_time=TIME_1015, cpus=1))
        self.report.add_report(TimedResourceReport(start_time=TIME_1015, finish_time=TIME_1030, cpus=1))
        self.report.add_report(TimedResourceReport(start_time=TIME_1030, finish_time=TIME_1045, cpus=1))
        self.report.add_report(TimedResourceReport(start_time=TIME_1045, finish_time=TIME_1100, cpus=1))
        self.assertEqual(self.report.total_tasks(), 4)
        self.assertEqual(self.report.total_cpu_hours(), 1)
        self.assertEqual(self.report.max_parallel_cpus(), 1)

    def test_max_parallel_cpus_overlap(self):
        # 1 cpu over 3 30 minute intervals, with the middle interval overlapping the first and last
        self.report.add_report(TimedResourceReport(start_time=TIME_1000, finish_time=TIME_1030, cpus=1))
        self.report.add_report(TimedResourceReport(start_time=TIME_1015, finish_time=TIME_1045, cpus=1))
        self.report.add_report(TimedResourceReport(start_time=TIME_1030, finish_time=TIME_1100, cpus=1))
        self.assertEqual(self.report.total_tasks(), 3)
        self.assertEqual(self.report.max_parallel_cpus(), 2)

    def test_max_parallel_cpus_complex(self):
        # 4 CPUs for a short burst, overlapping with 1 cpu, then a later period of 1 that doesnt overlap
        self.report.add_report(TimedResourceReport(start_time=TIME_1000, finish_time=TIME_1015, cpus=4))
        self.report.add_report(TimedResourceReport(start_time=TIME_1000, finish_time=TIME_1045, cpus=1))
        self.report.add_report(TimedResourceReport(start_time=TIME_1030, finish_time=TIME_1100, cpus=1))
        self.assertEqual(self.report.total_tasks(), 3)
        self.assertEqual(self.report.max_parallel_cpus(), 5)

    def test_max_parallel_ram_megabytes_discrete(self):
        # 4 discrete 15 minute intervals of 1024 MB
        self.report.add_report(TimedResourceReport(start_time=TIME_1000, finish_time=TIME_1015, ram_megabytes=1024))
        self.report.add_report(TimedResourceReport(start_time=TIME_1015, finish_time=TIME_1030, ram_megabytes=1024))
        self.report.add_report(TimedResourceReport(start_time=TIME_1030, finish_time=TIME_1045, ram_megabytes=1024))
        self.report.add_report(TimedResourceReport(start_time=TIME_1045, finish_time=TIME_1100, ram_megabytes=1024))
        self.assertEqual(self.report.total_tasks(), 4)
        self.assertEqual(self.report.total_ram_megabyte_hours(), 1024)
        self.assertEqual(self.report.max_parallel_ram_megabytes(), 1024)

    def test_max_parallel_ram_megabytes_overlap(self):
        # 1024 MB over 3 30 minute intervals, with the middle interval overlapping the first and last
        self.report.add_report(TimedResourceReport(start_time=TIME_1000, finish_time=TIME_1030, ram_megabytes=1024))
        self.report.add_report(TimedResourceReport(start_time=TIME_1015, finish_time=TIME_1045, ram_megabytes=1024))
        self.report.add_report(TimedResourceReport(start_time=TIME_1030, finish_time=TIME_1100, ram_megabytes=1024))
        self.assertEqual(self.report.total_tasks(), 3)
        self.assertEqual(self.report.total_ram_megabyte_hours(), 1536)
        self.assertEqual(self.report.max_parallel_ram_megabytes(), 2048)


class EventTestCase(TestCase):
    pass

class EventProcessorTestCase(TestCase):
    pass

class MaxParallelCountProcessorTestCase(TestCase):
    pass

class MaxParallelCPUsProcessorTestCase(TestCase):
    pass

class MaxParallelRAMProcessorTestCase(TestCase):
    pass
