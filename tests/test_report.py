from unittest import TestCase
from calrissian.report import TimedReport, TimedResourceReport, ParentReport
from freezegun import freeze_time
import datetime

TIME_1000 = datetime.datetime(2000, 1, 1, 10, 0, 0)
TIME_1015 = datetime.datetime(2000, 1, 1, 10, 15, 0)
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


class ParentReportTestCase(TestCase):

    def setUp(self):
        self.parent = ParentReport()

    def test_add_report(self):
        child = TimedResourceReport()
        self.parent.add_report(child)
        self.assertIn(child, self.parent.children)

    def test_total_cpu_hours(self):
        # 1 hour at 1 CPU and 15 minutes at 4 cpu should total 2 CPU hours
        report1 = TimedResourceReport(start_time=TIME_1000, finish_time=TIME_1100, cpus=1)
        report2 = TimedResourceReport(start_time=TIME_1000, finish_time=TIME_1015, cpus=4)
        self.parent.add_report(report1)
        self.parent.add_report(report2)
        self.assertEqual(self.parent.total_cpu_hours(), 2)

    def test_total_ram_megabyte_hours(self):
        # 1 hour at 1024MB and 15 minutes at 8192MB cpu should total 3072 MB/hours
        report1 = TimedResourceReport(start_time=TIME_1000, finish_time=TIME_1100, ram_megabytes=1024)
        report2 = TimedResourceReport(start_time=TIME_1000, finish_time=TIME_1015, ram_megabytes=8192)
        self.parent.add_report(report1)
        self.parent.add_report(report2)
        self.assertEqual(self.parent.total_ram_megabyte_hours(), 3072)

    def test_max_parallel_tasks(self):
        # TODO: walk through the timeline and see how many tasks stack up
        pass

    def test_max_parallel_cpus(self):
        # TODO: walk through the timeline and see how many tasks stack up
        pass

    def test_max_parallel_ram_megabytes(self):
        # TODO: walk through the timeline and see how many tasks stack up
        pass
