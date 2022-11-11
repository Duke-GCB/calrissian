from unittest import TestCase
from calrissian.report import TimedReport, TimedResourceReport, TimelineReport
from calrissian.report import Event, MaxParallelCountProcessor, MaxParallelCPUsProcessor, MaxParallelRAMProcessor
from calrissian.report import MemoryParser, CPUParser, Reporter
from calrissian.report import initialize_reporter, write_report, default_serializer, sum_ignore_none
from calrissian.k8s import CompletionResult
from freezegun import freeze_time
from unittest.mock import Mock, call, patch
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

    def test_elapsed_is_none_if_not_started(self):
        self.assertIsNone(self.report.start_time)
        self.assertIsNone(self.report.elapsed_seconds())

    def test_elapsed_is_none_if_not_finished(self):
        self.report.start()
        self.assertIsNone(self.report.finish_time)
        self.assertIsNone(self.report.elapsed_seconds())

    def test_elapsed_raises_if_negative(self):
        self.report.start_time = TIME_1100
        self.report.finish_time = TIME_1000
        with self.assertRaises(ValueError):
            self.report.elapsed_seconds()

    def test_to_dict(self):
        self.report.start_time = TIME_1000
        self.report.finish_time = TIME_1100
        self.assertEqual(1.0, self.report.elapsed_hours())
        report_dict = self.report.to_dict()
        self.assertEqual(report_dict['elapsed_seconds'], 3600.0)
        self.assertEqual(report_dict['elapsed_hours'], 1.0)

    def test_to_dict_drops_none(self):
        self.assertIsNone(self.report.name)
        report_dict = self.report.to_dict()
        self.assertNotIn('name', report_dict)

class TimedResourceReportTestCase(TestCase):

    def setUp(self):
        self.report = TimedResourceReport(name='timed-resource-report', start_time=TIME_1000, finish_time=TIME_1015)

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

    def test_create(self):
        name = 'test-job'
        completion_result = CompletionResult(0, '4', '3G', TIME_1000, TIME_1100, [])
        disk_bytes = 10000000
        report = TimedResourceReport.create(name, completion_result, disk_bytes)
        self.assertEqual(report.cpu_hours(), 4)
        self.assertEqual(report.ram_megabyte_hours(), 3000)
        self.assertEqual(report.start_time, TIME_1000)
        self.assertEqual(report.finish_time, TIME_1100)
        self.assertEqual(report.name, 'test-job')
        self.assertEqual(report.disk_megabytes, 10)
        self.assertEqual(report.exit_code, 0)


    def test_to_dict(self):
        self.report.ram_megabytes = 1024
        self.report.disk_megabytes = 512
        self.report.cpus = 8
        self.report.exit_code = 0
        report_dict = self.report.to_dict()
        self.assertEqual(report_dict['start_time'], TIME_1000)
        self.assertEqual(report_dict['finish_time'], TIME_1015)
        self.assertEqual(report_dict['cpu_hours'], 2)
        self.assertEqual(report_dict['ram_megabyte_hours'], 256)
        self.assertEqual(report_dict['disk_megabytes'], 512)
        self.assertEqual(report_dict['exit_code'], 0)
        self.assertEqual(report_dict['name'], 'timed-resource-report')


class TimelineReportTestCase(TestCase):

    def setUp(self):
        self.report = TimelineReport()

    def test_init(self):
        report = TimelineReport(cores_allowed=4, ram_mb_allowed=1024)
        self.assertEqual(report.cores_allowed, 4)
        self.assertEqual(report.ram_mb_allowed, 1024)

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

    def test_calculates_start_finish_times(self):
        self.assertIsNone(self.report.start_time)
        self.assertIsNone(self.report.finish())
        self.report.add_report(TimedResourceReport(start_time=TIME_1015, finish_time=TIME_1100))
        self.report.add_report(TimedResourceReport(start_time=TIME_1000, finish_time=TIME_1030))
        self.report.add_report(TimedResourceReport(start_time=TIME_1030, finish_time=TIME_1045))
        self.assertEqual(self.report.start_time, TIME_1000)
        self.assertEqual(self.report.finish_time, TIME_1100)

    def test_calculates_duration(self):
        self.report.add_report(TimedResourceReport(start_time=TIME_1000, finish_time=TIME_1015))
        self.report.add_report(TimedResourceReport(start_time=TIME_1045, finish_time=TIME_1100))
        self.assertEqual(self.report.elapsed_hours(), 1.0)

    def test_elapsed_is_none_with_no_tasks(self):
        self.assertIsNone(self.report.elapsed_seconds())

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

    def test_to_dict(self):
        self.report.cores_allowed = 4
        self.report.ram_mb_allowed = 4096
        self.report.add_report(TimedResourceReport(start_time=TIME_1000, finish_time=TIME_1030, ram_megabytes=1024))
        self.report.add_report(TimedResourceReport(start_time=TIME_1015, finish_time=TIME_1045, ram_megabytes=1024))
        self.report.add_report(TimedResourceReport(start_time=TIME_1030, finish_time=TIME_1100, ram_megabytes=1024))
        report_dict = self.report.to_dict()
        self.assertEqual(len(report_dict['children']), 3)
        self.assertEqual(report_dict['start_time'], TIME_1000)
        self.assertEqual(report_dict['finish_time'], TIME_1100)
        self.assertEqual(report_dict['cores_allowed'], 4)
        self.assertEqual(report_dict['ram_mb_allowed'], 4096)


class EventTestCase(TestCase):

    def test_init(self):
        mock_report = Mock()
        event = Event(TIME_1100, Event.START, mock_report)
        self.assertEqual(event.time, TIME_1100)
        self.assertEqual(event.type, Event.START)
        self.assertEqual(event.report, mock_report)

    def test_start_event_classmethod(self):
        mock_report = Mock(start_time=TIME_1000)
        event = Event.start_event(mock_report)
        self.assertEqual(event.time, TIME_1000)
        self.assertEqual(event.type, Event.START)
        self.assertEqual(event.report, mock_report)

    def test_finish_event_classmethod(self):
        mock_report = Mock(finish_time=TIME_1100)
        event = Event.finish_event(mock_report)
        self.assertEqual(event.time, TIME_1100)
        self.assertEqual(event.type, Event.FINISH)
        self.assertEqual(event.report, mock_report)

    def test_processor(self):
        mock_report = Mock()
        event = Event(TIME_1100, Event.START, mock_report)
        mock_processor = Mock()
        event.process(mock_processor)
        self.assertTrue(mock_processor.process.call_args, call(mock_report, Event.START))


class MaxParallelCountProcessorTestCase(TestCase):

    def test_init(self):
        processor = MaxParallelCountProcessor()
        self.assertEqual(processor.count, 0)
        self.assertEqual(processor.max, 0)

    def test_count_unit(self):
        processor = MaxParallelCountProcessor()
        self.assertEqual(processor.count_unit(Mock()), 1)

    def test_process_start_event(self):
        processor = MaxParallelCountProcessor()
        mock_report = Mock()
        self.assertEqual(processor.count, 0)
        processor.process(mock_report, Event.START)
        self.assertEqual(processor.count, 1)
        self.assertEqual(processor.max, 1)

    def test_process_finish_event(self):
        processor = MaxParallelCountProcessor()
        processor.count = 1 # set to 1 since finish should step down
        processor.max = 1
        mock_report = Mock()
        self.assertEqual(processor.count, 1)
        processor.process(mock_report, Event.FINISH)
        self.assertEqual(processor.count, 0)
        self.assertEqual(processor.max, 1) # Max should not drop down

    def test_process_simple_event_stream(self):
        processor = MaxParallelCountProcessor()
        mock_report = Mock()
        processor.process(mock_report, Event.START)
        processor.process(mock_report, Event.FINISH)
        self.assertEqual(processor.result(), 1)

    def test_process_complicated_event_stream(self):
        # get the max up to 3
        processor = MaxParallelCountProcessor()
        mock_report = Mock()
        processor.process(mock_report, Event.START)
        processor.process(mock_report, Event.START)
        processor.process(mock_report, Event.START)
        processor.process(mock_report, Event.FINISH)
        processor.process(mock_report, Event.START)
        processor.process(mock_report, Event.FINISH)
        processor.process(mock_report, Event.FINISH)
        processor.process(mock_report, Event.FINISH)
        self.assertEqual(processor.result(), 3)


class MaxParallelCPUsProcessorTestCase(TestCase):

    def test_count_unit(self):
        processor = MaxParallelCPUsProcessor()
        mock_report = Mock(cpus=10)
        self.assertEqual(processor.count_unit(mock_report), 10)


class MaxParallelRAMProcessorTestCase(TestCase):

    def test_count_unit(self):
        processor = MaxParallelRAMProcessor()
        mock_report = Mock(ram_megabytes=512)
        self.assertEqual(processor.count_unit(mock_report), 512)


class MemoryParserTestCase(TestCase):

    def test_parse(self):
        self.assertEqual(MemoryParser.parse('200'), 200)
        self.assertEqual(MemoryParser.parse('1Gi'), 1073741824)
        self.assertEqual(MemoryParser.parse('1G'), 1000000000)
        self.assertEqual(MemoryParser.parse('16Mi'), 16777216)
        self.assertEqual(MemoryParser.parse('16M'), 16000000)
        self.assertEqual(MemoryParser.parse('2.5G'), 2500000000)

    def test_parse_to_megabytes(self):
        self.assertEqual(MemoryParser.parse_to_megabytes('1M'), 1)
        self.assertEqual(MemoryParser.parse_to_megabytes('1G'), 1000)
        self.assertEqual(MemoryParser.parse_to_megabytes('1K'), .001)

    def test_raises_when_not_string(self):
        with self.assertRaises(ValueError) as context:
            MemoryParser.parse(100)
        self.assertIn('Unable to parse \'100\' as memory', str(context.exception))
        self.assertIn(MemoryParser.url, str(context.exception))

    def test_raises_with_invalid_suffix(self):
        with self.assertRaises(ValueError) as context:
            MemoryParser.parse('3GW')
        self.assertIn('Unable to parse \'3GW\' as memory', str(context.exception))
        self.assertIn(MemoryParser.url, str(context.exception))

    def test_raises_with_invalid_value(self):
        with self.assertRaises(ValueError) as context:
            MemoryParser.parse('FiveGB')
        self.assertIn('Unable to parse \'FiveGB\' as memory', str(context.exception))
        self.assertIn(MemoryParser.url, str(context.exception))


class CPUParserTestCase(TestCase):

    def test_parse(self):
        self.assertEqual(CPUParser.parse('6'), 6)
        self.assertEqual(CPUParser.parse('300m'), 0.3)
        self.assertEqual(CPUParser.parse('0.1'), 0.1)

    def test_raises_when_not_string(self):
        with self.assertRaises(ValueError) as context:
            CPUParser.parse(10)
        self.assertIn('Unable to parse \'10\' as cpu', str(context.exception))
        self.assertIn(CPUParser.url, str(context.exception))

    def test_raises_with_invalid_suffix(self):
        with self.assertRaises(ValueError) as context:
            CPUParser.parse('3L')
        self.assertIn('Unable to parse \'3L\' as cpu', str(context.exception))
        self.assertIn(CPUParser.url, str(context.exception))

    def test_raises_with_invalid_value(self):
        with self.assertRaises(ValueError) as context:
            CPUParser.parse('FiveCores')
        self.assertIn('Unable to parse \'FiveCores\' as cpu', str(context.exception))
        self.assertIn(CPUParser.url, str(context.exception))


class ReporterTestCase(TestCase):

    def setUp(self):
        Reporter.initialize()

    def test_add_report(self):
        mock_report = Mock()
        Reporter.add_report(mock_report)
        self.assertIn(mock_report, Reporter.get_report().children)

    def test_get_report(self):
        mock_timeline_report = Mock()
        Reporter.timeline_report = mock_timeline_report
        self.assertEqual(Reporter.get_report(), mock_timeline_report)


class ReporterFunctionsTestCase(TestCase):

    def test_initialize_reporter(self):
        self.assertIsNone(Reporter.get_report())
        initialize_reporter(0, 0)
        self.assertIsNotNone(Reporter.get_report())

    @patch('builtins.open')
    def test_write_report_raises_if_not_initialized(self, mock_open):
        Reporter.timeline_report = None
        self.assertIsNone(Reporter.get_report())
        with self.assertRaises(AttributeError) as context:
            write_report('filename.out')
        self.assertIn('NoneType', str(context.exception))

    @patch('builtins.open')
    @patch('calrissian.report.json')
    def test_write_report(self, mock_json, mock_open):
        initialize_reporter(128, 4)
        Reporter.add_report(TimedResourceReport(cpus=1, ram_megabytes=128, start_time=TIME_1000, finish_time=TIME_1100))
        write_report('output.json')
        self.assertEqual(mock_open.call_args, call('output.json', 'w'))
        self.assertTrue(mock_json.dump.called)

    def test_default_serializer_handles_dates(self):
        serialized = default_serializer(TIME_1100)
        self.assertEqual(serialized, '2000-01-01T11:00:00')

    def test_default_serializer_raises_on_other(self):
        with self.assertRaises(TypeError) as context:
            default_serializer('other')
        self.assertIn('not serializable', str(context.exception))

    @patch('builtins.open')
    @patch('calrissian.report.json')
    def test_write_report_recovers(self, mock_json, mock_open):
        initialize_reporter(128, 4)
        Reporter.add_report(TimedResourceReport(cpus=1, ram_megabytes=128))
        write_report('output.json')
        self.assertEqual(mock_open.call_args, call('output.json', 'w'))
        self.assertTrue(mock_json.dump.called)


class SumIgnoreNoneTestCase(TestCase):

    def test_sum(self):
        self.assertEqual(sum_ignore_none([1,2,3]), 6)

    def test_drops_none(self):
        self.assertEqual(sum_ignore_none([1,2,None]), 3)

    def test_nones_become_zero(self):
        self.assertEqual(sum_ignore_none([None,None,False]), 0)
