from unittest import TestCase
from unittest.mock import patch, call, Mock

from calrissian.main import main, add_arguments, parse_arguments, handle_sigterm, install_signal_handler
from cwltool.argparser import arg_parser


class CalrissianMainTestCase(TestCase):

    @patch('calrissian.main.cwlmain')
    @patch('calrissian.main.arg_parser')
    @patch('calrissian.main.CalrissianExecutor')
    @patch('calrissian.main.CalrissianLoadingContext')
    @patch('calrissian.main.CalrissianRuntimeContext')
    @patch('calrissian.main.version')
    @patch('calrissian.main.parse_arguments')
    @patch('calrissian.main.add_arguments')
    @patch('calrissian.main.delete_pods')
    @patch('calrissian.main.install_signal_handler')
    @patch('calrissian.main.write_report')
    @patch('calrissian.main.initialize_reporter')
    @patch('calrissian.main.CPUParser', autospec=True)
    @patch('calrissian.main.MemoryParser', autospec=True)
    def test_main_calls_cwlmain_returns_exit_code(self, mock_memory_parser, mock_cpu_parser,
                                                  mock_initialize_reporter, mock_write_report,
                                                  mock_install_signal_handler, mock_delete_pods,
                                                  mock_add_arguments, mock_parse_arguments, mock_version,
                                                  mock_runtime_context, mock_loading_context, mock_executor,
                                                  mock_arg_parser, mock_cwlmain):
        mock_exit_code = Mock()
        mock_cwlmain.return_value = mock_exit_code  # not called before main
        result = main()
        self.assertTrue(mock_arg_parser.called)
        self.assertEqual(mock_add_arguments.call_args, call(mock_arg_parser.return_value))
        self.assertEqual(mock_parse_arguments.call_args, call(mock_arg_parser.return_value))
        self.assertEqual(mock_memory_parser.parse_to_megabytes.call_args, call(mock_arg_parser.return_value.max_ram))
        self.assertEqual(mock_cpu_parser.parse.call_args, call(mock_arg_parser.return_value.max_cores))
        self.assertEqual(mock_executor.call_args,
                         call(mock_memory_parser.parse_to_megabytes.return_value, mock_cpu_parser.parse.return_value))
        self.assertTrue(mock_runtime_context.called)
        self.assertEqual(mock_cwlmain.call_args, call(args=mock_parse_arguments.return_value,
                                                      executor=mock_executor.return_value,
                                                      loadingContext=mock_loading_context.return_value,
                                                      runtimeContext=mock_runtime_context.return_value,
                                                      versionfunc=mock_version))
        self.assertEqual(mock_runtime_context.return_value.select_resources,
                         mock_executor.return_value.select_resources)
        self.assertEqual(result, mock_exit_code)
        self.assertTrue(mock_delete_pods.called)  # called after main()
        self.assertTrue(mock_write_report.called)
        self.assertEqual(mock_initialize_reporter.call_args, call(mock_memory_parser.parse_to_megabytes.return_value, mock_cpu_parser.parse.return_value))

    def test_add_arguments(self):
        mock_parser = Mock()
        add_arguments(mock_parser)
        self.assertEqual(mock_parser.add_argument.call_count, 4)

    @patch('calrissian.main.sys')
    def test_parse_arguments_exits_without_ram_or_cores(self, mock_sys):
        mock_parser = Mock()
        mock_parser.parse_args.return_value = Mock(max_ram=None, max_cores=None, version=None)
        parse_arguments(mock_parser)
        self.assertEqual(mock_sys.exit.call_args, call(1))

    @patch('calrissian.main.sys')
    def test_parse_arguments_exits_with_ram_but_no_cores(self, mock_sys):
        mock_parser = Mock()
        mock_parser.parse_args.return_value = Mock(max_ram=2048, max_cores=None, version=None)
        parse_arguments(mock_parser)
        self.assertEqual(mock_sys.exit.call_args, call(1))

    @patch('calrissian.main.sys')
    def test_parse_arguments_succeeds_with_ram_and_cores(self, mock_sys):
        mock_parser = Mock()
        mock_parser.parse_args.return_value = Mock(max_ram=2048, max_cores=3, version=None)
        parsed = parse_arguments(mock_parser)
        self.assertEqual(parsed, mock_parser.parse_args.return_value)
        self.assertFalse(mock_sys.exit.called)

    @patch('calrissian.main.sys')
    @patch('calrissian.main.print_version')
    def test_parse_arguments_exits_with_version(self, mock_print_version, mock_sys):
        mock_parser = Mock()
        mock_parser.parse_args.return_value = Mock(version=True)
        parsed = parse_arguments(mock_parser)
        self.assertEqual(parsed, mock_parser.parse_args.return_value)
        self.assertTrue(mock_print_version.called)
        self.assertEqual(mock_sys.exit.call_args, call(0))

    @patch('calrissian.main.sys')
    @patch('calrissian.main.delete_pods')
    def test_handle_sigterm_exits_with_signal(self, mock_delete_pods, mock_sys):
        frame = Mock()
        signum = 15
        handle_sigterm(signum, frame)
        self.assertEqual(mock_sys.exit.call_args, call(signum))
        self.assertTrue(mock_delete_pods.called)

    @patch('calrissian.main.signal')
    @patch('calrissian.main.handle_sigterm')
    def test_install_signal_handler(self, mock_handle_sigterm, mock_signal):
        install_signal_handler()
        self.assertEqual(mock_signal.signal.call_args, call(mock_signal.SIGTERM, mock_handle_sigterm))
