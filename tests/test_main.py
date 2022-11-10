from unittest import TestCase
from unittest.mock import patch, call, Mock
from calrissian.main import main, add_arguments, parse_arguments
from calrissian.main import handle_sigterm, install_signal_handler, install_tees, flush_tees
from calrissian.main import activate_logging, get_log_level, print_version
import logging

class CalrissianMainTestCase(TestCase):

    @patch('calrissian.main.cwlmain')
    @patch('calrissian.main.arg_parser')
    @patch('calrissian.main.ThreadPoolJobExecutor', autospec=True)
    @patch('calrissian.main.CalrissianLoadingContext', autospec=True)
    @patch('calrissian.main.CalrissianRuntimeContext', autospec=True)
    @patch('calrissian.main.version')
    @patch('calrissian.main.parse_arguments')
    @patch('calrissian.main.add_arguments')
    @patch('calrissian.main.delete_pods')
    @patch('calrissian.main.install_signal_handler')
    @patch('calrissian.main.write_report')
    @patch('calrissian.main.initialize_reporter')
    @patch('calrissian.main.CPUParser', autospec=True)
    @patch('calrissian.main.MemoryParser', autospec=True)
    @patch('calrissian.main.install_tees')
    @patch('calrissian.main.flush_tees')
    @patch('calrissian.main.get_log_level')
    @patch('calrissian.main.activate_logging')
    def test_main_calls_cwlmain_returns_exit_code(self, mock_activate_logging, mock_get_log_level,
                                                  mock_flush_tees, mock_install_tees,
                                                  mock_memory_parser, mock_cpu_parser,
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
        self.assertEqual(mock_get_log_level.call_args, call(mock_parse_arguments.return_value))
        self.assertEqual(mock_activate_logging.call_args, call(mock_get_log_level.return_value))
        self.assertEqual(mock_memory_parser.parse_to_megabytes.call_args, call(mock_parse_arguments.return_value.max_ram))
        self.assertEqual(mock_cpu_parser.parse.call_args, call(mock_parse_arguments.return_value.max_cores))
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
        self.assertEqual(mock_install_tees.call_args, call(mock_parse_arguments.return_value.stdout, mock_parse_arguments.return_value.stderr))
        self.assertTrue(mock_flush_tees.called)

    def test_add_arguments(self):
        mock_parser = Mock()
        add_arguments(mock_parser)
        self.assertEqual(mock_parser.add_argument.call_count, 9)

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

    @patch('calrissian.main.subprocess')
    @patch('calrissian.main.os')
    @patch('calrissian.main.sys')
    def test_install_tees_handles_stdout(self, mock_sys, mock_os, mock_subprocess):
        install_tees(stdout_path='/stdout.txt')
        self.assertEqual(mock_subprocess.Popen.call_args, call(['tee','/stdout.txt'], stdin=mock_subprocess.PIPE))
        self.assertEqual(mock_os.dup2.call_args, call(mock_subprocess.Popen.return_value.stdin.fileno.return_value, mock_sys.stdout.fileno.return_value))

    @patch('calrissian.main.subprocess')
    @patch('calrissian.main.os')
    @patch('calrissian.main.sys')
    def test_install_tees_handles_stderr(self, mock_sys, mock_os, mock_subprocess):
        install_tees(stderr_path='/stderr.txt')
        self.assertEqual(mock_subprocess.Popen.call_args, call(['tee >&2 /stderr.txt'], stdin=mock_subprocess.PIPE, shell=True))
        self.assertEqual(mock_os.dup2.call_args, call(mock_subprocess.Popen.return_value.stdin.fileno.return_value, mock_sys.stderr.fileno.return_value))

    @patch('calrissian.main.subprocess')
    @patch('calrissian.main.os')
    @patch('calrissian.main.sys')
    def test_install_tees_quotes_if_needed(self, mock_sys, mock_os, mock_subprocess):
        install_tees(stdout_path='/stdout file with spaces.txt', stderr_path='/stderr.txt; ls')
        self.assertIn(call(['tee', '/stdout file with spaces.txt'], stdin=mock_subprocess.PIPE), mock_subprocess.Popen.mock_calls)
        self.assertIn(call(['tee >&2 \'/stderr.txt; ls\''], shell=True, stdin=mock_subprocess.PIPE), mock_subprocess.Popen.mock_calls)

    @patch('calrissian.main.subprocess')
    @patch('calrissian.main.os')
    @patch('calrissian.main.sys')
    def test_install_tees_does_nothing_without_files(self, mock_sys, mock_os, mock_subprocess):
        install_tees()
        self.assertFalse(mock_subprocess.Popen.called)
        self.assertFalse(mock_os.dup2.called)
        self.assertFalse(mock_sys.stdout.fileno.called)
        self.assertFalse(mock_sys.stderr.fileno.called)

    @patch('calrissian.main.sys')
    def test_flush_tees(self, mock_sys):
        flush_tees()
        self.assertTrue(mock_sys.stdout.flush.called)
        self.assertTrue(mock_sys.stderr.flush.called)

    @patch('calrissian.main.logging')
    def test_activate_logging(self, mock_logging):
        mock_level = Mock()
        activate_logging(mock_level)
        self.assertEqual(mock_logging.getLogger.call_count, 12) #
        #  setLevel should be called 6 times
        self.assertEqual([call(mock_level)] * 6, mock_logging.getLogger.return_value.setLevel.mock_calls)
        # addHandler should be called 6 times
        mock_streamhandler = mock_logging.StreamHandler.return_value
        self.assertEqual([call(mock_streamhandler)] * 6, mock_logging.getLogger.return_value.addHandler.mock_calls)

    def test_get_log_level(self):
        args_quiet = Mock(quiet=True, verbose=False, debug=False)
        args_verbose = Mock(quiet=False, verbose=True, debug=False)
        args_debug = Mock(quiet=False, verbose=False, debug=True)
        args_default = Mock(quiet=False, verbose=False, debug=False)
        self.assertEqual(get_log_level(args_quiet), logging.CRITICAL)
        self.assertEqual(get_log_level(args_verbose), logging.INFO)
        self.assertEqual(get_log_level(args_debug), logging.DEBUG)
        self.assertEqual(get_log_level(args_default), logging.WARNING)

    @patch('calrissian.main.version')
    @patch('builtins.print')
    def test_print_version(self, mock_print, mock_version):
        print_version()
        self.assertEqual(mock_print.call_args, call(mock_version.return_value))
