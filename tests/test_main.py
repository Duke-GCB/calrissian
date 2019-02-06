from unittest import TestCase
from unittest.mock import patch, call, Mock
from calrissian.main import main, add_arguments, parse_arguments


class CalrissianMainTestCase(TestCase):

    @patch('calrissian.main.cwlmain')
    @patch('calrissian.main.arg_parser')
    @patch('calrissian.main.CalrissianExecutor')
    @patch('calrissian.main.CalrissianLoadingContext')
    @patch('calrissian.main.RuntimeContext')
    @patch('calrissian.main.version')
    @patch('calrissian.main.parse_arguments')
    @patch('calrissian.main.add_arguments')
    def test_main_calls_cwlmain_returns_exit_code(self, mock_add_arguments, mock_parse_arguments, mock_version, mock_runtime_context, mock_loading_context, mock_executor, mock_arg_parser, mock_cwlmain):
        mock_exit_code = Mock()
        mock_cwlmain.return_value = mock_exit_code
        result = main()
        self.assertTrue(mock_arg_parser.called)
        self.assertEqual(mock_add_arguments.call_args, call(mock_arg_parser.return_value))
        self.assertEqual(mock_parse_arguments.call_args, call(mock_arg_parser.return_value))
        self.assertEqual(mock_executor.call_args, call(mock_parse_arguments.return_value.max_ram, mock_parse_arguments.return_value.max_cores))
        self.assertTrue(mock_runtime_context.called)
        self.assertEqual(mock_cwlmain.call_args, call(args=mock_parse_arguments.return_value,
                                                      executor=mock_executor.return_value,
                                                      loadingContext=mock_loading_context.return_value,
                                                      runtimeContext=mock_runtime_context.return_value,
                                                      versionfunc=mock_version))
        self.assertEqual(mock_runtime_context.return_value.select_resources, mock_executor.return_value.select_resources)
        self.assertEqual(result, mock_exit_code)

    def test_add_arguments(self):
        mock_parser = Mock()
        add_arguments(mock_parser)
        self.assertEqual(mock_parser.add_argument.call_count, 2)

    @patch('calrissian.main.sys')
    def test_parse_arguments_exits_without_ram_or_cores(self, mock_sys):
        mock_parser = Mock()
        mock_parser.parse_args.return_value = Mock(max_ram=None, max_cores=None)
        parse_arguments(mock_parser)
        self.assertEqual(mock_sys.exit.call_args, call(1))

    @patch('calrissian.main.sys')
    def test_parse_arguments_exits_with_ram_but_no_cores(self, mock_sys):
        mock_parser = Mock()
        mock_parser.parse_args.return_value = Mock(max_ram=2048, max_cores=None)
        parse_arguments(mock_parser)
        self.assertEqual(mock_sys.exit.call_args, call(1))

    @patch('calrissian.main.sys')
    def test_parse_arguments_succeeds_with_ram_and_cores(self, mock_sys):
        mock_parser = Mock()
        mock_parser.parse_args.return_value = Mock(max_ram=2048, max_cores=3)
        parsed = parse_arguments(mock_parser)
        self.assertEqual(parsed, mock_parser.parse_args.return_value)
        self.assertFalse(mock_sys.exit.called)

    @patch('calrissian.main.sys')
    @patch('calrissian.main.version')
    def test_parse_arguments_exits_with_version(self, mock_version, mock_sys):
        mock_parser = Mock()
        mock_parser.parse_args.return_value = Mock(version=True)
        parsed = parse_arguments(mock_parser)
        self.assertEqual(parsed, mock_parser.parse_args.return_value)
        self.assertTrue(mock_version.called)
        self.assertEqual(mock_sys.exit.call_args, call(0))
