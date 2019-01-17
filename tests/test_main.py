from unittest import TestCase
from unittest.mock import patch, call, Mock
from calrissian.main import main


class CalrissianMainTestCase(TestCase):

    @patch('calrissian.main.cwlmain')
    @patch('calrissian.main.arg_parser')
    @patch('calrissian.main.CalrissianExecutor')
    @patch('calrissian.main.CalrissianLoadingContext')
    @patch('calrissian.main.version')
    def test_main_calls_cwlmain_returns_exit_code(self, mock_version, mock_loading_context, mock_executor, mock_arg_parser, mock_cwlmain):
        mock_exit_code = Mock()
        mock_cwlmain.return_value = mock_exit_code
        result = main()
        self.assertTrue(mock_arg_parser.called)
        self.assertEqual(mock_cwlmain.call_args, call(args=mock_arg_parser.return_value.parse_args.return_value,
                                                      executor=mock_executor.return_value,
                                                      loadingContext=mock_loading_context.return_value,
                                                      versionfunc=mock_version))
        self.assertEqual(result, mock_exit_code)
