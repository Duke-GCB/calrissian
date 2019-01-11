from unittest import TestCase
from unittest.mock import Mock, patch, call
from calrissian.tool import CalrissianCommandLineTool, calrissian_make_tool
from calrissian.context import CalrissianLoadingContext


class CalrissianMakeToolTestCase(TestCase):

    @patch('calrissian.tool.CalrissianCommandLineTool')
    def test_make_tool_clt(self, mock_calrissian_clt):
        spec = {'class': 'CommandLineTool'}
        loadingContext = CalrissianLoadingContext()
        made_tool = calrissian_make_tool(spec, loadingContext)
        self.assertEqual(made_tool, mock_calrissian_clt.return_value)
        self.assertEqual(mock_calrissian_clt.call_args, call(spec, loadingContext))

    @patch('calrissian.tool.default_make_tool')
    def test_make_tool_default_make_tool(self, mock_default_make_tool):
        spec = {'class': 'AnyOtherThing'}
        loadingContext = Mock()
        made_tool = calrissian_make_tool(spec, loadingContext)
        self.assertEqual(made_tool, mock_default_make_tool.return_value)
        self.assertEqual(mock_default_make_tool.call_args, call(spec, loadingContext))


class CalrissianCommandLineToolTestCase(TestCase):

    @patch('calrissian.tool.CalrissianCommandLineJob')
    def test_make_job_runner(self, mock_command_line_job):
        toolpath_object = {'id': '1', 'inputs': [], 'outputs': []}
        loadingContext = CalrissianLoadingContext()
        tool = CalrissianCommandLineTool(toolpath_object, loadingContext)
        runner = tool.make_job_runner(Mock())
        self.assertEqual(runner, mock_command_line_job)
