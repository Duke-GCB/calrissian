from unittest import TestCase
from unittest.mock import Mock, patch, call
from calrissian.dask import CalrissianCommandLineDaskJob
from calrissian.tool import CalrissianCommandLineTool, calrissian_make_tool, CalrissianCommandLineToolException
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

    def setUp(self):
        self.toolpath_object = {'id': '1', 'inputs': [], 'outputs': []}
        self.loadingContext = CalrissianLoadingContext()

    @patch('calrissian.tool.CalrissianCommandLineJob')
    def test_make_job_runner(self, mock_command_line_job):
        tool = CalrissianCommandLineTool(self.toolpath_object, self.loadingContext)
        runner = tool.make_job_runner(Mock())
        self.assertEqual(runner, mock_command_line_job)

    def test_fails_use_container_false(self):
        tool = CalrissianCommandLineTool(self.toolpath_object, self.loadingContext)
        runtimeContext = Mock(use_container=False)
        with self.assertRaises(CalrissianCommandLineToolException) as context:
            tool.make_job_runner(runtimeContext)
        self.assertIn('use_container is disabled', str(context.exception))

    def test_fails_no_default_container(self):
        tool = CalrissianCommandLineTool(self.toolpath_object, self.loadingContext)
        runtimeContext = Mock()
        runtimeContext.find_default_container.return_value = None
        with self.assertRaises(CalrissianCommandLineToolException) as context:
            tool.make_job_runner(runtimeContext)
        self.assertIn('no default_container', str(context.exception))

    def test_injects_default_container(self):
        tool = CalrissianCommandLineTool(self.toolpath_object, self.loadingContext)
        runtimeContext = Mock(use_container=True)
        runtimeContext.find_default_container.return_value = 'docker:default-container'
        self.assertEqual([], tool.requirements)
        tool.make_job_runner(runtimeContext)
        self.assertIn({'class': 'DockerRequirement', 'dockerPull': 'docker:default-container'}, tool.requirements)

    def test_uses_docker_requirement_container(self):
        self.toolpath_object['requirements'] = [{'class': 'DockerRequirement', 'dockerPull': 'docker:tool-container'}]
        tool = CalrissianCommandLineTool(self.toolpath_object, self.loadingContext)
        runtimeContext = Mock(use_container=True)
        runtimeContext.find_default_container.return_value = 'docker:default-container'
        tool.make_job_runner(runtimeContext)
        self.assertNotIn({'class': 'DockerRequirement', 'dockerPull': 'docker:default-container'}, tool.requirements)
        self.assertIn({'class': 'DockerRequirement', 'dockerPull': 'docker:tool-container'}, tool.requirements)


    def test_uses_dask_requirement(self):

        # Set up the tool with a Dask requirement
        self.toolpath_object['https://calrissian-cwl.github.io/schema#DaskGatewayRequirement'] = [
            {
                "workerCores": 2,
                "workerCoresLimit": 2,
                "workerMemory": "4G",
                "clusterMaxCores": 8,
                "clusterMaxMemory": "16G",
                "class": "https://calrissian-cwl.github.io/schema#DaskGatewayRequirement"  # From cwl
            }
        ]

        tool = CalrissianCommandLineTool(self.toolpath_object, self.loadingContext)
        runtimeContext = Mock(use_container=True)
        runtimeContext.find_default_container.return_value = 'docker:default-container'

        runner = tool.make_job_runner(runtimeContext)

        self.assertTrue(runner, CalrissianCommandLineDaskJob)
        
