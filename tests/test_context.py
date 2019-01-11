from unittest import TestCase
from unittest.mock import patch
from calrissian.context import CalrissianLoadingContext


class CalrissianLoadingContextTestCase(TestCase):

    @patch('calrissian.context.calrissian_make_tool')
    def test_uses_calrissian_make_tool(self, mock_make_tool):
        ctx = CalrissianLoadingContext()
        self.assertEqual(ctx.construct_tool_object, mock_make_tool)
