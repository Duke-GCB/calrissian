from unittest import TestCase
from unittest.mock import patch
from calrissian.context import CalrissianLoadingContext, CalrissianRuntimeContext


class CalrissianLoadingContextTestCase(TestCase):

    @patch('calrissian.context.calrissian_make_tool')
    def test_uses_calrissian_make_tool(self, mock_make_tool):
        ctx = CalrissianLoadingContext()
        self.assertEqual(ctx.construct_tool_object, mock_make_tool)


class CalrissianRuntimeContextTestCase(TestCase):

    def test_has_pod_labels_field(self):
        ctx = CalrissianRuntimeContext()
        self.assertIsNone(ctx.pod_labels)

    def test_sets_pod_labels_field(self):
        labels = {'key1': 'val1'}
        ctx = CalrissianRuntimeContext({'pod_labels':labels})
        self.assertEqual(ctx.pod_labels, labels)
        
    def test_has_pod_nodeselectors_field(self):
        ctx = CalrissianRuntimeContext()
        self.assertIsNone(ctx.pod_nodeselectors)

    def test_sets_nodeselectors_field(self):
        nodeselectors = {'disktype': 'ssd'}
        ctx = CalrissianRuntimeContext({'pod_nodeselectors':nodeselectors})
        self.assertEqual(ctx.pod_nodeselectors, nodeselectors)
    
    def test_has_pod_env_vars_field(self):
        ctx = CalrissianRuntimeContext()
        self.assertIsNone(ctx.pod_labels)

    def test_sets_env_vars_field(self):
        env_vars = {'key1': 'val1'}
        ctx = CalrissianRuntimeContext({'pod_env_vars':env_vars})
        self.assertEqual(ctx.pod_env_vars, env_vars)
        
    def test_has_serviceaccount_field(self):
        ctx = CalrissianRuntimeContext()
        self.assertIsNone(ctx.pod_serviceaccount)

    def test_sets_serviceaccount_field(self):
        serviceaccount = "podmanager"
        ctx = CalrissianRuntimeContext({'pod_serviceaccount':serviceaccount})
        self.assertEqual(ctx.pod_serviceaccount, serviceaccount)
