import os
from unittest import TestCase
from tests.environment_variables import env_vars


class TestUtilsTestCase(TestCase):
    def test_env_vars_context_can_temporarily_unset_environment_variable(self):
        pp = os.environ.get("PATH")
        self.assertIsNotNone(pp)
        with env_vars(PATH=None):
            self.assertIsNone(os.environ.get("PATH"))
        self.assertEqual(pp, os.environ.get("PATH"))

    def test_env_vars_context_can_temporarily_override_environment_variable(self):
        pp = os.environ.get("PATH")
        self.assertIsNotNone(pp)
        override_value = "/non/existing/path"
        with env_vars(PATH=override_value):
            self.assertEqual(override_value, os.environ.get("PATH"))
        self.assertEqual(pp, os.environ.get("PATH"))

    def test_env_vars_context_can_temporarily_add_environment_variables(self):
        test_var1 = "test_env_varname_1_non3xistan1"
        test_var2 = "test_env_varname_2_non3xistan1"
        test_value1 = "value1"
        test_value2 = "value2"
        self.assertIsNone(os.environ.get(test_var1))
        self.assertIsNone(os.environ.get(test_var2))
        with env_vars(test_env_varname_1_non3xistan1=test_value1, test_env_varname_2_non3xistan1=test_value2):
            self.assertEqual(test_value1, os.environ.get(test_var1))
            self.assertEqual(test_value2, os.environ.get(test_var2))

        self.assertIsNone(os.environ.get(test_var1))
        self.assertIsNone(os.environ.get(test_var2))
