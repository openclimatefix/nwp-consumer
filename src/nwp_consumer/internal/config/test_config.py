"""Tests for the config module."""

import unittest.mock

from .config import _EnvParseMixin


class Test_EnvParseMixin(unittest.TestCase):
    """Tests for the _EnvParseMixin class."""

    @unittest.mock.patch.dict("os.environ", {
        "TEST_STR": "test",
        "TEST_BOOL": "True",
        "TEST_INT": "1",
        "TEST_FLOAT": "1.0",
    })
    def test_parsesEnvVarsToEachType(self):
        class TestConfig(_EnvParseMixin):
            TEST_STR: str
            TEST_BOOL: bool
            TEST_INT: int
            TEST_FLOAT: float

        config = TestConfig()

        self.assertEqual("test", config.TEST_STR)
        self.assertTrue(config.TEST_BOOL)
        self.assertEqual(1, config.TEST_INT)
        self.assertEqual(1.0, config.TEST_FLOAT)

    @unittest.mock.patch.dict("os.environ", {}, clear=True)
    def test_errorsIfRequiredFieldNotSet(self):
        class TestConfig(_EnvParseMixin):
            TEST_STR: str

        with self.assertRaises(EnvironmentError):
            TestConfig()

    @unittest.mock.patch.dict("os.environ", {"TEST_BOOL": "not a bool"})
    def test_errorsIfUnableToCastEnvVarToType(self):
        class TestConfig(_EnvParseMixin):
            TEST_BOOL: bool

        with self.assertRaises(EnvironmentError):
            TestConfig()
