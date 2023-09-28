"""Tests for the config module."""

import unittest.mock

from .env import EnvParser


class Test_EnvParser(unittest.TestCase):
    """Tests for the _EnvParseMixin class."""

    @unittest.mock.patch.dict("os.environ", {
        "TEST_STR": "test",
    })
    def test_parsesEnvVars(self):
        class TestConfig(EnvParser):
            TEST_STR: str

        config = TestConfig()

        self.assertEqual("test", config.TEST_STR)

    def test_emptyStringIfRequiredFieldNotSet(self):
        class TestConfig(EnvParser):
            TEST_STR: str

        cfg = TestConfig()
        self.assertEqual("", cfg.TEST_STR)
