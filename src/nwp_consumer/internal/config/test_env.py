"""Tests for the config module."""

import unittest.mock

from .env import EnvParser, ICONEnv


class TestConfig(EnvParser):
    """Test config class."""

    REQUIRED_STR: str
    REQUIRED_BOOL: bool
    REQUIRED_INT: int
    OPTIONAL_STR: str = "default"
    OPTIONAL_BOOL: bool = True
    OPTIONAL_INT: int = 4


class Test_EnvParser(unittest.TestCase):
    """Tests for the _EnvParseMixin class."""

    @unittest.mock.patch.dict(
        "os.environ",
        {
            "REQUIRED_STR": "required_str",
            "REQUIRED_BOOL": "false",
            "REQUIRED_INT": "5",
        },
    )
    def test_parsesEnvVars(self) -> None:
        tc = TestConfig()

        self.assertEqual("required_str", tc.REQUIRED_STR)
        self.assertFalse(tc.REQUIRED_BOOL)
        self.assertEqual(5, tc.REQUIRED_INT)
        self.assertEqual("default", tc.OPTIONAL_STR)
        self.assertTrue(tc.OPTIONAL_BOOL)
        self.assertEqual(4, tc.OPTIONAL_INT)

    @unittest.mock.patch.dict(
        "os.environ",
        {
            "REQUIRED_STR": "required_str",
            "REQUIRED_BOOL": "not a bool",
            "REQUIRED_INT": "5.7",
        },
    )
    def test_errorsIfCantCastType(self) -> None:
        with self.assertRaises(ValueError):
            TestConfig()

    def test_errorsIfRequiredFieldNotSet(self) -> None:
        with self.assertRaises(OSError):
            TestConfig()

    @unittest.mock.patch.dict(
        "os.environ", {"ICON_HOURS": "3", "ICON_PARAMETER_GROUP": "basic"}
    )
    def test_parsesIconConfig(self) -> None:
        tc = ICONEnv()

        self.assertEqual(3, tc.ICON_HOURS)
        self.assertEqual("basic", tc.ICON_PARAMETER_GROUP)
