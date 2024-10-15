"""Tests for the metoffice module."""

import datetime as dt
import pathlib
import unittest.mock

from ._models import MetOfficeFileInfo
from .client import Client, _isWantedFile

# --------- Test setup --------- #

testClient = Client(
    orderID="tmp",
    apiKey="tmp",
)

# --------- Client methods --------- #


class TestClient_Init(unittest.TestCase):
    """Tests for the MetOfficeClient.__init__ method."""

    def test_errorsWhenVariablesAreNotSet(self) -> None:
        with self.assertRaises(KeyError):
            _ = Client(orderID="tmp", apiKey="")


class TestClient(unittest.TestCase):
    """Tests for the MetOfficeClient."""

    def test_mapCachedRaw(self) -> None:

        tests = [
            {
                "filename": "test_knownparam.grib",
                "expected_dims": ["init_time", "step", "y", "x"],
                "expected_var": "dswrf",
            },
            {
                "filename": "test_unknownparam1.grib",
                "expected_dims": ["init_time", "step", "y", "x"],
                "expected_var": "wdir10",
            },
            {
                "filename": "test_unknownparam2.grib",
                "expected_dims": ["init_time", "step", "y", "x"],
                "expected_var": "si10",
            },
        ]

        for tst in tests:
            with self.subTest(f"test file {tst['filename']}"):
                out = testClient.mapCachedRaw(p=pathlib.Path(__file__).parent / tst["filename"])

                # Ensure the dimensions of the variables are correct
                for data_var in out.data_vars:
                    self.assertEqual(list(out[data_var].dims), tst["expected_dims"])
                # Ensure the correct variable is in the data_vars
                self.assertTrue(tst["expected_var"] in list(out.data_vars.keys()))
                # Ensure no unknowns
                self.assertNotIn("unknown", list(out.data_vars.keys()))


# --------- Static methods --------- #


class Test_IsWantedFile(unittest.TestCase):
    """Tests for the _isWantedFile method."""

    def test_correctlyFiltersMetOfficeFileInfos(self) -> None:
        initTime: dt.datetime = dt.datetime(
            year=2023,
            month=3,
            day=24,
            hour=0,
            minute=0,
            tzinfo=dt.timezone.utc,
        )

        wantedFileInfos: list[MetOfficeFileInfo] = [
            MetOfficeFileInfo(
                fileId="agl_temperature_1.5_2023032400",
                runDateTime=dt.datetime(
                    year=2023, month=3, day=24, hour=0, minute=0, tzinfo=dt.timezone.utc,
                ),
            ),
            MetOfficeFileInfo(
                fileId="ground_downward-short-wave-radiation-flux_2023032400",
                runDateTime=dt.datetime(
                    year=2023, month=3, day=24, hour=0, minute=0, tzinfo=dt.timezone.utc,
                ),
            ),
        ]

        unwantedFileInfos: list[MetOfficeFileInfo] = [
            MetOfficeFileInfo(
                fileId="agl_temperature_1.5+00",
                runDateTime=dt.datetime(
                    year=2023, month=3, day=24, hour=0, minute=0, tzinfo=dt.timezone.utc,
                ),
            ),
            MetOfficeFileInfo(
                fileId="agl_temperature_1.5_2023032403",
                runDateTime=dt.datetime(
                    year=2023, month=3, day=24, hour=3, minute=0, tzinfo=dt.timezone.utc,
                ),
            ),
        ]

        self.assertTrue(all(_isWantedFile(fi=fo, dit=initTime) for fo in wantedFileInfos))
        self.assertFalse(all(_isWantedFile(fi=fo, dit=initTime) for fo in unwantedFileInfos))
