"""Tests for the metoffice module."""

import datetime as dt
import pathlib
import unittest.mock

from ._models import MetOfficeFileInfo
from .client import Client, _isWantedFile

# --------- Test setup --------- #

testClient = Client(
    orderID="tmp",
    clientID="tmp",
    clientSecret="tmp",
)

# --------- Client methods --------- #


class TestClient_Init(unittest.TestCase):
    """Tests for the MetOfficeClient.__init__ method."""

    def test_errorsWhenVariablesAreNotSet(self) -> None:
        with self.assertRaises(KeyError):
            _ = Client(orderID="unset", clientID="", clientSecret="test_client_secret")


class TestClient_ConvertRawFileToDataset(unittest.TestCase):
    """Tests for the MetOfficeClient.convertRawFileToDataset method."""

    def test_convertsCorrectly(self) -> None:
        testFilePath: pathlib.Path = pathlib.Path(__file__).parent / "test_knownparam.grib"

        out = testClient.mapTemp(p=testFilePath)

        # Ensure the dimensions have the right sizes
        self.assertDictEqual(
            {"init_time": 1, "variable": 1, "step": 13, "y": 639, "x": 455},
            dict(out.dims.items()),
        )
        # Ensure the dimensions of the variables are in the correct order
        self.assertEqual(("variable", "init_time", "step", "y", "x"), out["UKV"].dims)
        # Ensure the correct variables are in the variable dimension
        self.assertListEqual(["dswrf"], sorted(out.coords["variable"].values))

    def test_renamesVariables(self) -> None:
        testFilePath: pathlib.Path = pathlib.Path(__file__).parent / "test_wrongnameparam.grib"

        out = testClient.mapTemp(p=testFilePath)

        # Ensure the dimensions have the right sizes
        self.assertDictEqual(
            {"init_time": 1, "variable": 1, "step": 13, "y": 639, "x": 455},
            dict(out.dims.items()),
        )
        # Ensure the dimensions of the variables are in the correct order
        self.assertEqual(out["UKV"].dims, ("variable", "init_time", "step", "y", "x"))
        # Ensure the correct variables are in the variable dimension
        self.assertListEqual(["prate"], sorted(out.coords["variable"].values))

    def test_handlesUnknownsInMetOfficeData(self) -> None:
        testFilePath: pathlib.Path = pathlib.Path(__file__).parent / "test_unknownparam1.grib"

        out = testClient.mapTemp(p=testFilePath)

        # Ensure the dimensions have the right sizes
        self.assertDictEqual(
            {"init_time": 1, "variable": 1, "step": 43, "y": 639, "x": 455},
            dict(out.dims.items()),
        )
        # Ensure the dimensions of the variables are in the correct order
        self.assertEqual(out["UKV"].dims, ("variable", "init_time", "step", "y", "x"))
        # Ensure the correct variables are in the variable dimension
        self.assertListEqual(["wdir10"], sorted(out.coords["variable"].values))
        self.assertNotEqual(["unknown"], sorted(out.coords["variable"].values))

        testFilePath: pathlib.Path = pathlib.Path(__file__).parent / "test_unknownparam2.grib"

        out = testClient.mapTemp(p=testFilePath)

        # Ensure the dimensions have the right sizes
        self.assertDictEqual(
            {"init_time": 1, "variable": 1, "step": 10, "y": 639, "x": 455},
            dict(out.dims.items()),
        )
        # Ensure the dimensions of the variables are in the correct order
        self.assertEqual(("variable", "init_time", "step", "y", "x"), out["UKV"].dims)
        # Ensure the correct variables are in the variable dimension
        self.assertListEqual(["si10"], sorted(out.coords["variable"].values))
        self.assertNotEqual(["unknown"], sorted(out.coords["variable"].values))


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
