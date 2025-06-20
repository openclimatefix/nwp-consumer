import dataclasses
import datetime as dt
import os
import pathlib
import unittest
from typing import TYPE_CHECKING
from unittest.mock import patch

from returns.result import Failure, ResultE, Success

from ...entities import NWPDimensionCoordinateMap, Parameter
from .ecmwf_realtime import ECMWFRealTimeS3RawRepository

if TYPE_CHECKING:
    import xarray as xr

    from nwp_consumer.internal import entities


class TestECMWFRealTimeS3RawRepository(unittest.TestCase):
    """Test the business methods of the ECMWFRealTimeS3RawRepository class."""

    @unittest.skipIf(
        condition="CI" in os.environ,
        reason="Skipping integration test that requires S3 access.",
    )  # TODO: Move into integration tests, or remove
    def test__download_and_convert(self) -> None:
        """Test the _download_and_convert method."""

        auth_result = ECMWFRealTimeS3RawRepository.authenticate()
        self.assertIsInstance(auth_result, Success, msg=f"{auth_result!s}")
        c: ECMWFRealTimeS3RawRepository = auth_result.unwrap()

        test_it: dt.datetime = dt.datetime(2024, 10, 25, 0, tzinfo=dt.UTC)
        test_coordinates: entities.NWPDimensionCoordinateMap = dataclasses.replace(
            c.model().expected_coordinates,
            init_time=[test_it],
        )

        urls: list[str] = [
            f"s3://{f}"
            for f in c._fs.ls(f"{c.bucket}/ecmwf")
            if c._wanted_file(
                filename=f.split("/")[-1],
                it=test_it,
                max_step=max(c.model().expected_coordinates.step),
            )
        ]

        for url in urls:
            with self.subTest(url=url):
                result = c._download_and_convert(url)

                self.assertIsInstance(result, Success, msg=f"{result!s}")

                da: xr.DataArray = result.unwrap()[0]
                determine_region_result = NWPDimensionCoordinateMap.from_xarray(da).bind(
                    test_coordinates.determine_region,
                )
                self.assertIsInstance(
                    determine_region_result,
                    Success,
                    msg=f"{determine_region_result!s}",
                )

    def test__wanted_file(self) -> None:
        """Test the _wanted_file method."""

        @dataclasses.dataclass
        class TestCase:
            name: str
            filename: str
            expected: bool

        test_it: dt.datetime = dt.datetime(2024, 10, 25, 0, tzinfo=dt.UTC)

        tests: list[TestCase] = [
            TestCase(
                name="valid_filename",
                filename=f"A2D{test_it:%m%d%H%M}102516001",
                expected=True,
            ),
            TestCase(
                name="invalid_init_time",
                filename="A2D09250600102516002",
                expected=False,
            ),
            TestCase(
                name="invalid_prefix",
                filename=f"GGC{test_it:%m%d%H%M}102516002",
                expected=False,
            ),
            TestCase(
                name="unexpected_extension",
                filename="A2D10251200102516001.nc",
                expected=False,
            ),
            TestCase(
                name="step_too_large",
                filename="A2D10251200102916001",
                expected=False,
            ),
        ]

        for t in tests:
            with self.subTest(name=t.name):
                result = ECMWFRealTimeS3RawRepository._wanted_file(
                    filename=t.filename,
                    it=test_it,
                    max_step=max(ECMWFRealTimeS3RawRepository.model().expected_coordinates.step),
                )
                self.assertEqual(result, t.expected)

    def test__convert(self) -> None:
        """Test the _convert method."""

        @dataclasses.dataclass
        class TestCase:
            filename: str
            expected_coords: NWPDimensionCoordinateMap
            should_error: bool

        tests: list[TestCase] = [
            TestCase(
                filename="test_ECMWFRealtime_HRES-IFS_ssrd_20241104T00_S60.grib",
                expected_coords=dataclasses.replace(
                    ECMWFRealTimeS3RawRepository.model().expected_coordinates,
                    init_time=[dt.datetime(2024, 11, 4, 0, tzinfo=dt.UTC)],
                    variable=[Parameter.DOWNWARD_SHORTWAVE_RADIATION_FLUX_GL],
                    step=[60],
                ),
                should_error=False,
            ),
            TestCase(
                filename="test_ECMWFRealtime_HRES-IFS_10u_20241104T00_S60.grib",
                expected_coords=dataclasses.replace(
                    ECMWFRealTimeS3RawRepository.model().expected_coordinates,
                    init_time=[dt.datetime(2024, 11, 4, 0, tzinfo=dt.UTC)],
                    variable=[Parameter.WIND_U_COMPONENT_10m],
                    step=[60],
                ),
                should_error=False,
            ),
            TestCase(
                filename="test_NOAAS3_HRES-GFS_10u_20210509T06_S00.grib",
                expected_coords=ECMWFRealTimeS3RawRepository.model().expected_coordinates,
                should_error=True,
            ),
        ]

        for t in tests:
            with self.subTest(name=t.filename):
                # Attempt to convert the file
                result = ECMWFRealTimeS3RawRepository._convert(
                    path=pathlib.Path(__file__).parent.absolute() / "test_gribs" / t.filename,
                )
                region_result: ResultE[dict[str, slice]] = result.do(
                    region
                    for das in result
                    for da in das
                    for region in NWPDimensionCoordinateMap.from_xarray(da).bind(
                        t.expected_coords.determine_region,
                    )
                )
                if t.should_error:
                    self.assertIsInstance(region_result, Failure, msg=f"{region_result}")
                else:
                    self.assertIsInstance(region_result, Success, msg=f"{region_result}")

    @patch.dict(os.environ, {"MODEL": "hres-ifs-india"}, clear=True)
    def test_convert_india(self) -> None:
        return self.test__convert()
