import dataclasses
import datetime as dt
import os
import pathlib
import unittest
from typing import TYPE_CHECKING

import s3fs
from returns.result import Failure, ResultE, Success

from ...entities import NWPDimensionCoordinateMap, Parameter
from .noaa_s3 import NOAAS3RawRepository

if TYPE_CHECKING:
    import xarray as xr

    from nwp_consumer.internal import entities


class TestNOAAS3RawRepository(unittest.TestCase):
    """Test the business methods of the NOAAS3RawRepository class."""

    @unittest.skipIf(
        condition="CI" in os.environ,
        reason="Skipping integration test that requires S3 access.",
    )  # TODO: Move into integration tests, or remove
    def test__download_and_convert(self) -> None:
        """Test the _download_and_convert method."""

        c: NOAAS3RawRepository = NOAAS3RawRepository.authenticate().unwrap()

        test_it: dt.datetime = dt.datetime(2024, 10, 24, 12, tzinfo=dt.UTC)
        test_coordinates: entities.NWPDimensionCoordinateMap = dataclasses.replace(
            c.model().expected_coordinates,
            init_time=[test_it],
        )

        fs = s3fs.S3FileSystem(anon=True)
        bucket_path: str = f"noaa-gfs-bdp-pds/gfs.{test_it:%Y%m%d}/{test_it:%H}/atmos"
        urls: list[str] = [
            f"s3://{f}"
            for f in fs.ls(bucket_path)
            if c._wanted_file(
                filename=f.split("/")[-1],
                it=test_it,
                steps=c.model().expected_coordinates.step,
            )
        ]

        for url in urls:
            with self.subTest(url=url):
                result = c._download_and_convert(url=url, it=test_it)

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
                filename=f"gfs.t{test_it:%H}z.pgrb2.1p00.f003",
                expected=True,
            ),
            TestCase(
                name="invalid_init_time",
                filename="gfs.t02z.pgrb2.1p00.f000",
                expected=False,
            ),
            TestCase(
                name="invalid_prefix",
                filename=f"gfs.t{test_it:%H}z.pgrb2.0p20.f006",
                expected=False,
            ),
            TestCase(
                name="unexpected_extension",
                filename=f"gfs.t{test_it:%H}z.pgrb2.1p00.f030.nc",
                expected=False,
            ),
            TestCase(
                name="step_too_large",
                filename=f"gfs.t{test_it:%H}z.pgrb2.1p00.f049",
                expected=False,
            ),
            TestCase(
                name="step_too_small",
                filename=f"gfs.t{test_it:%H}z.pgrb2.1p00.f000",
                expected=False,
            ),
        ]

        for t in tests:
            with self.subTest(name=t.name):
                result = NOAAS3RawRepository._wanted_file(
                    filename=t.filename,
                    it=test_it,
                    steps=NOAAS3RawRepository.model().expected_coordinates.step,
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
                filename="test_NOAAS3_HRES-GFS_10u_20210509T06_S00.grib",
                expected_coords=dataclasses.replace(
                    NOAAS3RawRepository.model().expected_coordinates,
                    init_time=[dt.datetime(2021, 5, 9, 6, tzinfo=dt.UTC)],
                    variable=[Parameter.WIND_U_COMPONENT_10m],
                    step=[0],
                ),
                should_error=False,
            ),
            TestCase(
                filename="test_NOAAS3_HRES-GFS_dswrf-dlwrf_20250129T06_S27.grib",
                expected_coords=dataclasses.replace(
                    NOAAS3RawRepository.model().expected_coordinates,
                    init_time=[dt.datetime(2025, 1, 29, 6, tzinfo=dt.UTC)],
                    variable=[
                        Parameter.DOWNWARD_LONGWAVE_RADIATION_FLUX_GL,
                        Parameter.DOWNWARD_SHORTWAVE_RADIATION_FLUX_GL,
                    ],
                    step=[27],
                ),
                should_error=False,
            ),
            TestCase(
                filename="test_NOAAS3_HRES-GFS_tcc_20250129T00_S06.grib",
                expected_coords=dataclasses.replace(
                    NOAAS3RawRepository.model().expected_coordinates,
                    init_time=[dt.datetime(2025, 1, 29, 0, tzinfo=dt.UTC)],
                    variable=[Parameter.CLOUD_COVER_TOTAL],
                    step=[6],
                ),
                should_error=False,
            ),
            TestCase(
                filename="test_NOAAS3_HRES-GFS_aptmp_20210509T06_S00.grib",
                expected_coords=NOAAS3RawRepository.model().expected_coordinates,
                should_error=True,
            ),
            TestCase(
                filename="test_MODatahub_UM-Global_t2m_20241120T00_S00.grib",
                expected_coords=NOAAS3RawRepository.model().expected_coordinates,
                should_error=True,
            ),
        ]

        for t in tests:
            with self.subTest(name=t.filename):
                # Attempt to convert the file
                result = NOAAS3RawRepository._convert(
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
