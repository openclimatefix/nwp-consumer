import dataclasses
import datetime as dt
import os
import pathlib
import unittest

from returns.result import Failure, ResultE, Success

from ...entities import NWPDimensionCoordinateMap
from .mo_datahub import MetOfficeDatahubModelRepository


class TestMetOfficeDatahubModelRepository(unittest.TestCase):
    """Test the business methods of the MetOfficeDatahubModelRepository class."""

    @unittest.skipIf(
        condition="CI" in os.environ,
        reason="Skipping integration test that requires MetOffice DataHub access.",
    )
    def test__download(self) -> None:
        """Test the _download method."""

        auth_result = MetOfficeDatahubModelRepository.authenticate()
        self.assertIsInstance(auth_result, Success, msg=f"{auth_result!s}")
        c = auth_result.unwrap()

        test_it = c.repository().determine_latest_it_from(dt.datetime.now(tz=dt.UTC))

        dl_result = c._download(
            f"{c.request_url}/agl_u-component-of-wind-surface-adjusted_10.0_{test_it:%Y%m%d%H}_1/data",
        )

        self.assertIsInstance(dl_result, Success, msg=f"{dl_result!s}")


    def test__convert(self) -> None:
        """Test the _convert method."""

        @dataclasses.dataclass
        class TestCase:
            filename: str
            should_error: bool

        tests: list[TestCase] = [
            TestCase(
                filename="test_UM-Global_10u.grib",
                should_error=False,
            ),
            TestCase(
                filename="test_UM-Global_t2m.grib",
                should_error=False,
            ),
            TestCase(
                filename="test_HRES-IFS_10u.grib",
                should_error=True,
            ),
        ]

        expected_coords = dataclasses.replace(
            MetOfficeDatahubModelRepository.model().expected_coordinates,
            init_time=[dt.datetime(2024, 11, 20, 0, tzinfo=dt.UTC)],
        )

        for t in tests:
            with self.subTest(name=t.filename):
                # Attempt to convert the file
                result = MetOfficeDatahubModelRepository._convert(
                    path=pathlib.Path(__file__).parent.absolute() / t.filename,
                )
                region_result: ResultE[dict[str, slice]] = result.do(
                    region
                    for das in result
                    for da in das
                    for region in NWPDimensionCoordinateMap.from_xarray(da).bind(
                        expected_coords.determine_region,
                    )
                )
                if t.should_error:
                    self.assertIsInstance(region_result, Failure, msg=f"{region_result}")
                else:
                    self.assertIsInstance(region_result, Success, msg=f"{region_result}")

