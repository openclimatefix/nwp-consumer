import dataclasses
import datetime as dt
import pathlib
import unittest

from nwp_consumer.internal.core import domain

from .dwd_icon import DWDIconRollingArchive


class TestDWDIcon(unittest.TestCase):

    def test_list_fileset(self) -> None:
        repo = DWDIconRollingArchive()
        init_time = dt.datetime(2021, 1, 1, 0, tzinfo=dt.UTC)

        @dataclasses.dataclass
        class TestContainer:
            name: str
            request: domain.DataRequest
            expected_paths: list[str]

        test_cases: list[TestContainer] = [
            TestContainer(
                name="basic_europe",
                request=domain.DataRequest(
                    area=domain.AREAS.eu,
                    steps=[0, 1],
                    init_time=init_time,
                    parameters=[
                        domain.Parameter("temperature_2m", "t_2m", domain.ureg.Unit("K")),
                    ],
                ),
                expected_paths=[
                    f"opendata.dwd.de/weather/nwp/icon-eu/grib/00/t_2m/icon-eu_europe_regular-lat-lon_single-level_{init_time:%Y%m%d%H}_000_T_2M.grib2.bz2",
                    f"opendata.dwd.de/weather/nwp/icon-eu/grib/00/t_2m/icon-eu_europe_regular-lat-lon_single-level_{init_time:%Y%m%d%H}_001_T_2M.grib2.bz2",
                ],
            ),
            TestContainer(
                name="basic_global",
                request=domain.DataRequest(
                    area=domain.AREAS.gl,
                    steps=[0, 1],
                    init_time=init_time,
                    parameters=[
                        domain.Parameter("temperature_2m", "t_2m", domain.ureg.Unit("K")),
                        domain.Parameter("downward_shortwave_radiation_flux", "asob_s", domain.ureg.Unit("W/m^2")),
                    ],
                ),
                expected_paths=[
                    "opendata.dwd.de/weather/nwp/icon/grib/00/t_2m/icon_global_icosahedral_single-level_2021010100_000_T_2M.grib2.bz2",
                    "opendata.dwd.de/weather/nwp/icon/grib/00/t_2m/icon_global_icosahedral_single-level_2021010100_001_T_2M.grib2.bz2",
                    "opendata.dwd.de/weather/nwp/icon/grib/00/asob_s/icon_global_icosahedral_single-level_2021010100_000_ASOB_S.grib2.bz2",
                    "opendata.dwd.de/weather/nwp/icon/grib/00/asob_s/icon_global_icosahedral_single-level_2021010100_001_ASOB_S.grib2.bz2",
                ],
            ),
            TestContainer(
                name="single_and_multi_europe",
                request=domain.DataRequest(
                    area=domain.AREAS.eu,
                    steps=[0, 1],
                    init_time=init_time,
                    parameters=[
                        domain.Parameter("temperature_2m", "t_2m", domain.ureg.Unit("K")),
                        domain.Parameter(
                            "temperature_850hPa",
                            "t_850",
                            domain.ureg.Unit("K"),
                            level_type="multi",
                            level_units="hPa",
                            level_value=850,
                        ),
                        domain.Parameter(
                            "snow_depth",
                            "w_snow",
                            domain.ureg.Unit("m"),
                            level_type="multi",
                            level_units="hPa",
                            level_value=900,
                        ),
                    ],
                ),
                expected_paths=[
                    "opendata.dwd.de/weather/nwp/icon-eu/grib/00/t_2m/icon-eu_europe_regular-lat-lon_single-level_2021010100_000_T_2M.grib2.bz2",
                    "opendata.dwd.de/weather/nwp/icon-eu/grib/00/t_2m/icon-eu_europe_regular-lat-lon_single-level_2021010100_001_T_2M.grib2.bz2",
                    "opendata.dwd.de/weather/nwp/icon-eu/grib/00/t_850/icon-eu_europe_regular-lat-lon_pressure-level_2021010100_000_850_T_850.grib2.bz2",
                    "opendata.dwd.de/weather/nwp/icon-eu/grib/00/t_850/icon-eu_europe_regular-lat-lon_pressure-level_2021010100_001_850_T_850.grib2.bz2",
                    "opendata.dwd.de/weather/nwp/icon-eu/grib/00/w_snow/icon-eu_europe_regular-lat-lon_pressure-level_2021010100_000_900_W_SNOW.grib2.bz2",
                    "opendata.dwd.de/weather/nwp/icon-eu/grib/00/w_snow/icon-eu_europe_regular-lat-lon_pressure-level_2021010100_001_900_W_SNOW.grib2.bz2",
                ],
            ),
        ]

        for case in test_cases:
            with self.subTest(name=case.name):
                result = repo.list_fileset(init_time, case.request).unwrap()
                self.assertEqual(len(result), len(case.expected_paths))
                self.assertListEqual(
                    [file.path.as_posix() for file in result],
                    case.expected_paths,
                )

    def test_download_file(self) -> None:
        repo = DWDIconRollingArchive()
        init_time = dt.datetime.now(tz=dt.UTC).replace(hour=0, minute=0, second=0, microsecond=0)

        def _gen_filemetadata(name: str) -> domain.SourceFileMetadata:
            model = "icon-eu" if "icon-eu" in name else "icon"
            return domain.SourceFileMetadata(
                name=name,
                extension=name.split(".", maxsplit=1)[-1],
                path=pathlib.Path(f"opendata.dwd.de/weather/nwp/{model}/grib/00/t_2m/{name}"),
                scheme="https",
                size_bytes=600000,
                parameters=[domain.Parameter("temperature_2m", "t_2m", domain.ureg.Unit("K"))],
                coordinates=domain.ISVTensorDimensionMap(
                    init_time=[init_time],
                    step=[0],
                    values=list(range(2949120)),
                ),
            )

        @dataclasses.dataclass
        class TestContainer:
            name: str
            request: domain.SourceFileMetadata
            expected_path: str

        test_cases: list[TestContainer] = [
            TestContainer(
                name="basic_europe",
                request=_gen_filemetadata(
                    name=f"icon-eu_europe_regular-lat-lon_single-level_{init_time:%Y%m%d%H}_000_T_2M.grib2.bz2",
                ),
                expected_path=f"~/.local/cache/nwp/raw/icon-eu_europe_regular-lat-lon_single-level_{init_time:%Y%m%d%H}_000_T_2M.grib2",
            ),
            TestContainer(
                name="basic_global",
                request=_gen_filemetadata(
                    name=f"icon_global_icosahedral_single-level_{init_time:%Y%m%d%H}_000_T_2M.grib2.bz2",
                ),
                expected_path=f"~/.local/cache/nwp/raw/icon_global_icosahedral_single-level_{init_time:%Y%m%d%H}_000_T_2M.grib2",
            ),
        ]

        for case in test_cases:
            with self.subTest(name=case.name):
                result = repo.download_file(case.request).unwrap()
                self.assertEqual(result.path.as_posix(), case.expected_path)
                self.assertTrue(result.path.exists())
                self.assertGreater(result.size_bytes, 100000)
                result.path.unlink()


if __name__ == "__main__":
    unittest.main()
