import dataclasses
import datetime as dt
import unittest

from dwd_icon import DWDIconRollingArchive
from nwp_consumer.internal.core import domain


class TestDWDIcon(unittest.TestCase):

    def test_list_fileset(self) -> None:
        repo = DWDIconRollingArchive()
        init_time = dt.datetime.now(tz=dt.UTC).replace(hour=0, minute=0, second=0, microsecond=0)

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
                    "https:/opendata.dwd.de/weather/nwp/icon-eu/grib/00/t_2m/icon_eu_single-level_2024052100_000_T_2M.grib2.bz2",
                    "https:/opendata.dwd.de/weather/nwp/icon-eu/grib/00/t_2m/icon_eu_single-level_2024052100_001_T_2M.grib2.bz2",
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
                    "https:/opendata.dwd.de/weather/nwp/icon/grib/00/t_2m/icon_global_icosahedral_single-level_2024052100_000_T_2M.grib2.bz2",
                    "https:/opendata.dwd.de/weather/nwp/icon/grib/00/t_2m/icon_global_icosahedral_single-level_2024052100_001_T_2M.grib2.bz2",
                    "https:/opendata.dwd.de/weather/nwp/icon/grib/00/asob_s/icon_global_icosahedral_single-level_2024052100_000_ASOB_S.grib2.bz2",
                    "https:/opendata.dwd.de/weather/nwp/icon/grib/00/asob_s/icon_global_icosahedral_single-level_2024052100_001_ASOB_S.grib2.bz2",
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
                        domain.Parameter("temperature_850hPa", "t_850", domain.ureg.Unit("K"), level_type="multi", level_units="hPa", level_value=850),
                        domain.Parameter("snow_depth", "w_snow", domain.ureg.Unit("m"), level_type="multi", level_units="hPa", level_value=900),
                    ],
                ),
                expected_paths=[
                    "https:/opendata.dwd.de/weather/nwp/icon-eu/grib/00/t_2m/icon_eu_single-level_2024052100_000_T_2M.grib2.bz2",
                    "https:/opendata.dwd.de/weather/nwp/icon-eu/grib/00/t_2m/icon_eu_single-level_2024052100_001_T_2M.grib2.bz2",
                    "https:/opendata.dwd.de/weather/nwp/icon-eu/grib/00/t_850/icon_eu_pressure-level_2024052100_000_850_T_850.grib2.bz2",
                    "https:/opendata.dwd.de/weather/nwp/icon-eu/grib/00/t_850/icon_eu_pressure-level_2024052100_001_850_T_850.grib2.bz2",
                    "https:/opendata.dwd.de/weather/nwp/icon-eu/grib/00/w_snow/icon_eu_pressure-level_2024052100_000_900_W_SNOW.grib2.bz2",
                    "https:/opendata.dwd.de/weather/nwp/icon-eu/grib/00/w_snow/icon_eu_pressure-level_2024052100_001_900_W_SNOW.grib2.bz2",
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


if __name__ == "__main__":
    unittest.main()