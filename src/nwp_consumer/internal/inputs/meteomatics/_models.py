import datetime as dt
from dataclasses import dataclass

import nwp_consumer.internal as internal


@dataclass
class MeteomaticsFileInfo(internal.FileInfoModel):
    inittime: dt.datetime
    area: str
    params: list[str]
    steplist: list[int]
    sitelist: list[tuple[float, float]]

    def filename(self) -> str:
        """Overrides the corresponding method in the parent class."""
        # Meteomatics does not have explicit filenames when using the API
        # * As such, name manually based on their inittime and area covered
        #   e.g. `meteomatics_uk_20210101T0000.csv`
        return f"meteomatics_{self.area}_{self.inittime.strftime('%Y%m%dT%H%M')}.csv"

    def filepath(self) -> str:
        """Overrides the corresponding method in the parent class."""
        return ""

    def it(self) -> dt.datetime:
        """Overrides the corresponding method in the parent class."""
        return self.inittime

    def variables(self) -> list[str]:
        """Overrides the corresponding method in the parent class."""
        return self.params

    def steps(self) -> list[int]:
        """Overrides the corresponding method in the parent class."""
        return self.steplist

    def sites(self) -> list[tuple[float, float]]:
        """Overrides the corresponding method in the parent class."""
        return self.sitelist


# The order of these coordinate lists are used to determine the station_id
solar_coords = [
    (26.264, 71.237),
    (26.671, 71.262),
    (26.709, 71.413),
    (26.871, 71.49),
    (26.833, 71.815),
    (26.792, 72.008),
    (26.892, 72.06),
    (27.179, 71.841),
    (27.476, 71.971),
    (27.387, 72.218),
    (27.951, 72.987),
    (28.276, 73.341),
    (24.687, 75.132),
    (26.731, 73.2),
    (26.524, 72.862),
    (27.207, 74.252),
    (27.388, 72.208),
    (27.634, 72.698),
    (28.344, 73.435),
    (28.022, 73.067),
    # Adani
    (13.995, 78.428),
    (26.483, 71.232),
    (14.225, 77.43),
    (24.12, 69.34),
]

wind_coords = [
    (27.035, 70.515),
    (27.188, 70.661),
    (27.085, 70.638),
    (27.055, 70.72),
    (27.186, 70.81),
    (27.138, 71.024),
    (26.97, 70.917),
    (26.898, 70.996),
    (26.806, 70.732),
    (26.706, 70.81),
    (26.698, 70.875),
    (26.708, 70.982),
    (26.679, 71.027),
    (26.8, 71.128),
    (26.704, 71.127),
    (26.5, 71.285),
    (26.566, 71.369),
    (26.679, 71.452),
    (26.201, 71.295),
    (26.501, 72.512),
    (26.463, 72.836),
    (26.718, 73.049),
    (26.63, 73.581),
    (24.142, 74.731),
    (23.956, 74.625),
    (23.657, 74.772),
    # Adani
    (26.479, 1.220),
    (23.098, 75.255),
    (23.254, 69.252),
]

wind_parameters = [
    "wind_speed_10m:ms",
    "wind_speed_100m:ms",
    "wind_speed_200m:ms",
    "wind_dir_10m:d",
    "wind_dir_100m:d",
    "wind_dir_200m:d",
    "wind_gusts_10m:ms",
    "wind_gusts_100m:ms",
    "wind_gusts_200m:ms",
    "air_density_10m:kgm3",
    "air_density_25m:kgm3",
    "air_density_100m:kgm3",
    "air_density_200m:kgm3",
    "cape:Jkg",
]


solar_parameters = [
    "direct_rad:W",
    "diffuse_rad:W",
    "global_rad:W",
]