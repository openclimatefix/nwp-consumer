"""ICON API adaptor."""

import bz2
import datetime as dt
import pathlib

import requests
import attrs
import urllib.parse
from nwp_consumer.internal.core import domain
from nwp_consumer.internal.core.ports import SourceRepository
from result import Err, Ok, Result


class DWDIconRollingArchive(SourceRepository):
    """ICON API adaptor for the Rolling Archive."""

    @classmethod
    def from_env(cls) -> "DWDIconRollingArchive":
        """Overrides the corresponding method in the parent class."""
        return cls()

    @classmethod
    def metadata(cls) -> domain.SourceRepositoryMetadata:
        """Overrides corresponding method in the parent class."""
        return domain.SourceRepositoryMetadata(
            name="dwd-icon",
            is_archive=False,
            is_order_based=False,
            running_hours=[0, 6, 12, 18],
            delay_minutes=180,
            available_steps=[
                *list(range(0, 73)),
            ],
            available_areas=[
                domain.AREAS.gl,
                domain.AREAS.eu,
            ],
            required_env=[],
            optional_env={},
        )

    @classmethod
    def map_file(
            cls,
            cached_file: domain.SourceFileMetadata,
            store_metadata: domain.StoreMetadata,
    ) -> Result[domain.SourceFileMetadata, str]:
        """Overrides the corresponding method in the parent class."""

        return Err("Not implemented")

    def validate_request(self, request: domain.DataRequest) -> Result[domain.DataRequest, str]:
        """Overrides the corresponding method in the parent class."""

    def list_fileset(
            self,
            it: dt.datetime,
            request: domain.DataRequest,
    ) -> Result[list[domain.SourceFileMetadata], str]:
        """Overrides the corresponding method in the parent class."""
        files: list[domain.SourceFileMetadata] = []
        base_url = "opendata.dwd.de/weather/nwp"
        model_url = "icon-eu/grib" if request.area == domain.AREAS.eu else "icon/grib"

        parameter: domain.Parameter
        for parameter in request.parameters:
            parameter_stub: str
            match (parameter.level_type, request.area):
                case ("single", domain.AREAS.gl):
                    parameter_stub = "icon_global_icosahedral_single-level"
                case ("single", domain.AREAS.eu):
                    parameter_stub = "icon-eu_europe_regular-lat-lon_single-level"
                case ("multi", domain.AREAS.gl):
                    parameter_stub = "icon_global_icosahedral_pressure-level"
                case ("multi", domain.AREAS.eu):
                    parameter_stub = "icon-eu_europe_regular-lat-lon_pressure-level"
                case _:
                    return Err(
                        f"Invalid parameter level type or area  encountered in requested parameters:"
                        f"level_type: {parameter.level_type}, area: {request.area.name}.",
                    )

            for step in request.steps:
                filename_stub: str = f"{parameter_stub}_{it:%Y%m%d%H}_{step:03d}"
                ext: str = ".grib2.bz2"
                filename: str
                match (parameter.level_type, parameter.level_units):
                    case ("multi", "hPa"):
                        filename = f"{filename_stub}_{parameter.level_value:03d}_{parameter.shortname.upper()}{ext}"
                    case ("single", None):
                        filename = f"{filename_stub}_{parameter.shortname.upper()}{ext}"
                    case _:
                        return Err(
                            f"Invalid parameter level type or units encountered in requested parameters:"
                            f"level_type: {parameter.level_type}, level_units: {parameter.level_units}.",
                        )

                path = pathlib.Path(f"{base_url}/{model_url}/{it:%H}/{parameter.shortname}/{filename}")

                files.append(domain.SourceFileMetadata(
                    name=filename,
                    extension=".grib2.bz2",
                    path=path,
                    scheme="https",
                    size_bytes=600000,
                    steps=[step],
                    parameters=[parameter],
                    init_time=it,
                ))

        return Ok(files)

    def download_file(
            self,
            file: domain.SourceFileMetadata,
    ) -> Result[domain.SourceFileMetadata, str]:
        """Overrides the corresponding method in the parent class."""
        cached_path = pathlib.Path("~/.local/cache/nwp/raw") / file.name.replace(".bz2", "")
        cached_path.parent.mkdir(parents=True, exist_ok=True)
        if cached_path.exists():
            return Ok(attrs.evolve(file, path=cached_path, size_bytes=cached_path.stat().st_size))
        else:
            try:
                url = urllib.parse.quote_plus(
                    f"{file.scheme}://{file.path.as_posix()}",
                    safe=":/?=&",
                )
                r: requests.Response = requests.get(
                    url=url,
                    stream=True,
                    timeout=5 * 60,
                )
            except requests.exceptions.RequestException as e:
                return Err(f"Failed to download file at {file.path} due to exception in requests: {e}")
            if r.status_code == requests.codes.ok:
                with r.raw as source, cached_path.open(mode="wb") as dest:
                    decompressed = bz2.decompress(source.read())
                    dest.write(decompressed)
                return Ok(attrs.evolve(file, path=cached_path, size_bytes=cached_path.stat().st_size))
            else:
                return Err(f"Failed to download file {file.path}: {r.reason}")

