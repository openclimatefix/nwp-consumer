"""ECMWF MARS API adaptor."""

import datetime as dt
import pathlib

import dask.array
import xarray as xr
from ecmwfapi.api import ECMWFService
from nwp_consumer.internal.core import domain
from nwp_consumer.internal.core.ports import SourceRepository
from result import Err, Ok, Result


class MARSOperationalArchive(SourceRepository):
    """ECMWF MARS API adaptor for the Operational Archive."""

    service: ECMWFService


    def metadata(self) -> domain.SourceRepositoryMetadata:
        """Overrides corresponding method in parent class."""
        return domain.SourceRepositoryMetadata(
            name="ecmwf-mars",
            is_archive=True,
            is_order_based=False,
            running_hours=[0, 12],
            available_steps=[
                *list(range(90)),
                *list(range(90, 144, 3)),
                *list(range(144, 240, 6)),
            ],
            available_areas=[
                domain.UK,
                domain.NW_INDIA,
                domain.MALTA,
            ],
            required_env=["ECMWF_API_KEY", "ECMWF_API_EMAIL", "ECMWF_API_URL"],
        )

    def validate_request(
        self,
        request: domain.DataRequest,
    ) -> Result[domain.DataRequest, str]:
        """Overrides corresponding method in parent class."""
        return Err("Not implemented")

    def initialize_store(self, request: domain.DataRequest) -> Result[pathlib.Path, str]:
        """Overrides corresponding method in parent class."""
        path = pathlib.Path("~/.local/cache/nwp_consumer") / request.init_time.strftime(
            "%Y%m%dT%H%M-mars.zarr",
        )

        # Create empty store, chunking as follows:
        # * Every init time and step in their own chunk
        # * Latitude and longitude in 2 chunks
        coords = request.ds_coords()
        shape = (
            len(coords["init_time"]),
            len(coords["step"]),
            len(coords["latitude"]),
            len(coords["longitude"]),
        )
        data_vars = {
            param: (
                ("init_time", "step", "latitude", "longitude"),
                dask.array.zeros(
                    shape,
                    chunks=(shape[0], shape[1], 2, 2),
                ),
            )
            for param in request.parameters
        }
        ds = xr.Dataset(coords=coords, data_vars=data_vars)
        try:
            # Create the store. Mode "w" will overwrite any existing store.
            ds.to_zarr(path, compute=False, mode="w")
        except Exception as e:
            return Err(f"Failed to initialize store: {e}")
        return Ok(path)

    def download_file(
        self,
        file: domain.SourceFileMetadata,
    ) -> Result[domain.SourceFileMetadata, str]:
        """Download a single source NWP file."""
        return Err("Not implemented")

    def list_fileset(
        self,
        it: dt.datetime,
        request: domain.DataRequest,
    ) -> Result[list[domain.SourceFileMetadata], str]:
        """Overrides corresponding method in parent class."""
        return Ok([domain.SourceFileMetadata(
            name=f"operational-archive-{it.strftime('%Y%m%dT%H%M')}",
            extension=".grib",
            init_time=it,
            steps=request.steps,
            path=pathlib.Path("~/.local/cache/nwp_consumer") / it.strftime(
                "%Y%m%dT%H%M-mars.zarr",
            ),
        )])

    def map_file(
        self,
        cached_file: domain.SourceFileMetadata,
        store_path: pathlib.Path,
    ) -> Result[domain.SourceFileMetadata, str]:
        """Overrides corresponding method in parent class."""
        return Err("Not implemented")


def build_request(domain.DataRequest, metadata_only: bool) -> str:
    """Build an ECMWF MARS API request from a DataRequest."""
    mars_request: str = f"""
        {"list" if metadata_only else "retrieve"},
            class   = od,
            date    = {request.init_time.strftime("%Y%m%d")},
            expver  = 1,
            levtype = sfc,
            param   = {",".join(request.parameters)},
    """



