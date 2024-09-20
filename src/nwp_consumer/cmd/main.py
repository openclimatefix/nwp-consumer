"""nwp-consumer.

Usage:
  nwp-consumer download --source=SOURCE [--sink=SINK --from=FROM --to=TO --rdir=RDIR --zdir=ZDIR --rsink=RSINK --no-rename-vars --no-variable-dim --create-latest]
  nwp-consumer convert --source=SOURCE [--sink=SINK --from=FROM --to=TO --rdir=RDIR --zdir=ZDIR --rsink=RSINK --no-rename-vars --no-variable-dim --create-latest]
  nwp-consumer consume --source=SOURCE [--sink=SINK --from=FROM --to=TO --rdir=RDIR --zdir=ZDIR --rsink=RSINK --no-rename-vars --no-variable-dim --create-latest]
  nwp-consumer env (--source=SOURCE | --sink=SINK)
  nwp-consumer check [--sink=SINK] [--rdir=RDIR] [--zdir=ZDIR]
  nwp-consumer (-h | --help)
  nwp-consumer --version

Commands:
  download            Download raw data from source to raw sink
  convert             Convert raw data present in raw sink to zarr sink
  consume             Download and convert raw data from source to sink
  check               Perform a healthcheck on the service
  env                 Print the unset environment variables required by the source/sink

Options:
  --from=FROM         Start datetime in YYYY-MM-DDTHH:MM or YYYY-MM-DD format [default: today].
  --to=TO             End datetime in YYYY-MM-DD or YYYY-MM-DDTHH:MM format.
  --source=SOURCE     Data source (ceda/metoffice/ecmwf-mars/ecmwf-s3/icon/cmc/gfs).
  --sink=SINK         Data sink (local/s3/huggingface) [default: local].
  --rsink=RSINK       Data sink for raw data, if different (local/s3/huggingface) [default: SINK].
  --rdir=RDIR         Directory of raw data store [default: /tmp/raw].
  --zdir=ZDIR         Directory of zarr data store [default: /tmp/zarr].
  --create-latest     Create a zarr of the dataset with the latest init time [default: False].
  --no-rename-vars    Don't rename parameters to standard names.
  --no-variable-dim   Don't stack data variables into a single dimension.

Generic Options:
  --version           Show version.
  -h, --help          Show this screen.
  -v, --verbose       Enable verbose logging [default: False].
"""

import contextlib
import datetime as dt
import importlib.metadata
import pathlib
import shutil
import sys
from distutils.util import strtobool

import dask
import dask.distributed
import sentry_sdk
import structlog
import os
from docopt import docopt

from nwp_consumer import internal
from nwp_consumer.internal import config
from nwp_consumer.internal.service import NWPConsumerService

__version__ = "local"

with contextlib.suppress(importlib.metadata.PackageNotFoundError):
    __version__ = importlib.metadata.version("package-name")

log = structlog.getLogger()

#sentry
sentry_sdk.init(
    dsn=os.getenv("SENTRY_DSN"),
    environment=os.getenv("ENVIRONMENT", "local"),
    traces_sample_rate=1
)

sentry_sdk.set_tag("app_name", "nwp_consumer")
sentry_sdk.set_tag("version", __version__)


def run(argv: list[str]) -> tuple[list[pathlib.Path], list[pathlib.Path]]:
    """Run the CLI.

    Args:
        argv: The command line arguments.

    Returns:
        A tuple of lists of raw and processed files.
    """
    # --- Map environment variables to service configuration --- ## Configure dask

    dask.config.set({"array.slicing.split_large_chunks": True})
    if config.ConsumerEnv().DASK_SCHEDULER_ADDRESS != "":
        # Connect to the dask scheduler if the address is set
        # * This becomes the default client for all dask operations
        client = dask.distributed.Client(
            address=config.ConsumerEnv().DASK_SCHEDULER_ADDRESS,
        )
        log.info(
            event="Connected to dask scheduler",
            address=config.ConsumerEnv().DASK_SCHEDULER_ADDRESS,
        )

    # --- Run the service with the desired command --- #
    arguments = docopt(__doc__, argv=argv, version=__version__)

    # Logic for the env command
    if arguments["env"]:
        parse_actor(source=arguments["--source"], sink=arguments["--sink"]).print_env()
        return [], []

    # Create the service using the fetcher and storer
    fetcher = parse_actor(arguments["--source"], None)().configure_fetcher()
    storer = parse_actor(None, arguments["--sink"])().configure_storer()
    if arguments["--rsink"] == "SINK":
        rawstorer = storer
    else:
        rawstorer = parse_actor(None, arguments["--rsink"])().configure_storer()

    service = NWPConsumerService(
        fetcher=fetcher,
        storer=storer,
        rawstorer=rawstorer,
        zarrdir=arguments["--zdir"],
        rawdir=arguments["--rdir"],
        rename_vars=not arguments["--no-rename-vars"],
        variable_dim=not arguments["--no-variable-dim"],
    )

    # Logic for the "check" command
    if arguments["check"]:
        _ = service.Check()
        return [], []

    # Process the from and to arguments
    start, end = _parse_from_to(arguments["--from"], arguments["--to"])

    # Logic for the other commands
    log.info("nwp-consumer service starting", version=__version__, arguments=arguments)
    rawFiles: list[pathlib.Path] = []
    processedFiles: list[pathlib.Path] = []

    if arguments["download"]:
        rawFiles = service.DownloadRawDataset(start=start, end=end)

    if arguments["convert"]:
        processedFiles = service.ConvertRawDatasetToZarr(start=start, end=end)

    if arguments["consume"]:
        service.Check()
        rawFiles = service.DownloadRawDataset(start=start, end=end)
        processedFiles = service.ConvertRawDatasetToZarr(start=start, end=end)

    if arguments["--create-latest"]:
        processedFiles += service.CreateLatestZarr()

    return rawFiles, processedFiles


def main() -> None:
    """Entry point for the nwp-consumer CLI."""
    erred = False

    programStartTime = dt.datetime.now(tz=dt.UTC)
    try:
        files: tuple[list[pathlib.Path], list[pathlib.Path]] = run(argv=sys.argv[1:])
        log.info(
            event="processed files",
            raw_files=len(files[0]),
            processed_files=len(files[1]),
        )
    except Exception as e:
        log.error("encountered error running nwp-consumer", error=str(e), exc_info=True)
        erred = True
    finally:
        clearableCache: list[pathlib.Path] = list(internal.CACHE_DIR.glob("*"))
        for p in clearableCache:
            if p.exists() and p.is_dir():
                shutil.rmtree(p)
            if p.is_file():
                p.unlink(missing_ok=True)
        elapsedTime = dt.datetime.now(tz=dt.UTC) - programStartTime
        log.info(event="nwp-consumer finished", elapsed_time=str(elapsedTime), version=__version__)
        if erred:
            exit(1)


def _parse_from_to(fr: str, to: str | None) -> tuple[dt.datetime, dt.datetime]:
    """Process the from and to arguments."""
    # Modify the default "today" argument to today's date
    if fr == "today":
        fr = dt.datetime.now(tz=dt.UTC).strftime("%Y-%m-%d")
    # Modify the "latest" argument to the most recent 6 hour interval
    if fr == "latest":
        now = dt.datetime.now(tz=dt.UTC)
        fr = now.replace(hour=(now.hour // 6) * 6, minute=0).strftime("%Y-%m-%dT%H:%M")
    # If --from specifies a date, and --to is not set, set --to to the next day
    if len(fr) == 10 and to is None:
        to = (
            dt.datetime.strptime(
                fr,
                "%Y-%m-%d",
            ).replace(tzinfo=dt.UTC)
            + dt.timedelta(days=1)
        ).strftime("%Y-%m-%d")
    # Otherwise, --from specifies a datetime,
    # so if --to is not set, set --to to the same time
    if to is None:
        to = fr
    # If --from and --to are missing time information, assume midnight
    if len(fr) == 10:
        fr += "T00:00"
    if len(to) == 10:
        to += "T00:00"
    # Process to datetime objects
    start: dt.datetime = dt.datetime.strptime(
        fr,
        "%Y-%m-%dT%H:%M",
    ).replace(tzinfo=dt.UTC)
    end: dt.datetime = dt.datetime.strptime(
        to,
        "%Y-%m-%dT%H:%M",
    ).replace(tzinfo=dt.UTC)

    if end < start:
        raise ValueError("argument '--from' cannot specify date prior to '--to'")

    return start, end


def parse_actor(source: str | None, sink: str | None) -> type[config.EnvParser]:
    """Parse the actor argument into a class that can parse environment variables."""
    SOURCE_ENV_MAP: dict[str, type[config.EnvParser]] = {
        "ceda": config.CEDAEnv,
        "metoffice": config.MetOfficeEnv,
        "ecmwf-mars": config.ECMWFMARSEnv,
        "ecmwf-s3": config.ECMWFS3Env,
        "icon": config.ICONEnv,
        "cmc": config.CMCEnv,
        "gfs": config.GFSEnv,
    }
    SINK_ENV_MAP: dict[str, type[config.EnvParser]] = {
        "local": config.LocalEnv,
        "s3": config.S3Env,
        "huggingface": config.HuggingFaceEnv,
    }

    if source:
        try:
            return SOURCE_ENV_MAP[source]
        except KeyError as e:
            raise ValueError(
                f"Unknown source {source}. Expected one of {list(SOURCE_ENV_MAP.keys())}",
            ) from e
    if sink:
        try:
            return SINK_ENV_MAP[sink]
        except KeyError as e:
            raise ValueError(
                f"Unknown sink {sink}. Expected one of {list(SINK_ENV_MAP.keys())}",
            ) from e
    raise ValueError("Either source or sink must be specified")


if __name__ == "__main__":
    main()
