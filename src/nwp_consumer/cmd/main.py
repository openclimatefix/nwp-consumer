"""nwp-consumer.

Usage:
  nwp-consumer download [options]
  nwp-consumer convert [options]
  nwp-consumer consume [options]
  nwp-consumer check
  nwp-consumer (-h | --help)
  nwp-consumer --version

Options:
  download            Download raw data from source to sink
  convert             Convert raw data present in sink
  consume             Download and convert raw data from source to sink
  check               Perform a healthcheck.py on the service

  -h --help           Show this screen.
  --version           Show version.
  --from <startDate>  Start date in YYYY-MM-DD format [default: today].
  --to <endDate>      End date in YYYY-MM-DD format [default: today].
  --source <source>   Data source to use (ceda/metoffice/ecmwf-mars) [default: ceda].
  --sink <sink>       Data sink to use (local/s3/huggingface) [default: local].
  --rdir <rawdir>     Directory of raw data store [default: /tmp/raw].
  --zdir <zarrdir>    Directory of zarr data store [default: /tmp/zarr].
  --create-latest     Create a zarr of the dataset with the latest init time [default: False].
  --verbose           Enable verbose logging [default: False].
"""

import contextlib
import datetime as dt
import importlib.metadata
import shutil

import structlog
from docopt import docopt

from nwp_consumer.internal import (
    TMP_DIR,
    FetcherInterface,
    StorageInterface,
    config,
    inputs,
    outputs,
)
from nwp_consumer.internal.service import NWPConsumerService

__version__ = "local"

with contextlib.suppress(importlib.metadata.PackageNotFoundError):
    __version__ = importlib.metadata.version("package-name")

log = structlog.getLogger()

def run(arguments: dict) -> int:
    """Run the CLI."""
    fetcher: FetcherInterface = inputs.ceda.Client(
        ftpUsername="anonymous",
        ftpPassword="anonymous",
    )
    storer: StorageInterface = outputs.localfs.Client()
    env: config.EnvParser = config.EnvParser()

    if arguments['check']:
        # Perform a healthcheck on the service
        # * Don't care here about the source/sink
        return NWPConsumerService(
            fetcher=fetcher,
            storer=storer,
            rawdir=arguments['--rdir'],
            zarrdir=arguments['--zdir'],
        ).Check()


    match arguments['--sink']:
        # Create the storer based on the sink
        case 'local':
            storer = outputs.localfs.Client()
        case 's3':
            env = config.S3Env()
            storer = outputs.s3.Client(
                key=env.AWS_ACCESS_KEY,
                bucket=env.AWS_S3_BUCKET,
                secret=env.AWS_ACCESS_SECRET,
                region=env.AWS_REGION
            )
        case 'huggingface':
            env = config.HuggingFaceEnv()
            storer = outputs.huggingface.Client(
                token=env.HUGGINGFACE_TOKEN,
                repoID=env.HUGGINGFACE_REPO_ID,
            )
        case _:
            raise ValueError(f"unknown sink {arguments['--sink']}")

    match arguments['--source']:
        # Create the fetcher based on the source
        case 'ceda':
            env = config.CEDAEnv()
            fetcher = inputs.ceda.Client(
                ftpUsername=env.CEDA_FTP_USER,
                ftpPassword=env.CEDA_FTP_PASS,
            )
        case 'metoffice':
            env = config.MetOfficeEnv()
            fetcher = inputs.metoffice.Client(
                orderID=env.METOFFICE_ORDER_ID,
                clientID=env.METOFFICE_CLIENT_ID,
                clientSecret=env.METOFFICE_CLIENT_SECRET,
            )
        case 'ecmwf-mars':
            env = config.ECMWFMARSEnv()
            fetcher = inputs.ecmwf.MARSClient(
                area=env.ECMWF_AREA,
            )
        case _:
            raise ValueError(f"unknown source {arguments['--source']}")

    # Create the service using the fetcher and storer
    service = NWPConsumerService(
        fetcher=fetcher,
        storer=storer,
        zarrdir=arguments['--zdir'],
        rawdir=arguments['--rdir'],
    )

    # Set default values for start and end dates
    if arguments['--from'] == "today" or arguments['--from'] is None:
        arguments['--from'] = dt.datetime.now().strftime("%Y-%m-%d")
    if arguments['--to'] == "today" or arguments['--to'] is None:
        arguments['--to'] = dt.datetime.now().strftime("%Y-%m-%d")

    # Parse start and end dates
    startDate: dt.date = dt.datetime.strptime(arguments['--from'], "%Y-%m-%d").date()
    endDate: dt.date = dt.datetime.strptime(arguments['--to'], "%Y-%m-%d").date()
    if endDate < startDate:
        raise ValueError("argument '--from' cannot specify date prior to '--to'")

    log.info("nwp-consumer starting", version=__version__, arguments=arguments)

    # Carry out the desired function

    if arguments['download']:
        service.DownloadRawDataset(
            start=startDate,
            end=endDate
        )

    if arguments['convert']:
        service.ConvertRawDatasetToZarr(
            start=startDate,
            end=endDate
        )

    if arguments['consume']:
        service.Check()
        service.DownloadAndConvert(
            start=startDate,
            end=endDate
        )

    if arguments['--create-latest']:
        service.CreateLatestZarr()


def main() -> None:
    """Entry point for the nwp-consumer CLI."""
    # Parse command line arguments from docstring
    arguments = docopt(__doc__, version=__version__)

    programStartTime = dt.datetime.now()
    try:
        run(arguments=arguments)
    except Exception as e:
        log.error("nwp-consumer error", error=str(e))
        raise e
    finally:
        leftoverTempPaths = list(TMP_DIR.glob("*"))
        for p in leftoverTempPaths:
            if p.exists() and p.is_dir():
                shutil.rmtree(p)
            if p.is_file():
                p.unlink(missing_ok=True)
        elapsedTime = dt.datetime.now() - programStartTime
        log.info(
            "nwp-consumer finished",
            elapsed_time=str(elapsedTime),
            version=__version__
        )


if __name__ == "__main__":
    main()

