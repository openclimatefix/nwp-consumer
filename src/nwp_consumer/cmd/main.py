"""nwp-consumer.

Usage:
  nwp-consumer download --source <source> [options]
  nwp-consumer convert --source <source> [options]
  nwp-consumer consume --source <source> [options]
  nwp-consumer env (--source <source> | --sink <sink>)
  nwp-consumer check [--sink <sink> --rdir <rawdir> --zdir <zarrdir>]
  nwp-consumer (-h | --help)
  nwp-consumer --version

Options:
  download            Download raw data from source to sink
  convert             Convert raw data present in sink
  consume             Download and convert raw data from source to sink
  check               Perform a healthcheck.py on the service
  env                 Print the unset environment variables required by the source/sink

  -h --help           Show this screen.
  --version           Show version.
  --from <startDate>  Start date in YYYY-MM-DD format [default: today].
  --to <endDate>      End date in YYYY-MM-DD format [default: today].
  --source <source>   Data source to use (ceda/metoffice/ecmwf-mars/icon).
  --sink <sink>       Data sink to use (local/s3) [default: local].
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

from nwp_consumer import internal
from nwp_consumer.internal import config, inputs, outputs
from nwp_consumer.internal.service import NWPConsumerService

__version__ = "local"

with contextlib.suppress(importlib.metadata.PackageNotFoundError):
    __version__ = importlib.metadata.version("package-name")

log = structlog.getLogger()


def run(arguments: dict) -> int:
    """Run the CLI."""
    # --- Map arguments to service configuration --- #

    # Defaults for the fetcher, storer and env parser
    fetcher: internal.FetcherInterface = inputs.ceda.Client(
        ftpUsername="na",
        ftpPassword="na",
    )
    storer: internal.StorageInterface = outputs.localfs.Client()
    env: config.EnvParser = config.LocalEnv()

    # Map sink argument to storer
    match arguments['--sink']:
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

    # Map source argument to fetcher
    match arguments['--source']:
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
            fetcher = inputs.ecmwf.mars.Client(
                area=env.ECMWF_AREA,
                hours=env.ECMWF_HOURS,
            )
        case 'icon':
            fetcher = inputs.icon.Client()
        case None:
            pass
        case _:
            raise ValueError(f"unknown source {arguments['--source']}")

    # Map from and to arguments to datetime objects
    if arguments['--from'] == "today" or arguments['--from'] is None:
        arguments['--from'] = dt.datetime.now().strftime("%Y-%m-%d")
    if arguments['--to'] == "today" or arguments['--to'] is None:
        arguments['--to'] = dt.datetime.now().strftime("%Y-%m-%d")
    startDate: dt.date = dt.datetime.strptime(arguments['--from'], "%Y-%m-%d").date()
    endDate: dt.date = dt.datetime.strptime(arguments['--to'], "%Y-%m-%d").date()
    if endDate < startDate:
        raise ValueError("argument '--from' cannot specify date prior to '--to'")

    # Create the service using the fetcher and storer
    service = NWPConsumerService(
        fetcher=fetcher,
        storer=storer,
        zarrdir=arguments['--zdir'],
        rawdir=arguments['--rdir'],
    )

    # --- Run the service with the desired command --- #

    # Logic for the "check" command
    if arguments['check']:
        return service.Check()

    # Logic for the env command
    if arguments['env']:
        # Missing env vars are printed during mapping of source/sink args
        return 0

    log.info("nwp-consumer starting", version=__version__, arguments=arguments)

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
        log.error("nwp-consumer error", error=str(e), exc_info=True)
    finally:
        leftoverTempPaths = list(internal.TMP_DIR.glob("*"))
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

