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
  --source <source>   Data source to use (ceda/metoffice) [default: ceda].
  --sink <sink>       Data sink to use (local/s3) [default: local].
  --rdir <rawdir>     Directory of raw data store [default: /tmp/raw].
  --zdir <zarrdir>    Directory of zarr data store [default: /tmp/zarr].
  --create-latest     Create a zarr of the dataset with the latest init time [default: False].
  --verbose           Enable verbose logging [default: False].
"""

import datetime as dt
import importlib.metadata
import shutil

import structlog
from docopt import docopt

from nwp_consumer.internal import config, inputs, outputs, TMP_DIR
from nwp_consumer.internal.service import NWPConsumerService

__version__ = "local"

try:
    __version__ = importlib.metadata.version("nwp-consumer")
except importlib.metadata.PackageNotFoundError:
    # package is not installed
    pass

log = structlog.getLogger()


def run():
    """Run the CLI."""
    # Parse command line arguments from docstring
    arguments = docopt(__doc__, version=__version__)

    fetcher = None
    storer = None

    if arguments['check']:
        # Perform a healthcheck on the service
        # * Don't care here about the source/sink
        return NWPConsumerService(
            fetcher=inputs.ceda.CEDAClient(
                ftpUsername="anonymous",
                ftpPassword="anonymous",
            ),
            storer=outputs.localfs.LocalFSClient(),
            rawdir=arguments['--rdir'],
            zarrdir=arguments['--zdir'],
        ).Check()

    match arguments['--sink']:
        # Create the storer based on the sink
        case 'local':
            storer = outputs.localfs.LocalFSClient()
        case 's3':
            s3c = config.S3Config()
            storer = outputs.s3.S3Client(
                key=s3c.AWS_ACCESS_KEY,
                bucket=s3c.AWS_S3_BUCKET,
                secret=s3c.AWS_ACCESS_SECRET,
                region=s3c.AWS_REGION
            )
        case _:
            raise ValueError(f"unknown sink {arguments['--sink']}")

    match arguments['--source']:
        # Create the fetcher based on the source
        case 'ceda':
            cc = config.CEDAConfig()
            fetcher = inputs.ceda.CEDAClient(
                ftpUsername=cc.CEDA_FTP_USER,
                ftpPassword=cc.CEDA_FTP_PASS,
            )
        case 'metoffice':
            mc = config.MetOfficeConfig()
            fetcher = inputs.metoffice.MetOfficeClient(
                orderID=mc.METOFFICE_ORDER_ID,
                clientID=mc.METOFFICE_CLIENT_ID,
                clientSecret=mc.METOFFICE_CLIENT_SECRET,
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


def main():
    """Entry point for the nwp-consumer CLI."""
    programStartTime = dt.datetime.now()
    try:
        run()
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
