"""nwp-consumer.

Usage:
  nwp-consumer download --from <startDate> --to <endDate> [options]
  nwp-consumer convert --from <startDate> --to <endDate> [options]
  nwp-consumer consume [options]
  nwp-consumer (-h | --help)
  nwp-consumer --version

Options:
  download            Download raw data from source to sink
  convert             Convert raw data present in sink
  consume             Download and convert raw data from source to sink

  -h --help           Show this screen.
  --version           Show version.
  --from <startDate>  Start date in YYYY-MM-DD format [default: today].
  --to <endDate>      End date in YYYY-MM-DD format [default: today].
  --source <source>   Data source to use (ceda/metoffice) [default: ceda].
  --sink <sink>       Data sink to use (local/s3) [default: local].
  --rdir <rawdir>     Directory of raw data store [default: /tmp/raw].
  --zdir <zarrdir>    Directory of zarr data store [default: /tmp/zarr].
  --create-latest     Create a zarr of the dataset with the latest init time [default: False].
"""

import datetime as dt
import importlib.metadata

import structlog
from docopt import docopt

from nwp_consumer.internal import config, inputs, outputs
from nwp_consumer.internal.service import NWPConsumerService

__version__ = "local"

try:
    __version__ = importlib.metadata.version("nwp-consumer")
except importlib.metadata.PackageNotFoundError:
    # package is not installed
    pass

log = structlog.stdlib.get_logger()


def run():
    """Entry point for the nwp-consumer CLI."""
    programStartTime = dt.datetime.now()

    # Parse command line arguments from docstring
    arguments = docopt(__doc__, version=__version__)

    log.info("Starting nwp-consumer", version=__version__, arguments=arguments)

    fetcher = None
    storer = None

    match arguments['--sink']:
        case 'local':
            storer = outputs.localfs.LocalFSClient(
                rawDir=arguments['--rdir'],
                zarrDir=arguments['--zdir'],
                createDirs=True
            )
        case 's3':
            s3c = config.S3Config()
            storer = outputs.s3.S3Client(
                key=s3c.AWS_ACCESS_KEY,
                bucket=s3c.AWS_S3_BUCKET,
                secret=s3c.AWS_ACCESS_SECRET,
                region=s3c.AWS_REGION,
                rawDir=arguments['--rdir'],
                zarrDir=arguments['--zdir'],
            )
        case _:
            raise ValueError(f"Unknown sink {arguments['--sink']}")

    match arguments['--source']:
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
            raise ValueError(f"Unknown source {arguments['--source']}")

    service = NWPConsumerService(
        fetcher=fetcher,
        storer=storer
    )

    # Set default values for start and end dates
    if arguments['--from'] is "today" or None:
        arguments['--from'] = dt.datetime.now().strftime("%Y-%m-%d")
    if arguments['--to'] is "today" or None:
        arguments['--to'] = dt.datetime.now().strftime("%Y-%m-%d")

    # Parse start and end dates
    startDate: dt.date = dt.datetime.strptime(arguments['--from'], "%Y-%m-%d").date()
    endDate: dt.date = dt.datetime.strptime(arguments['--to'], "%Y-%m-%d").date()
    if endDate < startDate:
        raise ValueError("Argument '--from' cannot specify date prior to '--to'")

    downloaded = []
    converted = []

    if arguments['download']:
        downloaded = service.DownloadRawDataset(
            start=startDate,
            end=endDate
        )

    if arguments['convert']:
        converted = service.ConvertRawDatasetToZarr(
            start=startDate,
            end=endDate
        )

    if arguments['consume']:
        converted = service.DownloadAndConvert(
            start=startDate,
            end=endDate
        )

    if arguments['--create-latest']:
        service.CreateLatestZarr()

    programEndTime = dt.datetime.now()
    log.info(
        "Finished nwp-consumer.",
        elapsed_time=programEndTime - programStartTime,
        version=__version__)


if __name__ == "__main__":
    run()
