"""nwp-consumer.

Usage:
  nwp-consumer download-raw-historic-dataset --start-date <startDate> --end-date <endDate> (--ceda | --metoffice)
  nwp-consumer convert-raw-dataset --start-date <startDate> --end-date <endDate> (--ceda | --metoffice)
  nwp-consumer (-h | --help)
  nwp-consumer --version

Options:
  -h --help     Show this screen.
  --version     Show version.
  --start-date  Start date in YYYY-MM-DD format
  --end-date    End date in YYYY-MM-DD format
  --ceda        Use CEDA as the data source
  --metoffice   Use Met Office as the data source
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
    # Parse command line arguments from docstring
    arguments = docopt(__doc__, version=__version__)

    log.info("Starting nwp-consumer", version=__version__, arguments=arguments)

    fetcher = None
    storer = None

    sc = config.LocalFSConfig()
    storer = outputs.localfs.LocalFSClient(
        rawDir=sc.RAW_DIR,
        zarrDir=sc.ZARR_DIR,
        createDirs=True
    )

    if arguments['--ceda']:
        cc = config.CEDAConfig()
        fetcher = inputs.ceda.CEDAClient(
            ftpUsername=cc.CEDA_FTP_USER,
            ftpPassword=cc.CEDA_FTP_PASS,
        )

    if arguments['--metoffice']:
        mc = config.MetOfficeConfig()
        fetcher = inputs.metoffice.MetOfficeClient(
            orderID=mc.METOFFICE_ORDER_ID,
            clientID=mc.METOFFICE_CLIENT_ID,
            clientSecret=mc.METOFFICE_CLIENT_SECRET,
        )

    service = NWPConsumerService(
        fetcher=fetcher,
        storer=storer
    )

    if arguments['download-raw-dataset']:
        service.DownloadRawDataset(
            startDate=dt.datetime.strptime(arguments['<startDate>'], "%Y-%m-%d"),
            endDate=dt.datetime.strptime(arguments['<endDate>'], "%Y-%m-%d")
        )

    if arguments['convert-raw-dataset']:
        service.ConvertRawDatasetToZarr(
            startDate=dt.datetime.strptime(arguments['<startDate>'], "%Y-%m-%d"),
            endDate=dt.datetime.strptime(arguments['<endDate>'], "%Y-%m-%d")
        )


if __name__ == "__main__":
    run()
