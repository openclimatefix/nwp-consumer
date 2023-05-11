"""nwp-consumer.

Usage:
  nwp-consumer download-raw-dataset --start-date <startDate> --end-date <endDate>
  nwp-consumer (-h | --help)
  nwp-consumer --version

Options:
  -h --help     Show this screen.
  --version     Show version.
  --start-date  Start date in YYYY-MM-DD format
  --end-date    End date in YYYY-MM-DD format
"""

import datetime as dt
import importlib.metadata

import structlog
from docopt import docopt

from nwp_consumer.internal import config
from nwp_consumer.internal.inputs import ceda
from nwp_consumer.internal.outputs import localfs
from nwp_consumer.internal.service import NWPConsumerService

__version__ = "local"

try:
    __version__ = importlib.metadata.version("nwp-consumer")
except importlib.metadata.PackageNotFoundError:
    # package is not installed
    pass

log = structlog.stdlib.get_logger()


def main():
    # Parse command line arguments from docstring
    arguments = docopt(__doc__, version=__version__)

    log.info("Starting nwp-consumer", version=__version__, arguments=arguments)

    sc = config.LocalFSConfig()
    storer = localfs.LocalFSClient(
        rawDir=sc.RAW_DIR,
        zarrDir=sc.ZARR_DIR,
        createDirs=True
    )

    cc = config.CEDAConfig()
    fetcher = ceda.CEDAClient(
        ftpUsername=cc.CEDA_FTP_USER,
        ftpPassword=cc.CEDA_FTP_PASS,
        storer=storer
    )

    service = NWPConsumerService(
        fetcher=fetcher,
        storer=storer
    )

    if arguments['download-raw-dataset']:
        service.download_raw_dataset(
            start_date=dt.datetime.strptime(arguments['<startDate>'], "%Y-%m-%d"),
            end_date=dt.datetime.strptime(arguments['<endDate>'], "%Y-%m-%d")
        )


if __name__ == "__main__":
    main()
