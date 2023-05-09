"""nwp-consumer.

Usage:
  nwp_consumer create-monthly-zarr-dataset --start-date <startDate> --end-date <endDate>
  nwp_consumer (-h | --help)
  nwp_consumer --version

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
from nwp_consumer.internal.service import CreateMonthlyZarrDataset

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

    if arguments['create-monthly-zarr-dataset']:
        sc = config.LocalFSConfig()
        storer = localfs.LocalFSClient(
            rawDir=sc.RAW_DIR,
            zarrDir=sc.ZARR_DIR, createDirs=True
        )

        cc = config.CEDAConfig()
        CreateMonthlyZarrDataset(
            fetcher=ceda.CEDAClient(
                ftpUsername=cc.CEDA_FTP_USER,
                ftpPassword=cc.CEDA_FTP_PASS,
                storer=storer
            ),
            startDate=dt.datetime.strptime(arguments['<startDate>'], "%Y-%m-%d").date(),
            endDate=dt.datetime.strptime(arguments['<endDate>'], "%Y-%m-%d").date(),
            storer=storer
        )


if __name__ == "__main__":
    main()
