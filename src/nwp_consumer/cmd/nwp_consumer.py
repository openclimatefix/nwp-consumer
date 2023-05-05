"""nwp-consumer

Usage:
  nwp_consumer.py create-monthly-zarr-dataset --start-date <startDate> --end-date <endDate>
  nwp_consumer.py (-h | --help)
  nwp_consumer.py --version

Options:
  -h --help     Show this screen.
  --version     Show version.
  --start-date  Start date in YYYY-MM-DD format
  --end-date    End date in YYYY-MM-DD format
  --raw-dir     Directory to store raw grib files
  --zarr-dir    Directory to store zarr files
"""

import os
import datetime as dt

import structlog

from docopt import docopt
import importlib.metadata

from src.nwp_consumer.internal import config
from src.nwp_consumer.internal.inputs import ceda
from src.nwp_consumer.internal.outputs import localfs
from src.nwp_consumer.internal.service import CreateMonthlyZarrDataset

__version__ = "local"

try:
    __version__ = importlib.metadata.version("nwp-consumer")
except importlib.metadata.PackageNotFoundError:
    # package is not installed
    pass

log = structlog.stdlib.get_logger()


if __name__ == "__main__":
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
