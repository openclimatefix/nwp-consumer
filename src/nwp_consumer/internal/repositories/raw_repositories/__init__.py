"""Model Repositories

TODO: Add description
"""

from .ceda_ftp import CEDAFTPRawRepository
from .ecmwf_realtime import ECMWFRealTimeS3RawRepository
from .ecmwf_mars import ECMWFMARSRawRepository
from .noaa_s3 import NOAAS3RawRepository
from .mo_datahub import MetOfficeDatahubRawRepository

__all__ = [
    "CEDAFTPRawRepository",
    "ECMWFRealTimeS3RawRepository",
    "NOAAS3RawRepository",
    "MetOfficeDatahubRawRepository",
    "ECMWFMARSRawRepository",
]

