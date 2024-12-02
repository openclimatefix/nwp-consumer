"""Model Repositories

TODO: Add description
"""

from .ceda_ftp import CEDAFTPModelRepository
from .ecmwf_realtime import ECMWFRealTimeS3ModelRepository
from .ecmwf_mars import ECMWFMARSModelRepository
from .noaa_s3 import NOAAS3ModelRepository
from .mo_datahub import MetOfficeDatahubModelRepository

__all__ = [
    "CEDAFTPModelRepository",
    "ECMWFRealTimeS3ModelRepository",
    "NOAAS3ModelRepository",
    "MetOfficeDatahubModelRepository",
    "ECMWFMARSModelRepository",
]

