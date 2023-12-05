"""The internal package contains code not intended for external import."""

__all__ = [
    "OCFShortName",
    "FetcherInterface",
    "StorageInterface",
    "FileInfoModel",
    "IT_FOLDER_FMTSTR",
    "IT_FOLDER_GLOBSTR",
    "TMP_DIR",
    "ZARR_FMTSTR",
    "ZARR_GLOBSTR",
]

from .models import (
    IT_FOLDER_FMTSTR,
    IT_FOLDER_GLOBSTR,
    TMP_DIR,
    ZARR_FMTSTR,
    ZARR_GLOBSTR,
    FetcherInterface,
    FileInfoModel,
    OCFShortName,
    StorageInterface,
)

TMP_DIR.mkdir(parents=True, exist_ok=True)
