import datetime as dt
from typing import ClassVar

from marshmallow import EXCLUDE, Schema, fields
from marshmallow_dataclass import dataclass

from nwp_consumer import internal


@dataclass
class MetOfficeFileInfo(internal.FileInfoModel):

    class Meta:
        unknown = EXCLUDE

    fileId: str
    runDateTime: dt.datetime

    Schema: ClassVar[type[Schema]] = Schema  # To prevent confusing type checkers

    def it(self) -> dt.datetime:
        """Overrides the corresponding method in the parent class."""
        return self.runDateTime.replace(tzinfo=None)

    def filename(self) -> str:
        """Overrides the corresponding method in the parent class."""
        return self.fileId + ".grib"

    def filepath(self) -> str:
        """Overrides the corresponding method in the parent class."""
        return f"{self.fileId}/data"

    def steps(self) -> list[int]:
        """Overrides the corresponding method in the parent class."""
        raise NotImplementedError()

    def variables(self) -> list[str]:
        """Overrides the corresponding method in the parent class."""
        raise NotImplementedError()


@dataclass
class MetOfficeOrderDetails:

    class Meta:
        unknown = EXCLUDE

    files: list[MetOfficeFileInfo] = fields.List(fields.Nested(MetOfficeFileInfo.Schema()))

    Schema: ClassVar[type[Schema]] = Schema  # To prevent confusing type checkers


@dataclass
class MetOfficeResponse:

    orderDetails: MetOfficeOrderDetails

    Schema: ClassVar[type[Schema]] = Schema  # To prevent confusing type checkers
