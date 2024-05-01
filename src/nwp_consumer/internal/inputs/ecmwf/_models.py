import datetime as dt
from dataclasses import dataclass

import nwp_consumer.internal as internal


@dataclass
class ECMWFMarsFileInfo(internal.FileInfoModel):
    inittime: dt.datetime
    area: str
    params: list[str]
    steplist: list[int]

    def filename(self) -> str:
        """Overrides the corresponding method in the parent class."""
        # ECMWF does not have explicit filenames when using the MARS API
        # * As such, name manually based on their inittime and area covered
        #   e.g. `ecmwf_uk_20210101T0000.grib`
        return f"ecmwf_{self.area}_{self.inittime.strftime('%Y%m%dT%H%M')}.grib"

    def filepath(self) -> str:
        """Overrides the corresponding method in the parent class."""
        return ""

    def it(self) -> dt.datetime:
        """Overrides the corresponding method in the parent class."""
        return self.inittime

    def variables(self) -> list[str]:
        """Overrides the corresponding method in the parent class."""
        return self.params

    def steps(self) -> list[int]:
        """Overrides the corresponding method in the parent class."""
        return self.steplist


@dataclass
class ECMWFLiveFileInfo(internal.FileInfoModel):
    """Dataclass for ECMWF live data files.

    Live ECMWF files are extensionless grib files named e.g. 'A1D02200000022001001'.
    The files contain data for two areas. The names contain the following information

    A1D%m%d%H%M%m'%d'%H'%M'1, where the first time is the initialisation time
    and the second the target time.
    """

    fname: str

    def filename(self) -> str:
        """Overrides the corresponding method in the parent class."""
        return self.fname + ".grib"

    def filepath(self) -> str:
        """Overrides the corresponding method in the parent class."""
        return f"ecmwf/{self.fname}"

    def it(self) -> dt.datetime:
        """Overrides the corresponding method in the parent class.

        The file name doesn't have the year in it, so we've added it.
        This might be a problem around the new year.
        """
        return dt.datetime.strptime(
            f"{self.fname[3:10]}-{dt.datetime.now().year}", "%m%d%H%M-%Y"
        ).replace(
            tzinfo=dt.UTC,
        )

    def variables(self) -> list[str]:
        """Overrides the corresponding method in the parent class."""
        raise NotImplementedError()

    def steps(self) -> list[int]:
        """Overrides the corresponding method in the parent class."""
        raise NotImplementedError()
