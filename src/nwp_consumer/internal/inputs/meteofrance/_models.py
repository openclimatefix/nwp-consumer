import datetime as dt

from nwp_consumer import internal


class ArpegeFileInfo(internal.FileInfoModel):
    def __init__(
        self,
        it: dt.datetime,
        filename: str,
        currentURL: str,
        step: int,
    ) -> None:
        self._it = it
        self._filename = filename
        self._url = currentURL
        self.step = step

    def filename(self) -> str:
        """Overrides the corresponding method in the parent class."""
        return self._filename

    def filepath(self) -> str:
        """Overrides the corresponding method in the parent class."""
        return self._url + self._filename

    def it(self) -> dt.datetime:
        """Overrides the corresponding method in the parent class."""
        return self._it

    def steps(self) -> list[int]:
        """Overrides the corresponding method in the parent class."""
        return [self.step]

    def variables(self) -> list[str]:
        """Overrides the corresponding method in the parent class."""
        raise NotImplementedError()
