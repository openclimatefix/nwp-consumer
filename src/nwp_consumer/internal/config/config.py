import dataclasses
import os

import dacite


@dataclasses.dataclass
class AppConfig:
    """App configuration."""
    LOGLEVEL: str = "INFO"
    MODEL_REPOSITORY: str = "ceda-metoffice-global"
    NOTIFICATION_REPOSITORY: str = "stdout"
    WORKDIR: str = "~/.local/cache/nwp"

def parse() -> AppConfig:
    """Parse the configuration from the environment."""
    return dacite.from_dict(
        data_class=AppConfig,
        data=os.environ,
    )

