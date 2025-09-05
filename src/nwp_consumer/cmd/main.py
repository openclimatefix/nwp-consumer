"""Entrypoints to the nwp-consumer service."""

import logging
import os
import sys
from typing import NamedTuple

from nwp_consumer.internal import handlers, ports, repositories

log = logging.getLogger("nwp-consumer")


class Adaptors(NamedTuple):
    """Adaptors for the CLI."""

    model_repository: type[ports.RawRepository]
    notification_repository: type[ports.NotificationRepository]


def parse_env() -> Adaptors:
    """Parse from the environment."""
    model_repository_adaptor: type[ports.RawRepository]
    match os.getenv("MODEL_REPOSITORY"):
        # Default to NOAA S3 as it is freely accessible
        case None | "gfs":
            model_repository_adaptor = repositories.raw_repositories.NOAAS3RawRepository
        case "ceda":
            model_repository_adaptor = repositories.raw_repositories.CEDARawRepository
        case "ecmwf-realtime":
            model_repository_adaptor = repositories.raw_repositories.ECMWFRealTimeS3RawRepository
        case "metoffice-datahub":
            model_repository_adaptor = repositories.raw_repositories.MetOfficeDatahubRawRepository
        case "ecmwf-mars":
            model_repository_adaptor = repositories.raw_repositories.ECMWFMARSRawRepository
        case _ as mr:
            log.error(
                f"Unknown model repository '{mr}'. Expected one of "
                f"['gfs', 'ceda', 'ecmwf-realtime', 'metoffice-datahub', 'ecmwf-mars']",
            )
            sys.exit(1)

    notification_repository_adaptor: type[ports.NotificationRepository]
    match os.getenv("NOTIFICATION_REPOSITORY", "stdout"):
        case "stdout":
            notification_repository_adaptor = (
                repositories.notification_repositories.StdoutNotificationRepository
            )
        case "dagster-pipes":
            notification_repository_adaptor = (
                repositories.notification_repositories.DagsterPipesNotificationRepository
            )
        case _ as notification:
            log.error(f"Unknown notification repository: {notification}")
            sys.exit(1)

    return Adaptors(
        model_repository=model_repository_adaptor,
        notification_repository=notification_repository_adaptor,
    )


def run_cli() -> None:
    """Entrypoint for the CLI handler."""
    adaptors = parse_env()
    c = handlers.CLIHandler(
        model_adaptor=adaptors.model_repository,
        notification_adaptor=adaptors.notification_repository,
    )
    returncode: int = c.run()
    sys.exit(returncode)
