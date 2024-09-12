"""Entrypoints to the nwp-consumer service."""

import logging
import os
import sys
from typing import NamedTuple

from nwp_consumer.internal import handlers, ports, repositories, services

log = logging.getLogger("nwp-consumer")

class Adaptors(NamedTuple):
    """Adaptors for the CLI."""
    model_repository: type[ports.ModelRepository]
    notification_repository: type[ports.NotificationRepository]

def parse_env() -> Adaptors:
    """Parse from the environment."""
    model_repository_adaptor: type[ports.ModelRepository]
    match os.getenv("MODEL_REPOSITORY"):
        case None:
            log.error("MODEL_REPOSITORY is not set in environment.")
            sys.exit(1)
        case "ceda":
            model_repository_adaptor = repositories.CedaMetOfficeGlobalModelRepository
        case "ecmwf-realtime-s3":
            model_repository_adaptor = repositories.ECMWFRealTimeS3ModelRepository
        case _ as model:
            log.error(f"Unknown model: {model}")
            sys.exit(1)

    notification_repository_adaptor: type[ports.NotificationRepository]
    match os.getenv("NOTIFICATION_REPOSITORY", "stdout"):
        case "stdout":
            notification_repository_adaptor = repositories.StdoutNotificationRepository
        case "dagster-pipes":
            notification_repository_adaptor = repositories.DagsterPipesNotificationRepository
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
        consumer_usecase=services.ConsumerService(
            model_repository=adaptors.model_repository,
            notification_repository=adaptors.notification_repository,
        ),
        archiver_usecase=services.ArchiverService(
            model_repository=adaptors.model_repository,
            notification_repository=adaptors.notification_repository,
        ),
    )
    returncode: int = c.run()
    sys.exit(returncode)

