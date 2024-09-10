import argparse
import logging
import os
import sys

from nwp_consumer.internal import handlers, repositories, services

log = logging.getLogger("nwp-consumer")

def parse_env() -> argparse.Namespace:
    """Parse from the environment."""
    config = argparse.Namespace()
    match os.getenv("NWP_CONSUMER_MODEL_REPOSITORY"):
        case None:
            log.error("NWP_CONSUMER_MODEL_REPOSITORY is not set in environment.")
            sys.exit(1)
        case "ceda-metoffice-global":
            config.model_repository = repositories.CedaMetOfficeGlobalModelRepository()
        case _ as model:
            log.error(f"Unknown model: {model}")
            sys.exit(1)

    match os.getenv("NWP_CONSUMER_NOTIFICATION_REPOSITORY", "stdout"):
        case "stdout":
            config.notification_repository = repositories.StdoutNotificationRepository()
        case _ as notification:
            log.error(f"Unknown notification repository: {notification}")
            sys.exit(1)

    return config


def run_cli() -> None:
    """Entrypoint for the CLI handler."""
    args = parse_env()
    c = handlers.CLIHandler(
        consumer_usecase=services.ConsumerService(
            model_repository=args.model_repository,
            zarr_repository=None,
            notification_repository=args.notification_repository,
        ),
        archiver_usecase=services.ArchiverService(
            model_repository=args.model_repository,
            notification_repository=args.notification_repository,
        ),
    )
    returncode: int = c.run()
    sys.exit(returncode)
