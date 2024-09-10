"""Stdout notification repository implementation."""

import logging
from typing import override

from returns.result import Result, ResultE

from nwp_consumer.internal import entities, ports

log = logging.getLogger("nwp-consumer")


class StdoutNotificationRepository(ports.NotificationRepository):
    """Stdout notification repository."""

    @override
    def notify(
        self,
        message: entities.StoreCreatedNotification | entities.StoreAppendedNotification,
    ) -> ResultE[str]:
        log.info(f"{message}")
        return Result.from_value("Notification sent to stdout successfully.")

