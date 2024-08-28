"""Stdout notification store."""

import logging

from returns.io import IOResultE, IOSuccess

from nwp_consumer.internal import entities
from nwp_consumer.internal.ports import NotificationRepository

log = logging.getLogger(__package__)


class StdoutNotificationRepository(NotificationRepository):
    """Stdout notification repository."""

    def notify(
            self,
            message: entities.StoreCreatedNotification | entities.StoreAppendedNotification,
    ) -> IOResultE[str]:
        """Overrides the corresponding method in the parent class."""
        log.info(f"Sent notification (type: {message.__class__.__name__}): {message}")
        return IOSuccess("Notification sent to stdout successfully.")
