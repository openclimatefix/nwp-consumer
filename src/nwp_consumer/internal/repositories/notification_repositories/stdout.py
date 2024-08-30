"""Stdout notification_repositories store."""

import logging

from returns.io import IOResultE, IOSuccess

from nwp_consumer.internal import entities
from nwp_consumer.internal.ports import NotificationRepository

log = logging.getLogger("nwp-consumer")


class StdoutNotificationRepository(NotificationRepository):
    """Stdout notification_repositories repository."""

    def notify(
            self,
            message: entities.StoreCreatedNotification | entities.StoreAppendedNotification,
    ) -> IOResultE[str]:
        """Overrides the corresponding method in the parent class."""
        log.info(f"Sent notification_repositories (type: {message.__class__.__name__}): {message}")
        return IOSuccess("Notification sent to stdout successfully.")
