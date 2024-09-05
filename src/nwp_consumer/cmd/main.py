import sys

from nwp_consumer.internal import handlers, repositories, services


def ceda_metoffice_entrypoint() -> None:
    """Entry point for downloading CEDA Met Office data."""
    c = handlers.CLIHandler(
        consumer_usecase=services.ConsumerService(
            model_repository=repositories.CedaMetOfficeGlobalModelRepository(),
            notification_repository=repositories.StdoutNotificationRepository(),
            zarr_repository=None,
        ),
    )
    returncode = c.run()
    sys.exit(returncode)
