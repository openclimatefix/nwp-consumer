from nwp_consumer.internal import handlers, entities, services, repositories


def metoffice_global() -> None:
    c = handlers.CLIHandler(
        consumer_usecase=services.ConsumerService(
            model_repository=repositories.MetOfficeGlobalModelRepository(),
            notification_repository=repositories.StdoutNotificationRepository(),
    )
    c.run()


if __name__ == "__main__":
    main()