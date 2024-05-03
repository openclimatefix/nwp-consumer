import structlog

processors = [
    structlog.stdlib.PositionalArgumentsFormatter(),
    structlog.processors.CallsiteParameterAdder(
        [
            structlog.processors.CallsiteParameter.FILENAME,
            structlog.processors.CallsiteParameter.LINENO,
        ],
    ),
    structlog.stdlib.add_log_level,
    structlog.processors.TimeStamper(fmt="iso"),
    structlog.processors.StackInfoRenderer(),
    structlog.processors.EventRenamer("message", replace_by="_event"),
    structlog.processors.dict_tracebacks,
    structlog.processors.JSONRenderer(sort_keys=True),
]

structlog.configure(processors=processors)
