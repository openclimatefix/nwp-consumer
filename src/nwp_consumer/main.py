
import structlog

from src.nwp_consumer.internal.inputs import ceda

log = structlog.stdlib.get_logger()

if __name__ == "__main__":

    client = ceda.CEDAClient()

    try:
        init

    except Exception as e:
        log.error(e, exc_info=True)




