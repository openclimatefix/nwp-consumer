from .. import (domain, ports)


class NWPArchivalService(ports.NWPConsumerService):
    """Service for archiving NWP data implementing the NWPConsumerService interface.

    This service consumes NWP data and writes it to an archival store.
    """


