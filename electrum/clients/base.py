import asyncio
import logging

from electrum import Network

_INSTANCE = None


class BaseElectrumClient:
    def __init__(self, *, logger: logging.Logger = None):
        self.results = {}

        self.logger = logger or logging.getLogger(self.__class__.__name__)

    def __enter__(self):
        self.loop = asyncio.get_event_loop()
        self.network = Network.get_instance()

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
