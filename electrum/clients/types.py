from dataclasses import dataclass
from typing import List, Any, Callable, Optional

from electrum.clients.base import BaseElectrumClient


@dataclass
class Req:
    req_id: str
    method: str
    params: List
    errors: Any = None
    resp_validate_fun: Callable = None
    _batch_client: BaseElectrumClient = None

    @property
    def result(self):
        if self._batch_client is None:
            raise Exception("_batch_client not defined")

        r = self._batch_client.results.get(self.req_id)
        if self.resp_validate_fun is not None and self.resp_validate_fun(r) is False:
            raise Exception("Validation ERROR")
        return r
