import asyncio
import logging
import math
import time
import uuid
from typing import List, Callable, Dict

from electrum.clients.base import BaseElectrumClient
from electrum.clients.types import Req
from electrum.clients.worker import Worker


class ElectrumBatchClient(BaseElectrumClient):
    def __init__(self, *, logger: logging.Logger = None, batch_limit: int = 50, raise_error: bool = False):
        super().__init__(logger=logger)
        self.batch_limit = batch_limit
        self.raise_error = raise_error
        self.requests = {}
        self.results = {}

    @staticmethod
    def chunks(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def get_balances(self, script_hash: str, **kwargs) -> Req:
        return self.add_request("blockchain.scripthash.get_balance", [script_hash], resp_validate_fun=None, **kwargs)

    def get_listunspents(self, script_hash: str, **kwargs) -> Req:
        return self.add_request("blockchain.scripthash.listunspent", [script_hash], resp_validate_fun=None, **kwargs)

    def get_listmempools(self, script_hash: str, **kwargs) -> Req:
        return self.add_request("blockchain.scripthash.get_mempool", [script_hash], resp_validate_fun=None, **kwargs)

    def get_transact_info(self, tx_hash: str, is_full_obj: bool = True, **kwargs) -> Req:
        return self.add_request("blockchain.transaction.get", [tx_hash, is_full_obj], resp_validate_fun=None, **kwargs)

    def add_request(self, method: str, params: list, *, resp_validate_fun: Callable = None, **kwargs) -> Req:
        req_id = str(uuid.uuid4())
        request = Req(req_id=req_id, method=method, params=params,
                      resp_validate_fun=resp_validate_fun, _batch_client=self)
        self.requests.update({
            req_id: request
        })
        return request

    async def _send_batch_request(self, requests: List[Req], **kwargs):
        try:
            async with kwargs.get("session").send_batch(raise_errors=self.raise_error) as batch:
                for req in requests:
                    self.logger.debug(f"add request {req.req_id} to batch: {req.method}  {req.params}")
                    batch.add_request(req.method, req.params)
            res = dict(zip(map(lambda x: x.req_id, requests), batch.results))
            self.results.update(res)
            count = len(self.results.items())
            if count % 1000 == 0:
                self.logger.info(f"count={count}")
        except Exception as e:
            self.logger.error(e)
            if self.raise_error is True:
                res = dict(zip(map(lambda x: x.req_id, requests), e.args[0].results))
                self.results.update(res)
            raise e
        return res

    async def _send_many_batch_requests(self, requests: Dict, **kwargs):
        thread_name = kwargs.get('__thread_name__', 'thread_#0')
        res = {}
        if len(requests) < 1:
            return res
        self.logger.info(f"""{thread_name} started. Target requests count is {len(requests)}""")
        time.sleep(2)
        try:
            for chunk in self.chunks(list(requests.values()), self.batch_limit):
                res.update(
                    await self._send_batch_request(requests=chunk, session=self.network.interface.session, **kwargs))
        except Exception as e:
            self.logger.error(e)
            raise e
        self.logger.info(f"""{thread_name} finished. count of processed requests is {len(self.results)}""")
        return res

    async def finalize(self):
        a = await self._send_many_batch_requests(self.requests)
        return a

    def __enter__(self):
        super(ElectrumBatchClient, self).__enter__()
        self.requests = {}
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            a = asyncio.run_coroutine_threadsafe(self.finalize(), self.loop)
            while not a.done():
                time.sleep(1)

        super(ElectrumBatchClient, self).__exit__(exc_type, exc_val, exc_tb)
        return True


class ElectrumAsyncBatchClient(ElectrumBatchClient):
    async def _send_many_batch_requests(self, requests: Dict, **kwargs):
        thread_name = kwargs.get('__thread_name__', 'thread_#0')
        res = {}
        if len(requests) < 1:
            return res
        self.logger.info(f"""{thread_name} started. Target requests count is {len(requests)}""")
        try:
            await asyncio.gather(
                *list(map(lambda chunk: self._send_batch_request(requests=chunk,
                                                                 session=self.network.interface.session, **kwargs),
                          self.chunks(list(requests.values()), self.batch_limit))),
                return_exceptions=not self.raise_error,
                loop=self.loop
            )
        except Exception as e:
            self.logger.error(e)
            raise e
        self.logger.info(f"""{thread_name} finished. count of processed requests is {len(self.results)}""")
        return res


class ElectrumThreadBatchClient(ElectrumBatchClient):
    def __init__(self, *, logger: logging.Logger = None,
                 batch_limit: int = 50, raise_error: bool = False, thread_count: int = None):
        super().__init__(logger=logger, batch_limit=batch_limit, raise_error=raise_error)
        self.thread_count = thread_count

    def finalize(self):
        def f(reqs, **kw):
            a = asyncio.run_coroutine_threadsafe(self._send_many_batch_requests(requests=reqs, **kw), self.loop)
            while not a.done():
                time.sleep(1)
            return a.result()

        tasks = []
        with Worker(threads_count=12) as worker:
            req_items = list(self.requests.items())
            for req_items_chunk in self.chunks(req_items, math.ceil(len(self.requests.items()) / worker.threads_count)):
                tasks.append(worker.add_task(function=f, reqs=dict(req_items_chunk)))
        return 1

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.finalize()
