import asyncio
import logging
import math
import time
import uuid
from typing import List, Any

from electrum import Network
from electrum.clients.worker import Worker


class ElectrumClient:
    def __init__(self, network: Network, loop, stopping_fut, loop_thread):
        self.network = network
        self.loop, self.stopping_fut, self.loop_thread = loop, stopping_fut, loop_thread
        self.logger = logging.getLogger(self.__class__.__name__)


class ElectrumBatchClient(ElectrumClient):
    class Req:
        req_id: str
        method: str
        params: List
        result: Any = None
        errors: Any = None

        def __init__(self, req_id: str, method: str, params: List, result: Any = None, errors: Any = None):
            self.req_id = req_id
            self.method = method
            self.params = params
            self.result = result
            self.errors = errors

        def set_result(self, res):
            self.result = res
            return self

    def __init__(self, network: Network,  loop, stopping_fut, loop_thread, batch_limit: int, *,
                 raise_error: bool = False):
        super().__init__(network, loop, stopping_fut, loop_thread)
        self.batch_limit = batch_limit
        self.raise_error = raise_error
        self.requests = {}
        self.results = {}

    @staticmethod
    def chunks(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def get_balances(self, script_hash: str, *args, **kwargs) -> str:
        return self.add_request("blockchain.scripthash.get_balance", [script_hash])

    def get_listunspents(self, script_hash: str, *args, **kwargs) -> str:
        return self.add_request("blockchain.scripthash.listunspent", [script_hash])

    def get_listmempools(self, script_hash: str, *args, **kwargs) -> str:
        return self.add_request("blockchain.scripthash.get_mempool", [script_hash])

    def add_request(self, method: str, params: List):
        req_id = str(uuid.uuid4())
        self.requests.update({
            req_id: self.Req(req_id=req_id, method=method, params=params)
        })

        return req_id

    async def __run__(self, requests, **kwargs):
        r = {}
        count = 0
        self.logger.info(f"""{kwargs.get('__thread_name__', 0)} started. Target requests count is {len(requests)}""")
        for chunk in self.chunks(list(requests.items()), self.batch_limit):
            r.update(await self.__send_batch__request__(requests=chunk))
            count += len(chunk)

            if count % 1000 == 0:
                self.logger.info(f"{kwargs.get('__thread_name__', 0)} -- count={count}")
        self.logger.info(f"""{kwargs.get('__thread_name__', 0)} finished. count of processed requests is {count}""")
        return r

    async def __send_batch__request__(self, requests: List, **kwargs):
        try:
            async with self.network.interface.session.send_batch(raise_errors=self.raise_error) as batch:
                for req_id, req in requests:
                    self.logger.debug(f"add request to batch: {req.method}  {req.params}")
                    batch.add_request(req.method, req.params)
            res = dict(map(lambda x: (x[0][0], x[0][1].set_result(x[1])), zip(requests, batch.results)))
            self.results.update(res)
        except Exception as e:
            self.logger.error(e)
            if self.raise_error is True:
                raise e
            return
        return res

    def __enter__(self):
        self.requests = {}
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            a = asyncio.run_coroutine_threadsafe(self.__run__(self.requests), self.loop)
            while not a.done():
                time.sleep(1)
        return


class ElectrumThreadBatchClient(ElectrumBatchClient):
    def __init__(self, network: Network, loop, stopping_fut, loop_thread, batch_limit: int,
                 *, raise_error: bool = False, thread_count: int = None):
        super().__init__(network, loop, stopping_fut, loop_thread, batch_limit, raise_error=raise_error)
        self.thread_count = thread_count

    def __run__(self, requests, **kwargs):
        def f(reqs, **kw):
            a = asyncio.run_coroutine_threadsafe(super(ElectrumThreadBatchClient, self).__run__(requests=reqs, **kw),
                                                 self.loop)
            while not a.done():
                time.sleep(1)
            return a.result()

        if len(requests.values()) < 1:
            return self.results
        tasks = []
        with Worker(threads_count=self.thread_count) as worker:
            req_items = list(requests.items())
            for req_items_chunk in self.chunks(req_items, math.ceil(len(requests.items()) / worker.threads_count)):
                tasks.append(worker.add_task(function=f, reqs=dict(req_items_chunk)))
        return self.results

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.__run__(self.requests)
        return
