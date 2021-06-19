# Deprecated
import asyncio
import math
import os
import queue
import threading
from typing import List, Callable, Any

from electrum import SimpleConfig
from electrum.clients.client import ElectrumClient
from electrum.clients.worker import Worker


class DeprecatedElectrumBatchClient(ElectrumClient):
    def __init__(self, config: SimpleConfig, loop, stopping_fut, loop_thread, batch_limit: int):
        super().__init__(config, loop, stopping_fut, loop_thread)
        self.batch_limit = batch_limit

    @staticmethod
    def chunks(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    async def get_balances(self, script_hashes: List[str], *args, **kwargs) -> dict:
        self.logger.info(
            f"""start ElectrumBatchClient.get_balances in {kwargs.get('__thread_name__', 0)}, count={len(script_hashes)}, batch_lim={self.batch_limit}""")

        result = {}
        count = 0

        try:
            for shs in self.chunks(script_hashes, self.batch_limit):
                res = await self.network.get_balances_for_scripthashes(shs)
                list(map(lambda i: result.update({shs[i[0]]: i[1]}), enumerate(res)))
                count += len(shs)
                if count % 1000 == 0:
                    self.logger.info(f"{kwargs.get('__thread_name__', 0)} -- count={count}")
        except Exception as e:
            self.logger.error(e)
        finally:
            self.logger.info(
                f"""stop ElectrumBatchClient.get_balances in {kwargs.get('__thread_name__', 0)}. count of processed addresses is {count}""")
            return result

    async def get_listunspents(self, script_hashes: List[str], *args, **kwargs) -> dict:
        self.logger.info(
            f"""start ElectrumBatchClient.get_listunspents in {kwargs.get('__thread_name__', 0)}, count={len(script_hashes)}, batch_lim={self.batch_limit} ---- {script_hashes[0]}""")
        count = 0
        result = {}
        try:
            for shs in self.chunks(script_hashes, self.batch_limit):
                res = await self.network.listunspents_for_scripthashes(shs)
                list(map(lambda i: result.update({shs[i[0]]: i[1]}), enumerate(res)))
                count += len(shs)
                if count % 1000 == 0:
                    self.logger.info(f"{kwargs.get('__thread_name__', 0)} -- count={count}")
        except Exception as e:
            self.logger.error(e)
        finally:
            self.logger.info(
                f"""stop ElectrumBatchClient.get_listunspents  #{kwargs.get('__thread_name__', 0)}. count of processed addresses is {count}""")
            return result

    async def get_listmempools(self, script_hashes: List[str], *args, **kwargs) -> dict:
        self.logger.info(
            f"""start ElectrumBatchClient.listmempools_for_scripthashes in {kwargs.get('__thread_name__', 0)}, count={len(script_hashes)}, batch_lim={self.batch_limit} ---- {script_hashes[0]}""")
        count = 0
        result = {}
        try:
            for shs in self.chunks(script_hashes, self.batch_limit):
                res = await self.network.listmempools_for_scripthashes(shs)
                list(map(lambda i: result.update({shs[i[0]]: i[1]}), enumerate(res)))
                count += len(shs)
                if count % 1000 == 0:
                    self.logger.info(f"{kwargs.get('__thread_name__', 0)} -- count={count}")
        except Exception as e:
            self.logger.error(e)
        finally:
            self.logger.info(
                f"""stop ElectrumBatchClient.listmempools_for_scripthashes in {kwargs.get('__thread_name__', 0)}. count of processed addresses is {count},""")
            return result


# Deprecated
class DeprecatedElectrumThreadClient(DeprecatedElectrumBatchClient):
    """
    example:
        with ElectrumThreadClient(config, loop, stopping_fut, loop_thread, 50) as client:
            listunspents = client.get_listunspents(script_hashes=script_hashes)
            listmempools = client.get_listmempools(script_hashes=script_hashes)
            balances = client.get_balances(script_hashes=script_hashes)
    """

    def get_listmempools(self, script_hashes: List[str], *args, **kwargs) -> dict:
        self.logger.info(f"run ElectrumThreadClient.get_listmempools")
        tasks = []
        with Worker(threads_count=kwargs.get("threads_count")) as worker:
            for chunk in self.chunks(script_hashes, math.ceil(len(script_hashes) / worker.threads_count)):
                __kwargs = kwargs
                __kwargs.update({"func": super().get_listmempools, "script_hashes": chunk})
                tasks.append(worker.add_task(function=self.call, **__kwargs))
        result = {}
        list(map(lambda x: result.update(worker.result[x]), tasks))
        return result

    def get_listunspents(self, script_hashes: List[str], *args, **kwargs) -> dict:
        self.logger.info(f"run ElectrumThreadClient.get_listunspents")
        tasks = []

        with Worker(threads_count=kwargs.get("threads_count")) as worker:
            for chunk in self.chunks(script_hashes, math.ceil(len(script_hashes) / worker.threads_count)):
                __kwargs = kwargs
                __kwargs.update({"func": super().get_listunspents, "script_hashes": chunk})
                tasks.append(worker.add_task(function=self.call, **__kwargs))
        result = {}
        list(map(lambda x: result.update(worker.result[x]), tasks))
        return result

    def get_balances(self, script_hashes: List[str], *args, **kwargs) -> dict:
        self.logger.info(f"run ElectrumThreadClient.get_balances")
        tasks = []

        with Worker(threads_count=kwargs.get("threads_count")) as worker:
            for chunk in self.chunks(script_hashes, math.ceil(len(script_hashes) / worker.threads_count)):
                __kwargs = kwargs
                __kwargs.update({"func": super().get_balances, "script_hashes": chunk})
                tasks.append(worker.add_task(function=self.call, **__kwargs))
        result = {}
        list(map(lambda x: result.update(worker.result[x]), tasks))
        return result


# deprecated
class DeprecatedElectrumThreadClient_(DeprecatedElectrumBatchClient):
    def __init__(self, config: SimpleConfig, loop, stopping_fut, loop_thread, batch_limit: int):
        self.threads_count = int(os.cpu_count()) - 1
        self.threads = []

        self.header_queue = asyncio.Queue()
        self.q = queue.Queue()
        super().__init__(config, loop, stopping_fut, loop_thread, batch_limit)

    def worker(self, func: Callable, *args, **kwargs):
        while True:
            item = self.q.get()
            if item is None:
                break
            self.do_work(func, *item)
            self.q.task_done()

    def do_work(self, func, params, thread_numb=0):
        res = self.call(func, params, thread_numb=thread_numb)
        self.logger.info(f"data: {len(res)}")
        return self.result.update(res)

    def start(self, func: Callable, params: List[Any]):
        self.logger.info(f"run ElectrumThreadClient.start for func {func.__name__} with {self.threads_count} threads")
        for i, chunk in enumerate(self.chunks(params, math.ceil(len(params) / self.threads_count))):
            t = threading.Thread(target=self.do_work, args=[func, chunk, i], name=f"thread #{i}")
            t.start()
            self.threads.append(t)

        for t in self.threads:
            t.join()

        return self.result

    def __enter__(self):
        super().__enter__()
        self.result = {}
        return self
