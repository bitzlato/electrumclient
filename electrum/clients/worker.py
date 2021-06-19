import asyncio
import logging
import os
import queue
import threading
import uuid
from typing import Callable


class Worker:
    def __init__(self, *, threads_count: int = None):
        self.threads_count = threads_count if threads_count is not None else int(os.cpu_count()) - 1
        self.threads = []

        self.header_queue = asyncio.Queue()
        self.q = queue.Queue()
        self.logger = logging.getLogger(self.__class__.__name__)

    def do_work(self, *, __func__, __task_id__, **kwargs):
        return self.result.update({__task_id__: __func__(**kwargs)})

    def worker(self):
        tasks_count = {}
        thread_name = threading.current_thread().name
        while True:
            item = self.q.get()
            if item is None:
                break
            item.update({"__thread_name__": thread_name})
            self.do_work(__func__=item.pop("__func__"), __task_id__=item.pop("__task_id__"), **item)
            self.q.task_done()
            tasks_count.update({thread_name: tasks_count.get(thread_name, 0) + 1})
            self.logger.debug(f"task_completed_count: {tasks_count}")

    def add_task(self, *, function: Callable, **kwargs):
        if "__func__" in kwargs:
            raise Exception("Invalid parameter name: `__func__`")
        if "__task_id__" in kwargs:
            raise Exception("Invalid parameter name: `__task_id__`")
        if "__thread_name__" in kwargs:
            raise Exception("Invalid parameter name: `__thread_name__`")

        kwargs.update({"__func__": function, "__task_id__": str(uuid.uuid4())})
        self.q.put(kwargs)
        return kwargs["__task_id__"]

    def __enter__(self):
        self.result = {}
        for i in range(self.threads_count):
            t = threading.Thread(target=self.worker, name=f"thread_#{i}")
            t.start()
            self.threads.append(t)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for i in range(self.threads_count):
            self.q.put(None)
        for t in self.threads:
            t.join()
        return
