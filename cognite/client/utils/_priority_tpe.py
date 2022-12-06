"""
This code has been modified from the original created by Oleg Lupats, 2019, under an MIT license:
project = 'PriorityThreadPoolExecutor'
url = 'https://github.com/oleglpts/PriorityThreadPoolExecutor'
copyright = '2019, Oleg Lupats'
author = 'Oleg Lupats'
release = '0.0.1'

MIT License

Copyright (c) 2019 The Python Packaging Authority

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import atexit
import inspect
import itertools
import sys
import threading
import weakref
from concurrent.futures.thread import ThreadPoolExecutor, _base, _python_exit, _threads_queues, _WorkItem
from queue import PriorityQueue
from threading import Lock

NULL_ENTRY = (sys.maxsize, None, _WorkItem(None, None, (), {}))
_SHUTDOWN = False


def python_exit():
    global _SHUTDOWN
    _SHUTDOWN = True
    items = list(_threads_queues.items())
    for thread, queue in items:
        queue.put(NULL_ENTRY)
    for thread, queue in items:
        thread.join()


atexit.unregister(_python_exit)
atexit.register(python_exit)


def _worker(executor_reference, work_queue):
    try:
        while True:
            priority, _, work_item = work_queue.get(block=True)
            if priority != sys.maxsize:
                work_item.run()
                del work_item
                continue
            executor = executor_reference()
            if _SHUTDOWN or executor is None or executor._shutdown:
                work_queue.put(NULL_ENTRY)
                return None
            del executor
    except BaseException:
        _base.LOGGER.critical("Exception in worker", exc_info=True)


class PriorityThreadPoolExecutor(ThreadPoolExecutor):
    """Thread pool executor with queue.PriorityQueue()"""

    def __init__(self, max_workers=None):
        super().__init__(max_workers)
        self._work_queue = PriorityQueue()
        self._lock = Lock()
        self._counter = itertools.count()

    def counter(self):
        with self._lock:
            return next(self._counter)

    def submit(self, fn, *args, **kwargs):
        if "priority" in inspect.signature(fn).parameters:
            raise TypeError(f"Given function {fn} cannot accept reserved parameter name `priority`")

        with self._shutdown_lock:
            if self._shutdown:
                raise RuntimeError("Cannot schedule new futures after shutdown")

            priority = kwargs.pop("priority", None)
            assert isinstance(priority, int), "`priority` has to be an integer"

            future = _base.Future()
            work_item = _WorkItem(future, fn, args, kwargs)

            # `counter` to break ties, but keep order:
            self._work_queue.put((priority, self.counter(), work_item))
            self._adjust_thread_count()
            return future

    def _adjust_thread_count(self):
        def weak_ref_cb(_, queue=self._work_queue):
            queue.put(NULL_ENTRY)

        if len(self._threads) < self._max_workers:
            thread = threading.Thread(target=_worker, args=(weakref.ref(self, weak_ref_cb), self._work_queue))
            thread.daemon = True
            thread.start()
            self._threads.add(thread)
            _threads_queues[thread] = self._work_queue

    def shutdown(self, wait=True):
        with self._shutdown_lock:
            self._shutdown = True
            self._work_queue.put(NULL_ENTRY)
        if wait:
            for thread in self._threads:
                thread.join()
        else:
            # See: https://gist.github.com/clchiou/f2608cbe54403edb0b13
            self._threads.clear()
            _threads_queues.clear()
