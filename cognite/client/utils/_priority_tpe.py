"""
This code has been heavily modified from the original created by Oleg Lupats, 2019, under an MIT license:
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
from __future__ import annotations

import inspect
import itertools
import sys
import weakref
from concurrent.futures import Future, as_completed
from concurrent.futures.thread import ThreadPoolExecutor, _base, _WorkItem
from queue import Empty, PriorityQueue
from threading import Lock, Thread
from typing import Iterable, Iterator

NULL_ENTRY = (-1, None, None)
_THREADS_QUEUES = weakref.WeakKeyDictionary()
_SHUTDOWN = False
# Lock that ensures that new workers are not created while the interpreter is
# shutting down. Must be held while mutating _THREADS_QUEUES and _SHUTDOWN.
_GLOBAL_SHUTDOWN_LOCK = Lock()


def python_exit():
    global _SHUTDOWN
    with _GLOBAL_SHUTDOWN_LOCK:
        _SHUTDOWN = True
    items = list(_THREADS_QUEUES.items())
    for thread, queue in items:
        queue.put(NULL_ENTRY)
    for thread, queue in items:
        thread.join()


# Starting with 3.9, ThreadPoolExecutor no longer uses daemon threads, and so instead, an internal
# function hook very similar to atexit.register() gets called at 'threading' shutdown instead of
# interpreter shutdown. Check out https://github.com/python/cpython/issues/83993
if sys.version_info[:2] < (3, 9):
    from atexit import register as _register_atexit
else:
    from threading import _register_atexit

_register_atexit(python_exit)


def _worker(executor_reference, work_queue):
    try:
        while True:
            work_item = work_queue.get(block=True)[-1]
            if work_item is not None:
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
    """Thread pool executor with queue.PriorityQueue() as its work queue. Accepts a 'priority' parameter
    that's controls the prioritisation of tasks: lower numbers being run before higher numbers, and
    0 (zero) being the highest possible priority.

    All tasks not given a priority will be given `priority=0` to work seamlessly as a stand-in for the
    regular ThreadPoolExecutor.

    Args:
        max_workers (int | None): Max number of workers threads to spawn."""

    def __init__(self, max_workers: int | None = None) -> None:
        super().__init__(max_workers)
        self._work_queue = PriorityQueue()
        self._task_counter = itertools.count().__next__

    @staticmethod
    def as_completed(futures: Iterable[Future]) -> Iterator[Future]:
        # This is just here to make a serial non-threading version "hot-swappable"
        return as_completed(futures)

    def submit(self, fn, *args, **kwargs):
        if "priority" in inspect.signature(fn).parameters:
            raise TypeError(f"Given function {fn} cannot accept reserved parameter name `priority`")

        with self._shutdown_lock, _GLOBAL_SHUTDOWN_LOCK:
            if self._shutdown:
                raise RuntimeError("Cannot schedule new futures after shutdown")

            if _SHUTDOWN:
                raise RuntimeError("Cannot schedule new futures after interpreter shutdown")

            if (priority := kwargs.pop("priority", 0)) < 0:
                raise ValueError("'priority' has to be a nonnegative number, 0 being the highest possible priority")

            future = Future()
            work_item = _WorkItem(future, fn, args, kwargs)

            # We use a counter to break ties, but keep order:
            self._work_queue.put((priority, self._task_counter(), work_item))
            self._adjust_thread_count()
            return future

    def _adjust_thread_count(self):
        def weak_ref_cb(_, queue=self._work_queue):
            queue.put(NULL_ENTRY)

        if len(self._threads) < self._max_workers:
            thread = Thread(target=_worker, args=(weakref.ref(self, weak_ref_cb), self._work_queue))
            thread.daemon = True
            thread.start()
            self._threads.add(thread)
            _THREADS_QUEUES[thread] = self._work_queue

    def shutdown(self, wait=True):
        with self._shutdown_lock:
            self._shutdown = True

            # Empty the entire work queue: (mod. from py39's added option 'cancel_futures')
            while True:
                try:
                    work_item = self._work_queue.get_nowait()[-1]
                except Empty:
                    break
                if work_item is not None:
                    work_item.future.cancel()

            # Send a wake-up to prevent threads calling _work_queue.get(block=True) from permanently blocking
            self._work_queue.put(NULL_ENTRY)

        if wait:
            for thread in self._threads:
                thread.join()
