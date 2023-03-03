import inspect
import itertools
import sys
import weakref
from concurrent.futures import Future, as_completed
from concurrent.futures.thread import ThreadPoolExecutor, _base, _WorkItem
from queue import Empty, PriorityQueue
from threading import Lock, Thread

NULL_ENTRY = ((-1), None, None)
_THREADS_QUEUES = weakref.WeakKeyDictionary()
_SHUTDOWN = False
_GLOBAL_SHUTDOWN_LOCK = Lock()


def python_exit():
    global _SHUTDOWN
    with _GLOBAL_SHUTDOWN_LOCK:
        _SHUTDOWN = True
    items = list(_THREADS_QUEUES.items())
    for (thread, queue) in items:
        queue.put(NULL_ENTRY)
    for (thread, queue) in items:
        thread.join()


if sys.version_info[:2] < (3, 9):
    from atexit import register as _register_atexit
else:
    from threading import _register_atexit
_register_atexit(python_exit)


def _worker(executor_reference, work_queue):
    try:
        while True:
            work_item = work_queue.get(block=True)[(-1)]
            if work_item is not None:
                work_item.run()
                del work_item
                continue
            executor = executor_reference()
            if _SHUTDOWN or (executor is None) or executor._shutdown:
                work_queue.put(NULL_ENTRY)
                return None
            del executor
    except BaseException:
        _base.LOGGER.critical("Exception in worker", exc_info=True)


class PriorityThreadPoolExecutor(ThreadPoolExecutor):
    def __init__(self, max_workers=None):
        super().__init__(max_workers)
        self._work_queue = PriorityQueue()
        self._task_counter = itertools.count().__next__

    @staticmethod
    def as_completed(futures):
        return as_completed(futures)

    def submit(self, fn, *args, **kwargs):
        if "priority" in inspect.signature(fn).parameters:
            raise TypeError(f"Given function {fn} cannot accept reserved parameter name `priority`")
        with self._shutdown_lock, _GLOBAL_SHUTDOWN_LOCK:
            if self._shutdown:
                raise RuntimeError("Cannot schedule new futures after shutdown")
            if _SHUTDOWN:
                raise RuntimeError("Cannot schedule new futures after interpreter shutdown")
            priority = kwargs.pop("priority", 0)
            if priority < 0:
                raise ValueError("'priority' has to be a nonnegative number, 0 being the highest possible priority")
            future = Future()
            work_item = _WorkItem(future, fn, args, kwargs)
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
            while True:
                try:
                    work_item = self._work_queue.get_nowait()[(-1)]
                except Empty:
                    break
                if work_item is not None:
                    work_item.future.cancel()
            self._work_queue.put(NULL_ENTRY)
        if wait:
            for thread in self._threads:
                thread.join()
