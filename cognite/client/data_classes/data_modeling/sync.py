from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import asyncio
    from datetime import datetime


@dataclass
class SubscriptionContext:
    """Manages an active subscription to an instance sync query."""

    last_successful_sync: datetime | None = None
    last_successful_callback: datetime | None = None
    _task: asyncio.Task | None = None

    def cancel(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()

    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    def is_alive(self) -> bool:
        return self.is_running()
