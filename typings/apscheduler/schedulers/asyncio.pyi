from collections.abc import Callable
from typing import Any


class AsyncIOScheduler:
    def __init__(self, **kwargs: Any) -> None: ...
    def add_job(
        self,
        func: Callable[..., Any] | str,
        trigger: str,
        *args: Any,
        **kwargs: Any,
    ) -> Any: ...
    def start(self, paused: bool = False) -> None: ...
    def shutdown(self, wait: bool = True) -> None: ...

