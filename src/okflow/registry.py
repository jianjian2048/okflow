from __future__ import annotations

from typing import Any, Awaitable, Callable

from okflow.exceptions import UnknownHandlerError


class ActionRegistry:
    """引擎与外部系统的唯一边界。注册并调用异步处理函数。"""

    def __init__(self) -> None:
        self._handlers: dict[str, Callable[..., Awaitable[Any]]] = {}

    def register(self, name: str, handler: Callable[..., Awaitable[Any]]) -> None:
        """注册一个具名异步处理函数。重复注册时覆盖旧值。"""
        self._handlers[name] = handler

    async def call(self, handler_name: str, params: dict[str, Any]) -> Any:
        """按名称调用已注册的处理函数，params 展开为关键字参数。"""
        fn = self._handlers.get(handler_name)
        if fn is None:
            raise UnknownHandlerError(handler_name)
        return await fn(**params)
