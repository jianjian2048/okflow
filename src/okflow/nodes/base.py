from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from okflow.context import RunContext
    from okflow.registry import ActionRegistry
    from okflow.scope import ScopeGroup


class NodeExecutor(ABC):
    """节点执行器抽象基类。"""

    @abstractmethod
    async def execute(
        self,
        node: Any,
        ctx: RunContext,
        registry: ActionRegistry,
    ) -> ScopeGroup | None:
        """
        执行节点。
        返回 None：节点直接执行完毕，无子 Scope。
        返回 ScopeGroup：引擎按模式调度子 Scope。
        """
        ...
