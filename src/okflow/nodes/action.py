from __future__ import annotations

from typing import TYPE_CHECKING

from okflow.nodes.base import NodeExecutor
from okflow.schema.nodes import ActionNodeDef

if TYPE_CHECKING:
    from okflow.context import RunContext
    from okflow.registry import ActionRegistry
    from okflow.scope import ScopeGroup


class ActionNodeExecutor(NodeExecutor):
    """执行 action 节点：调用 handler，将结果写入 ctx。"""

    async def execute(
        self,
        node: ActionNodeDef,
        ctx: RunContext,
        registry: ActionRegistry,
    ) -> ScopeGroup | None:
        resolved = ctx.resolve(node.params)
        result = await registry.call(node.handler, resolved)
        ctx.set(f"{node.id}.{node.output_key}", result)
        return None
