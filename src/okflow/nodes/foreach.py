from __future__ import annotations

from typing import TYPE_CHECKING

from okflow.nodes.base import NodeExecutor
from okflow.schema.nodes import ForEachNodeDef
from okflow.scope import Scope, ScopeGroup

if TYPE_CHECKING:
    from okflow.context import RunContext
    from okflow.registry import ActionRegistry


class ForEachNodeExecutor(NodeExecutor):
    """执行 foreach 节点：为每个 item 创建独立 Scope，返回 pooled ScopeGroup。"""

    async def execute(
        self,
        node: ForEachNodeDef,
        ctx: RunContext,
        registry: ActionRegistry,
    ) -> ScopeGroup | None:
        items = ctx.resolve(node.items)
        scopes = [
            Scope(
                workflow=node.sub_workflow,
                inputs={node.item_var: item},
                inherit_parent=False,
                outputs=[node.collect_key],
            )
            for item in items
        ]
        return ScopeGroup(
            scopes=scopes,
            mode="pooled",
            concurrency=node.concurrency,
            collect_key=f"{node.id}.collected",
        )
