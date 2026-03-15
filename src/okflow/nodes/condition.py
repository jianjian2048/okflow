from __future__ import annotations

from typing import TYPE_CHECKING

from okflow.nodes.base import NodeExecutor
from okflow.schema.nodes import ConditionNodeDef
from okflow.scope import Scope, ScopeGroup

if TYPE_CHECKING:
    from okflow.context import RunContext
    from okflow.registry import ActionRegistry


class ConditionNodeExecutor(NodeExecutor):
    """执行 condition 节点：调用 handler 决定分支，返回 exclusive ScopeGroup。"""

    async def execute(
        self,
        node: ConditionNodeDef,
        ctx: RunContext,
        registry: ActionRegistry,
    ) -> ScopeGroup | None:
        resolved = ctx.resolve(node.params)
        branch_key = str(await registry.call(node.handler, resolved))
        chosen_workflow = node.branches[branch_key]

        scope = Scope(
            workflow=chosen_workflow,
            inputs={},
            inherit_parent=True,
            outputs=node.outputs,
        )
        return ScopeGroup(scopes=[scope], mode="exclusive")
