from __future__ import annotations

from typing import TYPE_CHECKING

from okflow.nodes.base import NodeExecutor
from okflow.schema.nodes import WhileNodeDef
from okflow.scope import Scope, ScopeGroup

if TYPE_CHECKING:
    from okflow.context import RunContext
    from okflow.registry import ActionRegistry


class WhileNodeExecutor(NodeExecutor):
    """执行 while 节点：返回 sequential+repeat ScopeGroup，引擎负责循环调度。"""

    async def execute(
        self,
        node: WhileNodeDef,
        ctx: RunContext,
        registry: ActionRegistry,
    ) -> ScopeGroup | None:
        scope = Scope(
            workflow=node.sub_workflow,
            inputs={},
            inherit_parent=True,
            outputs=node.outputs,
        )
        condition = node.condition

        return ScopeGroup(
            scopes=[scope],
            mode="sequential",
            repeat=True,
            repeat_until=lambda c: not c.eval_condition(condition),
            max_iterations=node.max_iterations,
        )
