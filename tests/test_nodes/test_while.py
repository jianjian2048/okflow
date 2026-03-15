import pytest
from okflow.context import RunContext
from okflow.registry import ActionRegistry
from okflow.nodes.while_ import WhileNodeExecutor
from okflow.schema.nodes import WhileNodeDef
from okflow.schema.workflow import WorkflowDef
from okflow.scope import ScopeGroup


def _empty_wf() -> WorkflowDef:
    return WorkflowDef(id="sub", name="sub", nodes=[], edges=[])


async def test_while_returns_sequential_repeat_scope_group():
    registry = ActionRegistry()
    ctx = RunContext({"count": 0})
    node = WhileNodeDef(
        id="wh",
        condition="$count < 5",
        outputs=["count"],
        max_iterations=10,
        sub_workflow=_empty_wf(),
    )
    executor = WhileNodeExecutor()
    result = await executor.execute(node, ctx, registry)

    assert isinstance(result, ScopeGroup)
    assert result.mode == "sequential"
    assert result.repeat is True
    assert result.max_iterations == 10


async def test_while_scope_inherits_parent():
    registry = ActionRegistry()
    ctx = RunContext({"count": 0})
    node = WhileNodeDef(
        id="wh",
        condition="$count < 3",
        outputs=["count"],
        sub_workflow=_empty_wf(),
    )
    executor = WhileNodeExecutor()
    result = await executor.execute(node, ctx, registry)

    assert result is not None
    scope = result.scopes[0]
    assert scope.inherit_parent is True
    assert scope.outputs == ["count"]
    assert scope.inputs == {}


async def test_while_repeat_until_stops_when_condition_false():
    registry = ActionRegistry()
    ctx = RunContext({"count": 10})
    node = WhileNodeDef(
        id="wh",
        condition="$count < 5",
        outputs=["count"],
        sub_workflow=_empty_wf(),
    )
    executor = WhileNodeExecutor()
    result = await executor.execute(node, ctx, registry)

    assert result is not None
    assert result.repeat_until is not None
    assert result.repeat_until(ctx) is True


async def test_while_repeat_until_continues_when_condition_true():
    registry = ActionRegistry()
    ctx = RunContext({"count": 2})
    node = WhileNodeDef(
        id="wh",
        condition="$count < 5",
        outputs=["count"],
        sub_workflow=_empty_wf(),
    )
    executor = WhileNodeExecutor()
    result = await executor.execute(node, ctx, registry)

    assert result is not None
    assert result.repeat_until is not None
    assert result.repeat_until(ctx) is False
