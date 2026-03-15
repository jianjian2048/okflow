import pytest
from okflow.context import RunContext
from okflow.registry import ActionRegistry
from okflow.nodes.condition import ConditionNodeExecutor
from okflow.schema.nodes import ConditionNodeDef
from okflow.schema.workflow import WorkflowDef
from okflow.scope import ScopeGroup


def _empty_wf(wf_id: str) -> WorkflowDef:
    return WorkflowDef(id=wf_id, name="test", nodes=[], edges=[])


@pytest.fixture
def registry():
    r = ActionRegistry()

    async def check_flag(flag: bool) -> str:
        return "true" if flag else "false"

    r.register("check.flag", check_flag)
    return r


async def test_condition_returns_scope_group(registry):
    node = ConditionNodeDef(
        id="cond",
        handler="check.flag",
        params={"flag": True},
        branches={"true": _empty_wf("true_branch"), "false": _empty_wf("false_branch")},
        outputs=["result"],
    )
    ctx = RunContext()
    executor = ConditionNodeExecutor()

    result = await executor.execute(node, ctx, registry)

    assert isinstance(result, ScopeGroup)
    assert result.mode == "exclusive"
    assert len(result.scopes) == 1


async def test_condition_selects_correct_branch(registry):
    true_wf = _empty_wf("true")
    false_wf = _empty_wf("false")
    node = ConditionNodeDef(
        id="cond",
        handler="check.flag",
        params={"flag": False},
        branches={"true": true_wf, "false": false_wf},
        outputs=[],
    )
    ctx = RunContext()
    executor = ConditionNodeExecutor()

    result = await executor.execute(node, ctx, registry)

    assert result is not None
    selected_scope = result.scopes[0]
    assert selected_scope.workflow is false_wf


async def test_condition_scope_inherits_parent(registry):
    node = ConditionNodeDef(
        id="cond",
        handler="check.flag",
        params={"flag": True},
        branches={"true": _empty_wf("t")},
        outputs=["x.v"],
    )
    ctx = RunContext()
    executor = ConditionNodeExecutor()

    result = await executor.execute(node, ctx, registry)

    assert result is not None
    scope = result.scopes[0]
    assert scope.inherit_parent is True
    assert scope.outputs == ["x.v"]
    assert scope.inputs == {}


async def test_condition_resolves_params():
    registry = ActionRegistry()

    async def route(value: str) -> str:
        return value

    registry.register("route", route)
    ctx = RunContext({"status.code": "success"})
    node = ConditionNodeDef(
        id="c",
        handler="route",
        params={"value": "$status.code"},
        branches={"success": _empty_wf("s"), "failure": _empty_wf("f")},
        outputs=[],
    )
    executor = ConditionNodeExecutor()
    result = await executor.execute(node, ctx, registry)
    assert result is not None
    assert result.scopes[0].workflow.id == "s"
