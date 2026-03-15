from okflow.context import RunContext
from okflow.nodes.foreach import ForEachNodeExecutor
from okflow.registry import ActionRegistry
from okflow.schema.nodes import ForEachNodeDef
from okflow.schema.workflow import WorkflowDef
from okflow.scope import ScopeGroup


def _empty_wf() -> WorkflowDef:
    return WorkflowDef(id="sub", name="sub", nodes=[], edges=[])


async def test_foreach_returns_pooled_scope_group():
    registry = ActionRegistry()
    items = [1, 2, 3]
    ctx = RunContext({"data.items": items})
    node = ForEachNodeDef(
        id="fe",
        items="$data.items",
        item_var="item",
        collect_key="process.result",
        sub_workflow=_empty_wf(),
    )
    executor = ForEachNodeExecutor()
    result = await executor.execute(node, ctx, registry)

    assert isinstance(result, ScopeGroup)
    assert result.mode == "pooled"
    assert len(result.scopes) == 3
    assert result.collect_key == "fe.collected"


async def test_foreach_scopes_have_item_var():
    registry = ActionRegistry()
    ctx = RunContext({"data.items": ["a", "b"]})
    node = ForEachNodeDef(
        id="fe",
        items="$data.items",
        item_var="current_item",
        collect_key="r",
        sub_workflow=_empty_wf(),
    )
    executor = ForEachNodeExecutor()
    result = await executor.execute(node, ctx, registry)

    assert result is not None
    assert result.scopes[0].inputs == {"current_item": "a"}
    assert result.scopes[1].inputs == {"current_item": "b"}


async def test_foreach_scopes_are_isolated():
    registry = ActionRegistry()
    ctx = RunContext({"data.items": [1]})
    node = ForEachNodeDef(
        id="fe",
        items="$data.items",
        item_var="item",
        collect_key="r",
        sub_workflow=_empty_wf(),
    )
    executor = ForEachNodeExecutor()
    result = await executor.execute(node, ctx, registry)

    assert result is not None
    assert result.scopes[0].inherit_parent is False


async def test_foreach_concurrency():
    registry = ActionRegistry()
    ctx = RunContext({"items": list(range(10))})
    node = ForEachNodeDef(
        id="fe",
        items="$items",
        item_var="x",
        collect_key="r",
        concurrency=3,
        sub_workflow=_empty_wf(),
    )
    executor = ForEachNodeExecutor()
    result = await executor.execute(node, ctx, registry)

    assert result is not None
    assert result.concurrency == 3


async def test_foreach_scope_outputs():
    registry = ActionRegistry()
    ctx = RunContext({"data.items": [1, 2]})
    node = ForEachNodeDef(
        id="fe",
        items="$data.items",
        item_var="item",
        collect_key="my.result",
        sub_workflow=_empty_wf(),
    )
    executor = ForEachNodeExecutor()
    result = await executor.execute(node, ctx, registry)

    assert result is not None
    for scope in result.scopes:
        assert scope.outputs == ["my.result"]
