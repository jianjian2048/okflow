import pytest

from okflow.context import RunContext
from okflow.exceptions import (
    NodeExecutionError,
    ScopeOutputConflictError,
)
from okflow.executor import DAGExecutor, build_ctx
from okflow.registry import ActionRegistry
from okflow.schema.nodes import ActionNodeDef
from okflow.schema.workflow import EdgeDef, WorkflowDef
from okflow.scope import Scope, ScopeGroup


def _wf(wf_id: str = "wf", nodes=None, edges=None) -> WorkflowDef:
    return WorkflowDef(id=wf_id, name="test", nodes=nodes or [], edges=edges or [])


# ── build_ctx ─────────────────────────────────────────────────────────────────


def test_build_ctx_no_inherit():
    parent = RunContext({"parent_var": "parent"})
    scope = Scope(workflow=_wf(), inputs={"x": 1}, inherit_parent=False)
    ctx = build_ctx(scope, parent)
    assert ctx.get("x") == 1
    with pytest.raises(KeyError):
        ctx.get("parent_var")


def test_build_ctx_inherit():
    parent = RunContext({"parent_var": "parent", "shared": "p"})
    scope = Scope(
        workflow=_wf(),
        inputs={"x": 1, "shared": "override"},
        inherit_parent=True,
    )
    ctx = build_ctx(scope, parent)
    assert ctx.get("parent_var") == "parent"
    assert ctx.get("x") == 1
    assert ctx.get("shared") == "override"


def test_build_ctx_empty_parent():
    parent = RunContext()
    scope = Scope(workflow=_wf(), inputs={})
    ctx = build_ctx(scope, parent)
    assert ctx.snapshot() == {}


# ── single action ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_executor_single_action_node():
    registry = ActionRegistry()

    async def double(x: int) -> int:
        return x * 2

    registry.register("math.double", double)

    node = ActionNodeDef(id="n1", handler="math.double", params={"x": 5}, output_key="result")
    workflow = _wf(nodes=[node])
    scope = Scope(workflow=workflow, inputs={}, outputs=["n1.result"])
    executor = DAGExecutor(registry)

    ctx = await executor.run(scope)
    assert ctx.get("n1.result") == 10


# ── sequential chain ──────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_executor_sequential_nodes():
    registry = ActionRegistry()

    async def fetch() -> list:
        return [1, 2, 3]

    async def count(items: list) -> int:
        return len(items)

    registry.register("data.fetch", fetch)
    registry.register("data.count", count)

    n1 = ActionNodeDef(id="fetch", handler="data.fetch", params={}, output_key="items")
    n2 = ActionNodeDef(
        id="count",
        handler="data.count",
        params={"items": "$fetch.items"},
        output_key="total",
    )
    workflow = _wf(nodes=[n1, n2], edges=[EdgeDef(from_="fetch", to="count")])
    scope = Scope(workflow=workflow, outputs=["count.total"])

    ctx = await DAGExecutor(registry).run(scope)
    assert ctx.get("count.total") == 3


# ── parallel nodes ────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_executor_parallel_nodes():
    registry = ActionRegistry()

    async def task_a() -> str:
        return "A"

    async def task_b() -> str:
        return "B"

    registry.register("task.a", task_a)
    registry.register("task.b", task_b)

    n1 = ActionNodeDef(id="ta", handler="task.a", params={}, output_key="val")
    n2 = ActionNodeDef(id="tb", handler="task.b", params={}, output_key="val")
    workflow = _wf(nodes=[n1, n2])
    scope = Scope(workflow=workflow, outputs=["ta.val", "tb.val"])

    ctx = await DAGExecutor(registry).run(scope)
    assert ctx.get("ta.val") == "A"
    assert ctx.get("tb.val") == "B"


# ── exception wrapping ────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_executor_wraps_node_exception():
    registry = ActionRegistry()

    async def fail() -> None:
        raise ValueError("oops")

    registry.register("fail", fail)
    node = ActionNodeDef(id="bad", handler="fail", params={}, output_key="r")
    workflow = _wf(nodes=[node])
    scope = Scope(workflow=workflow)

    with pytest.raises(NodeExecutionError) as exc_info:
        await DAGExecutor(registry).run(scope)

    assert exc_info.value.node_id == "bad"
    assert isinstance(exc_info.value.cause, ValueError)


# ── parallel output conflict ──────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_executor_parallel_output_conflict_raises():
    registry = ActionRegistry()

    async def produce_value() -> int:
        return 42

    registry.register("produce", produce_value)

    inner_node = ActionNodeDef(id="n", handler="produce", params={}, output_key="key")
    inner_wf = _wf(nodes=[inner_node])

    scope_a = Scope(workflow=inner_wf, outputs=["n.key"])
    scope_b = Scope(workflow=inner_wf, outputs=["n.key"])

    group = ScopeGroup(scopes=[scope_a, scope_b], mode="parallel")

    executor = DAGExecutor(registry)
    parent_ctx = RunContext()

    with pytest.raises(ScopeOutputConflictError) as exc_info:
        await executor._run_scope_group(group, parent_ctx)

    assert exc_info.value.key == "n.key"
