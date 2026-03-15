"""
端到端集成测试：覆盖四种节点类型和多种组合场景。
测试通过 okflow 公开 API 驱动，不依赖内部实现细节。
"""

import pytest

from okflow import (
    ActionNodeDef,
    ActionRegistry,
    ConditionNodeDef,
    DAGExecutor,
    EdgeDef,
    ForEachNodeDef,
    MaxIterationsExceeded,
    Scope,
    WhileNodeDef,
    WorkflowDef,
    validate_workflow,
)


def _wf(wf_id: str, nodes=None, edges=None) -> WorkflowDef:
    return WorkflowDef(id=wf_id, name="t", nodes=nodes or [], edges=edges or [])


# ── 场景 1：action 节点链 ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_integration_action_chain():
    """fetch → transform → summarize 三节点链。"""
    registry = ActionRegistry()

    async def fetch(source: str) -> list[int]:
        return [1, 2, 3, 4, 5]

    async def transform(items: list[int]) -> list[int]:
        return [x * 2 for x in items]

    async def summarize(items: list[int]) -> int:
        return sum(items)

    registry.register("data.fetch", fetch)
    registry.register("data.transform", transform)
    registry.register("data.summarize", summarize)

    n_fetch = ActionNodeDef(id="fetch", handler="data.fetch", params={"source": "db"}, output_key="items")
    n_transform = ActionNodeDef(
        id="transform", handler="data.transform", params={"items": "$fetch.items"}, output_key="items"
    )
    n_sum = ActionNodeDef(id="sum", handler="data.summarize", params={"items": "$transform.items"}, output_key="total")

    workflow = _wf(
        "main",
        nodes=[n_fetch, n_transform, n_sum],
        edges=[EdgeDef(from_="fetch", to="transform"), EdgeDef(from_="transform", to="sum")],
    )
    validate_workflow(workflow)
    scope = Scope(workflow=workflow, outputs=["sum.total"])
    ctx = await DAGExecutor(registry).run(scope)

    assert ctx.get("sum.total") == 30  # (1+2+3+4+5)*2 = 30


# ── 场景 2：condition 节点 ───────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_integration_condition_true_branch():
    registry = ActionRegistry()

    async def check(value: int) -> str:
        return "big" if value > 10 else "small"

    async def handle_big(x: int) -> str:
        return f"big:{x}"

    async def handle_small(x: int) -> str:
        return f"small:{x}"

    registry.register("check", check)
    registry.register("handle.big", handle_big)
    registry.register("handle.small", handle_small)

    big_branch = _wf(
        "big",
        nodes=[ActionNodeDef(id="hb", handler="handle.big", params={"x": "$input.value"}, output_key="result")],
    )
    small_branch = _wf(
        "small",
        nodes=[ActionNodeDef(id="hs", handler="handle.small", params={"x": "$input.value"}, output_key="result")],
    )

    cond = ConditionNodeDef(
        id="route",
        handler="check",
        params={"value": "$input.value"},
        branches={"big": big_branch, "small": small_branch},
        outputs=["hb.result", "hs.result"],
    )
    workflow = _wf("main", nodes=[cond])
    validate_workflow(workflow)

    scope = Scope(workflow=workflow, inputs={"input.value": 20}, outputs=["hb.result"])
    ctx = await DAGExecutor(registry).run(scope)
    assert ctx.get("hb.result") == "big:20"


@pytest.mark.asyncio
async def test_integration_condition_false_branch():
    registry = ActionRegistry()

    async def check(value: int) -> str:
        return "big" if value > 10 else "small"

    async def handle_small(x: int) -> str:
        return f"small:{x}"

    registry.register("check", check)
    registry.register("handle.small", handle_small)

    small_branch = _wf(
        "small",
        nodes=[ActionNodeDef(id="hs", handler="handle.small", params={"x": "$input.value"}, output_key="result")],
    )
    big_branch = _wf("big")

    cond = ConditionNodeDef(
        id="route",
        handler="check",
        params={"value": "$input.value"},
        branches={"big": big_branch, "small": small_branch},
        outputs=["hs.result"],
    )
    workflow = _wf("main", nodes=[cond])
    scope = Scope(workflow=workflow, inputs={"input.value": 5}, outputs=["hs.result"])
    ctx = await DAGExecutor(registry).run(scope)
    assert ctx.get("hs.result") == "small:5"


# ── 场景 3：foreach 节点（并发 + 结果收集）─────────────────────────────────


@pytest.mark.asyncio
async def test_integration_foreach_collects_results():
    registry = ActionRegistry()

    async def process(item: int) -> int:
        return item * item

    registry.register("math.square", process)

    sub_wf = _wf(
        "sub",
        nodes=[ActionNodeDef(id="sq", handler="math.square", params={"item": "$item"}, output_key="value")],
    )
    fe_node = ForEachNodeDef(
        id="foreach",
        items="$data.items",
        item_var="item",
        collect_key="sq.value",
        sub_workflow=sub_wf,
    )
    workflow = _wf("main", nodes=[fe_node])
    validate_workflow(workflow)

    scope = Scope(workflow=workflow, inputs={"data.items": [1, 2, 3, 4]}, outputs=["foreach.collected"])
    ctx = await DAGExecutor(registry).run(scope)
    assert ctx.get("foreach.collected") == [1, 4, 9, 16]


@pytest.mark.asyncio
async def test_integration_foreach_with_concurrency_limit():
    """并发上限不影响结果正确性。"""
    registry = ActionRegistry()

    async def double(item: int) -> int:
        return item * 2

    registry.register("math.double", double)

    sub_wf = _wf(
        "sub",
        nodes=[ActionNodeDef(id="d", handler="math.double", params={"item": "$x"}, output_key="val")],
    )
    fe_node = ForEachNodeDef(
        id="fe",
        items="$items",
        item_var="x",
        collect_key="d.val",
        concurrency=2,
        sub_workflow=sub_wf,
    )
    workflow = _wf("main", nodes=[fe_node])
    scope = Scope(workflow=workflow, inputs={"items": [10, 20, 30]}, outputs=["fe.collected"])
    ctx = await DAGExecutor(registry).run(scope)
    assert ctx.get("fe.collected") == [20, 40, 60]


# ── 场景 4：while 节点 ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_integration_while_correct():
    """while 循环：利用节点输出键作为条件变量。"""
    registry = ActionRegistry()

    async def increment(n: int) -> int:
        return n + 1

    registry.register("math.increment", increment)

    sub_wf = _wf(
        "sub",
        nodes=[
            ActionNodeDef(
                id="inc",
                handler="math.increment",
                params={"n": "$inc.result"},
                output_key="result",
            )
        ],
    )
    while_node = WhileNodeDef(
        id="loop",
        condition="$inc.result < 3",
        outputs=["inc.result"],
        max_iterations=10,
        sub_workflow=sub_wf,
    )
    workflow = _wf("main", nodes=[while_node])

    scope = Scope(
        workflow=workflow,
        inputs={"inc.result": 0},
        outputs=["inc.result"],
    )
    ctx = await DAGExecutor(registry).run(scope)
    assert ctx.get("inc.result") == 3


@pytest.mark.asyncio
async def test_integration_while_max_iterations_exceeded():
    """while 超出最大迭代次数时抛出 MaxIterationsExceeded。"""
    registry = ActionRegistry()

    async def noop(x: int) -> int:
        return x

    registry.register("noop", noop)

    sub_wf = _wf(
        "sub",
        nodes=[ActionNodeDef(id="n", handler="noop", params={"x": "$counter"}, output_key="counter")],
    )
    while_node = WhileNodeDef(
        id="loop",
        condition="$counter < 100",
        outputs=["n.counter"],
        max_iterations=3,
        sub_workflow=sub_wf,
    )
    workflow = _wf("main", nodes=[while_node])

    scope = Scope(
        workflow=workflow,
        inputs={"counter": 0, "n.counter": 0},
        outputs=["n.counter"],
    )
    with pytest.raises(MaxIterationsExceeded) as exc_info:
        await DAGExecutor(registry).run(scope)

    assert exc_info.value.node_id == "loop"
    assert exc_info.value.max_iterations == 3


# ── 场景 5：混合工作流（action + condition + foreach）────────────────────────


@pytest.mark.asyncio
async def test_integration_mixed_workflow():
    """fetch → condition → foreach（真分支中处理列表）。"""
    registry = ActionRegistry()

    async def fetch(source: str) -> list[int]:
        return [1, 2, 3]

    async def check_nonempty(items: list) -> str:
        return "nonempty" if items else "empty"

    async def process(item: int) -> int:
        return item * 10

    registry.register("data.fetch", fetch)
    registry.register("check.nonempty", check_nonempty)
    registry.register("data.process", process)

    sub_wf = _wf(
        "sub",
        nodes=[ActionNodeDef(id="proc", handler="data.process", params={"item": "$item"}, output_key="value")],
    )
    foreach_node = ForEachNodeDef(
        id="fe",
        items="$fetch.items",
        item_var="item",
        collect_key="proc.value",
        sub_workflow=sub_wf,
    )
    nonempty_branch = _wf("nonempty", nodes=[foreach_node])
    empty_branch = _wf("empty")

    cond = ConditionNodeDef(
        id="check",
        handler="check.nonempty",
        params={"items": "$fetch.items"},
        branches={"nonempty": nonempty_branch, "empty": empty_branch},
        outputs=["fe.collected"],
    )
    fetch_node = ActionNodeDef(id="fetch", handler="data.fetch", params={"source": "db"}, output_key="items")

    workflow = _wf(
        "main",
        nodes=[fetch_node, cond],
        edges=[EdgeDef(from_="fetch", to="check")],
    )
    validate_workflow(workflow)

    scope = Scope(workflow=workflow, outputs=["fe.collected"])
    ctx = await DAGExecutor(registry).run(scope)
    assert ctx.get("fe.collected") == [10, 20, 30]
