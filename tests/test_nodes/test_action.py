import pytest
from okflow.context import RunContext
from okflow.registry import ActionRegistry
from okflow.nodes.action import ActionNodeExecutor
from okflow.schema.nodes import ActionNodeDef


@pytest.fixture
def registry():
    r = ActionRegistry()

    async def add(a: int, b: int) -> int:
        return a + b

    r.register("math.add", add)
    return r


async def test_action_executor_writes_output(registry):
    node = ActionNodeDef(id="sum_node", handler="math.add", params={"a": 3, "b": 4}, output_key="result")
    ctx = RunContext()
    executor = ActionNodeExecutor()

    result = await executor.execute(node, ctx, registry)

    assert result is None  # action 节点不返回 ScopeGroup
    assert ctx.get("sum_node.result") == 7


async def test_action_executor_resolves_refs(registry):
    ctx = RunContext({"input.a": 10, "input.b": 5})
    node = ActionNodeDef(
        id="sum_node",
        handler="math.add",
        params={"a": "$input.a", "b": "$input.b"},
        output_key="result",
    )
    executor = ActionNodeExecutor()

    await executor.execute(node, ctx, registry)

    assert ctx.get("sum_node.result") == 15


async def test_action_executor_static_params():
    registry = ActionRegistry()

    async def greet(name: str) -> str:
        return f"Hello, {name}!"

    registry.register("str.greet", greet)
    node = ActionNodeDef(id="g", handler="str.greet", params={"name": "World"}, output_key="msg")
    ctx = RunContext()
    executor = ActionNodeExecutor()

    await executor.execute(node, ctx, registry)
    assert ctx.get("g.msg") == "Hello, World!"
