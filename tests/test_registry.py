import pytest
from okflow.registry import ActionRegistry
from okflow.exceptions import UnknownHandlerError


async def test_register_and_call():
    registry = ActionRegistry()

    async def double(x: int) -> int:
        return x * 2

    registry.register("math.double", double)
    result = await registry.call("math.double", {"x": 5})
    assert result == 10


async def test_unknown_handler_raises():
    registry = ActionRegistry()
    with pytest.raises(UnknownHandlerError) as exc_info:
        await registry.call("missing.handler", {})
    assert exc_info.value.handler_name == "missing.handler"


async def test_register_overwrites():
    registry = ActionRegistry()

    async def v1() -> str:
        return "v1"

    async def v2() -> str:
        return "v2"

    registry.register("fn", v1)
    registry.register("fn", v2)
    result = await registry.call("fn", {})
    assert result == "v2"


async def test_call_passes_kwargs():
    registry = ActionRegistry()

    async def concat(a: str, b: str) -> str:
        return a + b

    registry.register("str.concat", concat)
    result = await registry.call("str.concat", {"a": "hello", "b": " world"})
    assert result == "hello world"
