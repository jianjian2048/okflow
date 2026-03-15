import pytest
from okflow.context import RunContext


def test_set_and_get():
    ctx = RunContext()
    ctx.set("node1.result", 42)
    assert ctx.get("node1.result") == 42


def test_get_missing_key_raises():
    ctx = RunContext()
    with pytest.raises(KeyError):
        ctx.get("nonexistent")


def test_snapshot_returns_copy():
    ctx = RunContext({"a": 1})
    snap = ctx.snapshot()
    snap["a"] = 999
    assert ctx.get("a") == 1


def test_init_with_store():
    ctx = RunContext({"x": 10, "y": 20})
    assert ctx.get("x") == 10


def test_resolve_string_ref():
    ctx = RunContext({"fetch.body": [1, 2, 3]})
    assert ctx.resolve("$fetch.body") == [1, 2, 3]


def test_resolve_plain_string():
    ctx = RunContext()
    assert ctx.resolve("hello") == "hello"


def test_resolve_integer():
    ctx = RunContext()
    assert ctx.resolve(5) == 5


def test_resolve_dict():
    ctx = RunContext({"a.v": 10})
    result = ctx.resolve({"key": "$a.v", "static": "x"})
    assert result == {"key": 10, "static": "x"}


def test_resolve_list():
    ctx = RunContext({"a": 1, "b": 2})
    result = ctx.resolve(["$a", "$b", "literal"])
    assert result == [1, 2, "literal"]


def test_resolve_nested():
    ctx = RunContext({"val": 99})
    result = ctx.resolve({"outer": {"inner": "$val"}})
    assert result == {"outer": {"inner": 99}}


def test_resolve_missing_ref_raises():
    ctx = RunContext()
    with pytest.raises(KeyError):
        ctx.resolve("$missing.key")


def test_condition_is_null_true():
    ctx = RunContext({"x": None})
    assert ctx.eval_condition("$x is null") is True


def test_condition_is_null_false():
    ctx = RunContext({"x": 1})
    assert ctx.eval_condition("$x is null") is False


def test_condition_is_not_null():
    ctx = RunContext({"x": "hello"})
    assert ctx.eval_condition("$x is not null") is True


def test_condition_eq_int():
    ctx = RunContext({"count": 5})
    assert ctx.eval_condition("$count == 5") is True
    assert ctx.eval_condition("$count == 6") is False


def test_condition_lt():
    ctx = RunContext({"count": 3})
    assert ctx.eval_condition("$count < 5") is True
    assert ctx.eval_condition("$count < 3") is False


def test_condition_gt():
    ctx = RunContext({"score": 10})
    assert ctx.eval_condition("$score > 5") is True


def test_condition_lte():
    ctx = RunContext({"n": 5})
    assert ctx.eval_condition("$n <= 5") is True
    assert ctx.eval_condition("$n <= 4") is False


def test_condition_gte():
    ctx = RunContext({"n": 5})
    assert ctx.eval_condition("$n >= 5") is True


def test_condition_neq():
    ctx = RunContext({"status": "ok"})
    assert ctx.eval_condition('$status != "fail"') is True


def test_condition_eq_string():
    ctx = RunContext({"status": "done"})
    assert ctx.eval_condition('$status == "done"') is True


def test_condition_eq_bool():
    ctx = RunContext({"flag": True})
    assert ctx.eval_condition("$flag == true") is True


def test_condition_eq_null():
    ctx = RunContext({"val": None})
    assert ctx.eval_condition("$val == null") is True


def test_condition_unsupported_raises():
    ctx = RunContext({"x": 1})
    with pytest.raises(ValueError, match="Unsupported"):
        ctx.eval_condition("x > 1")
