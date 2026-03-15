from __future__ import annotations

import re
from typing import Any


_REF_PATTERN = re.compile(r"^\$([a-zA-Z_][\w.]*)$")
_CONDITION_IS_NULL = re.compile(r"^\$(\S+)\s+is\s+null$")
_CONDITION_IS_NOT_NULL = re.compile(r"^\$(\S+)\s+is\s+not\s+null$")
_CONDITION_OP = re.compile(r"^\$(\S+)\s+(==|!=|<=|>=|<|>)\s+(.+)$")


class RunContext:
    """执行上下文：变量存储、$ref 解析、条件 DSL 求值。"""

    def __init__(self, store: dict[str, Any] | None = None) -> None:
        self._store: dict[str, Any] = dict(store) if store else {}

    def set(self, key: str, value: Any) -> None:
        self._store[key] = value

    def get(self, key: str) -> Any:
        return self._store[key]

    def snapshot(self) -> dict[str, Any]:
        return dict(self._store)

    def resolve(self, value: Any) -> Any:
        if isinstance(value, str):
            m = _REF_PATTERN.match(value)
            if m:
                return self._store[m.group(1)]
            return value
        if isinstance(value, dict):
            return {k: self.resolve(v) for k, v in value.items()}
        if isinstance(value, list):
            return [self.resolve(v) for v in value]
        return value

    def eval_condition(self, expr: str) -> bool:
        expr = expr.strip()

        m = _CONDITION_IS_NULL.match(expr)
        if m:
            return self._store[m.group(1)] is None

        m = _CONDITION_IS_NOT_NULL.match(expr)
        if m:
            return self._store[m.group(1)] is not None

        m = _CONDITION_OP.match(expr)
        if m:
            ref_key, op, raw = m.group(1), m.group(2), m.group(3).strip()
            lhs = self._store[ref_key]
            rhs = _parse_rhs(raw)
            return _apply_op(lhs, op, rhs)

        raise ValueError(f"Unsupported condition expression: {expr!r}")


def _parse_rhs(raw: str) -> Any:
    if raw == "null":
        return None
    if raw == "true":
        return True
    if raw == "false":
        return False
    if (raw.startswith('"') and raw.endswith('"')) or (
        raw.startswith("'") and raw.endswith("'")
    ):
        return raw[1:-1]
    try:
        return int(raw)
    except ValueError:
        pass
    try:
        return float(raw)
    except ValueError:
        pass
    return raw


def _apply_op(lhs: Any, op: str, rhs: Any) -> bool:
    match op:
        case "==":
            return bool(lhs == rhs)
        case "!=":
            return bool(lhs != rhs)
        case "<":
            return bool(lhs < rhs)
        case "<=":
            return bool(lhs <= rhs)
        case ">":
            return bool(lhs > rhs)
        case ">=":
            return bool(lhs >= rhs)
    return False
