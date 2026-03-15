from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Literal

if TYPE_CHECKING:
    from okflow.context import RunContext
    from okflow.schema.workflow import WorkflowDef


@dataclass
class Scope:
    """带显式 I/O 边界的执行单元。"""

    workflow: WorkflowDef
    inputs: dict[str, Any] = field(default_factory=dict)
    inherit_parent: bool = False
    outputs: list[str] = field(default_factory=list)


@dataclass
class ScopeGroup:
    """一批 Scope 加上执行模式，由节点的 execute() 动态创建。"""

    scopes: list[Scope]
    mode: Literal["exclusive", "parallel", "sequential", "pooled"]

    # pooled 模式专用
    concurrency: int | None = None
    collect_key: str | None = None

    # sequential + repeat 专用（while 节点使用）
    repeat: bool = False
    repeat_until: Callable[[RunContext], bool] | None = None
    max_iterations: int | None = None
