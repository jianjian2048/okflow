# Scope/ScopeGroup 实现计划

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 从零实现 `okflow` DAG 引擎的完整 Scope/ScopeGroup 架构，使设计文档 `document/dag-engine-design.md` 中描述的所有功能可正常运行。

**Architecture:** 三层设计——Schema 层（Pydantic 模型）、Engine 层（DAGExecutor + RunContext + Scope/ScopeGroup + NodeExecutors）、扩展层（ActionRegistry）。NodeExecutors 返回 `ScopeGroup | None`，使任意节点类型都能表达控制流，无需修改调度器。

**Tech Stack:** Python 3.12+, Pydantic v2, asyncio, uv（包管理），pytest + pytest-asyncio（测试），ruff（格式化/lint），mypy（类型检查）

---

## File Map

**新建文件：**
- `src/okflow/exceptions.py` — 全部异常类（6 个）
- `src/okflow/schema/__init__.py` — 重导出
- `src/okflow/schema/workflow.py` — `WorkflowDef`, `EdgeDef`
- `src/okflow/schema/nodes.py` — `NodeDef` 判别联合（4 种子类型）
- `src/okflow/schema/validators.py` — `validate_workflow()`
- `src/okflow/scope.py` — `Scope`, `ScopeGroup` 数据类
- `src/okflow/context.py` — `RunContext`
- `src/okflow/registry.py` — `ActionRegistry`
- `src/okflow/nodes/__init__.py` — `_EXECUTORS` 分发表
- `src/okflow/nodes/base.py` — `NodeExecutor` 抽象基类
- `src/okflow/nodes/action.py` — `ActionNodeExecutor`
- `src/okflow/nodes/condition.py` — `ConditionNodeExecutor`
- `src/okflow/nodes/foreach.py` — `ForEachNodeExecutor`
- `src/okflow/nodes/while_.py` — `WhileNodeExecutor`
- `src/okflow/executor.py` — `DAGExecutor`, `build_ctx`
- `tests/__init__.py`
- `tests/conftest.py`
- `tests/test_exceptions.py`
- `tests/test_schema.py`
- `tests/test_scope.py`
- `tests/test_context.py`
- `tests/test_registry.py`
- `tests/test_nodes/__init__.py`
- `tests/test_nodes/test_action.py`
- `tests/test_nodes/test_condition.py`
- `tests/test_nodes/test_foreach.py`
- `tests/test_nodes/test_while.py`
- `tests/test_executor.py`
- `tests/test_validators.py`
- `tests/test_integration.py`

**修改文件：**
- `pyproject.toml` — 添加 pydantic 依赖、dev 工具、pytest 配置
- `src/okflow/__init__.py` — 公开 API 导出

---

## Chunk 1: 项目配置 + 异常 + Schema 层

### Task 1: 更新 pyproject.toml

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1: 添加依赖和工具配置**

修改 `pyproject.toml` 为以下内容：

```toml
[project]
name = "okflow"
version = "0.1.0"
description = "通用异步 DAG 工作流执行引擎"
readme = "README.md"
authors = [
    { name = "jian jian", email = "jianjian2048@gmail.com" }
]
requires-python = ">=3.12"
dependencies = [
    "pydantic>=2.0",
]

[build-system]
requires = ["uv_build>=0.9.10,<0.10.0"]
build-backend = "uv_build"

[dependency-groups]
dev = [
    "pytest>=8.0",
    "pytest-asyncio>=0.24",
    "mypy>=1.0",
    "ruff>=0.4",
]

[tool.pytest.ini_options]
asyncio_mode = "auto"

[tool.ruff.lint]
select = ["E", "F", "I"]

[tool.ruff.lint.isort]
known-first-party = ["okflow"]

[tool.mypy]
python_version = "3.12"
strict = true
```

- [ ] **Step 2: 安装依赖**

```bash
uv sync
```

Expected: 无报错，pydantic 和 dev 工具安装成功。

- [ ] **Step 3: 提交**

```bash
git add pyproject.toml uv.lock
git commit -m "chore: add pydantic dependency and dev tools"
```

---

### Task 2: 异常层次

**Files:**
- Create: `tests/test_exceptions.py`
- Create: `src/okflow/exceptions.py`

- [ ] **Step 1: 写失败测试**

创建 `tests/test_exceptions.py`：

```python
import pytest
from okflow.exceptions import (
    DAGEngineError,
    WorkflowValidationError,
    NodeExecutionError,
    MaxIterationsExceeded,
    UnknownHandlerError,
    ScopeOutputConflictError,
    ScopeConfigError,
)


def test_all_exceptions_inherit_dag_engine_error():
    assert issubclass(WorkflowValidationError, DAGEngineError)
    assert issubclass(NodeExecutionError, DAGEngineError)
    assert issubclass(MaxIterationsExceeded, DAGEngineError)
    assert issubclass(UnknownHandlerError, DAGEngineError)
    assert issubclass(ScopeOutputConflictError, DAGEngineError)
    assert issubclass(ScopeConfigError, DAGEngineError)


def test_node_execution_error_attributes():
    cause = ValueError("inner")
    err = NodeExecutionError("node_1", cause)
    assert err.node_id == "node_1"
    assert err.cause is cause
    assert "node_1" in str(err)


def test_max_iterations_exceeded_attributes():
    err = MaxIterationsExceeded("while_node", 100)
    assert err.node_id == "while_node"
    assert err.max_iterations == 100
    assert "while_node" in str(err)


def test_unknown_handler_error_attributes():
    err = UnknownHandlerError("my.handler")
    assert err.handler_name == "my.handler"
    assert "my.handler" in str(err)


def test_scope_output_conflict_error_attributes():
    err = ScopeOutputConflictError("result.value")
    assert err.key == "result.value"
    assert "result.value" in str(err)


def test_scope_config_error_attributes():
    err = ScopeConfigError("pooled scope must declare exactly one output")
    assert err.reason == "pooled scope must declare exactly one output"
```

- [ ] **Step 2: 确认测试失败**

```bash
uv run pytest tests/test_exceptions.py -v
```

Expected: ImportError（模块不存在）

- [ ] **Step 3: 实现异常类**

创建 `src/okflow/exceptions.py`：

```python
class DAGEngineError(Exception):
    """所有引擎异常的基类。"""


class WorkflowValidationError(DAGEngineError):
    """工作流静态校验失败。"""


class NodeExecutionError(DAGEngineError):
    """节点业务异常包装器。"""

    def __init__(self, node_id: str, cause: Exception) -> None:
        self.node_id = node_id
        self.cause = cause
        super().__init__(f"Node {node_id!r} failed: {cause}")


class MaxIterationsExceeded(DAGEngineError):
    """while 节点超出最大迭代次数。"""

    def __init__(self, node_id: str, max_iterations: int) -> None:
        self.node_id = node_id
        self.max_iterations = max_iterations
        super().__init__(
            f"Node {node_id!r} exceeded max iterations ({max_iterations})"
        )


class UnknownHandlerError(DAGEngineError):
    """ActionRegistry 中找不到指定 handler。"""

    def __init__(self, handler_name: str) -> None:
        self.handler_name = handler_name
        super().__init__(f"Unknown handler: {handler_name!r}")


class ScopeOutputConflictError(DAGEngineError):
    """parallel 模式下多个 Scope 写入同一 output 键。"""

    def __init__(self, key: str) -> None:
        self.key = key
        super().__init__(f"Output key conflict in parallel scopes: {key!r}")


class ScopeConfigError(DAGEngineError):
    """Scope/ScopeGroup 配置错误（如 pooled 模式声明了多个 outputs）。"""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)
```

- [ ] **Step 4: 确认测试通过**

```bash
uv run pytest tests/test_exceptions.py -v
```

Expected: 全部 PASS

- [ ] **Step 5: 提交**

```bash
git add src/okflow/exceptions.py tests/test_exceptions.py
git commit -m "feat: add exception hierarchy"
```

---

### Task 3: Schema — WorkflowDef 和 EdgeDef

**Files:**
- Create: `tests/test_schema.py`
- Create: `src/okflow/schema/__init__.py`
- Create: `src/okflow/schema/workflow.py`
- Create: `src/okflow/schema/nodes.py`

- [ ] **Step 1: 写失败测试（WorkflowDef + EdgeDef）**

创建 `tests/test_schema.py`：

```python
import pytest
from pydantic import ValidationError
from okflow.schema.workflow import WorkflowDef, EdgeDef
from okflow.schema.nodes import (
    ActionNodeDef,
    ConditionNodeDef,
    ForEachNodeDef,
    WhileNodeDef,
    NodeDef,
)


# ── EdgeDef ──────────────────────────────────────────────────────────────────

def test_edge_def_from_alias():
    """EdgeDef 使用 "from" 作为 JSON 字段名。"""
    edge = EdgeDef.model_validate({"from": "a", "to": "b"})
    assert edge.from_ == "a"
    assert edge.to == "b"


def test_edge_def_python_name():
    """EdgeDef 也接受 Python 字段名 from_。"""
    edge = EdgeDef(from_="a", to="b")
    assert edge.from_ == "a"


# ── ActionNodeDef ─────────────────────────────────────────────────────────────

def test_action_node_def_basic():
    node = ActionNodeDef(id="n1", handler="data.fetch", output_key="body")
    assert node.type == "action"
    assert node.id == "n1"
    assert node.params == {}


def test_action_node_def_with_params():
    node = ActionNodeDef(
        id="n1", handler="data.fetch", params={"url": "$src.url"}, output_key="body"
    )
    assert node.params == {"url": "$src.url"}


# ── ConditionNodeDef ──────────────────────────────────────────────────────────

def test_condition_node_def():
    branch_wf = WorkflowDef(id="branch", name="b", nodes=[], edges=[])
    node = ConditionNodeDef(
        id="cond",
        handler="check.status",
        branches={"true": branch_wf, "false": branch_wf},
        outputs=["result.value"],
    )
    assert node.type == "condition"
    assert "true" in node.branches
    assert node.outputs == ["result.value"]


# ── ForEachNodeDef ────────────────────────────────────────────────────────────

def test_foreach_node_def():
    sub_wf = WorkflowDef(id="sub", name="s", nodes=[], edges=[])
    node = ForEachNodeDef(
        id="fe",
        items="$fetch.items",
        item_var="item",
        collect_key="process.result",
        sub_workflow=sub_wf,
    )
    assert node.type == "foreach"
    assert node.concurrency is None


def test_foreach_node_def_with_concurrency():
    sub_wf = WorkflowDef(id="sub", name="s", nodes=[], edges=[])
    node = ForEachNodeDef(
        id="fe",
        items="$fetch.items",
        item_var="item",
        collect_key="process.result",
        concurrency=4,
        sub_workflow=sub_wf,
    )
    assert node.concurrency == 4


# ── WhileNodeDef ──────────────────────────────────────────────────────────────

def test_while_node_def():
    sub_wf = WorkflowDef(id="sub", name="s", nodes=[], edges=[])
    node = WhileNodeDef(
        id="wh",
        condition="$count < 10",
        outputs=["count"],
        sub_workflow=sub_wf,
    )
    assert node.type == "while"
    assert node.max_iterations == 100  # default
    assert node.outputs == ["count"]


# ── NodeDef 判别联合 ────────────────────────────────────────────────────────────

def test_node_def_discriminator_action():
    """WorkflowDef.nodes 可以通过判别联合解析 action 节点。"""
    data = {
        "id": "wf1",
        "name": "test",
        "nodes": [
            {"type": "action", "id": "n1", "handler": "h", "output_key": "out"}
        ],
        "edges": [],
    }
    wf = WorkflowDef.model_validate(data)
    assert isinstance(wf.nodes[0], ActionNodeDef)


def test_node_def_discriminator_foreach():
    sub_data = {"id": "sub", "name": "s", "nodes": [], "edges": []}
    data = {
        "id": "wf1",
        "name": "test",
        "nodes": [
            {
                "type": "foreach",
                "id": "fe",
                "items": "$x.list",
                "item_var": "item",
                "collect_key": "r",
                "sub_workflow": sub_data,
            }
        ],
        "edges": [],
    }
    wf = WorkflowDef.model_validate(data)
    assert isinstance(wf.nodes[0], ForEachNodeDef)


def test_node_def_unknown_type_raises():
    data = {
        "id": "wf1",
        "name": "test",
        "nodes": [{"type": "unknown", "id": "n1"}],
        "edges": [],
    }
    with pytest.raises(ValidationError):
        WorkflowDef.model_validate(data)
```

- [ ] **Step 2: 确认测试失败**

```bash
uv run pytest tests/test_schema.py -v
```

Expected: ImportError（模块不存在）

- [ ] **Step 3: 实现 workflow.py**

创建 `src/okflow/schema/workflow.py`：

```python
from __future__ import annotations

from typing import TYPE_CHECKING, Annotated, Union

from pydantic import BaseModel, ConfigDict, Field

if TYPE_CHECKING:
    pass


class EdgeDef(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    from_: str = Field(alias="from")
    to: str


# WorkflowDef 引用 NodeDef，后者又引用 WorkflowDef，是递归结构。
# 用 TYPE_CHECKING + model_rebuild() 解决前向引用。
class WorkflowDef(BaseModel):
    id: str
    name: str
    nodes: list[Annotated[Union[
        "ActionNodeDef",
        "ConditionNodeDef",
        "ForEachNodeDef",
        "WhileNodeDef",
    ], Field(discriminator="type")]]
    edges: list[EdgeDef]


# 导入放在模块末尾以避免循环引用
from okflow.schema.nodes import (  # noqa: E402
    ActionNodeDef,
    ConditionNodeDef,
    ForEachNodeDef,
    WhileNodeDef,
    _rebuild_all,
)

WorkflowDef.model_rebuild()
# 无论从哪个入口导入 workflow.py，确保所有含前向引用的模型都完成 rebuild
_rebuild_all()
```

- [ ] **Step 4: 实现 nodes.py**

创建 `src/okflow/schema/nodes.py`：

```python
from __future__ import annotations

from typing import TYPE_CHECKING, Annotated, Literal, Union

from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from okflow.schema.workflow import WorkflowDef


class ActionNodeDef(BaseModel):
    type: Literal["action"] = "action"
    id: str
    handler: str
    params: dict = Field(default_factory=dict)
    output_key: str


class ConditionNodeDef(BaseModel):
    type: Literal["condition"] = "condition"
    id: str
    handler: str
    params: dict = Field(default_factory=dict)
    branches: dict[str, "WorkflowDef"]
    outputs: list[str] = Field(default_factory=list)


class ForEachNodeDef(BaseModel):
    type: Literal["foreach"] = "foreach"
    id: str
    items: str
    item_var: str
    collect_key: str
    concurrency: int | None = None
    sub_workflow: "WorkflowDef"


class WhileNodeDef(BaseModel):
    type: Literal["while"] = "while"
    id: str
    condition: str
    max_iterations: int = 100
    outputs: list[str] = Field(default_factory=list)
    sub_workflow: "WorkflowDef"


# 类型别名：判别联合
NodeDef = Annotated[
    Union[ActionNodeDef, ConditionNodeDef, ForEachNodeDef, WhileNodeDef],
    Field(discriminator="type"),
]


def _rebuild_all() -> None:
    """解析所有前向引用（在 workflow.py 末尾导入本模块后调用）。"""
    from okflow.schema.workflow import WorkflowDef  # noqa: PLC0415

    ConditionNodeDef.model_rebuild()
    ForEachNodeDef.model_rebuild()
    WhileNodeDef.model_rebuild()
```

- [ ] **Step 5: 实现 schema/__init__.py**

创建 `src/okflow/schema/__init__.py`：

```python
from okflow.schema.nodes import (
    ActionNodeDef,
    ConditionNodeDef,
    ForEachNodeDef,
    NodeDef,
    WhileNodeDef,
)
from okflow.schema.workflow import EdgeDef, WorkflowDef

# 触发前向引用解析
from okflow.schema.nodes import _rebuild_all

_rebuild_all()

__all__ = [
    "WorkflowDef",
    "EdgeDef",
    "NodeDef",
    "ActionNodeDef",
    "ConditionNodeDef",
    "ForEachNodeDef",
    "WhileNodeDef",
]
```

- [ ] **Step 6: 确认测试通过**

```bash
uv run pytest tests/test_schema.py -v
```

Expected: 全部 PASS

- [ ] **Step 7: 提交**

```bash
git add src/okflow/schema/ tests/test_schema.py
git commit -m "feat: add schema layer (WorkflowDef, NodeDef discriminated union)"
```

---

## Chunk 2: Scope + RunContext + ActionRegistry

### Task 4: Scope 和 ScopeGroup

**Files:**
- Create: `tests/test_scope.py`
- Create: `src/okflow/scope.py`

- [ ] **Step 1: 写失败测试**

创建 `tests/test_scope.py`：

```python
from okflow.scope import Scope, ScopeGroup
from okflow.schema.workflow import WorkflowDef


def _empty_wf(wf_id: str = "wf") -> WorkflowDef:
    return WorkflowDef(id=wf_id, name="test", nodes=[], edges=[])


def test_scope_defaults():
    wf = _empty_wf()
    scope = Scope(workflow=wf)
    assert scope.inputs == {}
    assert scope.inherit_parent is False
    assert scope.outputs == []


def test_scope_with_values():
    wf = _empty_wf()
    scope = Scope(workflow=wf, inputs={"x": 1}, inherit_parent=True, outputs=["r.v"])
    assert scope.inputs == {"x": 1}
    assert scope.inherit_parent is True
    assert scope.outputs == ["r.v"]


def test_scope_group_exclusive():
    wf = _empty_wf()
    scope = Scope(workflow=wf)
    group = ScopeGroup(scopes=[scope], mode="exclusive")
    assert group.mode == "exclusive"
    assert group.repeat is False
    assert group.concurrency is None
    assert group.collect_key is None


def test_scope_group_pooled():
    wf = _empty_wf()
    scopes = [Scope(workflow=wf, outputs=["r"]) for _ in range(3)]
    group = ScopeGroup(scopes=scopes, mode="pooled", concurrency=2, collect_key="results")
    assert group.concurrency == 2
    assert group.collect_key == "results"


def test_scope_group_sequential_repeat():
    wf = _empty_wf()
    scope = Scope(workflow=wf)
    group = ScopeGroup(
        scopes=[scope],
        mode="sequential",
        repeat=True,
        repeat_until=lambda ctx: True,
        max_iterations=50,
    )
    assert group.repeat is True
    assert group.max_iterations == 50
    assert group.repeat_until is not None


```

- [ ] **Step 2: 确认测试失败**

```bash
uv run pytest tests/test_scope.py -v
```

Expected: ImportError

- [ ] **Step 3: 实现 scope.py**

创建 `src/okflow/scope.py`：

```python
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
```

- [ ] **Step 4: 确认测试通过**

```bash
uv run pytest tests/test_scope.py -v
```

Expected: 全部 PASS

- [ ] **Step 5: 提交**

```bash
git add src/okflow/scope.py tests/test_scope.py
git commit -m "feat: add Scope and ScopeGroup dataclasses"
```

---

### Task 5: RunContext

**Files:**
- Create: `tests/test_context.py`
- Create: `src/okflow/context.py`

- [ ] **Step 1: 写失败测试**

创建 `tests/test_context.py`：

```python
import pytest
from okflow.context import RunContext


# ── 基本存取 ──────────────────────────────────────────────────────────────────

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
    assert ctx.get("a") == 1  # 原上下文未受影响


def test_init_with_store():
    ctx = RunContext({"x": 10, "y": 20})
    assert ctx.get("x") == 10


# ── $ref 解析 ─────────────────────────────────────────────────────────────────

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


# ── 条件 DSL ──────────────────────────────────────────────────────────────────

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
        ctx.eval_condition("x > 1")  # 缺少 $ 前缀
```

- [ ] **Step 2: 确认测试失败**

```bash
uv run pytest tests/test_context.py -v
```

Expected: ImportError

- [ ] **Step 3: 实现 context.py**

创建 `src/okflow/context.py`：

```python
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
        """返回当前存储的浅拷贝。"""
        return dict(self._store)

    def resolve(self, value: Any) -> Any:
        """递归展开 $ref 引用。"""
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
        """求值受限条件表达式（不使用 eval）。"""
        expr = expr.strip()

        m = _CONDITION_IS_NULL.match(expr)
        if m:
            # 键不存在时抛 KeyError（与 resolve() 行为一致，不静默处理缺失键）
            return self._store[m.group(1)] is None

        m = _CONDITION_IS_NOT_NULL.match(expr)
        if m:
            return self._store[m.group(1)] is not None

        m = _CONDITION_OP.match(expr)
        if m:
            ref_key, op, raw = m.group(1), m.group(2), m.group(3).strip()
            lhs = self._store[ref_key]  # 键不存在时抛 KeyError（与 is null 分支保持一致）
            rhs = _parse_rhs(raw)
            return _apply_op(lhs, op, rhs)

        raise ValueError(f"Unsupported condition expression: {expr!r}")


def _parse_rhs(raw: str) -> Any:
    """将条件右值字符串转换为 Python 对象。"""
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
```

- [ ] **Step 4: 确认测试通过**

```bash
uv run pytest tests/test_context.py -v
```

Expected: 全部 PASS

- [ ] **Step 5: 提交**

```bash
git add src/okflow/context.py tests/test_context.py
git commit -m "feat: add RunContext with $ref resolution and condition DSL"
```

---

### Task 6: ActionRegistry

**Files:**
- Create: `tests/test_registry.py`
- Create: `src/okflow/registry.py`

- [ ] **Step 1: 写失败测试**

创建 `tests/test_registry.py`：

```python
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
```

- [ ] **Step 2: 确认测试失败**

```bash
uv run pytest tests/test_registry.py -v
```

Expected: ImportError

- [ ] **Step 3: 实现 registry.py**

创建 `src/okflow/registry.py`：

```python
from __future__ import annotations

from typing import Any, Awaitable, Callable

from okflow.exceptions import UnknownHandlerError


class ActionRegistry:
    """引擎与外部系统的唯一边界。注册并调用异步处理函数。"""

    def __init__(self) -> None:
        self._handlers: dict[str, Callable[..., Awaitable[Any]]] = {}

    def register(self, name: str, handler: Callable[..., Awaitable[Any]]) -> None:
        """注册一个具名异步处理函数。重复注册时覆盖旧值。"""
        self._handlers[name] = handler

    async def call(self, handler_name: str, params: dict[str, Any]) -> Any:
        """按名称调用已注册的处理函数，params 展开为关键字参数。"""
        fn = self._handlers.get(handler_name)
        if fn is None:
            raise UnknownHandlerError(handler_name)
        return await fn(**params)
```

- [ ] **Step 4: 确认测试通过**

```bash
uv run pytest tests/test_registry.py -v
```

Expected: 全部 PASS

- [ ] **Step 5: 提交**

```bash
git add src/okflow/registry.py tests/test_registry.py
git commit -m "feat: add ActionRegistry"
```

---

## Chunk 3: NodeExecutors

### Task 7: NodeExecutor 基类 + 分发表

**Files:**
- Create: `src/okflow/nodes/__init__.py`
- Create: `src/okflow/nodes/base.py`
- Create: `tests/test_nodes/__init__.py`

- [ ] **Step 1: 实现 nodes/base.py**

创建 `src/okflow/nodes/base.py`：

```python
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from okflow.context import RunContext
    from okflow.registry import ActionRegistry
    from okflow.scope import ScopeGroup


class NodeExecutor(ABC):
    """节点执行器抽象基类。"""

    @abstractmethod
    async def execute(
        self,
        node: Any,
        ctx: RunContext,
        registry: ActionRegistry,
    ) -> ScopeGroup | None:
        """
        执行节点。
        返回 None：节点直接执行完毕，无子 Scope。
        返回 ScopeGroup：引擎按模式调度子 Scope。
        """
        ...
```

- [ ] **Step 2: 创建空文件**

创建 `tests/test_nodes/__init__.py`（空文件）。

- [ ] **Step 3: 提交**

```bash
git add src/okflow/nodes/base.py tests/test_nodes/__init__.py
git commit -m "feat: add NodeExecutor ABC"
```

---

### Task 8: ActionNodeExecutor

**Files:**
- Create: `tests/test_nodes/test_action.py`
- Create: `src/okflow/nodes/action.py`

- [ ] **Step 1: 写失败测试**

创建 `tests/test_nodes/test_action.py`：

```python
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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
```

- [ ] **Step 2: 确认测试失败**

```bash
uv run pytest tests/test_nodes/test_action.py -v
```

Expected: ImportError

- [ ] **Step 3: 实现 action.py**

创建 `src/okflow/nodes/action.py`：

```python
from __future__ import annotations

from typing import TYPE_CHECKING

from okflow.nodes.base import NodeExecutor
from okflow.schema.nodes import ActionNodeDef

if TYPE_CHECKING:
    from okflow.context import RunContext
    from okflow.registry import ActionRegistry
    from okflow.scope import ScopeGroup


class ActionNodeExecutor(NodeExecutor):
    """执行 action 节点：调用 handler，将结果写入 ctx。"""

    async def execute(
        self,
        node: ActionNodeDef,
        ctx: RunContext,
        registry: ActionRegistry,
    ) -> ScopeGroup | None:
        resolved = ctx.resolve(node.params)
        result = await registry.call(node.handler, resolved)
        ctx.set(f"{node.id}.{node.output_key}", result)
        return None
```

- [ ] **Step 4: 确认测试通过**

```bash
uv run pytest tests/test_nodes/test_action.py -v
```

Expected: 全部 PASS

- [ ] **Step 5: 提交**

```bash
git add src/okflow/nodes/action.py tests/test_nodes/test_action.py
git commit -m "feat: add ActionNodeExecutor"
```

---

### Task 9: ConditionNodeExecutor

**Files:**
- Create: `tests/test_nodes/test_condition.py`
- Create: `src/okflow/nodes/condition.py`

- [ ] **Step 1: 写失败测试**

创建 `tests/test_nodes/test_condition.py`：

```python
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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
```

- [ ] **Step 2: 确认测试失败**

```bash
uv run pytest tests/test_nodes/test_condition.py -v
```

Expected: ImportError

- [ ] **Step 3: 实现 condition.py**

创建 `src/okflow/nodes/condition.py`：

```python
from __future__ import annotations

from typing import TYPE_CHECKING

from okflow.nodes.base import NodeExecutor
from okflow.schema.nodes import ConditionNodeDef
from okflow.scope import Scope, ScopeGroup

if TYPE_CHECKING:
    from okflow.context import RunContext
    from okflow.registry import ActionRegistry


class ConditionNodeExecutor(NodeExecutor):
    """执行 condition 节点：调用 handler 决定分支，返回 exclusive ScopeGroup。"""

    async def execute(
        self,
        node: ConditionNodeDef,
        ctx: RunContext,
        registry: ActionRegistry,
    ) -> ScopeGroup | None:
        resolved = ctx.resolve(node.params)
        branch_key = str(await registry.call(node.handler, resolved))
        chosen_workflow = node.branches[branch_key]

        scope = Scope(
            workflow=chosen_workflow,
            inputs={},
            inherit_parent=True,
            outputs=node.outputs,
        )
        return ScopeGroup(scopes=[scope], mode="exclusive")
```

- [ ] **Step 4: 确认测试通过**

```bash
uv run pytest tests/test_nodes/test_condition.py -v
```

Expected: 全部 PASS

- [ ] **Step 5: 提交**

```bash
git add src/okflow/nodes/condition.py tests/test_nodes/test_condition.py
git commit -m "feat: add ConditionNodeExecutor"
```

---

### Task 10: ForEachNodeExecutor

**Files:**
- Create: `tests/test_nodes/test_foreach.py`
- Create: `src/okflow/nodes/foreach.py`

- [ ] **Step 1: 写失败测试**

创建 `tests/test_nodes/test_foreach.py`：

```python
import pytest
from okflow.context import RunContext
from okflow.registry import ActionRegistry
from okflow.nodes.foreach import ForEachNodeExecutor
from okflow.schema.nodes import ForEachNodeDef
from okflow.schema.workflow import WorkflowDef
from okflow.scope import ScopeGroup


def _empty_wf() -> WorkflowDef:
    return WorkflowDef(id="sub", name="sub", nodes=[], edges=[])


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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
    # 每个 Scope 的 outputs 应该是 [collect_key]
    for scope in result.scopes:
        assert scope.outputs == ["my.result"]
```

- [ ] **Step 2: 确认测试失败**

```bash
uv run pytest tests/test_nodes/test_foreach.py -v
```

Expected: ImportError

- [ ] **Step 3: 实现 foreach.py**

创建 `src/okflow/nodes/foreach.py`：

```python
from __future__ import annotations

from typing import TYPE_CHECKING

from okflow.nodes.base import NodeExecutor
from okflow.schema.nodes import ForEachNodeDef
from okflow.scope import Scope, ScopeGroup

if TYPE_CHECKING:
    from okflow.context import RunContext
    from okflow.registry import ActionRegistry


class ForEachNodeExecutor(NodeExecutor):
    """执行 foreach 节点：为每个 item 创建独立 Scope，返回 pooled ScopeGroup。"""

    async def execute(
        self,
        node: ForEachNodeDef,
        ctx: RunContext,
        registry: ActionRegistry,
    ) -> ScopeGroup | None:
        items = ctx.resolve(node.items)
        scopes = [
            Scope(
                workflow=node.sub_workflow,
                inputs={node.item_var: item},
                inherit_parent=False,
                outputs=[node.collect_key],
            )
            for item in items
        ]
        return ScopeGroup(
            scopes=scopes,
            mode="pooled",
            concurrency=node.concurrency,
            collect_key=f"{node.id}.collected",
        )
```

- [ ] **Step 4: 确认测试通过**

```bash
uv run pytest tests/test_nodes/test_foreach.py -v
```

Expected: 全部 PASS

- [ ] **Step 5: 提交**

```bash
git add src/okflow/nodes/foreach.py tests/test_nodes/test_foreach.py
git commit -m "feat: add ForEachNodeExecutor"
```

---

### Task 11: WhileNodeExecutor

**Files:**
- Create: `tests/test_nodes/test_while.py`
- Create: `src/okflow/nodes/while_.py`

- [ ] **Step 1: 写失败测试**

创建 `tests/test_nodes/test_while.py`：

```python
import pytest
from okflow.context import RunContext
from okflow.registry import ActionRegistry
from okflow.nodes.while_ import WhileNodeExecutor
from okflow.schema.nodes import WhileNodeDef
from okflow.schema.workflow import WorkflowDef
from okflow.scope import ScopeGroup


def _empty_wf() -> WorkflowDef:
    return WorkflowDef(id="sub", name="sub", nodes=[], edges=[])


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_while_repeat_until_stops_when_condition_false():
    """repeat_until 在条件不满足时返回 True（停止循环）。"""
    registry = ActionRegistry()
    ctx = RunContext({"count": 10})  # 10 >= 5，条件为 False，应停止
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
    # 条件 "$count < 5" 当 count=10 时为 False，repeat_until 应返回 True（停止）
    assert result.repeat_until(ctx) is True


@pytest.mark.asyncio
async def test_while_repeat_until_continues_when_condition_true():
    """repeat_until 在条件满足时返回 False（继续循环）。"""
    registry = ActionRegistry()
    ctx = RunContext({"count": 2})  # 2 < 5，条件为 True，应继续
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


```

- [ ] **Step 2: 确认测试失败**

```bash
uv run pytest tests/test_nodes/test_while.py -v
```

Expected: ImportError

- [ ] **Step 3: 实现 while_.py**

创建 `src/okflow/nodes/while_.py`：

```python
from __future__ import annotations

from typing import TYPE_CHECKING

from okflow.nodes.base import NodeExecutor
from okflow.schema.nodes import WhileNodeDef
from okflow.scope import Scope, ScopeGroup

if TYPE_CHECKING:
    from okflow.context import RunContext
    from okflow.registry import ActionRegistry


class WhileNodeExecutor(NodeExecutor):
    """执行 while 节点：返回 sequential+repeat ScopeGroup，引擎负责循环调度。"""

    async def execute(
        self,
        node: WhileNodeDef,
        ctx: RunContext,
        registry: ActionRegistry,
    ) -> ScopeGroup | None:
        scope = Scope(
            workflow=node.sub_workflow,
            inputs={},
            inherit_parent=True,
            outputs=node.outputs,
        )
        condition = node.condition

        return ScopeGroup(
            scopes=[scope],
            mode="sequential",
            repeat=True,
            repeat_until=lambda c: not c.eval_condition(condition),
            max_iterations=node.max_iterations,
        )
```

- [ ] **Step 4: 确认测试通过**

```bash
uv run pytest tests/test_nodes/test_while.py -v
```

Expected: 全部 PASS

- [ ] **Step 5: 实现 nodes/__init__.py（分发表）**

创建 `src/okflow/nodes/__init__.py`：

```python
from okflow.nodes.action import ActionNodeExecutor
from okflow.nodes.base import NodeExecutor
from okflow.nodes.condition import ConditionNodeExecutor
from okflow.nodes.foreach import ForEachNodeExecutor
from okflow.nodes.while_ import WhileNodeExecutor

_EXECUTORS: dict[str, NodeExecutor] = {
    "action": ActionNodeExecutor(),
    "condition": ConditionNodeExecutor(),
    "foreach": ForEachNodeExecutor(),
    "while": WhileNodeExecutor(),
}

__all__ = [
    "NodeExecutor",
    "ActionNodeExecutor",
    "ConditionNodeExecutor",
    "ForEachNodeExecutor",
    "WhileNodeExecutor",
    "_EXECUTORS",
]
```

- [ ] **Step 6: 提交**

```bash
git add src/okflow/nodes/ tests/test_nodes/test_while.py
git commit -m "feat: add WhileNodeExecutor and _EXECUTORS dispatch table"
```

---

## Chunk 4: DAGExecutor

### Task 12: build_ctx + DAGExecutor 骨架

**Files:**
- Create: `tests/test_executor.py`
- Create: `src/okflow/executor.py`

- [ ] **Step 1: 写失败测试（build_ctx）**

创建 `tests/test_executor.py`：

```python
import pytest
from okflow.executor import DAGExecutor, build_ctx
from okflow.context import RunContext
from okflow.scope import Scope
from okflow.schema.workflow import WorkflowDef
from okflow.registry import ActionRegistry
from okflow.exceptions import NodeExecutionError, MaxIterationsExceeded


def _wf(wf_id: str = "wf", nodes=None, edges=None) -> WorkflowDef:
    return WorkflowDef(
        id=wf_id, name="test", nodes=nodes or [], edges=edges or []
    )


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
    assert ctx.get("shared") == "override"  # inputs 覆盖父变量


def test_build_ctx_empty_parent():
    parent = RunContext()
    scope = Scope(workflow=_wf(), inputs={})
    ctx = build_ctx(scope, parent)
    assert ctx.snapshot() == {}
```

- [ ] **Step 2: 确认测试失败**

```bash
uv run pytest tests/test_executor.py::test_build_ctx_no_inherit -v
```

Expected: ImportError

- [ ] **Step 3: 实现 executor.py（完整实现）**

创建 `src/okflow/executor.py`：

```python
from __future__ import annotations

import asyncio
from typing import Any

from okflow.context import RunContext
from okflow.exceptions import (
    DAGEngineError,
    MaxIterationsExceeded,
    NodeExecutionError,
    ScopeOutputConflictError,
)
from okflow.registry import ActionRegistry
from okflow.scope import Scope, ScopeGroup
from okflow.schema.workflow import WorkflowDef


def build_ctx(scope: Scope, parent_ctx: RunContext) -> RunContext:
    """根据 Scope 的 inherit_parent 和 inputs 构建子 RunContext。"""
    store: dict[str, Any] = {}
    if scope.inherit_parent:
        store.update(parent_ctx.snapshot())
    store.update(scope.inputs)
    return RunContext(store)


class DAGExecutor:
    """DAG 工作流调度引擎：Kahn 算法 + Wave 并发 + ScopeGroup 调度。"""

    def __init__(self, registry: ActionRegistry) -> None:
        self._registry = registry

    async def run(self, scope: Scope) -> RunContext:
        """运行一个根 Scope，返回执行后的 RunContext。"""
        ctx = build_ctx(scope, RunContext())
        await self._run_workflow(scope.workflow, ctx)
        return ctx

    async def run_with_parent(self, scope: Scope, parent_ctx: RunContext) -> RunContext:
        """运行子 Scope，parent_ctx 用于 inherit_parent=True 的场景。"""
        ctx = build_ctx(scope, parent_ctx)
        await self._run_workflow(scope.workflow, ctx)
        return ctx

    async def _run_workflow(self, workflow: WorkflowDef, ctx: RunContext) -> None:
        """Kahn 算法 + Wave 并发调度工作流内的所有节点。"""
        node_map = {n.id: n for n in workflow.nodes}
        successors: dict[str, list[str]] = {n.id: [] for n in workflow.nodes}
        in_degree: dict[str, int] = {n.id: 0 for n in workflow.nodes}

        for edge in workflow.edges:
            successors[edge.from_].append(edge.to)
            in_degree[edge.to] += 1

        ready = [nid for nid, deg in in_degree.items() if deg == 0]
        completed = 0
        total = len(workflow.nodes)

        while completed < total:
            wave = ready[:]
            ready.clear()
            if not wave:
                # 有环或孤立节点（静态校验应预先捕获）
                break

            results = await asyncio.gather(
                *[self._execute_node(node_map[nid], ctx) for nid in wave],
                return_exceptions=True,
            )

            for nid, result in zip(wave, results):
                if isinstance(result, BaseException):
                    raise result
                if result is not None:
                    # 传入 nid 供 sequential+repeat 模式抛出 MaxIterationsExceeded 使用
                    await self._run_scope_group(result, ctx, source_node_id=nid)

                completed += 1
                for succ in successors[nid]:
                    in_degree[succ] -= 1
                    if in_degree[succ] == 0:
                        ready.append(succ)

    async def _execute_node(self, node: Any, ctx: RunContext) -> ScopeGroup | None:
        from okflow.nodes import _EXECUTORS  # noqa: PLC0415

        executor = _EXECUTORS[node.type]
        try:
            return await executor.execute(node, ctx, self._registry)
        except DAGEngineError:
            raise
        except Exception as e:
            raise NodeExecutionError(node.id, e) from e

    async def _run_scope_group(
        self,
        group: ScopeGroup,
        parent_ctx: RunContext,
        source_node_id: str | None = None,
    ) -> None:
        match group.mode:
            case "exclusive":
                await self._run_exclusive(group, parent_ctx)
            case "parallel":
                await self._run_parallel(group, parent_ctx)
            case "sequential":
                await self._run_sequential(group, parent_ctx, source_node_id)
            case "pooled":
                await self._run_pooled(group, parent_ctx)

    async def _run_exclusive(
        self, group: ScopeGroup, parent_ctx: RunContext
    ) -> None:
        for scope in group.scopes:
            child_ctx = await self.run_with_parent(scope, parent_ctx)
            _write_back(scope.outputs, child_ctx, parent_ctx)

    async def _run_parallel(
        self, group: ScopeGroup, parent_ctx: RunContext
    ) -> None:
        child_ctxs = await asyncio.gather(
            *[self.run_with_parent(scope, parent_ctx) for scope in group.scopes]
        )
        written: set[str] = set()
        for scope, child_ctx in zip(group.scopes, child_ctxs):
            for key in scope.outputs:
                if key in written:
                    raise ScopeOutputConflictError(key)
                written.add(key)
                parent_ctx.set(key, child_ctx.get(key))

    async def _run_sequential(
        self,
        group: ScopeGroup,
        parent_ctx: RunContext,
        source_node_id: str | None = None,
    ) -> None:
        if group.repeat:
            await self._run_repeat(group, parent_ctx, source_node_id)
        else:
            await self._run_pipeline(group, parent_ctx)

    async def _run_repeat(
        self,
        group: ScopeGroup,
        parent_ctx: RunContext,
        source_node_id: str | None = None,
    ) -> None:
        """sequential + repeat：while 循环语义。"""
        scope = group.scopes[0]
        iterations = 0
        max_iter = group.max_iterations

        while True:
            if max_iter is not None and iterations >= max_iter:
                node_id = source_node_id or "unknown"
                raise MaxIterationsExceeded(node_id, max_iter)

            child_ctx = await self.run_with_parent(scope, parent_ctx)
            _write_back(scope.outputs, child_ctx, parent_ctx)
            iterations += 1

            if group.repeat_until is not None and group.repeat_until(parent_ctx):
                break

    async def _run_pipeline(
        self, group: ScopeGroup, parent_ctx: RunContext
    ) -> None:
        """sequential（非 repeat）：pipeline 语义，前一个 outputs 注入下一个 inputs。"""
        extra_inputs: dict[str, Any] = {}
        last_idx = len(group.scopes) - 1

        for i, scope in enumerate(group.scopes):
            augmented = Scope(
                workflow=scope.workflow,
                inputs={**scope.inputs, **extra_inputs},
                inherit_parent=scope.inherit_parent,
                outputs=scope.outputs,
            )
            child_ctx = await self.run_with_parent(augmented, parent_ctx)

            if i == last_idx:
                _write_back(scope.outputs, child_ctx, parent_ctx)
            else:
                extra_inputs = {k: child_ctx.get(k) for k in scope.outputs}

    async def _run_pooled(
        self, group: ScopeGroup, parent_ctx: RunContext
    ) -> None:
        """pooled：并发执行，结果收集为列表。"""
        sem = asyncio.Semaphore(group.concurrency) if group.concurrency else None

        async def run_one(scope: Scope) -> RunContext:
            if sem:
                async with sem:
                    return await self.run_with_parent(scope, parent_ctx)
            return await self.run_with_parent(scope, parent_ctx)

        # 运行时防守：pooled 模式每个 Scope 必须恰好声明 1 个 output 键
        for scope in group.scopes:
            if len(scope.outputs) != 1:
                raise ScopeConfigError(
                    f"pooled scope must declare exactly one output, got {scope.outputs}"
                )

        child_ctxs = await asyncio.gather(*[run_one(scope) for scope in group.scopes])

        collected = []
        for scope, child_ctx in zip(group.scopes, child_ctxs):
            key = scope.outputs[0]
            collected.append(child_ctx.get(key))

        assert group.collect_key is not None  # pooled 模式必须设置
        parent_ctx.set(group.collect_key, collected)


def _write_back(
    outputs: list[str], child_ctx: RunContext, parent_ctx: RunContext
) -> None:
    """将子上下文的指定 outputs 键写回父上下文。"""
    for key in outputs:
        parent_ctx.set(key, child_ctx.get(key))
```

- [ ] **Step 4: 确认 build_ctx 测试通过**

```bash
uv run pytest tests/test_executor.py::test_build_ctx_no_inherit tests/test_executor.py::test_build_ctx_inherit tests/test_executor.py::test_build_ctx_empty_parent -v
```

Expected: 全部 PASS

- [ ] **Step 5: 写 DAGExecutor 集成测试（续接 test_executor.py）**

在 `tests/test_executor.py` 末尾追加：

```python
from okflow.schema.nodes import ActionNodeDef


# ── 简单 action 工作流 ─────────────────────────────────────────────────────────

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


@pytest.mark.asyncio
async def test_executor_sequential_nodes():
    """n1 → n2 顺序执行，n2 读取 n1 的输出。"""
    registry = ActionRegistry()

    async def fetch() -> list:
        return [1, 2, 3]

    async def count(items: list) -> int:
        return len(items)

    registry.register("data.fetch", fetch)
    registry.register("data.count", count)

    from okflow.schema.workflow import EdgeDef

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


@pytest.mark.asyncio
async def test_executor_parallel_nodes():
    """n1 和 n2 无依赖关系，应并发执行。"""
    import asyncio as _asyncio

    registry = ActionRegistry()
    order = []

    async def task_a() -> str:
        order.append("a_start")
        await _asyncio.sleep(0)
        order.append("a_end")
        return "A"

    async def task_b() -> str:
        order.append("b_start")
        await _asyncio.sleep(0)
        order.append("b_end")
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


@pytest.mark.asyncio
async def test_executor_parallel_output_conflict_raises():
    """parallel 模式下多个 Scope 声明同一 output 键时应抛 ScopeOutputConflictError。"""
    from okflow.scope import Scope, ScopeGroup
    from okflow.schema.workflow import WorkflowDef
    from okflow.exceptions import ScopeOutputConflictError

    # 使用内部 API 构造一个 parallel ScopeGroup，两个 Scope 都声明 "shared.key"
    registry = ActionRegistry()

    async def produce_value() -> int:
        return 42

    registry.register("produce", produce_value)

    inner_node = ActionNodeDef(id="n", handler="produce", params={}, output_key="key")
    inner_wf = _wf(nodes=[inner_node])

    # 两个 scope 都声明 outputs=["n.key"]（同名冲突）
    scope_a = Scope(workflow=inner_wf, outputs=["n.key"])
    scope_b = Scope(workflow=inner_wf, outputs=["n.key"])

    group = ScopeGroup(scopes=[scope_a, scope_b], mode="parallel")

    executor = DAGExecutor(registry)
    parent_ctx = RunContext()

    with pytest.raises(ScopeOutputConflictError) as exc_info:
        await executor._run_scope_group(group, parent_ctx)

    assert exc_info.value.key == "n.key"
```

- [ ] **Step 6: 确认 executor 测试通过**

```bash
uv run pytest tests/test_executor.py -v
```

Expected: 全部 PASS

- [ ] **Step 7: 提交**

```bash
git add src/okflow/executor.py tests/test_executor.py
git commit -m "feat: add DAGExecutor with Kahn+Wave scheduling and ScopeGroup dispatch"
```

---

## Chunk 5: 校验 + 公开 API + 集成测试

### Task 13: validate_workflow

**Files:**
- Create: `tests/test_validators.py`
- Create: `src/okflow/schema/validators.py`

- [ ] **Step 1: 写失败测试**

创建 `tests/test_validators.py`：

```python
import pytest
from okflow.schema.workflow import WorkflowDef, EdgeDef
from okflow.schema.nodes import ActionNodeDef, ConditionNodeDef, WhileNodeDef, ForEachNodeDef
from okflow.schema.validators import validate_workflow
from okflow.exceptions import WorkflowValidationError


def _action(node_id: str) -> ActionNodeDef:
    return ActionNodeDef(id=node_id, handler="h", params={}, output_key="r")


def _wf(wf_id: str, nodes=None, edges=None) -> WorkflowDef:
    return WorkflowDef(id=wf_id, name="t", nodes=nodes or [], edges=edges or [])


# ── 有效工作流不抛出异常 ────────────────────────────────────────────────────────

def test_valid_empty_workflow():
    validate_workflow(_wf("w"))


def test_valid_linear_workflow():
    n1, n2 = _action("n1"), _action("n2")
    wf = _wf("w", nodes=[n1, n2], edges=[EdgeDef(from_="n1", to="n2")])
    validate_workflow(wf)


# ── ① 节点 ID 唯一性 ──────────────────────────────────────────────────────────

def test_duplicate_node_ids_raises():
    n1a, n1b = _action("n1"), _action("n1")
    wf = _wf("w", nodes=[n1a, n1b])
    with pytest.raises(WorkflowValidationError, match="Duplicate"):
        validate_workflow(wf)


# ── ② 环检测 ─────────────────────────────────────────────────────────────────

def test_cycle_raises():
    n1, n2 = _action("n1"), _action("n2")
    wf = _wf(
        "w",
        nodes=[n1, n2],
        edges=[EdgeDef(from_="n1", to="n2"), EdgeDef(from_="n2", to="n1")],
    )
    with pytest.raises(WorkflowValidationError, match="[Cc]ycle"):
        validate_workflow(wf)


def test_self_loop_raises():
    n1 = _action("n1")
    wf = _wf("w", nodes=[n1], edges=[EdgeDef(from_="n1", to="n1")])
    with pytest.raises(WorkflowValidationError):
        validate_workflow(wf)


# ── ③ condition 分支目标校验 ──────────────────────────────────────────────────

def test_condition_valid_branches():
    branch_wf = _wf("b")
    cond = ConditionNodeDef(
        id="c", handler="h", branches={"ok": branch_wf, "fail": branch_wf}, outputs=[]
    )
    wf = _wf("w", nodes=[cond])
    validate_workflow(wf)


def test_condition_branch_sub_workflow_cycle_raises():
    """condition 分支的子工作流本身有环，应递归检测到。"""
    n1, n2 = _action("n1"), _action("n2")
    bad_branch = _wf(
        "bad",
        nodes=[n1, n2],
        edges=[EdgeDef(from_="n1", to="n2"), EdgeDef(from_="n2", to="n1")],
    )
    cond = ConditionNodeDef(id="c", handler="h", branches={"x": bad_branch}, outputs=[])
    wf = _wf("w", nodes=[cond])
    with pytest.raises(WorkflowValidationError):
        validate_workflow(wf)


# ── ④ while 条件表达式语法校验 ────────────────────────────────────────────────

def test_while_valid_condition():
    sub_wf = _wf("sub")
    node = WhileNodeDef(
        id="wh", condition="$count < 10", outputs=["count"], sub_workflow=sub_wf
    )
    wf = _wf("w", nodes=[node])
    validate_workflow(wf)


def test_while_invalid_condition_raises():
    sub_wf = _wf("sub")
    node = WhileNodeDef(
        id="wh", condition="count < 10", outputs=["count"], sub_workflow=sub_wf
    )  # 缺少 $ 前缀
    wf = _wf("w", nodes=[node])
    with pytest.raises(WorkflowValidationError, match="condition"):
        validate_workflow(wf)


# ── ⑤ outputs 键存在性检查 ────────────────────────────────────────────────────

def test_foreach_collect_key_exists_in_sub_workflow():
    """collect_key 存在于子工作流中，校验通过。"""
    sub = _wf(
        "sub",
        nodes=[ActionNodeDef(id="proc", handler="h", output_key="result")],
    )
    node = ForEachNodeDef(
        id="fe", items="$items", item_var="x", collect_key="proc.result", sub_workflow=sub
    )
    wf = _wf("w", nodes=[node])
    validate_workflow(wf)  # 不应抛出异常


def test_foreach_collect_key_missing_raises():
    """collect_key 不存在于子工作流中，应抛 WorkflowValidationError。"""
    sub = _wf(
        "sub",
        nodes=[ActionNodeDef(id="proc", handler="h", output_key="result")],
    )
    node = ForEachNodeDef(
        id="fe", items="$items", item_var="x", collect_key="nonexistent.key", sub_workflow=sub
    )
    wf = _wf("w", nodes=[node])
    with pytest.raises(WorkflowValidationError, match="collect_key"):
        validate_workflow(wf)


def test_while_output_missing_raises():
    """while outputs 中的键不存在于子工作流中，应抛 WorkflowValidationError。"""
    sub = _wf(
        "sub",
        nodes=[ActionNodeDef(id="inc", handler="h", output_key="val")],
    )
    node = WhileNodeDef(
        id="wh", condition="$inc.val < 5",
        outputs=["nonexistent.key"],  # 子工作流里没有这个键
        sub_workflow=sub,
    )
    wf = _wf("w", nodes=[node])
    with pytest.raises(WorkflowValidationError):
        validate_workflow(wf)


def test_condition_output_missing_in_branch_raises():
    """condition outputs 中的键在对应分支中不存在，应抛 WorkflowValidationError。"""
    empty_branch = _wf("empty")  # 没有任何节点
    cond = ConditionNodeDef(
        id="c", handler="h",
        branches={"ok": empty_branch},
        outputs=["result.value"],  # 分支里没有产生 result.value 的节点
    )
    wf = _wf("w", nodes=[cond])
    with pytest.raises(WorkflowValidationError):
        validate_workflow(wf)
```

- [ ] **Step 2: 确认测试失败**

```bash
uv run pytest tests/test_validators.py -v
```

Expected: ImportError（模块不存在）

- [ ] **Step 3: 实现 validators.py**

创建 `src/okflow/schema/validators.py`：

```python
from __future__ import annotations

import re

from okflow.exceptions import WorkflowValidationError
from okflow.schema.nodes import ConditionNodeDef, ForEachNodeDef, WhileNodeDef
from okflow.schema.workflow import WorkflowDef

# 合法的条件表达式正则（必须以 $ 开头）
_VALID_CONDITION = re.compile(
    r"^\$\S+\s+(is\s+null|is\s+not\s+null|(==|!=|<=|>=|<|>)\s+.+)$"
)


def validate_workflow(workflow: WorkflowDef) -> None:
    """在执行前静态校验工作流，包含 5 项检查，均递归校验子工作流。"""
    _check_unique_ids(workflow)
    _check_no_cycle(workflow)
    _check_condition_branches(workflow)
    _check_while_conditions(workflow)
    _check_outputs_referenced(workflow)


def _check_unique_ids(workflow: WorkflowDef) -> None:
    ids = [n.id for n in workflow.nodes]
    if len(ids) != len(set(ids)):
        raise WorkflowValidationError(
            f"Duplicate node IDs in workflow {workflow.id!r}: {ids}"
        )
    for node in workflow.nodes:
        if isinstance(node, ConditionNodeDef):
            for branch_wf in node.branches.values():
                _check_unique_ids(branch_wf)
        elif isinstance(node, (ForEachNodeDef, WhileNodeDef)):
            _check_unique_ids(node.sub_workflow)


def _check_no_cycle(workflow: WorkflowDef) -> None:
    node_ids = {n.id for n in workflow.nodes}
    in_degree: dict[str, int] = {nid: 0 for nid in node_ids}
    successors: dict[str, list[str]] = {nid: [] for nid in node_ids}

    for edge in workflow.edges:
        if edge.from_ not in node_ids or edge.to not in node_ids:
            raise WorkflowValidationError(
                f"Edge references unknown node: {edge.from_!r} -> {edge.to!r}"
            )
        successors[edge.from_].append(edge.to)
        in_degree[edge.to] += 1

    queue = [nid for nid, deg in in_degree.items() if deg == 0]
    visited = 0
    while queue:
        nid = queue.pop()
        visited += 1
        for succ in successors[nid]:
            in_degree[succ] -= 1
            if in_degree[succ] == 0:
                queue.append(succ)

    if visited != len(node_ids):
        raise WorkflowValidationError(
            f"Cycle detected in workflow {workflow.id!r}"
        )

    for node in workflow.nodes:
        if isinstance(node, ConditionNodeDef):
            for branch_wf in node.branches.values():
                _check_no_cycle(branch_wf)
        elif isinstance(node, (ForEachNodeDef, WhileNodeDef)):
            _check_no_cycle(node.sub_workflow)


def _check_condition_branches(workflow: WorkflowDef) -> None:
    for node in workflow.nodes:
        if isinstance(node, ConditionNodeDef):
            for branch_key, branch_wf in node.branches.items():
                # branch_wf 是 WorkflowDef，Pydantic 已校验结构；此处递归校验子工作流内容
                _check_condition_branches(branch_wf)
        elif isinstance(node, (ForEachNodeDef, WhileNodeDef)):
            _check_condition_branches(node.sub_workflow)


def _check_while_conditions(workflow: WorkflowDef) -> None:
    for node in workflow.nodes:
        if isinstance(node, WhileNodeDef):
            if not _VALID_CONDITION.match(node.condition.strip()):
                raise WorkflowValidationError(
                    f"Invalid while condition expression in node {node.id!r}: "
                    f"{node.condition!r}. Must start with $ref."
                )
            _check_while_conditions(node.sub_workflow)
        elif isinstance(node, ConditionNodeDef):
            for branch_wf in node.branches.values():
                _check_while_conditions(branch_wf)
        elif isinstance(node, ForEachNodeDef):
            _check_while_conditions(node.sub_workflow)


def _get_writable_keys(workflow: WorkflowDef) -> set[str]:
    """枚举工作流顶层所有节点能写入父上下文的键名。"""
    keys: set[str] = set()
    for node in workflow.nodes:
        if isinstance(node, ActionNodeDef):
            keys.add(f"{node.id}.{node.output_key}")
        elif isinstance(node, ConditionNodeDef):
            # condition 节点将其 outputs 声明的键提升到父上下文
            keys.update(node.outputs)
        elif isinstance(node, ForEachNodeDef):
            # foreach 节点将收集结果写入 "{node.id}.collected"
            keys.add(f"{node.id}.collected")
        elif isinstance(node, WhileNodeDef):
            # while 节点将其 outputs 声明的键提升到父上下文
            keys.update(node.outputs)
    return keys


def _check_outputs_referenced(workflow: WorkflowDef) -> None:
    """第⑤项：校验 outputs 中声明的变量键在对应子工作流中确实能被某个节点写入。"""
    for node in workflow.nodes:
        if isinstance(node, ConditionNodeDef):
            for branch_key, branch_wf in node.branches.items():
                branch_writable = _get_writable_keys(branch_wf)
                for out_key in node.outputs:
                    if out_key not in branch_writable:
                        raise WorkflowValidationError(
                            f"ConditionNode {node.id!r} declares output {out_key!r} "
                            f"but branch {branch_key!r} has no node that writes it. "
                            f"Writable keys: {sorted(branch_writable)}"
                        )
                # 递归校验分支子工作流
                _check_outputs_referenced(branch_wf)

        elif isinstance(node, ForEachNodeDef):
            sub_writable = _get_writable_keys(node.sub_workflow)
            if node.collect_key not in sub_writable:
                raise WorkflowValidationError(
                    f"ForEachNode {node.id!r} declares collect_key {node.collect_key!r} "
                    f"but sub_workflow has no node that writes it. "
                    f"Writable keys: {sorted(sub_writable)}"
                )
            _check_outputs_referenced(node.sub_workflow)

        elif isinstance(node, WhileNodeDef):
            sub_writable = _get_writable_keys(node.sub_workflow)
            for out_key in node.outputs:
                if out_key not in sub_writable:
                    raise WorkflowValidationError(
                        f"WhileNode {node.id!r} declares output {out_key!r} "
                        f"but sub_workflow has no node that writes it. "
                        f"Writable keys: {sorted(sub_writable)}"
                    )
            _check_outputs_referenced(node.sub_workflow)

- [ ] **Step 4: 确认测试通过**

```bash
uv run pytest tests/test_validators.py -v
```

Expected: 全部 PASS

- [ ] **Step 5: 提交**

```bash
git add src/okflow/schema/validators.py tests/test_validators.py
git commit -m "feat: add validate_workflow with 5 static checks"
```

---

### Task 14: 公开 API（__init__.py）

**Files:**
- Modify: `src/okflow/__init__.py`

- [ ] **Step 1: 更新公开 API**

修改 `src/okflow/__init__.py`：

```python
from okflow.context import RunContext
from okflow.exceptions import (
    DAGEngineError,
    MaxIterationsExceeded,
    NodeExecutionError,
    ScopeConfigError,
    ScopeOutputConflictError,
    UnknownHandlerError,
    WorkflowValidationError,
)
from okflow.executor import DAGExecutor, build_ctx
from okflow.registry import ActionRegistry
from okflow.schema import (
    ActionNodeDef,
    ConditionNodeDef,
    EdgeDef,
    ForEachNodeDef,
    NodeDef,
    WhileNodeDef,
    WorkflowDef,
)
from okflow.schema.validators import validate_workflow
from okflow.scope import Scope, ScopeGroup

__all__ = [
    # Schema
    "WorkflowDef",
    "EdgeDef",
    "NodeDef",
    "ActionNodeDef",
    "ConditionNodeDef",
    "ForEachNodeDef",
    "WhileNodeDef",
    # Core
    "Scope",
    "ScopeGroup",
    "RunContext",
    "ActionRegistry",
    "DAGExecutor",
    "build_ctx",
    "validate_workflow",
    # Exceptions
    "DAGEngineError",
    "WorkflowValidationError",
    "NodeExecutionError",
    "MaxIterationsExceeded",
    "UnknownHandlerError",
    "ScopeOutputConflictError",
    "ScopeConfigError",
]
```

- [ ] **Step 2: 验证公开 API 可导入**

```bash
uv run python -c "import okflow; print('OK:', okflow.DAGExecutor)"
```

Expected: `OK: <class 'okflow.executor.DAGExecutor'>`

- [ ] **Step 3: 提交**

```bash
git add src/okflow/__init__.py
git commit -m "feat: expose public API in okflow.__init__"
```

---

### Task 15: 集成测试

**Files:**
- Create: `tests/test_integration.py`
- Create: `tests/__init__.py`
- Create: `tests/conftest.py`

- [ ] **Step 1: 创建空文件**

创建 `tests/__init__.py`（空文件）。

创建 `tests/conftest.py`：

```python
# conftest.py — pytest 共享 fixtures
```

- [ ] **Step 2: 写集成测试**

创建 `tests/test_integration.py`：

```python
"""
端到端集成测试：覆盖四种节点类型和多种组合场景。
测试通过 okflow 公开 API 驱动，不依赖内部实现细节。
"""
import pytest
from okflow import (
    ActionNodeDef,
    ConditionNodeDef,
    DAGExecutor,
    EdgeDef,
    ForEachNodeDef,
    ActionRegistry,
    Scope,
    WhileNodeDef,
    WorkflowDef,
    MaxIterationsExceeded,
    NodeExecutionError,
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
    n_transform = ActionNodeDef(id="transform", handler="data.transform", params={"items": "$fetch.items"}, output_key="items")
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
async def test_integration_while_increments_until_limit():
    """while 循环：count 从 0 自增到 3 后停止。"""
    registry = ActionRegistry()

    async def increment(count: int) -> int:
        return count + 1

    registry.register("math.increment", increment)

    sub_wf = _wf(
        "sub",
        nodes=[ActionNodeDef(id="inc", handler="math.increment", params={"count": "$count"}, output_key="value")],
    )
    while_node = WhileNodeDef(
        id="loop",
        condition="$count < 3",
        outputs=["count"],
        max_iterations=10,
        sub_workflow=sub_wf,
    )
    workflow = _wf("main", nodes=[while_node])
    validate_workflow(workflow)

    # 注意：while 循环中子工作流写 "inc.value"，但 outputs=["count"]
    # 需要子工作流将 inc.value 写到 count
    # 重新设计子工作流：直接用 output_key="count" 让节点写 "inc.count"，不对
    # 正确做法：ActionNode 写 "{node_id}.{output_key}"，即 "inc.value"
    # outputs=["inc.value"] 才能写回父上下文
    # 让父上下文用 "inc.value" 作为 count 的键名……但条件读 $count
    # 解决：将 output_key 改为和条件变量相同名字，通过 outputs 声明
    # 实际上 while 的 outputs 声明的是子工作流中某节点会写的键
    # 子工作流：inc 节点 output_key="count_val" → ctx 中有 "inc.count_val"
    # while outputs=["inc.count_val"]，写回父上下文
    # 但条件是 $count，所以要在父上下文中有 "count" 这个键
    # 这个测试需要重新设计……
    #
    # 最简单的方案：让 action 节点的 output_key="count"，这样它写 "inc.count"
    # 然后 outputs=["inc.count"]，但条件用 $inc.count
    # 不，output_key 写的是 "{node_id}.{output_key}"，键名是 "inc.count" 这两部分
    # 条件变量 $inc.count 会匹配 ctx["inc.count"]
    pytest.skip("需要重新设计子工作流结构，见下方修正版本")


@pytest.mark.asyncio
async def test_integration_while_correct():
    """while 循环：利用节点输出键作为条件变量。"""
    registry = ActionRegistry()

    async def increment(n: int) -> int:
        return n + 1

    registry.register("math.increment", increment)

    # 子工作流：读父上下文的 "loop.counter"，写 "inc.result"
    # while outputs=["inc.result"]，写回父上下文
    # 条件：$inc.result < 3
    sub_wf = _wf(
        "sub",
        nodes=[
            ActionNodeDef(
                id="inc",
                handler="math.increment",
                params={"n": "$inc.result"},  # 读上一轮的 inc.result
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
        inputs={"inc.result": 0},  # 初始值
        outputs=["inc.result"],
    )
    ctx = await DAGExecutor(registry).run(scope)
    assert ctx.get("inc.result") == 3


@pytest.mark.asyncio
async def test_integration_while_max_iterations_exceeded():
    """while 超出最大迭代次数时抛出 MaxIterationsExceeded。"""
    registry = ActionRegistry()

    async def noop(x: int) -> int:
        return x  # 永远不改变值，条件永远为 True

    registry.register("noop", noop)

    sub_wf = _wf(
        "sub",
        nodes=[ActionNodeDef(id="n", handler="noop", params={"x": "$counter"}, output_key="counter")],
    )
    while_node = WhileNodeDef(
        id="loop",
        condition="$counter < 100",  # 永远为 True（counter 不变）
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
```

- [ ] **Step 3: 确认集成测试通过**

```bash
uv run pytest tests/test_integration.py -v
```

Expected: 除 `test_integration_while_increments_until_limit`（已 skip）外全部 PASS

- [ ] **Step 4: 运行完整测试套件**

```bash
uv run pytest -v
```

Expected: 全部 PASS（skip 1 个）

- [ ] **Step 5: 提交**

```bash
git add tests/ src/okflow/__init__.py
git commit -m "feat: add integration tests and complete public API"
```

---

### Task 16: 最终代码质量检查

- [ ] **Step 1: 类型检查**

```bash
uv run mypy src/
```

Expected: 无 error（warning 可接受）。若有类型错误按提示修复。

- [ ] **Step 2: Lint 检查**

```bash
uv run ruff check src/ tests/
```

Expected: 无 error（warning 可接受）。若有 lint 错误按提示修复。

- [ ] **Step 3: 代码格式化**

```bash
uv run ruff format src/ tests/
```

- [ ] **Step 4: 最终完整测试**

```bash
uv run pytest -v --tb=short
```

Expected: 全部 PASS

- [ ] **Step 5: 最终提交**

```bash
git add -u
git commit -m "chore: fix type errors and lint issues"
```

---

## 总结

实现完成后，`okflow` 库具备：

1. **Schema 层**：Pydantic v2 判别联合，支持 action/condition/foreach/while 四种节点，递归结构通过 `model_rebuild()` 解析
2. **Scope/ScopeGroup**：显式 I/O 边界 + 4 种执行模式（exclusive/parallel/sequential/pooled）
3. **RunContext**：变量存取、`$ref` 解析、受限条件 DSL（无 eval）
4. **ActionRegistry**：handler 注册与调用
5. **NodeExecutors**：4 种内置节点执行器，均返回 `ScopeGroup | None`
6. **DAGExecutor**：Kahn + Wave 并发调度，递归处理 ScopeGroup
7. **validate_workflow**：5 项静态校验，全部递归执行
