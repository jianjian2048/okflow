# okflow 使用指南

> 本文档面向两类读者：
> **使用者**——了解如何定义工作流、注册 handler、运行引擎；
> **贡献者**——了解如何扩展节点类型及引擎内部组件协作方式。
>
> 架构设计细节见 [`dag-engine-design.md`](dag-engine-design.md)。

---

## 目录

1. [快速入门](#1-快速入门)
2. [核心概念](#2-核心概念)
3. [节点类型详解](#3-节点类型详解)
4. [静态校验](#4-静态校验)
5. [异常处理](#5-异常处理)
6. [API 参考](#6-api-参考)
7. [扩展指南](#7-扩展指南)

---

## 1. 快速入门

五步运行第一个工作流。

### 第一步：安装

```bash
uv add okflow
# 或
pip install okflow
```

### 第二步：注册 Handler

Handler 是普通的 `async` 函数，参数名与工作流 `params` 中的键一一对应。

```python
from okflow import ActionRegistry

registry = ActionRegistry()

async def add(a: int, b: int) -> int:
    return a + b

registry.register("math.add", add)
```

### 第三步：定义工作流

```python
from okflow import WorkflowDef, ActionNodeDef, EdgeDef

workflow = WorkflowDef(
    id="demo",
    name="加法演示",
    nodes=[
        ActionNodeDef(id="step1", handler="math.add", params={"a": 1, "b": 2}, output_key="result"),
        ActionNodeDef(id="step2", handler="math.add", params={"a": "$step1.result", "b": 10}, output_key="final"),
    ],
    edges=[EdgeDef(from_="step1", to="step2")],
)
```

### 第四步：运行

```python
import asyncio
from okflow import DAGExecutor, Scope

async def main():
    scope = Scope(workflow=workflow)
    ctx = await DAGExecutor(registry).run(scope)
    print(ctx.get("step2.final"))  # 13

asyncio.run(main())
```

### 第五步：从 JSON 加载工作流

```python
import json
from okflow import WorkflowDef, validate_workflow

with open("workflow.json") as f:
    workflow = WorkflowDef.model_validate(json.load(f))

validate_workflow(workflow)  # 建议执行前校验
```

JSON 文件示例（与上方 Python 写法等价）：

```json
{
  "id": "demo",
  "name": "加法演示",
  "nodes": [
    {
      "type": "action",
      "id": "step1",
      "handler": "math.add",
      "params": { "a": 1, "b": 2 },
      "output_key": "result"
    },
    {
      "type": "action",
      "id": "step2",
      "handler": "math.add",
      "params": { "a": "$step1.result", "b": 10 },
      "output_key": "final"
    }
  ],
  "edges": [
    { "from": "step1", "to": "step2" }
  ]
}
```

> **完整示例** 见 [`example/main.py`](../example/main.py) 和 [`example/workflow.json`](../example/workflow.json)。

---

## 2. 核心概念

### WorkflowDef

描述一张 DAG（有向无环图）：**节点列表**定义要执行什么，**边列表**定义依赖关系。引擎使用 Kahn 算法调度，无入边的节点并发执行。

```
WorkflowDef
├── nodes: [NodeDef, ...]   # action / condition / foreach / while
└── edges: [EdgeDef, ...]   # from → to，声明先后依赖
```

### Scope

Scope 是带显式 I/O 边界的**执行单元**，包裹一个 `WorkflowDef` 并声明它的输入和输出：

| 字段 | 说明 |
|------|------|
| `workflow` | 要执行的工作流 |
| `inputs` | 注入子工作流的初始变量（优先级高于父上下文） |
| `outputs` | 执行后写回父上下文的变量键列表 |
| `inherit_parent` | `True` 时子工作流可读取父上下文全部变量 |

根 Scope（传给 `DAGExecutor.run()`）通常只需指定 `workflow`，其他字段保持默认值。

### RunContext

每个 Scope 执行时持有独立的 `RunContext` 实例，负责**变量存储**和 **`$ref` 解析**。

- 变量键格式：`"{node_id}.{output_key}"`
- 引用格式：参数值中以 `$` 开头的字符串会被自动解析为上下文中对应的值

```python
# 节点写入：
ctx.set("fetch.data", [1, 2, 3])

# 下游节点引用（通过 params 自动解析）：
params = {"items": "$fetch.data"}
# resolve 后 items = [1, 2, 3]
```

### ActionRegistry

引擎与业务逻辑之间的**唯一边界**。所有 handler 通过 `registry.register(name, fn)` 注册，引擎通过 `registry.call(name, params)` 调用。

```python
registry = ActionRegistry()
registry.register("my.handler", my_async_fn)
```

---

## 3. 节点类型详解

### 3.1 ActionNode

调用 `ActionRegistry` 中注册的 handler，将返回值写入上下文。

**Python：**

```python
ActionNodeDef(
    id="fetch",
    handler="data.fetch",       # 对应 registry 中的名称
    params={"url": "$config.url"},  # 支持 $ref 引用
    output_key="body",          # 结果写入 "fetch.body"
)
```

**等价 JSON：**

```json
{
  "type": "action",
  "id": "fetch",
  "handler": "data.fetch",
  "params": { "url": "$config.url" },
  "output_key": "body"
}
```

**变量写入规则：** 结果写入 `"{node_id}.{output_key}"`，本例为 `"fetch.body"`。

---

### 3.2 ConditionNode

Handler 返回一个字符串（分支键），引擎路由到 `branches[key]` 对应的子工作流执行，其他分支不执行。

**Python：**

```python
ConditionNodeDef(
    id="route",
    handler="check.size",               # 返回 "large" 或 "small"
    params={"value": "$input.count"},
    branches={
        "large": large_workflow,         # WorkflowDef
        "small": small_workflow,
    },
    # 声明哪些子工作流变量写回父上下文
    # union 语义：每个键只需在至少一个分支中存在即可
    outputs=["large_handler.result", "small_handler.result"],
)
```

**等价 JSON：**

```json
{
  "type": "condition",
  "id": "route",
  "handler": "check.size",
  "params": { "value": "$input.count" },
  "branches": {
    "large": { "id": "large_wf", "name": "大", "nodes": [...], "edges": [] },
    "small": { "id": "small_wf", "name": "小", "nodes": [...], "edges": [] }
  },
  "outputs": ["large_handler.result", "small_handler.result"]
}
```

**关键点：**
- `branches` 支持任意数量的具名分支（不限于 true/false）
- 未执行的分支变量不会出现在父上下文中，这是正常行为
- `outputs` 校验使用 union 语义：只要某个键在**任一**分支中存在即通过

---

### 3.3 ForEachNode

为列表中的每个 item 创建**独立隔离的 Scope** 并执行子工作流，所有迭代结果按顺序收集为列表。

**Python：**

```python
ForEachNodeDef(
    id="process_each",
    items="$fetch.items",          # 引用上下文中的列表
    item_var="item",               # 每次迭代绑定到 $item
    collect_key="worker.output",   # 子工作流每次迭代产出的键名
    concurrency=4,                 # 最大并发数（None 表示不限）
    sub_workflow=worker_workflow,
    # 父上下文中收集键固定为 "{node_id}.collected"，本例为 "process_each.collected"
)
```

**等价 JSON：**

```json
{
  "type": "foreach",
  "id": "process_each",
  "items": "$fetch.items",
  "item_var": "item",
  "collect_key": "worker.output",
  "concurrency": 4,
  "sub_workflow": {
    "id": "worker_sub",
    "name": "处理单项",
    "nodes": [
      {
        "type": "action",
        "id": "worker",
        "handler": "data.process",
        "params": { "x": "$item" },
        "output_key": "output"
      }
    ],
    "edges": []
  }
}
```

**关键点：**
- `collect_key`：子工作流每次迭代产出的**键名**（如 `"worker.output"`）
- 父上下文收集键：固定为 `"{node_id}.collected"`（如 `"process_each.collected"`）
- 每次迭代完全隔离（`inherit_parent=False`），防止跨迭代变量污染
- `concurrency=None` 时所有迭代完全并发；设置正整数可限制并发上限

---

### 3.4 WhileNode

反复执行子工作流，直到 `condition` 不再成立或达到 `max_iterations`。

**Python：**

```python
WhileNodeDef(
    id="retry_loop",
    # condition 是"继续循环的条件"：为真时继续执行，为假时退出
    condition="$job.status != 'done'",
    outputs=["job.status"],    # 声明循环体每轮写回父上下文的变量
    max_iterations=10,
    sub_workflow=retry_workflow,
)
```

**等价 JSON：**

```json
{
  "type": "while",
  "id": "retry_loop",
  "condition": "$job.status != 'done'",
  "outputs": ["job.status"],
  "max_iterations": 10,
  "sub_workflow": {
    "id": "retry_sub",
    "name": "单次重试",
    "nodes": [
      {
        "type": "action",
        "id": "job",
        "handler": "task.check_status",
        "params": {},
        "output_key": "status"
      }
    ],
    "edges": []
  }
}
```

**条件 DSL 语法（不使用 `eval`）：**

| 格式 | 示例 |
|------|------|
| `$ref is null` | `$result is null` |
| `$ref is not null` | `$result is not null` |
| `$ref OP value` | `$count < 5` |
| `$ref OP "string"` | `$status != 'done'` |

OP 支持：`==`、`!=`、`<`、`<=`、`>`、`>=`

**关键点：**
- `condition` 为**继续循环的条件**——为真时继续，为假时退出
- `outputs` 中的变量每轮都写回父上下文，供下一轮条件判断使用
- 子工作流使用 `inherit_parent=True`，可以读取父上下文的所有变量
- 超出 `max_iterations` 时抛出 `MaxIterationsExceeded`

---

## 4. 静态校验

`validate_workflow()` 在执行前按顺序运行五项检查，所有检查均**递归**到子工作流。

```python
from okflow import validate_workflow, WorkflowValidationError

try:
    validate_workflow(workflow)
except WorkflowValidationError as e:
    print(f"工作流配置错误: {e}")
```

**五项检查：**

| # | 检查项 | 触发条件 |
|---|--------|---------|
| ① | 节点 ID 唯一性 | 同一工作流内存在重复 ID |
| ② | 无环检测（Kahn） | edges 构成环路，或 edge 引用不存在的节点 |
| ③ | condition 分支目标存在 | branches 中的 WorkflowDef 本身被递归校验 |
| ④ | while condition DSL 语法 | condition 字符串不匹配受限 DSL 格式 |
| ⑤ | outputs 引用变量存在 | outputs 中的键在子工作流中没有任何节点能写入 |

**常见错误示例：**

```python
# 错误①：重复 ID
WorkflowDef(id="wf", name="t", nodes=[
    ActionNodeDef(id="a", handler="h", params={}, output_key="x"),
    ActionNodeDef(id="a", handler="h", params={}, output_key="y"),  # 重复
], edges=[])
# → WorkflowValidationError: Duplicate node IDs in workflow 'wf': ['a', 'a']

# 错误④：非法 condition
WhileNodeDef(id="loop", condition="count > 0", ...)  # 缺少 $ 前缀
# → WorkflowValidationError: Invalid while condition expression ...

# 错误⑤：outputs 引用不存在的键
ConditionNodeDef(id="c", ..., outputs=["nonexistent.key"])
# → WorkflowValidationError: ConditionNode 'c' declares output 'nonexistent.key' but no branch has a node that writes it.
```

---

## 5. 异常处理

所有引擎异常均继承自 `DAGEngineError`：

```
DAGEngineError
├── WorkflowValidationError   # 静态校验失败
├── NodeExecutionError        # 节点业务异常（node_id + cause）
├── MaxIterationsExceeded     # while 超出最大迭代次数（node_id + max_iterations）
├── UnknownHandlerError       # handler 未在 registry 中注册（handler_name）
├── ScopeOutputConflictError  # parallel 模式下多个 Scope 写入同一输出键（key）
└── ScopeConfigError          # Scope/ScopeGroup 配置错误（如 pooled 缺少 collect_key）
```

**典型捕获模式：**

```python
from okflow import (
    DAGEngineError,
    MaxIterationsExceeded,
    NodeExecutionError,
    WorkflowValidationError,
)

try:
    validate_workflow(workflow)
    ctx = await DAGExecutor(registry).run(scope)
except WorkflowValidationError as e:
    print(f"工作流配置错误: {e}")
except MaxIterationsExceeded as e:
    print(f"节点 '{e.node_id}' 超出最大迭代次数 {e.max_iterations}")
except NodeExecutionError as e:
    print(f"节点 '{e.node_id}' 执行失败: {type(e.cause).__name__}: {e.cause}")
except DAGEngineError as e:
    print(f"引擎错误: {e}")
```

**注意：** `NodeExecutionError` 的 `cause` 是原始业务异常。非 `DAGEngineError` 的异常（如 handler 内部的 `ValueError`）会被包装为 `NodeExecutionError` 后抛出。

---

## 6. API 参考

### WorkflowDef

```python
WorkflowDef(
    id: str,            # 工作流唯一标识
    name: str,          # 工作流名称
    nodes: list[NodeDef],   # 节点列表（判别联合，按 type 字段分发）
    edges: list[EdgeDef],   # 依赖边列表
)
```

### EdgeDef

```python
EdgeDef(
    from_: str,   # Python 侧用 from_（from 是保留字）
    to: str,
)
# JSON 侧用 "from"（通过 Pydantic alias 自动转换）
```

### ActionNodeDef

```python
ActionNodeDef(
    id: str,
    handler: str,               # registry 中的 handler 名称
    params: dict = {},          # 传给 handler 的关键字参数，支持 $ref
    output_key: str,            # 结果写入 "{id}.{output_key}"
)
```

### ConditionNodeDef

```python
ConditionNodeDef(
    id: str,
    handler: str,               # 返回分支键（字符串）的 handler
    params: dict = {},
    branches: dict[str, WorkflowDef],  # 分支名 → 子工作流
    outputs: list[str] = [],    # 写回父上下文的变量键（union 语义）
)
```

### ForEachNodeDef

```python
ForEachNodeDef(
    id: str,
    items: str,                 # 上下文中的列表引用，如 "$fetch.data"
    item_var: str,              # 每次迭代的绑定变量名，如 "item"
    collect_key: str,           # 子工作流每次迭代的产出键名，如 "worker.output"
                                # 父上下文收集键固定为 "{id}.collected"
    sub_workflow: WorkflowDef,
    concurrency: int | None = None,  # 并发上限（None = 不限）
)
```

### WhileNodeDef

```python
WhileNodeDef(
    id: str,
    condition: str,             # 继续循环的条件（受限 DSL，必须以 $ 开头）
    outputs: list[str] = [],    # 每轮循环后写回父上下文的变量键
    max_iterations: int = 100,  # 最大迭代次数，超出抛 MaxIterationsExceeded
    sub_workflow: WorkflowDef,
)
```

### Scope

```python
Scope(
    workflow: WorkflowDef,
    inputs: dict = {},          # 注入子工作流的初始变量
    inherit_parent: bool = False,  # True 时继承父上下文全部变量
    outputs: list[str] = [],    # 执行后写回父上下文的变量键
)
```

### DAGExecutor

```python
executor = DAGExecutor(registry: ActionRegistry)

# 运行根 Scope，返回执行后的 RunContext
ctx: RunContext = await executor.run(scope: Scope)
```

### RunContext

```python
ctx.get(key: str) -> Any          # 读取变量，键不存在时抛 KeyError
ctx.set(key: str, value: Any)     # 写入变量
ctx.snapshot() -> dict[str, Any]  # 返回当前所有变量的副本
```

### validate_workflow

```python
validate_workflow(workflow: WorkflowDef) -> None
# 通过时无返回值；失败时抛 WorkflowValidationError
```

---

## 7. 扩展指南

添加自定义节点类型只需三步，无需修改调度引擎。

### 第一步：定义 NodeDef 子类

```python
from typing import Literal
from pydantic import BaseModel

class ParallelFanOutNodeDef(BaseModel):
    type: Literal["parallel_fanout"] = "parallel_fanout"
    id: str
    sub_workflows: list[WorkflowDef]   # 要并发执行的子工作流列表
    outputs: list[str] = []
```

### 第二步：实现 NodeExecutor 子类

```python
from okflow.nodes.base import NodeExecutor
from okflow.scope import Scope, ScopeGroup
from okflow.context import RunContext
from okflow.registry import ActionRegistry

class ParallelFanOutExecutor(NodeExecutor):
    async def execute(
        self,
        node: ParallelFanOutNodeDef,
        ctx: RunContext,
        registry: ActionRegistry,
    ) -> ScopeGroup:
        scopes = [
            Scope(
                workflow=wf,
                inherit_parent=True,
                outputs=node.outputs,
            )
            for wf in node.sub_workflows
        ]
        # parallel 模式：所有子工作流并发运行
        # outputs 键名冲突时抛 ScopeOutputConflictError
        return ScopeGroup(scopes=scopes, mode="parallel")
```

> **`execute()` 返回值约定：**
> - 返回 `None`：节点直接执行完毕，不创建子 Scope（适合 action 类节点）
> - 返回 `ScopeGroup`：引擎按 mode 调度子 Scope（适合控制流节点）

### 第三步：注册到分发表

```python
from okflow.nodes import _EXECUTORS, NodeType

# 将新节点类型添加到判别联合（同时更新 NodeDef = Annotated[Union[...], ...]）
_EXECUTORS["parallel_fanout"] = ParallelFanOutExecutor()
```

### ScopeGroup 四种模式

| mode | 语义 | 典型场景 |
|------|------|---------|
| `exclusive` | 运行 scopes 中的全部 Scope（通常预选只有一个） | condition 分支 |
| `parallel` | 所有 Scope 并发运行，outputs 冲突时报错 | fan-out |
| `sequential` | 顺序运行，前一个 Scope 的 outputs 注入下一个的 inputs | pipeline |
| `sequential` + `repeat=True` | 反复运行同一 Scope，每轮 outputs 写回父上下文 | while 循环 |
| `pooled` | 并发运行（受 `concurrency` 限制），结果收集为列表 | foreach 批量处理 |
