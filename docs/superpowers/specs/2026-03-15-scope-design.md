# Scope 设计规范

> 本文档描述 okflow DAG 引擎引入 `Scope` / `ScopeGroup` 概念后的架构调整。
> 完整设计参见 `document/dag-engine-design.md`，本文聚焦核心设计决策与动机。

---

## 背景与动机

原设计中 `DAGExecutor` 对 condition / foreach / while 三类节点做了硬编码的特殊处理：

- `ConditionNodeExecutor` 内嵌 BFS 逻辑级联标记跳过节点，菱形汇聚图结构行为未定义
- `ForEachNodeExecutor` 强制顺序迭代，无并发选项
- `WhileNodeExecutor` 直接共享父上下文，子工作流写入会静默污染父级变量
- 新增控制流节点类型需修改调度引擎核心逻辑

引入 `Scope` 和 `ScopeGroup` 后，控制流成为引擎的通用能力，内置节点与用户自定义节点享有同等权限。

---

## 已确认的设计决策

### 决策 1：Scope I/O 采用显式接口

`outputs` 必须显式声明，子 Scope 只能通过声明的键影响父上下文，未声明的写入在 Scope 结束时丢弃。

`inputs` 支持两种模式，由 `inherit_parent` 控制：
- `inherit_parent=False`（隔离）：子工作流只能访问 `inputs` 中显式绑定的变量，适用于 foreach
- `inherit_parent=True`（透明）：在 `inputs` 基础上继承父上下文全部变量，适用于 condition / while

**选择 `inherit_parent` 而非严格要求所有 inputs 显式声明的原因：**
condition / while 的分支和循环体往往需要访问父上下文中的大量变量，逐一声明过于繁琐且容易遗漏。通过 `inherit_parent=True` 放开读取权限，同时保持 `outputs` 的严格约束，实现了"读灵活、写严格"的平衡。

### 决策 2：Scope 之间平级，通过执行模式表达关系

同一 ScopeGroup 内的 Scope 彼此不能互相引用，执行关系由 `mode` 声明：

| 模式 | 语义 | 对应场景 |
|------|------|---------|
| `exclusive` | 运行列表中的 Scope（节点已预选） | condition |
| `parallel` | 全部并发，outputs 冲突时报错 | fan-out |
| `sequential` | 顺序，前一个 outputs 自动注入下一个 inputs | pipeline / while |
| `pooled` | 并发 + 限流，结果收集为列表 | foreach |

不引入 Scope 间 DAG 依赖，保持调度逻辑在单一层次。

### 决策 3：执行模式由节点动态声明

`mode` 是节点在运行时创建 ScopeGroup 时传入的参数，而非节点类型的固有属性。自定义节点可以创建任意模式的 ScopeGroup，无需修改引擎。

### 决策 4：`exclusive` 模式由节点预选

condition 节点自己决定选哪个分支，只将被选中的 Scope 放入 ScopeGroup。引擎不做选择逻辑，`exclusive` 标签的作用是语义标注（便于观测性和日志），而非行为控制。这比"传入所有分支 + 选择函数"更简单，符合最小职责原则。

### 决策 5：`repeat` 是 `sequential` 模式的附加 flag

`repeat` 不是独立的 mode 值，而是 `sequential` 模式下的布尔字段。只有 `mode="sequential"` 且 `repeat=True` 时，`repeat_until` 和 `max_iterations` 才生效。

```python
class ScopeGroup:
    scopes: list[Scope]
    mode: Literal["exclusive", "parallel", "sequential", "pooled"]
    # pooled 专用
    concurrency: int | None = None
    collect_key: str | None = None
    # sequential + repeat 专用
    repeat: bool = False
    repeat_until: Callable[[RunContext], bool] | None = None
    max_iterations: int | None = None
```

---

## 核心数据结构

### Scope

```python
class Scope:
    inputs: dict[str, Any]   # 显式传入子工作流的变量绑定
    inherit_parent: bool     # True：在 inputs 基础上继承父上下文全部变量
    outputs: list[str]       # 执行结束后提升到父上下文的变量键列表
    workflow: WorkflowDef    # 要执行的子工作流
```

### ScopeGroup

```python
class ScopeGroup:
    scopes: list[Scope]
    mode: Literal["exclusive", "parallel", "sequential", "pooled"]
    concurrency: int | None = None       # pooled：最大并发数
    collect_key: str | None = None       # pooled：父上下文收集结果的目标键
    repeat: bool = False                 # sequential：是否开启重复执行
    repeat_until: Callable | None = None # sequential+repeat：终止判断
    max_iterations: int | None = None    # sequential+repeat：最大轮次
```

---

## 数据流

### 变量传入（构建子 RunContext）

```python
def build_ctx(scope: Scope, parent_ctx: RunContext) -> RunContext:
    store = {}
    if scope.inherit_parent:
        store.update(parent_ctx.snapshot())   # 继承父上下文全部变量
    store.update(scope.inputs)                # 显式 inputs 覆盖同名父变量
    return RunContext(store)
```

### 结果写回规则

| 模式 | 写回策略 |
|------|---------|
| `exclusive` | 被运行的 Scope 的 outputs 直接提升到父上下文 |
| `parallel` | 每个 Scope 的 outputs 各自提升；键名冲突抛 `ScopeOutputConflictError` |
| `sequential`（非 repeat）| 最后一个 Scope 的 outputs 提升；中间 Scope 仅供相邻传递 |
| `sequential` + `repeat` | 每轮 outputs 提升，用于下轮条件判断 |
| `pooled` | 每个 Scope 的 outputs 按序收集为 `collect_key` 键下的列表 |

### sequential 模式的相邻变量传递

引擎在每个 Scope 执行完毕后，将其 outputs 合并注入下一个 Scope 的 inputs（追加/覆盖）。用户无需手动声明，引擎自动处理。

```
scope[0] outputs: {"step1.result": "A"}
    ↓ 引擎注入
scope[1] inputs 追加 {"step1.result": "A"}
scope[1] outputs: {"step2.result": "B"}
    ↓ 最后一个 scope 的 outputs 提升至父上下文
```

### pooled 收集示例

```
ScopeGroup(mode="pooled", collect_key="foreach_node.collected")
  scope[item=A] outputs: {"do_work.result": "x"}
  scope[item=B] outputs: {"do_work.result": "y"}
  scope[item=C] outputs: {"do_work.result": "z"}
           ↓
父上下文: "foreach_node.collected" = ["x", "y", "z"]
```

collect_key 由 ForEachNode 在创建 ScopeGroup 时指定（格式为 `"{node.id}.collected"`），Scope.outputs 只需声明子工作流内的输出键（`[node.collect_key]`），两者通过引擎的收集逻辑关联。

**pooled 模式下 Scope.outputs 的单键约束：**
pooled 模式中每个 Scope 只允许声明一个 outputs 键（`len(outputs) == 1`）。`collect_key` 是 ScopeGroup 上的单个目标键，若允许多 outputs 则需要多个 collect_key，接口复杂度剧增而收益有限。声明了多个 outputs 键的 Scope 在 pooled 模式下会在校验阶段抛 `ScopeConfigError`。

---

## 内置节点实现

### condition

```python
branch_key = str(await registry.call(node.handler, resolved_params))
scope = Scope(
    inputs={},
    inherit_parent=True,
    outputs=node.outputs,
    workflow=node.branches[branch_key],
)
return ScopeGroup(scopes=[scope], mode="exclusive")
```

未选中分支从不创建 Scope，BFS 跳过逻辑和菱形汇聚问题自然消失。

### foreach

```python
scopes = [
    Scope(inputs={node.item_var: item}, inherit_parent=False,
          outputs=[node.collect_key], workflow=node.sub_workflow)
    for item in ctx.resolve(node.items)
]
return ScopeGroup(scopes=scopes, mode="pooled",
                  concurrency=node.concurrency,
                  collect_key=f"{node.id}.collected")
```

### while

```python
scope = Scope(inputs={}, inherit_parent=True,
              outputs=node.outputs, workflow=node.sub_workflow)
return ScopeGroup(
    scopes=[scope], mode="sequential", repeat=True,
    repeat_until=lambda ctx: not ctx.eval_condition(node.condition),
    max_iterations=node.max_iterations,
)
```

---

## 原缺陷解决对照

| 原缺陷 | 解决方式 |
|--------|---------|
| condition BFS 跳过 + 菱形汇聚 | `exclusive` 模式只启动被选中的 Scope，未选中的从不创建 |
| foreach 强制顺序 | `pooled` 模式原生支持并发，`concurrency` 控制上限 |
| condition 只支持二元分支 | `branches` 是 `dict[str, WorkflowDef]`，支持任意具名分支 |
| while / foreach 上下文污染 | 显式 `outputs`，子 Scope 只能写回声明的变量 |
| 缺少节点生命周期 hook | 在 `DAGExecutor._run_scope_group` 的各模式入口处统一注入（待实现） |
| 缺少超时 / 重试 | 在 `Scope` 或 `ScopeGroup` 层面增加 `timeout` / `retry` 配置字段（待实现） |
| Wave 并发写 RunContext 竞态 | 并发 Scope 各自拥有独立 RunContext，不共享写入目标 |

---

## 待定问题

- `hook`（on_enter / on_exit）和 `timeout` / `retry` 的接口签名，留待实现阶段设计
- Scope 嵌套时 outputs 提升路径的层次规则（多层嵌套如何逐级写回）
- `parallel` 模式下 Scope 部分失败时的取消策略（全部取消 / 继续 / 等待完成后汇报）
- while 每轮 Scope 是复用同一对象还是每轮新建（影响 inputs 的 snapshot 时机）
