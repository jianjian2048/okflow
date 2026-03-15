# 设计规格：使用文档与入门示例

## 概述

为 okflow 库编写两份产出物：
1. `document/usage.md` — 面向使用者和贡献者的完整使用文档
2. `example/main.py` + `example/workflow.json` — 可运行的入门示例

---

## 产出物一：`document/usage.md`

### 目标读者

- **使用者**（外部接入方）：了解如何定义工作流、注册 handler、运行引擎
- **贡献者**（内部开发者）：了解如何扩展节点类型

### 文档结构（七节）

#### 1. 快速入门

五步入门流程：
1. 安装：`uv add okflow` / `pip install okflow`
2. 注册 handler：`ActionRegistry` + `registry.register()`
3. 定义工作流：Python 对象方式（`WorkflowDef`、`ActionNodeDef` 等）
4. 运行：`DAGExecutor(registry).run(scope)`
5. 读取结果：`ctx.get("node_id.output_key")`

配套一个最小完整示例（约 20 行），可直接复制运行。

#### 2. 核心概念

覆盖四个核心对象，每个用一段话说明职责：
- **WorkflowDef**：节点列表 + 有向边列表，描述执行拓扑
- **Scope**：带 I/O 边界的执行单元（`inputs`、`outputs`、`inherit_parent`、`workflow`）
- **RunContext**：执行期变量存储，支持 `$ref` 解析和条件 DSL
- **ActionRegistry**：引擎与业务逻辑的唯一边界，注册异步 handler 函数

#### 3. 节点类型详解

每种节点各一小节，统一格式：简介 → Python 写法 → 等价 JSON → 关键字段说明

- **ActionNode**：调用 handler，结果写入 `{id}.{output_key}`
- **ConditionNode**：handler 返回分支键（字符串），路由到 `branches[key]` 对应子工作流；
  `outputs` 声明写回父上下文的变量键（只需在至少一个分支中存在即可）
- **ForEachNode**：为每个 item 创建独立隔离 Scope 并运行子工作流；
  `collect_key` 是子工作流每次迭代产出的**单项结果键名**（如 `"grade.value"`）；
  所有迭代结果按顺序收集为 `{foreach_node_id}.collected` 列表写入父上下文；
  支持 `concurrency` 限制并发数。
- **WhileNode**：`condition` 字段为**继续循环的条件**（condition 为真时继续，为假时退出），支持受限 DSL；
  `outputs` 声明循环体每轮执行后写回父上下文的变量键；
  超出 `max_iterations` 时抛 `MaxIterationsExceeded`

#### 4. 静态校验

说明 `validate_workflow()` 的五项检查：
1. 节点 ID 唯一性
2. 无环检测（Kahn 算法）
3. condition 分支目标存在
4. while condition DSL 语法合法
5. outputs 引用的变量键在子工作流中存在

附常见错误示例 + 对应 `WorkflowValidationError` 信息。

#### 5. 异常处理

异常层次树 + 典型捕获模式：
- `WorkflowValidationError`：校验失败
- `NodeExecutionError`：节点业务异常（含 `node_id` 和 `cause`）
- `MaxIterationsExceeded`：while 超出上限
- `UnknownHandlerError`：handler 未注册
- `ScopeOutputConflictError`：parallel 模式键名冲突

#### 6. API 参考

关键类和方法的参数说明（表格形式）：
- `WorkflowDef(id, name, nodes, edges)`
- `EdgeDef(from_=..., to=...)` — Python 侧用 `from_`，JSON 侧用 `"from"`（alias）
- `ActionNodeDef(id, handler, params, output_key)`
- `ConditionNodeDef(id, handler, params, branches, outputs)`
- `ForEachNodeDef(id, items, item_var, collect_key, sub_workflow, concurrency=None)`
  - `collect_key`：子工作流每次迭代的产出键名（非父上下文收集键）
  - 父上下文收集键固定为 `{node_id}.collected`
- `WhileNodeDef(id, condition, outputs, max_iterations, sub_workflow)`
  - `condition`：**继续循环的条件**（True → 继续，False → 退出）
- `Scope(workflow, inputs={}, outputs=[], inherit_parent=False)`
- `DAGExecutor(registry).run(scope)` → `RunContext`
- `RunContext.get(key)` / `.set(key, value)` / `.snapshot()`
- `validate_workflow(workflow)`

#### 7. 扩展指南

三步添加自定义节点类型：
1. 定义 `NodeDef` 子类（Pydantic model，`type` 字段为 Literal）
2. 实现 `NodeExecutor` 子类（`execute()` 返回 `ScopeGroup | None`）
3. 注册到 `_EXECUTORS` 分发表

附最小代码示例（约 30 行）。

---

## 产出物二：`example/main.py` + `example/workflow.json`

### 场景：学生成绩批量处理

选择原因：数据直观，逻辑清晰，能自然覆盖四种节点类型，无需外部依赖。

### 工作流拓扑（修正版）

`while` 节点放在 condition 的 "low" 分支内，确保可达性：

```
父工作流：
  fetch_scores (action)
      ↓
  grade_each (foreach, concurrency=2)
      ↓
  check_pass_rate (condition)
      branches:
        "low" → 子工作流 A：
                  notify_admin (action) + wait_ack (while)
                  while condition: $notify.status != "acked"
                  max_iterations=3
        "ok"  → 子工作流 B：
                  publish_report (action)
```

这样 `while` 节点自然出现在 condition 分支的子工作流内，逻辑通畅。

### `example/main.py` 规格

- 约 120 行，含注释
- 文件顶部有场景说明注释
- handler 全部用纯 Python 实现（无外部依赖），数据完全内嵌：
  - `fetch_scores`：返回硬编码列表 `[85, 42, 90, 55, 78, 30]`
  - `grade_score`：将分数转为等级（A/B/C/D/F），返回等级字符串
  - `calc_pass_rate`：计算及格率，返回 `"low"` 或 `"ok"`
  - `notify_admin`：模拟通知，第一次调用返回 `"pending"`，第二次返回 `"acked"`（用闭包实现状态）
  - `publish_report`：打印报告，返回 `"done"`
- 运行入口：`asyncio.run(main())`
- 展示 Python 对象方式定义工作流
- 最后打印最终结果

### `example/workflow.json` 规格

- 与 `main.py` 中完全相同的工作流，用 JSON 格式表达
- 涵盖所有四种节点类型
- `EdgeDef` 的 from 字段在 JSON 中写 `"from"`（alias）
- JSON 文件末尾附加载方式注释（JSON 不支持注释，改为附在文件外 — 作为文件开头的说明字段不添加，这部分在 usage.md 中说明）

---

## 不做的事

- 不修改现有 `document/dag-engine-design.md`（保持原样）
- 不添加新功能或修改引擎代码
- 不创建 `example/README.md`（用户选择了方案一，不含独立 README）
- 不覆盖集成测试中已有的场景（示例场景独立）
