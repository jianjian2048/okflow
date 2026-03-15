# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

`okflow` 是一个通用异步 DAG 工作流执行引擎 Python 库，核心设计文档在 `document/dag-engine-design.md`。引擎与业务逻辑完全解耦，通过 `ActionRegistry` 插件机制对接外部系统。

## 常用命令

```bash
# 安装依赖
uv sync

# 运行测试
uv run pytest

# 运行单个测试文件
uv run pytest tests/path/to/test_file.py

# 运行单个测试
uv run pytest tests/path/to/test_file.py::test_function_name

# 类型检查
uv run mypy src/

# 代码格式化
uv run ruff format src/ tests/

# Lint 检查
uv run ruff check src/ tests/
```

## 架构设计

引擎分为三层：

### Schema 层（工作流定义）
- `WorkflowDef`：顶层工作流，包含 `nodes` 列表和 `edges` 列表
- `EdgeDef`：有向边 `from → to`，定义节点执行依赖关系（节点本身不内嵌依赖）
- `NodeDef`：判别联合，按 `type` 字段分发到四种子类型：
  - `action`：调用 ActionRegistry 中注册的 handler，结果写入 `{node_id}.{output_key}`
  - `condition`：handler 返回 bool，BFS 级联标记落败分支的所有下游节点为跳过
  - `foreach`：顺序迭代，每次迭代创建**独立子上下文**（防止变量污染）
  - `while`：共享父上下文循环（子工作流需能修改条件变量），有 `max_iterations` 上界

### Engine 层（执行调度）
- `DAGExecutor`：Kahn 算法 + Wave 并发，`asyncio.gather` 并发执行同一 wave 的就绪节点
- `RunContext`：执行上下文，负责变量存储、`$ref` 解析、条件 DSL 求值、跳过标记
  - 变量引用格式：`$node_id.output_key`（参数中 `$` 前缀触发解析）
  - 条件 DSL（不用 eval）：`$ref is null`、`$ref is not null`、`$ref OP value`
- `_drain_skipped`：不动点迭代，批量处理 condition 标记的跳过节点，无需额外 wave

### 扩展层（业务侧）
- `ActionRegistry`：引擎与外部系统的唯一边界，`registry.call(handler_name, params)` 展开为关键字参数调用
- 新增节点类型：定义 `NodeDef` 子类 → 实现 `NodeExecutor` 子类 → 注册到 `_EXECUTORS` 分发表

## 关键约定

- `foreach` / `while` 内部的 `DAGExecutor` 需**延迟导入**，避免 `executor.py ↔ nodes/` 循环导入
- `ForEachNodeDef` 和 `WhileNodeDef` 内嵌 `sub_workflow` 形成递归结构，需调用 `model_rebuild()` 解析前向引用
- 异常层次：`DAGEngineError` 为基类；节点业务异常包装为 `NodeExecutionError(node_id, cause)`；引擎内部预期异常直接透传
- 静态校验（`validate_workflow`）在执行前检查：节点 ID 唯一性、无环（Kahn）、condition 分支目标存在、while 条件语法合法，均递归校验子工作流

## 技术栈

- Python ≥ 3.12，包管理使用 `uv`（`uv_build` 构建后端）
- Pydantic 用于 Schema 反序列化与校验
- `asyncio` 原生异步，无第三方调度库依赖
