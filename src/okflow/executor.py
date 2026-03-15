from __future__ import annotations

import asyncio
from typing import Any

from okflow.context import RunContext
from okflow.exceptions import (
    DAGEngineError,
    MaxIterationsExceeded,
    NodeExecutionError,
    ScopeConfigError,
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
                break

            results = await asyncio.gather(
                *[self._execute_node(node_map[nid], ctx) for nid in wave],
                return_exceptions=True,
            )

            for nid, result in zip(wave, results):
                if isinstance(result, BaseException):
                    raise result
                if result is not None:
                    # source_node_id passed as parameter (not stored in ScopeGroup)
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
        # 运行时防守：pooled 模式每个 Scope 必须恰好声明 1 个 output 键
        for scope in group.scopes:
            if len(scope.outputs) != 1:
                raise ScopeConfigError(
                    f"pooled scope must declare exactly one output, got {scope.outputs}"
                )

        sem = asyncio.Semaphore(group.concurrency) if group.concurrency else None

        async def run_one(scope: Scope) -> RunContext:
            if sem:
                async with sem:
                    return await self.run_with_parent(scope, parent_ctx)
            return await self.run_with_parent(scope, parent_ctx)

        child_ctxs = await asyncio.gather(*[run_one(scope) for scope in group.scopes])

        collected = []
        for scope, child_ctx in zip(group.scopes, child_ctxs):
            key = scope.outputs[0]
            collected.append(child_ctx.get(key))

        if group.collect_key is None:
            raise ScopeConfigError("pooled mode requires collect_key to be set")
        parent_ctx.set(group.collect_key, collected)


def _write_back(
    outputs: list[str], child_ctx: RunContext, parent_ctx: RunContext
) -> None:
    """将子上下文的指定 outputs 键写回父上下文。跳过子上下文中不存在的键（如条件分支未执行的输出）。"""
    snapshot = child_ctx.snapshot()
    for key in outputs:
        if key in snapshot:
            parent_ctx.set(key, snapshot[key])
