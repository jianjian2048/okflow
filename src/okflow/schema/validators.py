from __future__ import annotations

import re

from okflow.exceptions import WorkflowValidationError
from okflow.schema.nodes import (
    ActionNodeDef,
    ConditionNodeDef,
    ForEachNodeDef,
    WhileNodeDef,
)
from okflow.schema.workflow import WorkflowDef

# 合法的条件表达式正则（必须以 $ 开头）
_VALID_CONDITION = re.compile(r"^\$\S+\s+(is\s+null|is\s+not\s+null|(==|!=|<=|>=|<|>)\s+.+)$")


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
        raise WorkflowValidationError(f"Duplicate node IDs in workflow {workflow.id!r}: {ids}")
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
            raise WorkflowValidationError(f"Edge references unknown node: {edge.from_!r} -> {edge.to!r}")
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
        raise WorkflowValidationError(f"Cycle detected in workflow {workflow.id!r}")

    for node in workflow.nodes:
        if isinstance(node, ConditionNodeDef):
            for branch_wf in node.branches.values():
                _check_no_cycle(branch_wf)
        elif isinstance(node, (ForEachNodeDef, WhileNodeDef)):
            _check_no_cycle(node.sub_workflow)


def _check_condition_branches(workflow: WorkflowDef) -> None:
    for node in workflow.nodes:
        if isinstance(node, ConditionNodeDef):
            for branch_wf in node.branches.values():
                _check_condition_branches(branch_wf)
        elif isinstance(node, (ForEachNodeDef, WhileNodeDef)):
            _check_condition_branches(node.sub_workflow)


def _check_while_conditions(workflow: WorkflowDef) -> None:
    for node in workflow.nodes:
        if isinstance(node, WhileNodeDef):
            if not _VALID_CONDITION.match(node.condition.strip()):
                raise WorkflowValidationError(
                    f"Invalid while condition expression in node {node.id!r}: {node.condition!r}. Must start with $ref."
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
            keys.update(node.outputs)
        elif isinstance(node, ForEachNodeDef):
            keys.add(f"{node.id}.collected")
        elif isinstance(node, WhileNodeDef):
            keys.update(node.outputs)
    return keys


def _check_outputs_referenced(workflow: WorkflowDef) -> None:
    """第⑤项：校验 outputs 中声明的变量键在对应子工作流中确实能被某个节点写入。"""
    for node in workflow.nodes:
        if isinstance(node, ConditionNodeDef):
            # 合并所有分支的可写键：每个 output 键只需在至少一个分支中可写即可
            all_branch_writable: set[str] = set()
            for branch_wf in node.branches.values():
                all_branch_writable.update(_get_writable_keys(branch_wf))
            for out_key in node.outputs:
                if out_key not in all_branch_writable:
                    raise WorkflowValidationError(
                        f"ConditionNode {node.id!r} declares output {out_key!r} "
                        f"but no branch has a node that writes it. "
                        f"Writable keys across all branches: {sorted(all_branch_writable)}"
                    )
            for branch_wf in node.branches.values():
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
                # 只校验 node_id.output_key 格式的引用；裸变量名由运行时处理
                if "." in out_key and out_key not in sub_writable:
                    raise WorkflowValidationError(
                        f"WhileNode {node.id!r} declares output {out_key!r} "
                        f"but sub_workflow has no node that writes it. "
                        f"Writable keys: {sorted(sub_writable)}"
                    )
            _check_outputs_referenced(node.sub_workflow)
