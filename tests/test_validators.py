import pytest

from okflow.exceptions import WorkflowValidationError
from okflow.schema.nodes import (
    ActionNodeDef,
    ConditionNodeDef,
    ForEachNodeDef,
    WhileNodeDef,
)
from okflow.schema.validators import validate_workflow
from okflow.schema.workflow import EdgeDef, WorkflowDef


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
    cond = ConditionNodeDef(id="c", handler="h", branches={"ok": branch_wf, "fail": branch_wf}, outputs=[])
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
    node = WhileNodeDef(id="wh", condition="$count < 10", outputs=["count"], sub_workflow=sub_wf)
    wf = _wf("w", nodes=[node])
    validate_workflow(wf)


def test_while_invalid_condition_raises():
    sub_wf = _wf("sub")
    node = WhileNodeDef(id="wh", condition="count < 10", outputs=["count"], sub_workflow=sub_wf)  # 缺少 $ 前缀
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
    node = ForEachNodeDef(id="fe", items="$items", item_var="x", collect_key="proc.result", sub_workflow=sub)
    wf = _wf("w", nodes=[node])
    validate_workflow(wf)  # 不应抛出异常


def test_foreach_collect_key_missing_raises():
    """collect_key 不存在于子工作流中，应抛 WorkflowValidationError。"""
    sub = _wf(
        "sub",
        nodes=[ActionNodeDef(id="proc", handler="h", output_key="result")],
    )
    node = ForEachNodeDef(id="fe", items="$items", item_var="x", collect_key="nonexistent.key", sub_workflow=sub)
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
        id="wh",
        condition="$inc.val < 5",
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
        id="c",
        handler="h",
        branches={"ok": empty_branch},
        outputs=["result.value"],  # 分支里没有产生 result.value 的节点
    )
    wf = _wf("w", nodes=[cond])
    with pytest.raises(WorkflowValidationError):
        validate_workflow(wf)
