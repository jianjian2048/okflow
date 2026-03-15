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
