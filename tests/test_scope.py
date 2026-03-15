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
