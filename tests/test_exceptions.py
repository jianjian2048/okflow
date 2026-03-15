from okflow.exceptions import (
    DAGEngineError,
    MaxIterationsExceeded,
    NodeExecutionError,
    ScopeConfigError,
    ScopeOutputConflictError,
    UnknownHandlerError,
    WorkflowValidationError,
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
