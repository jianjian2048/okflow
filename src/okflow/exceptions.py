class DAGEngineError(Exception):
    """所有引擎异常的基类。"""


class WorkflowValidationError(DAGEngineError):
    """工作流静态校验失败。"""


class NodeExecutionError(DAGEngineError):
    """节点业务异常包装器。"""

    def __init__(self, node_id: str, cause: Exception) -> None:
        self.node_id = node_id
        self.cause = cause
        super().__init__(f"Node {node_id!r} failed: {cause}")


class MaxIterationsExceeded(DAGEngineError):
    """while 节点超出最大迭代次数。"""

    def __init__(self, node_id: str, max_iterations: int) -> None:
        self.node_id = node_id
        self.max_iterations = max_iterations
        super().__init__(f"Node {node_id!r} exceeded max iterations ({max_iterations})")


class UnknownHandlerError(DAGEngineError):
    """ActionRegistry 中找不到指定 handler。"""

    def __init__(self, handler_name: str) -> None:
        self.handler_name = handler_name
        super().__init__(f"Unknown handler: {handler_name!r}")


class ScopeOutputConflictError(DAGEngineError):
    """parallel 模式下多个 Scope 写入同一 output 键。"""

    def __init__(self, key: str) -> None:
        self.key = key
        super().__init__(f"Output key conflict in parallel scopes: {key!r}")


class ScopeConfigError(DAGEngineError):
    """Scope/ScopeGroup 配置错误（如 pooled 模式声明了多个 outputs）。"""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)
