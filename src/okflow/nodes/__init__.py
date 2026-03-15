from okflow.nodes.action import ActionNodeExecutor
from okflow.nodes.base import NodeExecutor
from okflow.nodes.condition import ConditionNodeExecutor
from okflow.nodes.foreach import ForEachNodeExecutor
from okflow.nodes.while_ import WhileNodeExecutor

_EXECUTORS: dict[str, NodeExecutor] = {
    "action": ActionNodeExecutor(),
    "condition": ConditionNodeExecutor(),
    "foreach": ForEachNodeExecutor(),
    "while": WhileNodeExecutor(),
}

__all__ = [
    "NodeExecutor",
    "ActionNodeExecutor",
    "ConditionNodeExecutor",
    "ForEachNodeExecutor",
    "WhileNodeExecutor",
    "_EXECUTORS",
]
