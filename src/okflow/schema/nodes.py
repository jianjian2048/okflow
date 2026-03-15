from __future__ import annotations

from typing import TYPE_CHECKING, Annotated, Literal, Union

from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from okflow.schema.workflow import WorkflowDef


class ActionNodeDef(BaseModel):
    type: Literal["action"] = "action"
    id: str
    handler: str
    params: dict[str, object] = Field(default_factory=dict)
    output_key: str


class ConditionNodeDef(BaseModel):
    type: Literal["condition"] = "condition"
    id: str
    handler: str
    params: dict[str, object] = Field(default_factory=dict)
    branches: dict[str, "WorkflowDef"]
    outputs: list[str] = Field(default_factory=list)


class ForEachNodeDef(BaseModel):
    type: Literal["foreach"] = "foreach"
    id: str
    items: str
    item_var: str
    collect_key: str
    concurrency: int | None = None
    sub_workflow: "WorkflowDef"


class WhileNodeDef(BaseModel):
    type: Literal["while"] = "while"
    id: str
    condition: str
    max_iterations: int = 100
    outputs: list[str] = Field(default_factory=list)
    sub_workflow: "WorkflowDef"


# 类型别名：判别联合
NodeDef = Annotated[
    Union[ActionNodeDef, ConditionNodeDef, ForEachNodeDef, WhileNodeDef],
    Field(discriminator="type"),
]


def _rebuild_all() -> None:
    """解析所有前向引用（在 workflow.py 末尾导入本模块后调用）。"""
    from okflow.schema.workflow import WorkflowDef  # noqa: PLC0415, F401

    ns = {"WorkflowDef": WorkflowDef}
    ConditionNodeDef.model_rebuild(_types_namespace=ns)
    ForEachNodeDef.model_rebuild(_types_namespace=ns)
    WhileNodeDef.model_rebuild(_types_namespace=ns)
