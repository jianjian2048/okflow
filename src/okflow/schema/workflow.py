from __future__ import annotations

from typing import TYPE_CHECKING, Annotated, Union

from pydantic import BaseModel, ConfigDict, Field

if TYPE_CHECKING:
    pass


class EdgeDef(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    from_: str = Field(alias="from")
    to: str


# WorkflowDef 引用 NodeDef，后者又引用 WorkflowDef，是递归结构。
# 用 TYPE_CHECKING + model_rebuild() 解析前向引用。
class WorkflowDef(BaseModel):
    id: str
    name: str
    nodes: list[
        Annotated[
            Union[
                "ActionNodeDef",
                "ConditionNodeDef",
                "ForEachNodeDef",
                "WhileNodeDef",
            ],
            Field(discriminator="type"),
        ]
    ]
    edges: list[EdgeDef]


# 导入放在模块末尾以避免循环引用
from okflow.schema.nodes import (  # noqa: E402
    ActionNodeDef,
    ConditionNodeDef,
    ForEachNodeDef,
    WhileNodeDef,
    _rebuild_all,
)

WorkflowDef.model_rebuild()
# 无论从哪个入口导入 workflow.py，确保所有含前向引用的模型都完成 rebuild
_rebuild_all()
