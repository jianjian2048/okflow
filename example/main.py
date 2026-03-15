"""
okflow 入门示例：学生成绩批量处理
====================================

场景描述
--------
从数据源获取一批学生成绩，并发为每个分数评级，然后根据及格率做分支处理：

    fetch_scores  (action)    → 获取成绩列表 [85, 42, 90, 55, 78, 30]
         ↓
    grade_each    (foreach)   → 并发打等级（A/B/C/D/F），concurrency=2
         ↓
    check         (condition) → 判断及格率
      分支 "ok"  → publish_report (action)  — 直接发布报告
      分支 "low" → notify_admin  (action)   — 通知管理员
                   poll_ack      (while)    — 轮询直到管理员确认

本文件展示四种节点类型的完整用法：action / foreach / condition / while

运行方式：
    uv run python example/main.py
    python example/main.py
"""

import asyncio

from okflow import (
    ActionNodeDef,
    ActionRegistry,
    ConditionNodeDef,
    DAGExecutor,
    EdgeDef,
    ForEachNodeDef,
    Scope,
    WhileNodeDef,
    WorkflowDef,
    validate_workflow,
)

# ── 1. 注册 Action Handlers ───────────────────────────────────────────────────
# Handler 是普通的 async 函数，参数名与工作流 params 中的键一一对应。

registry = ActionRegistry()


async def fetch_scores() -> list[int]:
    """模拟从数据源获取学生成绩列表。"""
    print("[fetch] 获取成绩列表")
    return [85, 42, 90, 55, 78, 30]


async def grade_score(score: int) -> str:
    """将分数转换为等级字母（A/B/C/D/F）。"""
    if score >= 90:
        letter = "A"
    elif score >= 80:
        letter = "B"
    elif score >= 70:
        letter = "C"
    elif score >= 60:
        letter = "D"
    else:
        letter = "F"
    print(f"  [grade] {score} → {letter}")
    return letter


async def check_pass_rate(grades: list[str]) -> str:
    """计算及格率（D 及以上视为及格）。低于 60% 返回 'low'，否则返回 'ok'。"""
    pass_count = sum(1 for g in grades if g != "F")
    rate = pass_count / len(grades)
    result = "low" if rate < 0.6 else "ok"
    print(f"[check] 及格率 {pass_count}/{len(grades)} = {rate:.0%} → 分支: '{result}'")
    return result


async def notify_admin() -> str:
    """发送管理员通知。"""
    print("[notify] 通知管理员：成绩及格率偏低")
    return "notified"


def _make_poll_handler():
    """工厂：创建带状态的轮询 handler（第 2 次调用才返回 'acked'）。"""
    call_count = 0

    async def poll_status() -> str:
        nonlocal call_count
        call_count += 1
        status = "acked" if call_count >= 2 else "pending"
        print(f"  [poll] 第 {call_count} 次轮询 → {status}")
        return status

    return poll_status


async def publish_report(grades: list[str]) -> str:
    """发布成绩报告。"""
    counts = {g: grades.count(g) for g in ["A", "B", "C", "D", "F"]}
    print(f"[report] 成绩分布：{counts}")
    return "done"


registry.register("scores.fetch", fetch_scores)
registry.register("scores.grade", grade_score)
registry.register("scores.check_pass_rate", check_pass_rate)
registry.register("scores.publish_report", publish_report)
registry.register("admin.notify", notify_admin)
registry.register("admin.poll_status", _make_poll_handler())


# ── 2. 定义工作流（从内到外，避免前向引用）──────────────────────────────────

# --- foreach 子工作流：对单个成绩打等级 ---
grade_sub_workflow = WorkflowDef(
    id="grade_sub",
    name="打等级",
    nodes=[
        ActionNodeDef(
            id="grade",
            handler="scores.grade",
            params={"score": "$score"},  # "$score" 由 foreach 的 item_var 注入
            output_key="letter",  # 每次迭代写入 grade.letter
        )
    ],
    edges=[],
)

# --- while 子工作流：单次轮询 ---
poll_sub_workflow = WorkflowDef(
    id="poll_sub",
    name="单次轮询",
    nodes=[
        ActionNodeDef(
            id="poll",
            handler="admin.poll_status",
            params={},
            output_key="status",  # 写入 poll.status
        )
    ],
    edges=[],
)

# --- condition 分支 "low"：通知管理员 + while 轮询确认 ---
low_branch = WorkflowDef(
    id="low_wf",
    name="及格率偏低处理",
    nodes=[
        ActionNodeDef(
            id="notify_admin",
            handler="admin.notify",
            params={},
            output_key="done",
        ),
        WhileNodeDef(
            id="poll_ack",
            # condition 是"继续循环的条件"：为真时继续，为假时退出
            condition="$poll.status != 'acked'",
            outputs=["poll.status"],  # 声明哪些变量从循环体写回父上下文
            max_iterations=3,
            sub_workflow=poll_sub_workflow,
        ),
    ],
    edges=[EdgeDef(from_="notify_admin", to="poll_ack")],
)

# --- condition 分支 "ok"：直接发布报告 ---
ok_branch = WorkflowDef(
    id="ok_wf",
    name="及格率正常处理",
    nodes=[
        ActionNodeDef(
            id="report",
            handler="scores.publish_report",
            params={"grades": "$grade_each.collected"},  # 引用父上下文的收集结果
            output_key="done",
        )
    ],
    edges=[],
)

# --- 顶层工作流 ---
workflow = WorkflowDef(
    id="grade_pipeline",
    name="学生成绩批量处理",
    nodes=[
        # 节点 1（action）：获取成绩列表
        ActionNodeDef(
            id="fetch",
            handler="scores.fetch",
            params={},
            output_key="data",  # 写入 fetch.data
        ),
        # 节点 2（foreach）：并发打等级
        # collect_key="grade.letter" 是子工作流每次迭代产出的键名
        # 引擎自动将所有迭代结果收集为 grade_each.collected（列表）
        ForEachNodeDef(
            id="grade_each",
            items="$fetch.data",  # 引用 fetch.data 作为迭代列表
            item_var="score",  # 每次迭代将 item 绑定到 $score
            collect_key="grade.letter",  # 子工作流每次迭代的产出键名
            concurrency=2,  # 最多同时执行 2 个子工作流
            sub_workflow=grade_sub_workflow,
        ),
        # 节点 3（condition）：根据及格率分支
        # outputs 使用 union 语义：每个键至少在一个分支中存在即可通过校验
        ConditionNodeDef(
            id="check",
            handler="scores.check_pass_rate",
            params={"grades": "$grade_each.collected"},
            branches={"low": low_branch, "ok": ok_branch},
            outputs=["poll.status", "report.done"],
        ),
    ],
    edges=[
        EdgeDef(from_="fetch", to="grade_each"),
        EdgeDef(from_="grade_each", to="check"),
    ],
)


# ── 3. 静态校验（推荐在执行前调用）──────────────────────────────────────────
validate_workflow(workflow)


# ── 4. 运行工作流 ─────────────────────────────────────────────────────────────
async def main() -> None:
    print("=" * 50)
    print("okflow 示例：学生成绩批量处理")
    print("=" * 50)

    root_scope = Scope(workflow=workflow)
    ctx = await DAGExecutor(registry).run(root_scope)

    print()
    print("── 最终结果 ──")
    print(f"等级列表: {ctx.get('grade_each.collected')}")


asyncio.run(main())
