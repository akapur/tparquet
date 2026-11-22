---
name: write-plan
description: "Sizing and format rules for writing a plan or PRD in this repo. Use whenever writing or revising a plan under docs/plans/ or a PRD."
---

# Writing plans and PRDs

When writing a plan or a prd:

- Ensure the plan is fine grained enough that no single task uses over 100K tokens when executed or verified by a fresh subagent.
- Ensure an orchestrator agent, orchestrating the execution of the plan will not use over 150K tokens. If it will, break up the plan into multiple sub-plans.
- Ensure the plan is detailed enough and has enough context that a subagent can execute the task without making any non-trivial judgements. If something needs to be censused, checked, grilled or otherwise decided, do so at planning time.
- No line numbers in plans. Instead leave a quoted string that can be grepped. Line numbers can change if multiple plans are executed in sequence or even during the execution of a single plan.
- When writing a plan (not a PRD), the plan must be in the format described by `.claude/skills/execute-plan/reference/PLAN-FORMAT.md`.
