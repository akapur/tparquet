# CLAUDE-CODE-EFFORT-LADDER — Claude Code subscription dispatch

Use this table to choose a Claude subagent. Cost means **subscription quota burn**, estimated from Anthropic's API price ratios because Anthropic does not publish exact per-model subscription weights.

## How to dispatch a rung

The Agent tool has no `effort` parameter, so a rung needs both halves from two places:

- **Effort** comes from the agent definition. The role agents in `.claude/agents/` are
  `implementer-<effort>`, `verifier-<effort>`, and `fixer-<effort>`, for `low`, `medium`,
  `high`, and `xhigh`. Pass the name as `subagent_type`.
- **Model** comes from the caller. Pass `model` on every Agent call. Permitted values:
  `sonnet`, `opus`, `fable`.
- **Haiku is the exception.** `implementer-haiku` pins both halves, `claude-haiku-5-5` at
  high effort. Dispatch it with no `model`: an override replaces the pin, and the bare
  `haiku` alias resolves to Haiku 4.5. There is no Haiku verifier or fixer.

The two axes are independent, so an escalation may move either one. A jump from
`sonnet + high` (T3) to `opus + medium` (T4) costs more per token but usually less per task.

The role agents set no default model. `.claude/hooks/guard-role-agent-dispatch.py` denies a
dispatch that omits `model`, that names Haiku, or that passes `model` to `implementer-haiku`.
There is no `max` role agent, because the table has no `max` rung.

| Tier | Model + effort | Input burn / MTok | Output-burn index | Taste | Quality | Dispatch when |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| T0 | `implementer-haiku` (Haiku 5.5 + high) | 0.1 | 0.5 | 7 | 5 | **Every simple mechanical implementation task.** Implementer only |
| T1 | `sonnet` + low | 2 | 5 | 7 | 6 | Small, well-scoped tasks that are not purely mechanical; cheap fan-out |
| T2 | `sonnet` + medium | 2 | 7.5 | 6 | 7 | Routine implementation against a tight spec |
| T3 | `sonnet` + high | 2 | 10 | 6 | 8 | Broad search, context-heavy exploration |
| T4 | `opus` + medium | 4 | 15 | 8 | 9 | **Default workhorse.** Implementation, verification, triage, integration |
| T5 | `opus` + high | 4 | 20 | 8 | 9 | Hard design, subtle debugging, final review |
| T6 | `opus` + xhigh | 4 | 35 | 7 | 9 | The single hardest correctness-critical task |
| T6a | `fable` + low | 10 | 25* | 9 | 8 | A second-model verdict when two Opus verifications disagree; nothing else |

\* Fable also consumes its separate allowance, capped at half of the weekly Claude Code quota.

A task is **simple and mechanical** when the plan names every file and every edit, the
change needs no design choice and no debugging, and a command it names proves the work
done: a rename, a fully specified edit replicated across files, a moved block. Dispatch every
such task to T0. One that fails those tests goes to T1 or above. If `implementer-haiku` stops
and reports that the task needs judgement, re-dispatch it on a stronger rung; do not retry
Haiku. Its work is verified and fixed on the ordinary rungs.

Sonnet's output-burn index understates its cost. Sonnet spends more tokens per task than Opus, so at high effort and above it can cost more per task than Opus. When a task is not clearly small and well-scoped, dispatch `opus + medium`, not Sonnet.

Fable is dominated: Opus beats it on every published benchmark at less than half the price. Do not dispatch Fable for implementation, design or review.

Omitted combinations are not automatically dominated; they lack a reliable default niche. Add `opus + low`, `sonnet + xhigh` or another Fable rung only when representative tasks show lower total quota-to-result. Haiku runs only through `implementer-haiku`; never pass `haiku` as a `model`.

Complex judgements should be made at planning time, not execution time.

Pricing, assumptions and rating priors behind this table: `CLAUDE-CODE-EFFORT-LADDER-RATIONALE.md`.
