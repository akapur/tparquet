#!/usr/bin/env python3
"""Guard an Agent dispatch to an effort-suffixed role agent.

Three rules, scoped to `implementer|verifier|fixer`-`low|medium|high|xhigh` and to
`implementer-haiku`:

1. `model` must be explicit for an effort-suffixed agent. The Agent tool has no `effort`
   parameter, so effort is pinned in the agent definition and `model` is chosen per call.
   These agents therefore set no default model, and an omitted `model` would silently
   inherit one. Haiku is not a permitted value there: it runs only through
   `implementer-haiku`.
2. Only the main session dispatches a role agent. A PreToolUse payload carries
   `agent_type` when a subagent makes the call, and omits it for the main session. A role
   agent that orchestrates re-runs the whole protocol one level down; session c3cebef4
   ran one task's implementer/verifier/fixer cycle six times that way.
3. `implementer-haiku` pins its model. A `model` on the Agent call overrides the
   frontmatter, and the bare `haiku` alias resolves to Haiku 4.5, so any `model` is denied.

Every other `subagent_type` passes untouched, from the main session and from a subagent
alike, so a role agent keeps its Explore and general-purpose fan-out.
"""

import json
import re
import sys

GUARDED = re.compile(r"^((implementer|verifier|fixer)-(low|medium|high|xhigh)|implementer-haiku)$")
PINNED = "implementer-haiku"
VALID_MODELS = {"sonnet", "opus", "fable"}


def deny(reason: str) -> int:
    json.dump(
        {
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "permissionDecision": "deny",
                "permissionDecisionReason": reason,
            }
        },
        sys.stdout,
    )
    return 0


def main() -> int:
    payload = json.load(sys.stdin)
    if payload.get("tool_name") not in {"Agent", "Task"}:
        return 0

    tool_input = payload.get("tool_input") or {}
    subagent_type = tool_input.get("subagent_type", "")
    if not GUARDED.match(subagent_type):
        return 0

    caller = payload.get("agent_type")
    if caller:
        return deny(
            f"You are the `{caller}` subagent, and only the main session dispatches a "
            f"role agent. Do your own task with your own hands, then report. Dispatch "
            f"`Explore` or `general-purpose` subagents if you need help to read or to "
            f"search. If the task is underspecified, STOP and ask the user."
        )

    model = tool_input.get("model")
    if subagent_type == PINNED:
        return (
            0
            if model is None
            else deny(
                f"`{PINNED}` pins its model to Haiku 5.5. Re-dispatch without `model`; "
                f"the bare `haiku` alias resolves to Haiku 4.5."
            )
        )

    if model in VALID_MODELS:
        return 0

    return deny(
        f"`{subagent_type}` sets no default model. Re-dispatch with an explicit "
        f"`model` from {sorted(VALID_MODELS)}. "
        f"Consult .claude/skills/execute-plan/reference/CLAUDE-CODE-EFFORT-LADDER.md to pick the rung. "
        f"Haiku runs only through `{PINNED}`."
        if model is None
        else f"`{model}` is not a permitted model. Choose one of {sorted(VALID_MODELS)}, "
        f"or dispatch `{PINNED}` for a mechanical task."
    )


if __name__ == "__main__":
    sys.exit(main())
