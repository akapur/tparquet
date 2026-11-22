---
name: execute-plan
description: Orchestrate execution of a multi-task plan in this repo through the implementer/verifier/fixer role agents in .claude/agents/. For the main session only, when the user asks to execute a plan. Do not self-invoke: if you are a subagent that was dispatched to implement, verify, or fix one task, do that one task yourself and never invoke this skill.
---

# Execute a plan

You are the orchestrator. You dispatch every task. You wait for each role agent to
return. You decide the next step from its report. No role agent orchestrates.

Dispatch every task through a role agent in `.claude/agents/`. The names are
`implementer-<effort>`, `verifier-<effort>`, and `fixer-<effort>`, where `<effort>` is
`low`, `medium`, `high`, or `xhigh`. The one exception is `implementer-haiku`, which
pins Haiku 5.5 at high effort.

The Agent tool has no `effort` parameter. The agent file pins the effort. You pass the
model on every call.

- Pick the rung from `.claude/skills/execute-plan/reference/CLAUDE-CODE-EFFORT-LADDER.md`. Each row pairs a
  model with an effort.
- Pass `model` on every call to an effort-suffixed agent. Permitted values: `sonnet`, `opus`, `fable`. The
  `PreToolUse` hook at `.claude/hooks/guard-role-agent-dispatch.py` denies a dispatch that
  omits `model`. Haiku is not a permitted `model` value.
- A task on T0 goes to `implementer-haiku` with **no** `model`. The hook denies one that
  passes `model`. The plan, not the orchestrator, puts a task on T0.
- A plan names one rung per task: the first implementer's. Every later rung follows from
  it. Each verifier runs one rung above the agent whose work it checks, and each fixer
  runs on the rung of the verifier whose findings it fixes. A T2 task therefore runs
  implementer T2, verifier T3, fixer T3, verifier T4, fixer T4, verifier T5.
- The climb stops at T6. A verifier or fixer that would land above T6 runs at T6. T6a is
  never a step in the climb: it only tie-breaks two Opus verifications that disagree.

## Sequence

1. Dispatch a fresh implementer on the plan's rung, `implementer-<effort>` or
   `implementer-haiku`, to execute the task.
2. Dispatch a fresh `verifier-<effort>` to verify the implementer's work. A verifier is
   **read-only**. It changes nothing and fixes nothing. It reports findings, each with
   one severity: `blocker`, `major`, `minor` or `nit`.
3. A verifier that reports `pass` ends the task.
4. **Any** finding, down to a single `nit` → a fresh `fixer-<effort>` fixes all of them,
   then a fresh `verifier-<effort>` checks the result.
5. The second verifier reports findings → a second fixer, then a third verifier.
6. The third verifier still reports a finding above `nit` → **STOP and get a human.** An
   open `nit` at that point goes in the report instead. Three passes is the cap.
7. Run one task at a time. Never run two role agents at once. See **One task at a time**
   below.

The verifier fixes nothing because a fix it makes itself is the one change on the branch
that nobody verifies. That is not hypothetical: a verifier once logged "nits fixed" for a
change that rewrote a method signature into a generic and moved one file from 84 pyright
errors to 15. Correct, as it happens, and unreviewed.

The verifier files enforce this in frontmatter, not by instruction alone: `tools` lists
only `Read, Bash, Agent`, and `disallowedTools` removes `Edit, Write, NotebookEdit`.
Either key alone is sufficient, and both are set so that a change to one mechanism
cannot silently restore write access.

**Bash is still a write path.** A read-only tool set does not stop `sed -i` or a shell
redirect, so the verifier body carries the rule in words as well. Claude Code caches
agent definitions, and an edit to an agent file can take several dispatches to land. So
after any change to these files, confirm it took: ask the first verifier of the session
to list its own tools, and check that `Edit` and `Write` are absent.

## The orchestrator is the only orchestrator

Every dispatch in the sequence above comes from you, in the main session. A role agent
is a leaf. It implements, verifies, or fixes its one task with its own hands.

A role agent may dispatch an `Explore` or a `general-purpose` subagent to read files or
to search. It must never dispatch an `implementer-*`, a `verifier-*`, or a `fixer-*`
agent. The `PreToolUse` hook denies that dispatch, so a role agent that tries it loses a
turn to a rejection.

This rule exists because of a real failure where every dispatched
implementer read the protocol, treated it as its own instruction, and re-ran the whole
sequence one level down. One task ran the implementer/verifier/fixer cycle six times.

## One task at a time

Dispatch one role agent. Wait for it. Then dispatch the next. **Never run two role
agents at once**, even when the plan says two tasks touch no common file.

Role agents share this one working tree and cannot see each other, so a concurrent pair
can act on each other's uncommitted work. A fan-out of seven tasks produced three such
incidents in one stage:

- One implementer ran `git stash` to read the base version of its own file. It stashed
  six other agents' uncommitted work with it, and survived only because the pop restored
  everything.
- One verifier read another agent's in-flight edit as stray churn and proposed to revert
  it.
- Two tasks turned out to name the same file, because one task's file list ended in a
  catch-all ("whatever else remains under `tests/`").

Serial costs wall-clock time and buys back the whole class.

## Role agents commit their own work

The implementer commits its task. The fixer commits its fix. You do not stage or commit
for them. The agent knows exactly which files it touched, and you would have to
reconstruct that from its report. The verifier commits nothing, because it changes
nothing: its `disallowedTools` frontmatter removes `Edit`, `Write` and `NotebookEdit`.

The index race that once forbade this only existed under a parallel fan-out. Serial
removes it.

- The agent bodies also forbid amend, rebase, force and push. Every agent commit is
  additive, so history only ever grows during a run.
- **Check that the tree is clean between tasks.** `git status --short` after each agent
  returns. A dirty tree means the agent left work behind, and the next task would sweep
  it into its own commit.
- A task can therefore land more than one commit: the implementer's, then a fixer's.
  That is the honest record. Do not squash it into one.
- A late report can describe a tree that has moved on since. Check what has landed
  before you act on one.

## Standing instructions

- `.claude/skills/execute-plan/reference/PLAN-FORMAT.md` specifies the form a plan this skill executes takes. Read
  it before authoring or amending one, and hold a plan you dispatch from to it.
- Subagent instructions stay thin: _"execute task <number> of <plan_file_name>; the plan
  file is authoritative; report a ≤20-line summary"_. Add context only where the session
  learned something the plan does not say.
- The agent bodies already carry the piping rule and the STOP rule. Do not restate them
  in the dispatch prompt.
- Subagents pipe test output to a file. They tail or grep that file. They never dump full
  output into context.
- A verifier that makes no changes may trust the implementer's test output file. If that
  file shows that every test passed, the verifier does not re-run the suite.
- A subagent that finds its task underspecified must **STOP and ask the user**. It must
  not decide. Enforce this. Two subagents previously made silent judgment calls, and one
  was wrong.
