# Plan Format

Shared contract separating a plan that survives its own execution from a plan
that rots the day after it is written. Any multi-task plan, and any document
that cites one, is held to it — cite this, don't re-state.

A plan's job is to hand a fresh executor enough context to act without
re-deriving a decision the plan already made. Two things destroy that: a
citation that no longer points at what it claims, and an authoring habit that
leaves the next agent a judgement call the plan should have settled. The rules
below are about those two.

## 1. Cite by quoted string, never by line number

A plan cites another file by a quoted string the reader can search for, never by
a line number. A line number is stale the next time anything above it changes,
and it fails **silently**: the citation still points somewhere, just at the wrong
thing. A quoted string that no longer matches fails **loudly**, which is the
behaviour a plan wants.

This is measured, not theoretical. A `grep -oE` scan for `<file>:<line>` anchors
across the three sibling plans `docs/plans/executed/2026-09-15-P2-worktree-verbs.md`,
`docs/plans/executed/2026-09-15-P3-engine-review-gate.md` and
`docs/plans/2026-09-15-P4-the-sweep.md`, run on 2026-09-21 at commit `08b70e2b`,
found 67 line-anchored citations, split 30 in P2, 12 in P3 and 25 in P4. Read by
hand at that same commit, 29 of the 67 resolved onto unrelated text, and three
obligations named a target section that did not contain the thing the obligation
described. None of those failures announced itself; each had to be found by
hand.

Quote enough of the target's text to be unique. If the text you want to point at
is not quotable — a blank line, a whole section — name the section heading, which
is itself a quotable string.

## 2. Standing authoring rules

These bind any plan in any repo. Each names what it forbids and why.

1. **Sentinels, never count floors.** A non-vacuity guard plants a sentinel the
   guard must find. A count floor plus a comment explaining the count is a
   ledger that drifts every time the real count moves, and it stops proving
   anything the day someone bumps the number to make it pass.

2. **No allowlist ledgers in ratchets.** A rule that needs a curated list of
   exemptions is a rule that rots: the list grows, nobody re-derives it, and the
   exemptions outlive their reasons. Prefer a narrower rule with zero
   exemptions.

3. **A census scans whole text, never line by line.** What a plan counts can be
   wrapped across a line break, so a line-oriented search is blind to it. Scan
   the whole text, or say plainly in the plan that the count is line-based and
   therefore a floor.

4. **A task that cannot be green alone was cut wrongly.** If a task can only
   pass the repo's gates once a sibling task also lands, the boundary is in the
   wrong place. Report it as a plan defect rather than committing red and
   promising to fix it later.

5. **A task that cannot converge alone was cut wrongly.** Greenness is not the
   only test. A task that is green after every fix round and still cannot close
   is bundling several changes, or is holding a design decision that execution
   keeps re-opening. Cut one file or one decision per task, state the closing
   condition as a measurement, and cap the review rounds.

6. **No task over 100K tokens when executed or verified by a fresh subagent.**
   A task is read by an agent with no memory of the session that wrote it.
   Past that budget the agent starts dropping the earlier half of its own
   context, and the work it reports is not the work it did.

7. **No task requiring a non-trivial judgement.** Every
   design call belongs in the plan, settled, with the measurement that settled
   it. A task that reads like it wants an opinion is a plan defect. The
   executor's escalation is to stop and report, never to decide.

8. **Role agents commit their own work.** The implementer commits its task, the
   fixer commits its fix, and the reviewer commits nothing because it changes
   nothing. One agent's uncommitted work is never another agent's to adopt, and
   a shared working tree is how two agents destroy each other's edits.

9. **No archaeology in executed plans.** A decision that a later measurement
   falsified is deleted, not reworded into a note about what used to be true.
   Content that must outlive the plan moves to a living document; the rest goes.
   A plan is not a changelog, and a tombstone is a citation waiting to go stale.

## 3. Task entries

A task entry names *what* to build. It leaves *how* to the executor unless a
choice was settled at planning time, in which case the entry states it.

### Tier: the first implementer's rung only

Each task names one rung from `.claude/skills/execute-plan/reference/CLAUDE-CODE-EFFORT-LADDER.md`,
as `- **Tier** — <model> + <effort> (T<n>)`, or `implementer-haiku (T0)`. That is the
first implementer's rung. A task names no verifier or fixer rung: `execute-plan` derives
them from this one.

### Acceptance criteria, no placeholders

`Done when:` criteria are concrete and verifiable, phrased as behaviour observed
through the interface. They may also fix an implementation detail the plan
settled: a data layout or algorithm a performance objective needs, or a user
preference. They may not be vague.

- **Good:** *"`process(charge_id, amount)` where `amount > original_charge.amount` returns `Err(RefundError::Exceeds { ... })` and does not call the persistence adapter."*
- **Good:** *"Returns Y when input is X"*, *"rejects an empty list with a `ValueError`"*.
- **Good:** *"Bars are stored as one contiguous `int64` array per field, not a list of row objects"*, when the plan settled that layout.
- **Bad:** *"partial refunds should work correctly"*, *"add appropriate error handling"*, *"handle edge cases as needed"*, *"similar to Task N"*.

Every type, function, file or module a task references is defined elsewhere in
the plan or already exists in the repo. A reference to an undefined symbol is a
placeholder. A placeholder is worse than a gap: a gap invites a clarifying
question, a placeholder ships ambiguity into a subagent's context.

Before handing a plan off, scan it for `TBD`, `TODO`, `FIXME`, `XXX`,
`implement later`, `fill in`, `to be decided`, `as needed`, `add appropriate`,
`handle edge cases`, `similar to Task`, and angle-bracket placeholders
`<[^>]*>`. Each hit is resolved or justified.

### Smallest meaningful unit, not smallest diff

A task is the smallest unit that carries its own meaningful acceptance
criterion. Each task costs an implementer, at least one verifier, possibly
fixers, and a clean-tree check, and that fixed cost dominates a tiny task.
Merge adjacent work into one task when the pieces are serial and touch the same
file, or when a piece is under ~20 LOC with a single consumer. Split anyway only
when a criterion needs isolated verification: its own red/green cycle or its own
reviewer focus.
