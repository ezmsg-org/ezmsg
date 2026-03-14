# Deprecate `@ez.thread` Decorator

## Summary
Deprecate `@ez.thread` in `ezmsg.core` and guide users to explicit background execution patterns (`loop.run_in_executor(...)` / explicit task lifecycle management).

## Motivation
- `@ez.thread` has no cooperative termination contract and does not integrate cleanly with unit lifecycle shutdown.
- Equivalent behavior is already available with explicit executor usage.
- Keeping `@ez.thread` adds an extra concurrency model surface area without strong adoption.

## Proposal
1. Mark `@ez.thread` as deprecated by emitting `DeprecationWarning` when the decorator is applied.
2. Update docs to indicate deprecation and migration guidance.
3. Add tests to ensure the warning is emitted and existing decorator behavior remains intact for compatibility.

## Non-Goals
- Removing `@ez.thread` in this issue.
- Changing runtime behavior of existing `@ez.thread`-decorated functions beyond warning emission.

## Acceptance Criteria
- Calling `ez.thread(...)` emits `DeprecationWarning`.
- API docs clearly mark `@ez.thread` as deprecated with migration guidance.
- Test coverage exists for warning behavior and attribute tagging.

## Follow-Up
- Remove `@ez.thread` in a future major release after deprecation window.
- Add release note/migration note before removal.
