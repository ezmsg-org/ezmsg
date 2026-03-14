# Deprecate `@ez.thread`

## Summary
This changeset deprecates the `@ez.thread` decorator and documents the preferred replacement (`loop.run_in_executor(...)` / explicit task lifecycle management).

## Changes
- Emit `DeprecationWarning` from `ezmsg.core.unit.thread`.
- Add deprecation note to function decorator docs.
- Add a unit test verifying warning emission and backward-compatible decorator tagging.

## Files
- `src/ezmsg/core/unit.py`
- `docs/source/reference/API/functiondecorators.rst`
- `tests/test_unit_deprecations.py`
- `ISSUE_deprecate_ez_thread.md`

## Testing
- `PYTHONPYCACHEPREFIX=/tmp/pycache .venv/bin/pytest tests/test_unit_deprecations.py -q`

## Backward Compatibility
- Existing `@ez.thread` usage continues to function in this release.
- Users now receive a deprecation warning at decorator application time.

## Future Work
- Remove `@ez.thread` in a future major release after migration window and release-note notice.
