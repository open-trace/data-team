## Parent

PRD: data-eng transform-v2 reorganization

## What to build

When low-risk: rename `data-eng/data/local` → `data-eng/local` and update Makefile, compose, and scripts. Until then keep `data/local` and bronze-named script aliases deprecated.

## Acceptance criteria

- [ ] Single local partition path documented
- [ ] All active scripts resolve the new path (or explicit deferral note remains in README)
- [ ] No bronze folder names required for sync

## Blocked by

None - can start immediately (or defer)
