# Domain Docs

## Layout

This is a single-context repository.
The glossary belongs at root `GLOSSARY.md`; architecture decisions belong under `docs/adr/`.

## Before domain exploration or design

1. Read `GLOSSARY.md` if it exists.
2. Read ADRs under `docs/adr/` that concern the task.
3. For persistence work, read the approved [Celld persistence design](../celld_persistence_design.md) and its linked implementation specification.
4. Use the installed `codebase-memory` skill for structural code exploration, as required by the root instructions.

If a glossary or ADR directory is absent, continue silently. Do not treat absence as a blocker or create placeholder domain documents.
Create domain documents lazily when terminology or architectural decisions are resolved through the domain-modeling workflow.

## Vocabulary and decisions

Use glossary terms in specifications, issues, designs and tests.
If a needed concept is missing, check existing project usage before introducing a new term.
Surface a conflict with an existing ADR explicitly; explain which decision would need reopening.

The approved persistence design takes precedence over conflicting historical persistence and unmeasured SLO claims in older architecture documents.
Its approved design status does not establish that the implementation or production acceptance tests are complete.
