# Domain Docs

## Before exploring

Use the single-context layout for this repository:

- Read the existing root `GLOSSARY.md` before codebase exploration.
- Read relevant architectural decisions under root `docs/adr/` when that directory exists.

If a referenced document is absent, proceed silently. Domain-modeling workflows create glossary entries and ADRs when terminology or decisions are resolved; setup does not create placeholder ADRs.

## File structure

```text
/
├── GLOSSARY.md
├── docs/
│   ├── adr/
│   └── agents/
└── src/
```

The repository uses one shared vocabulary across its console and runtime components.

## Vocabulary and decisions

Use the glossary's defined terms in issues, specifications, tests, and implementation discussions. Respect its explicit avoided synonyms. Identify missing concepts for the domain-modeling workflow rather than silently inventing alternate terminology.

When a proposal conflicts with an existing ADR, identify that ADR and explain the proposed change explicitly.

Keep checked-in implementation plans and validation evidence in their existing `docs/` locations; these agent-consumption rules do not relocate them.
