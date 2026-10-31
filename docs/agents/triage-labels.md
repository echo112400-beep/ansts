# Triage Labels

The five canonical roles use their default label strings in `echo112400-beep/ansts`.

| Canonical role | GitHub label | Meaning |
|---|---|---|
| `needs-triage` | `needs-triage` | Maintainer needs to evaluate the issue |
| `needs-info` | `needs-info` | Waiting for information |
| `ready-for-agent` | `ready-for-agent` | Fully specified, ready for agent implementation |
| `ready-for-human` | `ready-for-human` | Requires human implementation |
| `wontfix` | `wontfix` | Will not be actioned |

When a skill names a role, apply the corresponding label from this table.
Use exactly one triage state on an issue; remove an obsolete state when changing it.
If existing states conflict, ask the maintainer before changing them.
For the triage workflow, reuse the existing `bug` and `enhancement` category labels.
