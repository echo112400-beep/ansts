# Issue tracker: GitHub

Issues and specs live in GitHub Issues for `echo112400-beep/ansts`.
Repository URL: https://github.com/echo112400-beep/ansts
Use the `gh` CLI for tracker operations.

## Target and operations

- Use `--repo echo112400-beep/ansts` explicitly for issue and label operations.
- Before the first tracker write in a session, verify the target with `gh repo view --json nameWithOwner,url,hasIssuesEnabled` from the project directory.
- Create an issue with `gh issue create --repo echo112400-beep/ansts --title "..." --body-file <file>`. Keep multiline bodies in a UTF-8 file.
- Read with `gh issue view <number> --repo echo112400-beep/ansts --json number,title,body,labels,comments`.
- List with `gh issue list --repo echo112400-beep/ansts --state open --json number,title,body,labels,comments`. Add the required label or state filter.
- Comment with `gh issue comment <number> --repo echo112400-beep/ansts --body-file <file>`.
- Apply or remove labels with `gh issue edit <number> --repo echo112400-beep/ansts --add-label <label>` or `--remove-label <label>`.
- Close after posting the explanation with `gh issue close <number> --repo echo112400-beep/ansts`.
- Resolve a bare issue number against this repository. GitHub issues and PRs share a number space; check the resource type before acting.

## Publish and fetch

When a skill says "publish to the issue tracker", create a GitHub issue in this repository. Check for an existing matching spec before creating a duplicate.
When a skill says "fetch the relevant ticket", read the referenced issue and its comments.
Use the mappings in `triage-labels.md`. A completed `/to-spec` specification receives `ready-for-agent` without another triage interview.
Keep local design documents as implementation references. Record the resulting issue URL in the local specification.

## Pull requests as a triage surface

**PRs as a request surface: no.**

## Wayfinding operations

- Map: one issue labelled `wayfinder:map`; its body holds Notes, Decisions-so-far and Fog.
- Child ticket: one GitHub sub-issue per ticket; fall back to `Part of #<parent>` and a parent task list when sub-issues are unavailable.
- Type labels: `wayfinder:research`, `wayfinder:prototype`, `wayfinder:grilling`, `wayfinder:task`. Create these only when that workflow is used.
- Blocking: use native GitHub issue dependencies; fall back to `Blocked by: #<number>` when unavailable. A ticket is unblocked when every blocker is closed.
- Frontier: open, unblocked, unassigned children in map order.
- Claim: assign the driving developer before work.
- Resolve: post the answer, close the child and add a gist with its link to the map.
