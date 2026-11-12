#!/usr/bin/env bash
set -euo pipefail

REMOTE_HTTPS="https://github.com/echo112400-beep/ansts.git"
REMOTE_SSH="git@github.com:echo112400-beep/ansts.git"
TARGET_REMOTE="${1:-${REMOTE_HTTPS}}"

echo "===> Configuring Git repository for ${TARGET_REMOTE}..."
git branch -M main

if git remote get-url origin >/dev/null 2>&1; then
  git remote set-url origin "${TARGET_REMOTE}"
else
  git remote add origin "${TARGET_REMOTE}"
fi

git add -A
if ! git diff --cached --quiet; then
  git commit -m "feat(v2.5): complete ANSTS v2.5 M1-M31 full-stack de-mocked implementation, TanStack BFF & 11-container compose cluster"
fi

echo "===> Pushing branch 'main' and tag 'v2.5.0' to ${TARGET_REMOTE}..."
git tag -f v2.5.0
git push -u origin main --tags
echo "===> Successfully pushed ANSTS v2.5 to https://github.com/echo112400-beep/ansts !"
