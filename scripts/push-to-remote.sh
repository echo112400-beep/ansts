#!/usr/bin/env bash
set -euo pipefail

REMOTE_URL="${1:-https://github.com/echo112400-beep/ansts.git}"
TARGET_BRANCH="${2:-feat/celld-v25}"

echo "===> Configuring remote origin -> ${REMOTE_URL} (branch: ${TARGET_BRANCH})"
if git remote get-url origin >/dev/null 2>&1; then
  git remote set-url origin "${REMOTE_URL}"
else
  git remote add origin "${REMOTE_URL}"
fi

git branch -M "${TARGET_BRANCH}"

echo "===> Fetching remote branch state from ${REMOTE_URL}..."
if git fetch origin "${TARGET_BRANCH}" 2>/dev/null; then
  echo "===> Pushing merged commits to origin/${TARGET_BRANCH}..."
  git push -u origin "${TARGET_BRANCH}" --force-with-lease
else
  echo "===> Pushing branch ${TARGET_BRANCH} to origin..."
  git push -u origin "${TARGET_BRANCH}" --force
fi

echo "===> [OK] Successfully updated ${REMOTE_URL} (${TARGET_BRANCH})!"
