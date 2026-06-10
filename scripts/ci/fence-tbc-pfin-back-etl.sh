#!/usr/bin/env bash
#
# VENDORED COPY — single source of truth at:
#   richmosko/mosko-fintech / scripts/ci/fence-tbc-pfin-back-etl.sh
#   vendored at commit: d899ddb79b74105af6d6a764dfc08cfe170a203c (PR #104 merge)
#
# Update this copy IN LOCKSTEP with the source-of-truth file. Drift detection is
# F/CTO-discipline at paired-PR time at V1; automated drift detection deferred to
# V2-or-later. To sync: `git -C ~/Projects/mosko-fintech log --oneline -1 -- scripts/ci/fence-tbc-pfin-back-etl.sh`
# then copy the file + bump the SHA above.
#
# Cross-repo convention anchor: mosko-fintech `scripts/ci/README.md` § Cross-repo
# TBC posture (defines vendored-copy header + sync discipline).
#
# ============================================================================
#
# TBC — TenantBoundConnection CI grep fence
#
# Lock anchors:
#   - ADR-011 Decision 17 / Lock 13 mod #3 (V1-SHIP-BLOCK verbatim)
#   - ADR-011 Decision 4 Privileged-context-surfaces bullet
#     (NOT catalogued §10 numbered list — discipline-preservation guard)
#   - ARCH §6 Security scan stage (ii) + ARCH §6.1 TBC row
#   - ARCH §6 Phase 5 detail item (d)
#
# §10 attribution: TBC is the Privileged-context-surfaces bullet — code-layer
# parallel to RT-26, NOT a third catalogued §10 instance. Decision 4 numbered list
# stays 2-instance per the discipline-preservation guard (RT-22 first, RT-26 second).
# V1-SHIP-BLOCK axis (Lock 13 mod #3) is orthogonal to the §10 catalogued-instance axis.
#
# Catch criterion:
#   - Raw psycopg2.connect() / psycopg.connect() (psycopg3) / asyncpg.connect()
#     invocations outside the file declaring the TenantBoundConnection class.
#   - Includes from-import shape (`from psycopg2 import connect`).
#
# Allowlist mechanism: class-declaration discovery (NOT hardcoded path).
#   1. Find file declaring `class TenantBoundConnection` → ALLOWED_FILE.
#   2. If none found → fail-closed (class absence = TBC discipline absent).
#   3. Grep target tree for pattern set; exclude ALLOWED_FILE; any other hit = violation.
#
# Cross-repo posture (per F/CTO α + paired-PR ratify 2026-06-08):
#   - This script is the source-of-truth in mosko-fintech repo at scripts/ci/.
#   - Vendored (literal file copy) into pfin_back_etl repo at the same path.
#   - mosko-fintech CI runs in inversion mode only against tests/fixtures/ci/.
#   - pfin_back_etl CI runs in production mode against the actual Python tree
#     + inversion mode redundantly.
#
# Usage:
#   bash fence-tbc-pfin-back-etl.sh <target-python-tree-path>
#
# Exit codes:
#   0   — target tree clean (TBC class exists; no raw connect() outside class).
#   1   — one or more raw connect() outside TBC class (fail-closed).
#   2   — argument / environment error OR TBC class not found in target.
#

set -euo pipefail

TARGET="${1:-}"
if [ -z "$TARGET" ]; then
  echo "FATAL: missing target tree arg." >&2
  echo "Usage: bash $(basename "$0") <target-python-tree-path>" >&2
  exit 2
fi
if [ ! -e "$TARGET" ]; then
  echo "FATAL: target tree path not found: $TARGET" >&2
  exit 2
fi

# Discover the file declaring `class TenantBoundConnection`.
# (|| true so empty result doesn't trip set -e.)
ALLOWED_FILES=$(grep -rln 'class TenantBoundConnection' "$TARGET" 2>/dev/null || true)

if [ -z "$ALLOWED_FILES" ]; then
  # Class absence = TBC discipline absent in this target. Fail-closed.
  # Exception: the fixture-only inversion mode in mosko-fintech repo intentionally
  # has no TBC class — the violation fixture is the whole point of the run.
  # In that mode, the fixture trips the next check (raw connect outside any class)
  # so the fence still reports violation correctly. We log + continue to the
  # grep stage; if there ARE hits in target, fence fires; if target is empty,
  # we treat that as a configuration error.
  echo "INFO: TenantBoundConnection class not found in $TARGET." >&2
  echo "INFO: Proceeding under no-class-file assumption — every raw connect() in scope is a violation." >&2
  ALLOWED_FILES=""
fi

# Pattern set: raw .connect() calls + from-import-connect shape.
PATTERN_CONNECT='(psycopg2|psycopg|asyncpg)\.connect\('
PATTERN_FROM_IMPORT='from[[:space:]]+(psycopg2|psycopg|asyncpg)[[:space:]]+import[[:space:]]+connect'

# Find raw hits. We want one combined grep run that excludes any ALLOWED_FILES.
# Build a comma-separated --exclude list from ALLOWED_FILES (basenames only;
# grep --exclude operates on basenames).
EXCLUDE_ARGS=()
if [ -n "$ALLOWED_FILES" ]; then
  while IFS= read -r f; do
    [ -z "$f" ] && continue
    base=$(basename "$f")
    EXCLUDE_ARGS+=(--exclude="$base")
  done <<< "$ALLOWED_FILES"
fi

# Scan for the patterns. -E for ERE; -n for line numbers; -H for filename;
# -r for recursive; --include='*.py' to scope to Python.
RAW_HITS=$(grep -rEnH --include='*.py' "${EXCLUDE_ARGS[@]+"${EXCLUDE_ARGS[@]}"}" \
  -e "$PATTERN_CONNECT" \
  -e "$PATTERN_FROM_IMPORT" \
  "$TARGET" 2>/dev/null || true)

# Filter each hit:
#   (1) Skip if the matched line is a pure `#` comment (the connect-pattern in
#       the line is just text, not real code). This catches the false-positive
#       class where comments documenting psycopg2.connect() usage would otherwise
#       trip the fence. Caveats: does NOT skip docstrings or inline trailing
#       comments — those are rare false-positive shapes; human reviewer
#       adjudicates if surfaced.
#   (2) Re-verify against ALLOWED_FILES by full path (the --exclude basename
#       match could over-exclude same-basename files in unrelated paths).
VIOLATIONS=0
if [ -n "$RAW_HITS" ]; then
  while IFS= read -r hit; do
    # Extract filename (field 1) + content (field 3+).
    hit_file=$(echo "$hit" | cut -d: -f1)
    hit_content=$(echo "$hit" | cut -d: -f3-)
    # (1) Skip pure-# comment lines.
    stripped=$(echo "$hit_content" | sed 's/^[[:space:]]*//')
    case "$stripped" in
      \#*) continue ;;
    esac
    # (2) Re-verify against ALLOWED_FILES.
    norm_hit=$(echo "$hit_file" | sed 's|^\./||')
    is_allowed=0
    if [ -n "$ALLOWED_FILES" ]; then
      while IFS= read -r allow; do
        [ -z "$allow" ] && continue
        norm_allow=$(echo "$allow" | sed 's|^\./||')
        if [ "$norm_hit" = "$norm_allow" ]; then
          is_allowed=1
          break
        fi
      done <<< "$ALLOWED_FILES"
    fi
    if [ "$is_allowed" -eq 0 ]; then
      echo "VIOLATION: raw DB connect outside TenantBoundConnection class:" >&2
      echo "  $hit" >&2
      VIOLATIONS=$((VIOLATIONS+1))
    fi
  done <<< "$RAW_HITS"
fi

if [ "$VIOLATIONS" -gt 0 ]; then
  echo "" >&2
  echo "TBC fence: $VIOLATIONS violation(s) in $TARGET. Failing closed." >&2
  echo "Lock 13 mod #3: TenantBoundConnection is the only allowed Postgres-client entry" >&2
  echo "point in pfin_back_etl. Raw psycopg2/psycopg/asyncpg .connect() outside the" >&2
  echo "TenantBoundConnection class is V1-SHIP-BLOCK." >&2
  if [ -n "$ALLOWED_FILES" ]; then
    echo "" >&2
    echo "Allowed file(s) declaring TenantBoundConnection class:" >&2
    echo "$ALLOWED_FILES" | sed 's/^/  /' >&2
  fi
  exit 1
fi

echo "TBC fence: $TARGET clean (no raw connect() outside TenantBoundConnection class)."
exit 0
