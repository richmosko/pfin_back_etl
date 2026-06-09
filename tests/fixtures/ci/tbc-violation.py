# VENDORED COPY — single source of truth at:
#   richmosko/mosko-fintech / tests/fixtures/ci/tbc-violation.py
#   vendored at commit: d899ddb79b74105af6d6a764dfc08cfe170a203c (PR #104 merge)
#
# Update this copy IN LOCKSTEP with the source-of-truth file. Drift detection
# discipline + cross-repo convention anchored at mosko-fintech
# scripts/ci/README.md § Cross-repo TBC posture.
#
# ============================================================================
#
# tests/fixtures/ci/tbc-violation.py
#
# DELIBERATELY OUTSIDE TenantBoundConnection — TBC golden-test fixture per ARCH §6
# Phase 5 detail item (d). Raw psycopg2.connect() outside the class binding
# users_id. The TBC fence MUST report violation against this file.
#
# CI inversion check (per Sec rubric (b)3 + ARCH §6.1 TBC row):
#   The TBC fence script MUST flag this fixture at every CI invocation. If the
#   fence reports clean, CI fails closed — the fence is broken.
#
# Fixture path discipline (per Sec rubric (b)3 #2 + agent-def):
#   This file lives at tests/fixtures/ci/ and is excluded from:
#     (a) pfin_back_etl production build context via .dockerignore (incumbent
#         exclusion of `tests/` predates this paired PR — no .dockerignore edit
#         required at paired-PR time per DevOps cross-repo inspection 2026-06-08).
#     (b) pfin_back_etl Python module discovery via the src-layout convention
#         (pyproject.toml `[project]` declares the package at `src/pfin_back_etl/`;
#         `tests/` is outside the package root by-construction).
#     (c) pytest collection — the filename `tbc-violation.py` does NOT match
#         pytest's default `test_*.py` / `*_test.py` patterns; pytest skips it
#         during collection (verified against pyproject.toml `[tool.pytest.ini_options]`
#         which does not override the default).
#   Net effect: this file is NEVER runtime-loadable in any production container
#   AND is never executed under pytest. Without these exclusions, the raw connect()
#   below would be a real security hazard — fixture-as-attack-surface is what
#   Sec rubric (b)3 #2 catches.
#
# DO NOT use this file as a template for any production work; use the
# TenantBoundConnection class instead (when Architect introduces it at the first
# raw-psycopg surface in pfin_back_etl).

import psycopg2

# Raw connect — bypasses TenantBoundConnection tenant binding.
# Lock 13 mod #3 V1-SHIP-BLOCK: TenantBoundConnection is the only allowed
# Postgres-client entry point in pfin_back_etl. This invocation pattern violates
# that by-construction.
conn = psycopg2.connect(
    host="localhost",
    database="pfin",
    user="service_role",
    password="fake-fixture-password",
)

# A second violation shape: from-import.
from psycopg2 import connect  # noqa: E402, F811
conn2 = connect(host="localhost", database="pfin")
