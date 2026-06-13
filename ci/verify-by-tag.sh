#!/usr/bin/env bash
# verify-by-tag.sh — prove the RELEASE consumption path: resolve the proto gen
# module (and any other gpufleet-* sibling) by its PINNED TAG, WITHOUT the local
# `replace` directives. The dev/workspace build always goes through `replace`, so
# the tag, the `require` version string, and the go.sum checksum for the gen
# module are NEVER exercised by a normal `go build`. This script is that proof.
#
# It must be run from the module root. It operates on a throwaway COPY of go.mod
# (no working-tree mutation is committed).
#
# Prerequisite that lives OUTSIDE this repo (see TASK-0031 findings):
#   1. proto must publish a Go-submodule tag for the gen subdir, i.e.
#        gen/go/v<VER>   (NOT proto/v<VER> — Go derives the tag from the module
#        subdir path; a module at .../gen/go is tagged gen/go/vX.Y.Z).
#   2. The private repo must be fetchable: GOPRIVATE=github.com/rocker-zhang/*
#      (set below) plus a credential for github.com on the runner.
# Until (1)+(2) exist, this script FAILS LOUDLY with the exact missing revision —
# that failure is the signal, not a flake.
#
# Modes:
#   (default)            hard-fail if the by-tag path does not resolve.
#   VERIFY_BY_TAG_SOFT=1 still run the full attempt and print the exact go error,
#                        but exit 0 (used while the out-of-repo tag/auth
#                        prerequisite is unmet, so PR CI is not red-walled by an
#                        infra gap; flip the gate once proto publishes the tag).
set -uo pipefail

GEN_MOD="github.com/rocker-zhang/gpufleet-proto/gen/go"
GEN_VER="$(go mod edit -json | python3 -c \
  'import json,sys;print(next(r["Version"] for r in json.load(sys.stdin)["Require"] if r["Path"]=="'"$GEN_MOD"'"))')"
echo ">> gen module require pin: ${GEN_MOD} ${GEN_VER}"
echo ">> expected proto submodule tag (Go convention): gen/go/${GEN_VER}"

# Resolve private sibling modules directly from GitHub, bypassing the public
# proxy + checksum DB (they cannot see a private repo).
export GOPRIVATE='github.com/rocker-zhang/*'
export GOFLAGS='-mod=mod'

SOFT="${VERIFY_BY_TAG_SOFT:-0}"
fail() {
  echo "FAIL: $*" >&2
  if [ "$SOFT" = "1" ]; then
    echo "(VERIFY_BY_TAG_SOFT=1 → reporting only; the out-of-repo prerequisite" >&2
    echo " above is the orchestrator action TASK-0031 documents. Exiting 0.)" >&2
    exit 0
  fi
  exit 1
}

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
cp -r . "$WORK/mod"
cd "$WORK/mod"

# Drop EVERY local gpufleet-* replace so resolution goes by tag.
for rep in $(go mod edit -json | python3 -c \
  'import json,sys;[print(r["Old"]["Path"]) for r in (json.load(sys.stdin).get("Replace") or []) if r["Old"]["Path"].startswith("github.com/rocker-zhang/")]'); do
  echo ">> dropping replace: $rep"
  go mod edit -dropreplace "$rep"
done

echo ">> go mod download (by tag) ..."
go mod download "$GEN_MOD" || fail "cannot resolve ${GEN_MOD}@${GEN_VER} by tag (need proto tag gen/go/${GEN_VER} pushed + private-repo auth)"

echo ">> go build ./... (by tag, no replace) ..."
go build ./... || fail "by-tag build failed after download"

echo ">> asserting go.sum now carries the gen module checksum ..."
grep -q "^${GEN_MOD} ${GEN_VER} " go.sum || fail "go.sum missing ${GEN_MOD} ${GEN_VER}"
grep -q "^${GEN_MOD} ${GEN_VER}/go.mod " go.sum || fail "go.sum missing ${GEN_MOD} ${GEN_VER}/go.mod"
echo "OK: by-tag release path resolves and go.sum carries ${GEN_MOD} ${GEN_VER}"
