#!/usr/bin/env bash
# Minimal assertion helpers for the compose-environment shell tests.
# LOCAL_TEST_ONLY / NOT_QUALIFIED.
# shellcheck shell=bash

FAILURES=0
CHECKS=0

_fail() {
  FAILURES=$((FAILURES + 1))
  printf '  FAIL: %s\n' "$1" >&2
}

# assert_eq <expected> <actual> <message>
assert_eq() {
  CHECKS=$((CHECKS + 1))
  if [ "$1" != "$2" ]; then
    _fail "$3 (expected '$1', got '$2')"
    return 1
  fi
}

# assert_true <message> <command...>   passes when the command exits 0
assert_true() {
  local msg="$1"
  shift
  CHECKS=$((CHECKS + 1))
  if ! "$@" >/dev/null 2>&1; then
    _fail "$msg (command failed: $*)"
    return 1
  fi
}

# assert_false <message> <command...>  passes when the command exits non-zero
assert_false() {
  local msg="$1"
  shift
  CHECKS=$((CHECKS + 1))
  if "$@" >/dev/null 2>&1; then
    _fail "$msg (command unexpectedly succeeded: $*)"
    return 1
  fi
}

# run_test <function-name>  runs one test function and prints its verdict
run_test() {
  local before="$FAILURES"
  "$1" || true
  if [ "$FAILURES" -eq "$before" ]; then
    printf 'ok   %s\n' "$1"
  else
    printf 'FAIL %s\n' "$1"
  fi
}

# finish  exits non-zero when any assertion failed
finish() {
  printf '%d checks, %d failures\n' "$CHECKS" "$FAILURES"
  [ "$FAILURES" -eq 0 ]
}
