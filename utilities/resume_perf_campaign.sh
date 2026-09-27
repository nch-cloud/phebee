#!/usr/bin/env bash
#
# Keep a performance campaign going across SSO session expiry.
#
# An 8-hour SSO sign-in session does not cover a 26-hour grid, and `aws sso login`
# cannot be automated: it is an OAuth device flow and needs a human to approve it
# in a browser. Refresh *within* a session is already automatic, so there is
# nothing to schedule -- what is worth automating is the resume. This waits for
# credentials to come back and re-runs the driver, which picks up from the markers
# in the output tree. The only manual step is `aws sso login` in any terminal,
# whenever you get to it; the loop notices within two minutes.
#
# This is only safe because the driver does not reset a size whose import is
# already recorded. An unattended retry loop in front of a driver that reset
# unconditionally would wipe loaded data and measure an empty database.
#
# Only credential failures are retried. Anything else -- a failing import, a
# missing dataset -- stops the loop, because silently re-running a genuinely
# broken cell all night would burn the night and bury the error.
#
# Usage:
#     caffeinate -i utilities/resume_perf_campaign.sh perf-campaign-2026-09-25 &
#
# The campaign log is <out-dir>.log, appended to, so the history of a campaign
# spread over several sessions stays in one file.
#
set -uo pipefail

OUT=${1:?usage: resume_perf_campaign.sh <out-dir> [max-attempts]}
MAX=${2:-8}
BENCHMARK_ROOT=${BENCHMARK_ROOT:-tests/data/benchmark}

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$REPO_ROOT" || exit 1

OUT=${OUT%/}
LOG="$OUT.log"

# This script is meant to be backgrounded, and a background process that touches
# the controlling terminal is stopped by SIGTTOU -- the whole process group with
# it. That already cost this campaign 36 minutes of looking slow while suspended.
# The AWS CLI pages its output through less by default, so disable the pager, and
# give every aws call /dev/null on stdin below.
export AWS_PAGER=""

say() { printf '[resume %s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$1" >>"$LOG"; }

# Markers of an expired session, as they appear in a driver or pytest traceback.
CRED_PATTERN='TokenRetrievalError|ExpiredToken|InvalidGrantException|UnauthorizedSSOTokenError|SSOTokenLoadError|refresh failed'

for attempt in $(seq 1 "$MAX"); do
    # Wait for working credentials. Logged sparsely: an overnight wait should not
    # produce a thousand lines in the middle of the campaign log.
    waited=0
    until aws sts get-caller-identity </dev/null >/dev/null 2>&1; do
        if [ $((waited % 15)) -eq 0 ]; then
            say "waiting for credentials -- run: aws sso login"
        fi
        waited=$((waited + 1))
        sleep 120
    done
    [ "$waited" -gt 0 ] && say "credentials available after $((waited * 2))m"

    say "attempt $attempt/$MAX"
    # Where this attempt's output starts. The log is shared by every attempt, so
    # searching the whole tail would let a previous attempt's expired-token
    # traceback classify this attempt's genuine failure as retryable.
    log_lines_before=$(wc -l <"$LOG" 2>/dev/null || echo 0)
    if python utilities/run_perf_campaign.py \
            --benchmark-root "$BENCHMARK_ROOT" \
            --out "$OUT" \
            </dev/null >>"$LOG" 2>&1; then
        say "campaign complete"
        exit 0
    fi

    # Decide whether to retry from what the failure left behind. The driver's own
    # boto3 errors land in the campaign log; a failure inside a test lands in that
    # cell's pytest.log, which the driver names in the campaign log just before it
    # gives up.
    newest_pytest=$(ls -t "$OUT"/*/import/pytest.log "$OUT"/*/c*/r*/pytest.log 2>/dev/null | head -1)
    if tail -n +$((log_lines_before + 1)) "$LOG" | grep -Eq "$CRED_PATTERN" ||
        { [ -n "$newest_pytest" ] && grep -Eq "$CRED_PATTERN" "$newest_pytest"; }; then
        # Back off before looping. If the session is genuinely dead the wait at the
        # top of the loop handles it, but a transient expiry that clears itself
        # would otherwise let this spend every attempt in a few seconds and give up
        # before the operator has a chance to log in.
        say "credentials expired; waiting for a new session before resuming"
        sleep 120
        continue
    fi

    say "stopping: the last failure does not look like credentials"
    [ -n "$newest_pytest" ] && say "see $newest_pytest"
    exit 1
done

say "gave up after $MAX attempts"
exit 1
