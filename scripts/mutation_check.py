#!/usr/bin/env python3
"""Check that the test suite actually has teeth.

A green suite only means something if it goes red when the code is wrong. This breaks one
guarantee at a time, runs just the test that is supposed to notice, and requires that
test to FAIL. Anything that survives is a hole in the suite, not a pass.

    python scripts/mutation_check.py          # all of them, ~2 minutes
    python scripts/mutation_check.py lease    # only mutations matching "lease"

Every patch is reverted in a `finally`, and the script re-checks every file at the end
before exiting; if it ever reports files left modified, `git checkout -- .` restores them.

This is a development tool, not part of the service.
"""

from __future__ import annotations

import pathlib
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent

# (description, file, text to find, text to replace it with, test that must fail)
MUTATIONS: list[tuple[str, str, str, str, str]] = [
    (
        "expired leases are never reclaimed",
        "app/queue.py",
        """    FROM deliveries d
    WHERE d.status IN ('pending', 'in_flight')
      AND d.claimable_at <= now()
    ORDER BY d.claimable_at
    LIMIT $3""",
        """    FROM deliveries d
    WHERE d.status IN ('pending')
      AND d.claimable_at <= now()
    ORDER BY d.claimable_at
    LIMIT $3""",
        "tests/test_queue.py::test_an_expired_lease_is_reclaimed",
    ),
    (
        "claiming does not consume an attempt (the no-infinite-loop guard)",
        "app/queue.py",
        "    attempts         = d.attempts + 1,\n    total_attempts   = d.total_attempts + 1,",
        "    attempts         = d.attempts,\n    total_attempts   = d.total_attempts,",
        "tests/test_queue.py::test_claiming_takes_ownership_and_burns_an_attempt",
    ),
    (
        "a stale worker may reschedule a row it no longer owns",
        "app/queue.py",
        "    WHERE id = $1 AND locked_by = $2 AND status = 'in_flight'",
        "    WHERE id = $1 AND status = 'in_flight'",
        "tests/test_queue.py::test_a_worker_that_lost_its_lease_cannot_reschedule",
    ),
    (
        "the per-recipient fairness cap becomes a throughput limit",
        "app/worker.py",
        "        if not claimed:\n            await self._sleep_unless_stopped",
        "        if len(claimed) < limit:\n            await self._sleep_unless_stopped",
        "tests/test_worker.py::test_a_burst_for_one_recipient_is_not_paced_by_the_poll_interval",
    ),
    (
        "backoff does not grow between attempts",
        "app/retry.py",
        "    raw = settings.retry_base_seconds * (settings.retry_factor**exponent)",
        "    raw = settings.retry_base_seconds",
        "tests/test_retry.py::test_delays_grow_then_flatten_at_the_cap",
    ),
    (
        "the attempt budget never runs out",
        "app/retry.py",
        "    return attempts_made >= settings.retry_max_attempts",
        "    return False",
        "tests/test_retry.py::test_budget_is_exhausted_exactly_at_max_attempts",
    ),
    (
        "a lease shorter than an attempt is accepted at startup",
        "app/config.py",
        "        if self.lease_seconds <= self.attempt_deadline_seconds:",
        "        if False:",
        "tests/test_retry.py::test_lease_shorter_than_an_attempt_is_rejected",
    ),
    (
        "a repeat is not flagged as a duplicate",
        "app/intake.py",
        "                duplicate=True,",
        "                duplicate=False,",
        "tests/test_intake.py::test_repeat_of_a_known_key_returns_the_original",
    ),
    (
        "the recipient gets no key to recognise a repeat by",
        "app/sender.py",
        '        "Idempotency-Key": str(claim.delivery_id),',
        '        "X-Not-An-Idempotency-Key": str(claim.delivery_id),',
        "tests/test_sender.py::test_the_recipient_gets_a_stable_key_for_dropping_duplicates",
    ),
    (
        "a lead with no recipients is rejected instead of accepted",
        "app/schemas.py",
        "    recipients: list[RecipientIn] = Field(\n        default_factory=list,",
        "    recipients: list[RecipientIn] = Field(\n        min_length=1,",
        "tests/test_intake.py::test_a_lead_without_recipients_is_accepted_and_flagged",
    ),
    (
        "leads with nowhere to go vanish from the problem list",
        "app/reporting.py",
        """    (SELECT count(*) FROM requests r
       WHERE NOT EXISTS (SELECT 1 FROM deliveries d WHERE d.request_id = r.id))
                                                          AS no_recipients,""",
        "    0 AS no_recipients,",
        "tests/test_visibility.py::test_problems_reports_all_three_kinds_of_trouble",
    ),
    (
        "a null byte is not rejected (crashes the insert instead)",
        "app/schemas.py",
        "        if _contains_null_byte(value):",
        "        if False:",
        "tests/test_intake.py::test_a_null_byte_is_a_clean_rejection_not_a_crash",
    ),
]


def run_test(test: str) -> bool:
    """True if the test passed."""
    proc = subprocess.run(
        [sys.executable, "-m", "pytest", test, "-q", "-x", "-p", "no:cacheprovider"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=600,
    )
    return proc.returncode == 0


def main() -> int:
    wanted = sys.argv[1].lower() if len(sys.argv) > 1 else None
    selected = [m for m in MUTATIONS if wanted is None or wanted in m[0].lower()]
    if not selected:
        print(f"no mutation matches {wanted!r}")
        return 2

    originals = {rel: (ROOT / rel).read_text() for _, rel, *_ in selected}
    survivors: list[str] = []

    print(f"{'mutation':64} {'caught?':>12}")
    print("-" * 78)
    try:
        for name, rel, find, replace, test in selected:
            path = ROOT / rel
            source = originals[rel]
            if source.count(find) != 1:
                print(f"{name:64} {'PATCH STALE':>12}")
                survivors.append(f"{name} (the code it patches has moved)")
                continue

            path.write_text(source.replace(find, replace, 1))
            try:
                passed = run_test(test)
            finally:
                path.write_text(source)

            print(f"{name:64} {'yes' if not passed else 'NO - SURVIVED':>12}")
            if passed:
                survivors.append(name)
    finally:
        left_dirty = [rel for rel, text in originals.items() if (ROOT / rel).read_text() != text]
        if left_dirty:
            print("\nWARNING: these files were left modified - run `git checkout -- .`:")
            for rel in left_dirty:
                print(f"  {rel}")

    print("-" * 78)
    if survivors:
        print(f"{len(survivors)} of {len(selected)} mutation(s) survived - the suite has a hole:")
        for item in survivors:
            print(f"  - {item}")
        return 1
    print(f"all {len(selected)} mutations were caught")
    return 0


if __name__ == "__main__":
    sys.exit(main())
