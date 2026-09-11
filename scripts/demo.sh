#!/usr/bin/env bash
#
# Walks through the ten scenarios from section 4 of the brief against a running stack.
#
#   make up && make demo
#
# Needs only curl and python3 (for reading JSON). Every scenario prints what it did and
# what came back, so the output can be read top to bottom as evidence.

set -euo pipefail

API="${API:-http://localhost:${API_PORT:-8000}}"
STUB="${STUB:-http://localhost:${STUB_PORT:-9000}}"
# The address the *service* uses to reach the stub. Inside compose they are on the same
# network, so this is a service name, not localhost.
STUB_INTERNAL="${STUB_INTERNAL:-http://stub:9000}"
RUN="demo-$(date +%s)"

bold() { printf '\n\033[1m%s\033[0m\n' "$*"; }
info() { printf '  %s\n' "$*"; }
ok()   { printf '  \033[32m✓\033[0m %s\n' "$*"; }
fail() { printf '  \033[31m✗ %s\033[0m\n' "$*"; exit 1; }

# Read a value out of JSON on stdin, e.g. `get 'recipients[0]["status"]'`.
get() { python3 -c "import json,sys; d=json.load(sys.stdin); print($1)"; }
pretty() { python3 -m json.tool; }

post() { curl -sS -X POST "$1" -H 'Content-Type: application/json' -d "$2"; }

send_lead() {   # send_lead <key> <recipients-json> -> request id
    post "$API/v1/requests" "{
        \"source_id\": \"$RUN\",
        \"idempotency_key\": \"$1\",
        \"payload\": {\"name\": \"Иван\", \"phone\": \"+7 900 000-00-00\"},
        \"recipients\": $2
    }"
}

configure_stub() {  # configure_stub <name> <behaviour-json>
    curl -sS -X PUT "$STUB/control/$1" -H 'Content-Type: application/json' -d "$2" > /dev/null
}

state_of() { curl -sS "$API/v1/requests/$1" | get 'd.get("state", "?")'; }

wait_for_state() {  # wait_for_state <request-id> <state> <timeout-seconds>
    local deadline=$(( $(date +%s) + $3 ))
    while [ "$(date +%s)" -lt "$deadline" ]; do
        [ "$(state_of "$1")" = "$2" ] && return 0
        sleep 1
    done
    info "last seen state: $(state_of "$1")"
    return 1
}

bold "Checking the stack is up"
curl -sSf "$API/healthz" > /dev/null || fail "the API is not answering at $API - run 'make up' first"
curl -sSf "$STUB/healthz" > /dev/null || fail "the stub is not answering at $STUB"
ok "API at $API, stub at $STUB"
info "retry policy in force:"
curl -sS "$API/healthz" | get 'json.dumps(d["retry_policy"], indent=4)'


# ---------------------------------------------------------------------------
bold "1. An ordinary lead, recipient working"
configure_stub "$RUN-ok" '{"mode": "ok"}'
ID1=$(send_lead "$RUN-1" "[{\"url\": \"$STUB_INTERNAL/hook/$RUN-ok\", \"name\": \"crm\"}]" | get 'd["request_id"]')
info "accepted as $ID1"
wait_for_state "$ID1" delivered 30 || fail "not delivered"
ok "delivered; status:"
curl -sS "$API/v1/requests/$ID1" | pretty | sed 's/^/    /'


# ---------------------------------------------------------------------------
bold "2. The same lead sent twice"
RESP=$(send_lead "$RUN-2" "[{\"url\": \"$STUB_INTERNAL/hook/$RUN-ok\"}]")
ID2=$(echo "$RESP" | get 'd["request_id"]')
AGAIN=$(send_lead "$RUN-2" "[{\"url\": \"$STUB_INTERNAL/hook/$RUN-ok\"}]")
[ "$(echo "$AGAIN" | get 'd["duplicate"]')" = "True" ] || fail "the repeat was not flagged"
[ "$(echo "$AGAIN" | get 'd["request_id"]')" = "$ID2" ] || fail "the repeat got a different id"
ok "second response: duplicate=true, same id $ID2"
wait_for_state "$ID2" delivered 30 || fail "not delivered"
info "what the recipient actually got:"
curl -sS "$STUB/received/$RUN-ok/summary" | pretty | sed 's/^/    /'
ok "one copy per lead - 'duplicates' above is 0"


# ---------------------------------------------------------------------------
bold "3. Recipient fails three times, then works"
configure_stub "$RUN-flaky" '{"mode": "error", "status_code": 500, "fail_first": 3}'
ID3=$(send_lead "$RUN-3" "[{\"url\": \"$STUB_INTERNAL/hook/$RUN-flaky\"}]" | get 'd["request_id"]')
info "accepted as $ID3; with the shipped policy the pauses are 5s, 10s, 20s - about 35s in total"
wait_for_state "$ID3" delivered 180 || fail "not delivered"
ok "delivered. All four attempts, with the pause each one scheduled:"
curl -sS "$API/v1/requests/$ID3" | get '"\n".join(
    "    attempt %d: %s %s -> next at %s" % (
        a["attempt_number"], a["outcome"], a["status_code"] or "", a["scheduled_next_at"] or "-")
    for a in d["recipients"][0]["attempt_log"])'


# ---------------------------------------------------------------------------
bold "4. Recipient accepts the connection and says nothing"
configure_stub "$RUN-silent" '{"mode": "silent"}'
ID4=$(send_lead "$RUN-4" "[{\"url\": \"$STUB_INTERNAL/hook/$RUN-silent\"}]" | get 'd["request_id"]')
info "accepted as $ID4; waiting out the 30s read timeout"
sleep 40
STARTED=$(date +%s%N)
curl -sSf "$API/v1/problems" > /dev/null || fail "the problem list stopped answering"
ELAPSED=$(( ($(date +%s%N) - STARTED) / 1000000 ))
ok "the service stayed responsive (status call took ${ELAPSED}ms while a delivery was stuck)"
curl -sS "$API/v1/requests/$ID4" | get '"    attempt %d: %s (%s) after %dms; state now %s" % (
    d["recipients"][0]["attempt_log"][0]["attempt_number"],
    d["recipients"][0]["attempt_log"][0]["outcome"],
    d["recipients"][0]["attempt_log"][0]["error_kind"],
    d["recipients"][0]["attempt_log"][0]["duration_ms"],
    d["recipients"][0]["status"])'
ok "timed out and went back into the queue, not into failure"


# ---------------------------------------------------------------------------
bold "5. Recipient stays down past the attempt limit"
configure_stub "$RUN-dead" '{"mode": "error", "status_code": 503}'
ID5=$(send_lead "$RUN-5" "[{\"url\": \"$STUB_INTERNAL/hook/$RUN-dead\", \"name\": \"broken-crm\"}]" | get 'd["request_id"]')
info "accepted as $ID5"
info "NOTE: with the shipped 34-attempt policy this takes ~24h to reach 'failed'."
info "      To see it now, restart with a short budget:"
info "        INTAKE_RETRY_MAX_ATTEMPTS=3 INTAKE_RETRY_BASE_SECONDS=1 docker compose up -d worker"
if wait_for_state "$ID5" failed 60; then
    ok "gave up; the lead is in the problem list and its data is intact:"
    curl -sS "$API/v1/problems?reason=failed" | pretty | sed 's/^/    /'
else
    info "still retrying, as expected with the default policy - moving on"
    curl -sS "$API/v1/requests/$ID5" | get '"    attempts so far: %d/%d, last error: %s %s" % (
        d["recipients"][0]["attempts"], d["recipients"][0]["max_attempts"],
        d["recipients"][0]["last_error_kind"], d["recipients"][0]["last_status_code"])'
fi


# ---------------------------------------------------------------------------
bold "6. The recipient is fixed, press retry"
configure_stub "$RUN-dead" '{"mode": "ok"}'
info "recipient repaired; requeuing $ID5 by hand"
post "$API/v1/requests/$ID5/retry" '{}' | pretty | sed 's/^/    /'
wait_for_state "$ID5" delivered 60 || fail "not delivered after the manual retry"
ok "delivered"
info "the journal still holds every attempt ever made:"
curl -sS "$API/v1/requests/$ID5" | get '"    budget attempts now: %d, attempts ever: %d" % (
    d["recipients"][0]["attempts"], d["recipients"][0]["total_attempts"])'


# ---------------------------------------------------------------------------
bold "7. Kill the workers mid-delivery, then start them again"
configure_stub "$RUN-slow7" '{"mode": "slow", "delay_seconds": 60}'
ID7=$(send_lead "$RUN-7" "[{\"url\": \"$STUB_INTERNAL/hook/$RUN-slow7\"}]" | get 'd["request_id"]')
info "accepted as $ID7; letting a worker pick it up"
sleep 5
if ! command -v docker > /dev/null || ! docker compose ps -q worker > /dev/null 2>&1; then
    info "skipped: this step drives docker compose, run it from the repository root"
else
    info "SIGKILL to the workers"
    docker compose kill -s SIGKILL worker > /dev/null 2>&1 \
        || fail "could not kill the workers - is the stack running? (make up)"
    curl -sS "$API/v1/requests/$ID7" | get '"    state while nobody is working on it: %s" % d["recipients"][0]["status"]'

    configure_stub "$RUN-slow7" '{"mode": "ok"}'
    info "recipient fixed; bringing the workers back"
    # Same ports the stack was started with, or compose refuses to reconcile the stack.
    docker compose up -d --no-deps worker > /dev/null 2>&1 \
        || fail "could not restart the workers; try 'make restart-workers' by hand"

    info "waiting for the lease (90s by default) to expire so another worker takes over"
    wait_for_state "$ID7" delivered 240 || fail "work did not resume after the restart"
    ok "work resumed and the lead was delivered; nothing was lost"
    curl -sS "$API/v1/requests/$ID7" | get '"\n".join(
        "    attempt %d: %s (%s)" % (a["attempt_number"], a["outcome"], a["error_kind"] or "-")
        for a in d["recipients"][0]["attempt_log"])'
    info "the interrupted attempt is journalled as 'unknown' - we cannot claim it never happened"
fi


# ---------------------------------------------------------------------------
bold "8. Three recipients, one of them slow"
configure_stub "$RUN-fast-a" '{"mode": "ok"}'
configure_stub "$RUN-fast-b" '{"mode": "ok"}'
configure_stub "$RUN-slow" '{"mode": "slow", "delay_seconds": 25}'
ID8=$(send_lead "$RUN-8" "[
    {\"url\": \"$STUB_INTERNAL/hook/$RUN-fast-a\", \"name\": \"fast-a\"},
    {\"url\": \"$STUB_INTERNAL/hook/$RUN-fast-b\", \"name\": \"fast-b\"},
    {\"url\": \"$STUB_INTERNAL/hook/$RUN-slow\",   \"name\": \"slow\"}
]" | get 'd["request_id"]')
info "accepted as $ID8; checking on the fast two after 5 seconds"
sleep 5
curl -sS "$API/v1/requests/$ID8" | get '"\n".join(
    "    %-8s %s" % (r["recipient_name"], r["status"]) for r in d["recipients"])'
DONE=$(curl -sS "$API/v1/requests/$ID8" | get 'sum(1 for r in d["recipients"] if r["status"] == "delivered")')
[ "$DONE" -ge 2 ] || fail "the fast recipients were held up by the slow one"
ok "both fast recipients were served while the slow one is still going"
wait_for_state "$ID8" delivered 90 || fail "the slow recipient never finished"
ok "the slow one finished too, on its own schedule"


# ---------------------------------------------------------------------------
bold "9. Five hundred leads in one batch"
configure_stub "$RUN-bulk" '{"mode": "ok"}'
info "sending 500 leads..."
STARTED=$(date +%s)
for i in $(seq 1 500); do
    send_lead "$RUN-bulk-$i" "[{\"url\": \"$STUB_INTERNAL/hook/$RUN-bulk\"}]" > /dev/null &
    [ $(( i % 50 )) -eq 0 ] && wait
done
wait
ok "all accepted in $(( $(date +%s) - STARTED ))s"
info "the status endpoint while everything is in flight:"
STARTED=$(date +%s%N)
curl -sSf "$API/v1/requests/$ID1" > /dev/null || fail "the status endpoint stopped answering"
info "    responded in $(( ($(date +%s%N) - STARTED) / 1000000 ))ms"
info "waiting for delivery..."
for _ in $(seq 1 120); do
    TOTAL=$(curl -sS "$STUB/received/$RUN-bulk/summary" | get 'd["total"]')
    [ "$TOTAL" -ge 500 ] && break
    sleep 1
done
curl -sS "$STUB/received/$RUN-bulk/summary" | pretty | sed 's/^/    /'
[ "$(curl -sS "$STUB/received/$RUN-bulk/summary" | get 'd["duplicates"]')" = "0" ] \
    || fail "the recipient got duplicates"
ok "all 500 arrived, none of them twice"


# ---------------------------------------------------------------------------
bold "10. A lead with no recipients at all"
ID10=$(send_lead "$RUN-10" '[]' | get 'd["request_id"]')
[ "$(state_of "$ID10")" = "no_recipients" ] || fail "not flagged as having nowhere to go"
ok "accepted as $ID10 and flagged"
info "in the problem list:"
curl -sS "$API/v1/problems?reason=no_recipients" | get '"\n".join(
    "    %s  source=%s  age=%ds" % (i["request_id"], i["source_id"], i["age_seconds"])
    for i in d["items"])'
info "and it can be resolved - attach an address and it goes out:"
post "$API/v1/requests/$ID10/recipients" \
    "{\"recipients\": [{\"url\": \"$STUB_INTERNAL/hook/$RUN-ok\", \"name\": \"crm\"}]}" \
    | pretty | sed 's/^/    /'
wait_for_state "$ID10" delivered 60 || fail "not delivered after attaching a recipient"
ok "delivered"


# ---------------------------------------------------------------------------
bold "Counters for this run"
curl -sS "$API/v1/stats" | pretty | sed 's/^/  /'

bold "Outstanding problems"
curl -sS "$API/v1/problems" | get 'json.dumps(d["counts"], indent=2)' | sed 's/^/  /'

printf '\n\033[32mDone.\033[0m\n'
