#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 3 ]; then
  echo "usage: $0 <profile> <warehouse_id> <sql_file>" >&2
  exit 1
fi

profile="$1"
warehouse_id="$2"
sql_file="$3"

if [ ! -f "$sql_file" ]; then
  echo "sql file not found: $sql_file" >&2
  exit 1
fi

statement_payload="$(mktemp)"
cleanup() {
  rm -f "$statement_payload"
}
trap cleanup EXIT

jq -n \
  --rawfile statement "$sql_file" \
  --arg warehouse_id "$warehouse_id" \
  '{
    statement: $statement,
    warehouse_id: $warehouse_id,
    wait_timeout: "10s"
  }' > "$statement_payload"

response="$(databricks api post /api/2.0/sql/statements --profile "$profile" --json @"$statement_payload")"
statement_id="$(jq -r '.statement_id' <<<"$response")"
state="$(jq -r '.status.state // empty' <<<"$response")"

if [ -z "$statement_id" ] || [ "$statement_id" = "null" ]; then
  echo "$response" >&2
  echo "missing statement id from Databricks response" >&2
  exit 1
fi

while [ "$state" != "SUCCEEDED" ] && [ "$state" != "FAILED" ] && [ "$state" != "CANCELED" ]; do
  sleep 2
  response="$(databricks api get "/api/2.0/sql/statements/$statement_id" --profile "$profile")"
  state="$(jq -r '.status.state // empty' <<<"$response")"
done

echo "$response"

if [ "$state" != "SUCCEEDED" ]; then
  exit 1
fi
