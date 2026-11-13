#!/usr/bin/env bash
# Register every schema in sim_host/schemas/ with the Schema Registry.
# File name = subject name (TopicNameStrategy: <topic>-value).
# For an existing subject it first asks the registry "is this change compatible?"
# and stops on NO, so a breaking change never reaches producers.
set -euo pipefail
REG=http://localhost:8081
DIR=/opt/rs/sim_host/schemas
CT='Content-Type: application/vnd.schemaregistry.v1+json'

shopt -s nullglob
for F in "$DIR"/*.avsc; do
  SUBJECT=$(basename "$F" .avsc)
  BODY=$(python3 -I -c 'import json,sys; print(json.dumps({"schema": json.dumps(json.load(open(sys.argv[1])))}))' "$F")

  if curl -fsS "$REG/subjects" | grep -q "\"$SUBJECT\""; then
    COMPAT=$(curl -fsS -X POST -H "$CT" --data "$BODY" \
      "$REG/compatibility/subjects/$SUBJECT/versions/latest")
    echo "~ $SUBJECT compatibility check: $COMPAT"
    grep -q '"is_compatible":true' <<<"$COMPAT" || { echo "  BLOCKED: not compatible with latest version"; exit 1; }
  fi

  RESP=$(curl -fsS -X POST -H "$CT" --data "$BODY" "$REG/subjects/$SUBJECT/versions")
  echo "+ $SUBJECT registered: $RESP   versions: $(curl -fsS "$REG/subjects/$SUBJECT/versions")"
done

echo
echo "All subjects: $(curl -fsS "$REG/subjects")"
