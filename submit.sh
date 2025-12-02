#!/bin/sh

set -e

echo "Submitting for the repository — $CI_REPOSITORY_URL"

RAND=$(awk 'BEGIN { srand(); print rand() }')

if awk "BEGIN { exit !($RAND < $VM_0_PROBA) }"; then
  VM_VALUE=0
else
  VM_VALUE=1
fi

BASE_URL=$(echo "$CI_REPOSITORY_URL" | sed -n 's|.*@\(.*\)\.git$|\1|p')
PROJECT=$(echo "$CI_REPOSITORY_URL" | awk -F'/' '{for (i=1; i<=NF; i++) if ($i ~ /^p[0-9]+$/) print $i}')
AUTOGRADER_HOST="cs544-autograder-$VM_VALUE.cs.wisc.edu"

echo "Selected VM: $VM_VALUE"
echo "BASE_URL: $BASE_URL"
echo "PROJECT: $PROJECT"

RESPONSE=$(curl -fsS -X POST -H "Content-Type: application/json" \
  -d "{\"repo_url\": \"https://$BASE_URL\",\"project\": \"$PROJECT\"}" \
  "http://$AUTOGRADER_HOST:$DEPLOYED_AUTOBADGER_PORT/register")

echo $RESPONSE | jq .