#!/bin/bash
set -e

# Load per-project config
source ./app.config

# --- Shared across all projects (from admin setup) ---
PROJECT_ID="dj-newsrm-stag-aiml"
SERVICE_ACCOUNT_NAME="wsj-pro-data"
REGION="us-central1"

# Load ACCESS_KEY from secrets.config (not checked in) or prompt
if [ -f ./secrets.config ]; then
  source ./secrets.config
fi
if [ -z "${ACCESS_KEY}" ]; then
  read -rsp "Enter ACCESS_KEY for this function: " ACCESS_KEY
  echo
fi

# --- Derived ---
FUNCTION_NAME="wsj-pro-data-${APP_NAME}"
SA_EMAIL="${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

# --- Auto-detect runtime ---
HAS_NODE=false
HAS_PYTHON=false
[ -f ./function_code/package.json ] && HAS_NODE=true
[ -f ./function_code/requirements.txt ] && HAS_PYTHON=true

if [ "$HAS_NODE" = true ] && [ "$HAS_PYTHON" = true ]; then
  echo "ERROR: Found both package.json and requirements.txt in function_code/"
  echo "Remove one to indicate which runtime to use."
  exit 1
elif [ "$HAS_NODE" = true ]; then
  RUNTIME="nodejs22"
  ENTRY_POINT="helloHttp"
elif [ "$HAS_PYTHON" = true ]; then
  RUNTIME="python312"
  ENTRY_POINT="hello_http"
else
  echo "ERROR: Could not detect runtime."
  echo "Place package.json (Node.js) or requirements.txt (Python) in function_code/"
  exit 1
fi

echo "=== Deploying Cloud Function: ${FUNCTION_NAME} ==="
echo "Project: ${PROJECT_ID}"
echo "Region:  ${REGION}"
echo "Runtime: ${RUNTIME}"
echo ""

gcloud functions deploy ${FUNCTION_NAME} \
  --gen2 \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --runtime="${RUNTIME}" \
  --source=./function_code \
  --entry-point="${ENTRY_POINT}" \
  --trigger-http \
  --run-service-account="${SA_EMAIL}" \
  --no-allow-unauthenticated \
  --no-invoker-iam-check \
  --ingress-settings="all" \
  --set-env-vars="ACCESS_KEY=${ACCESS_KEY}"

FUNCTION_URL=$(gcloud functions describe ${FUNCTION_NAME} \
  --region="${REGION}" --project="${PROJECT_ID}" \
  --format="value(serviceConfig.uri)")

echo ""
echo "=== Done! ==="
echo "Function URL: ${FUNCTION_URL}"
echo ""
echo "Share this URL with your teammates. They visit it, sign in, and use the app."
