#!/usr/bin/env bash
# setup_intern.sh — provisions one intern's BigQuery dataset + least-privilege
# service account. Run once per intern from a shell with gcloud/bq authenticated
# as an account that has project-owner or IAM-admin + BigQuery-admin rights.
#
# Usage:
#   ./setup_intern.sh <PROJECT_ID> <INTERN_HANDLE>
#
# Example:
#   ./setup_intern.sh intern-capstone-2026 alice
#
# Produces: ./gcp-service-account-<handle>.json  (hand this file to the intern)

set -euo pipefail

PROJECT="${1:?Usage: setup_intern.sh <PROJECT_ID> <INTERN_HANDLE>}"
INTERN="${2:?Usage: setup_intern.sh <PROJECT_ID> <INTERN_HANDLE>}"
SA_NAME="pipeline-intern-${INTERN}"
SA_EMAIL="${SA_NAME}@${PROJECT}.iam.gserviceaccount.com"
DATASET="pipeline_intern_${INTERN}"

echo "== Enabling BigQuery API (no-op if already enabled) =="
gcloud services enable bigquery.googleapis.com --project="${PROJECT}"

echo "== Creating dataset ${PROJECT}:${DATASET} =="
bq mk --location=US "${PROJECT}:${DATASET}" || echo "(dataset may already exist, continuing)"

echo "== Creating service account ${SA_EMAIL} =="
gcloud iam service-accounts create "${SA_NAME}" \
  --project="${PROJECT}" \
  --display-name="Intern ${INTERN} pipeline SA" || echo "(service account may already exist, continuing)"

echo "== Granting least-privilege BigQuery roles =="
gcloud projects add-iam-policy-binding "${PROJECT}" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/bigquery.dataEditor" --quiet

gcloud projects add-iam-policy-binding "${PROJECT}" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/bigquery.jobUser" --quiet

echo "== Downloading service account key =="
KEY_FILE="./gcp-service-account-${INTERN}.json"
gcloud iam service-accounts keys create "${KEY_FILE}" \
  --iam-account="${SA_EMAIL}"

echo ""
echo "Done. Hand the intern:"
echo "  - Key file: ${KEY_FILE}  (they place it at airflow/config/gcp-service-account.json)"
echo "  - Project ID: ${PROJECT}"
echo "  - Dataset: ${DATASET}"
echo ""
echo "Remind them to fill these into their pipeline_config.yaml under 'bigquery:'."
