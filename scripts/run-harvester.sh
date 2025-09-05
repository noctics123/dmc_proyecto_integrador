#!/usr/bin/env bash
set -euo pipefail

# Puedes sobreescribir con env vars si quieres
REGION="${REGION:-us-west2}"
JOB="${JOB:-simbad-harvester-job}"

echo "➡️ Ejecutando $JOB en $REGION..."
gcloud run jobs execute "$JOB" --region "$REGION" --wait

echo "✅ Última ejecución:"
gcloud run jobs executions list --job "$JOB" --region "$REGION" --limit=1
