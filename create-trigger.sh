#!/bin/bash
# Script para crear trigger automático de Cloud Build

echo "Creando trigger automático para Web App..."

gcloud builds triggers create github \
  --project=proyecto-integrador-dae-2025 \
  --repo-name=dmc_proyecto_integrador \
  --repo-owner=noctics123 \
  --branch-pattern="^main$" \
  --build-config=web_app/cloudbuild.webapp.yaml \
  --description="Auto-deploy Web App on main branch push" \
  --name="webapp-autodeploy"

echo "Trigger creado. Cada push a main desplegará automáticamente."
echo "Para ejecutar manualmente:"
echo "gcloud builds submit --config=web_app/cloudbuild.webapp.yaml --project=proyecto-integrador-dae-2025"