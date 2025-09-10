#!/bin/bash

# Setup Cloud Build Triggers for DMC Pipeline
# Run this script to configure all necessary Cloud Build triggers

set -e

PROJECT_ID="proyecto-integrador-dae-2025"
REGION="us-central1"
REPO_NAME="dmc_proyecto_integrador"
GITHUB_OWNER="your-github-username"  # Replace with actual GitHub username

echo "🚀 Setting up Cloud Build triggers for DMC Data Pipeline..."
echo "Project ID: $PROJECT_ID"
echo "Repository: $GITHUB_OWNER/$REPO_NAME"

# Enable required APIs
echo "📋 Enabling required GCP APIs..."
gcloud services enable cloudbuild.googleapis.com \
    dataproc.googleapis.com \
    bigquery.googleapis.com \
    storage.googleapis.com \
    secretmanager.googleapis.com \
    --project=$PROJECT_ID

# Create main ETL pipeline trigger
echo "🔧 Creating ETL Pipeline trigger..."
gcloud builds triggers create github \
    --project=$PROJECT_ID \
    --repo-name=$REPO_NAME \
    --repo-owner=$GITHUB_OWNER \
    --branch-pattern="^main$" \
    --build-config=cloudbuild.etl-pipeline.yaml \
    --name="etl-pipeline-main" \
    --description="Complete ETL pipeline execution on main branch" \
    --include-logs-with-status

# Create manual trigger for testing
echo "🧪 Creating manual ETL trigger..."
gcloud builds triggers create github \
    --project=$PROJECT_ID \
    --repo-name=$REPO_NAME \
    --repo-owner=$GITHUB_OWNER \
    --tag-pattern="^etl-v.*" \
    --build-config=cloudbuild.etl-pipeline.yaml \
    --name="etl-pipeline-manual" \
    --description="Manual ETL pipeline execution via tag" \
    --include-logs-with-status

# Create trigger for web app deployment
echo "🌐 Creating web app deployment trigger..."
cat > /tmp/cloudbuild-webapp.yaml << 'EOF'
steps:
  # Build backend Docker image
  - name: 'gcr.io/cloud-builders/docker'
    args: ['build', '-t', 'gcr.io/$PROJECT_ID/dmc-backend:$SHORT_SHA', 'web_app/backend']
  
  # Push backend image
  - name: 'gcr.io/cloud-builders/docker'
    args: ['push', 'gcr.io/$PROJECT_ID/dmc-backend:$SHORT_SHA']
  
  # Build frontend
  - name: 'node:18'
    entrypoint: 'bash'
    args:
      - '-c'
      - |
        cd web_app/frontend
        npm ci
        npm run build
        
  # Deploy to Cloud Run
  - name: 'gcr.io/google.com/cloudsdktool/cloud-sdk'
    entrypoint: 'gcloud'
    args:
      - 'run'
      - 'deploy'
      - 'dmc-pipeline-management'
      - '--image'
      - 'gcr.io/$PROJECT_ID/dmc-backend:$SHORT_SHA'
      - '--platform'
      - 'managed'
      - '--region'
      - 'us-central1'
      - '--allow-unauthenticated'
      - '--port'
      - '8000'
      - '--set-env-vars'
      - 'PROJECT_ID=$PROJECT_ID,REGION=us-central1'

options:
  logging: CLOUD_LOGGING_ONLY
images:
  - 'gcr.io/$PROJECT_ID/dmc-backend:$SHORT_SHA'
EOF

gcloud builds triggers create github \
    --project=$PROJECT_ID \
    --repo-name=$REPO_NAME \
    --repo-owner=$GITHUB_OWNER \
    --branch-pattern="^main$" \
    --build-config=/tmp/cloudbuild-webapp.yaml \
    --name="webapp-deployment" \
    --description="Deploy web app to Cloud Run" \
    --included-files="web_app/**" \
    --include-logs-with-status

# Create existing triggers (landing scrapers) if they don't exist
echo "🕷️ Checking existing scraper triggers..."

# Check if landing run trigger exists
if ! gcloud builds triggers describe landing-run-trigger --project=$PROJECT_ID &>/dev/null; then
    echo "Creating landing run trigger..."
    gcloud builds triggers create github \
        --project=$PROJECT_ID \
        --repo-name=$REPO_NAME \
        --repo-owner=$GITHUB_OWNER \
        --branch-pattern="^main$" \
        --build-config=landing_run/cloudbuild.run.yaml \
        --name="landing-run-trigger" \
        --description="Deploy landing macroeconomics scraper" \
        --included-files="landing_run/**"
else
    echo "Landing run trigger already exists"
fi

# Check if landing simbad trigger exists  
if ! gcloud builds triggers describe landing-simbad-trigger --project=$PROJECT_ID &>/dev/null; then
    echo "Creating landing simbad trigger..."
    gcloud builds triggers create github \
        --project=$PROJECT_ID \
        --repo-name=$REPO_NAME \
        --repo-owner=$GITHUB_OWNER \
        --branch-pattern="^main$" \
        --build-config=landing_simbad/cloudbuild.simbad.job.yaml \
        --name="landing-simbad-trigger" \
        --description="Deploy landing SIMBAD scraper" \
        --included-files="landing_simbad/**"
else
    echo "Landing SIMBAD trigger already exists"
fi

# Create Cloud Scheduler jobs
echo "⏰ Creating Cloud Scheduler jobs..."

# Monthly ETL pipeline job
gcloud scheduler jobs create http monthly-etl-pipeline \
    --project=$PROJECT_ID \
    --location=$REGION \
    --schedule="0 2 20 * *" \
    --time-zone="America/Lima" \
    --uri="https://cloudbuild.googleapis.com/v1/projects/$PROJECT_ID/triggers/etl-pipeline-main:run" \
    --http-method=POST \
    --headers="Content-Type=application/json" \
    --oauth-service-account-email="$PROJECT_ID@appspot.gserviceaccount.com" \
    --oauth-token-scope="https://www.googleapis.com/auth/cloud-platform" \
    --message-body='{"branchName":"main"}' \
    --description="Monthly ETL pipeline execution - day 20 at 2AM Lima time" \
    --max-retry-attempts=3 \
    --min-backoff-duration=60s \
    --max-backoff-duration=300s || echo "Scheduler job already exists"

# Grant necessary permissions to Cloud Build service account
echo "🔐 Setting up IAM permissions..."
PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format="value(projectNumber)")
CLOUDBUILD_SA="$PROJECT_NUMBER@cloudbuild.gserviceaccount.com"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$CLOUDBUILD_SA" \
    --role="roles/dataproc.editor"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$CLOUDBUILD_SA" \
    --role="roles/bigquery.admin"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$CLOUDBUILD_SA" \
    --role="roles/storage.admin"

echo "✅ Cloud Build triggers setup completed!"
echo ""
echo "📋 Summary of created triggers:"
echo "  1. etl-pipeline-main: Automatic ETL on main branch commits"
echo "  2. etl-pipeline-manual: Manual ETL via tags (etl-v*)"
echo "  3. webapp-deployment: Web app deployment on web_app/ changes"
echo "  4. landing-run-trigger: Macroeconomics scraper deployment"
echo "  5. landing-simbad-trigger: SIMBAD scraper deployment"
echo ""
echo "⏰ Scheduler job created:"
echo "  - monthly-etl-pipeline: Runs day 20 at 2AM Lima time"
echo ""
echo "🔧 To test the pipeline manually, run:"
echo "  git tag etl-v1.0.0 && git push origin etl-v1.0.0"
echo ""
echo "🌐 To view triggers:"
echo "  https://console.cloud.google.com/cloud-build/triggers?project=$PROJECT_ID"

# Clean up temp file
rm -f /tmp/cloudbuild-webapp.yaml