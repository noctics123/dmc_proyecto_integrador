#!/bin/bash

# Setup Cloud Monitoring for DMC Pipeline
# This script configures comprehensive monitoring, alerting, and dashboards

set -e

PROJECT_ID="proyecto-integrador-dae-2025"
EMAIL_NOTIFICATION="admin@example.com"  # Replace with actual email

echo "🔍 Setting up Cloud Monitoring for DMC Pipeline..."
echo "Project: $PROJECT_ID"
echo ""

# Enable required APIs
echo "📋 Enabling Monitoring APIs..."
gcloud services enable monitoring.googleapis.com \
    logging.googleapis.com \
    cloudasset.googleapis.com \
    --project=$PROJECT_ID

# Create notification channel for email
echo "📧 Creating email notification channel..."
EMAIL_CHANNEL=$(gcloud alpha monitoring channels create \
    --display-name="DMC Pipeline Email Alerts" \
    --type=email \
    --channel-labels=email_address=$EMAIL_NOTIFICATION \
    --description="Email notifications for pipeline alerts" \
    --project=$PROJECT_ID \
    --format="value(name)")

echo "Email notification channel created: $EMAIL_CHANNEL"

# Create custom metrics for data quality monitoring
echo "📊 Creating custom metrics..."

# Data freshness metric
gcloud logging metrics create data_freshness_hours \
    --description="Hours since last data update" \
    --log-filter='resource.type="cloud_run_revision" AND textPayload:"Data processing completed"' \
    --value-extractor='EXTRACT(timestamp)' \
    --project=$PROJECT_ID || echo "Metric already exists"

# Pipeline success rate metric
gcloud logging metrics create pipeline_success_rate \
    --description="Pipeline execution success rate" \
    --log-filter='resource.type="build" AND (textPayload:"BUILD_SUCCESS" OR textPayload:"BUILD_FAILURE")' \
    --project=$PROJECT_ID || echo "Metric already exists"

# Create alert policies programmatically
echo "🚨 Creating alert policies..."

# DataProc job failure alert
cat > /tmp/dataproc-alert.json << 'EOF'
{
  "displayName": "DataProc Job Failure",
  "documentation": {
    "content": "Alert when DataProc jobs fail in DMC pipeline"
  },
  "conditions": [
    {
      "displayName": "DataProc job failed",
      "conditionThreshold": {
        "filter": "resource.type=\"dataproc_job\" AND metric.type=\"dataproc.googleapis.com/job/failed_count\"",
        "comparison": "COMPARISON_GREATER_THAN",
        "thresholdValue": 0,
        "duration": "60s"
      }
    }
  ],
  "combiner": "OR",
  "enabled": true,
  "notificationChannels": ["'$EMAIL_CHANNEL'"],
  "alertStrategy": {
    "autoClose": "86400s"
  }
}
EOF

gcloud alpha monitoring policies create --policy-from-file=/tmp/dataproc-alert.json --project=$PROJECT_ID

# BigQuery error rate alert
cat > /tmp/bigquery-alert.json << 'EOF'
{
  "displayName": "BigQuery High Error Rate",
  "documentation": {
    "content": "Alert when BigQuery queries fail frequently"
  },
  "conditions": [
    {
      "displayName": "High BigQuery error rate",
      "conditionThreshold": {
        "filter": "resource.type=\"bigquery_project\" AND metric.type=\"bigquery.googleapis.com/job/num_failed_jobs\"",
        "comparison": "COMPARISON_GREATER_THAN", 
        "thresholdValue": 3,
        "duration": "300s",
        "aggregations": [
          {
            "alignmentPeriod": "300s",
            "perSeriesAligner": "ALIGN_RATE",
            "crossSeriesReducer": "REDUCE_SUM"
          }
        ]
      }
    }
  ],
  "combiner": "OR",
  "enabled": true,
  "notificationChannels": ["'$EMAIL_CHANNEL'"]
}
EOF

gcloud alpha monitoring policies create --policy-from-file=/tmp/bigquery-alert.json --project=$PROJECT_ID

# Data freshness alert
cat > /tmp/freshness-alert.json << 'EOF'
{
  "displayName": "Data Freshness Alert",
  "documentation": {
    "content": "Alert when data hasn't been updated in 48+ hours"
  },
  "conditions": [
    {
      "displayName": "Data older than 48 hours", 
      "conditionThreshold": {
        "filter": "resource.type=\"gcs_bucket\" AND resource.labels.bucket_name=\"dae-integrador-2025\"",
        "comparison": "COMPARISON_GREATER_THAN",
        "thresholdValue": 48,
        "duration": "3600s"
      }
    }
  ],
  "combiner": "OR",
  "enabled": true,
  "notificationChannels": ["'$EMAIL_CHANNEL'"]
}
EOF

gcloud alpha monitoring policies create --policy-from-file=/tmp/freshness-alert.json --project=$PROJECT_ID

# Create uptime checks for Cloud Run services
echo "⏱️ Creating uptime checks..."

# SIMBAD service uptime check
gcloud monitoring uptime create \
    --display-name="SIMBAD Landing Service" \
    --http-check-path="/health" \
    --hostname="landing-simbad-service-url.run.app" \
    --port=443 \
    --use-ssl \
    --timeout=10s \
    --period=300s \
    --project=$PROJECT_ID || echo "Uptime check already exists"

# Create dashboard
echo "📈 Creating monitoring dashboard..."
cat > /tmp/dashboard.json << 'EOF'
{
  "displayName": "DMC Pipeline Dashboard",
  "mosaicLayout": {
    "tiles": [
      {
        "width": 6,
        "height": 4,
        "widget": {
          "title": "DataProc Jobs",
          "xyChart": {
            "dataSets": [{
              "timeSeriesQuery": {
                "timeSeriesFilter": {
                  "filter": "resource.type=\"dataproc_job\"",
                  "aggregation": {
                    "alignmentPeriod": "300s",
                    "perSeriesAligner": "ALIGN_RATE"
                  }
                }
              }
            }]
          }
        }
      },
      {
        "width": 6,
        "height": 4,
        "xPos": 6,
        "widget": {
          "title": "BigQuery Jobs",
          "xyChart": {
            "dataSets": [{
              "timeSeriesQuery": {
                "timeSeriesFilter": {
                  "filter": "resource.type=\"bigquery_project\"",
                  "aggregation": {
                    "alignmentPeriod": "300s", 
                    "perSeriesAligner": "ALIGN_RATE"
                  }
                }
              }
            }]
          }
        }
      }
    ]
  }
}
EOF

DASHBOARD_ID=$(gcloud monitoring dashboards create --config-from-file=/tmp/dashboard.json \
    --project=$PROJECT_ID --format="value(name)")

echo "Dashboard created: $DASHBOARD_ID"

# Create log-based alert for pipeline failures
echo "📝 Creating log-based alerts..."
gcloud alpha logging metrics create pipeline_failures \
    --description="Count of pipeline failures" \
    --log-filter='resource.type="build" AND textPayload:"BUILD_FAILURE"' \
    --project=$PROJECT_ID || echo "Log metric already exists"

# Set up log retention policies
echo "🗂️ Configuring log retention..."
gcloud logging buckets update _Default \
    --location=global \
    --retention-days=30 \
    --project=$PROJECT_ID

# Create service account for monitoring automation
echo "🔐 Creating monitoring service account..."
gcloud iam service-accounts create dmc-monitoring \
    --description="Service account for DMC pipeline monitoring" \
    --display-name="DMC Monitoring SA" \
    --project=$PROJECT_ID || echo "Service account already exists"

# Grant necessary permissions
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:dmc-monitoring@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/monitoring.editor"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:dmc-monitoring@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/logging.viewer"

# Clean up temp files
rm -f /tmp/dataproc-alert.json /tmp/bigquery-alert.json /tmp/freshness-alert.json /tmp/dashboard.json

echo ""
echo "✅ Cloud Monitoring setup completed!"
echo ""
echo "📊 Created resources:"
echo "  - Email notification channel: $EMAIL_CHANNEL"
echo "  - Dashboard: $DASHBOARD_ID" 
echo "  - 3 alert policies for DataProc, BigQuery, and data freshness"
echo "  - Custom log metrics for pipeline monitoring"
echo "  - Uptime checks for Cloud Run services"
echo "  - Service account: dmc-monitoring@$PROJECT_ID.iam.gserviceaccount.com"
echo ""
echo "🔗 Access your dashboard:"
echo "  https://console.cloud.google.com/monitoring/dashboards/custom/$DASHBOARD_ID?project=$PROJECT_ID"
echo ""
echo "📋 Next steps:"
echo "  1. Replace placeholder email with actual notification email"
echo "  2. Configure Slack webhook for team notifications"
echo "  3. Set up PagerDuty integration for critical alerts"
echo "  4. Review and adjust alert thresholds based on usage patterns"
echo "  5. Set up automated anomaly detection for data quality"
echo ""
echo "🎯 Monitoring is now active for DMC Data Pipeline!"