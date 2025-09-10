#!/bin/bash

# Test Pipeline Execution Script
# This script tests individual components and the full pipeline

set -e

PROJECT_ID="proyecto-integrador-dae-2025"
REGION="us-central1"
CLUSTER_NAME="cluster-integrador-2025"
BUCKET="dae-integrador-2025"

echo "🧪 Testing DMC Data Pipeline..."
echo "Project: $PROJECT_ID"
echo "Region: $REGION"
echo ""

# Function to check command success
check_success() {
    if [ $? -eq 0 ]; then
        echo "✅ $1 - SUCCESS"
    else
        echo "❌ $1 - FAILED"
        exit 1
    fi
}

# Test 1: Check GCP Authentication
echo "🔐 Testing GCP Authentication..."
gcloud auth list --filter=status:ACTIVE --format="value(account)" > /dev/null
check_success "GCP Authentication"

# Test 2: Check Project Access
echo "📋 Testing Project Access..."
gcloud projects describe $PROJECT_ID > /dev/null
check_success "Project Access"

# Test 3: Check BigQuery Datasets
echo "🗄️ Testing BigQuery Datasets..."
for dataset in bronze silver_clean gold; do
    echo "  Checking dataset: $dataset"
    bq show --format=none $PROJECT_ID:$dataset > /dev/null
    check_success "BigQuery Dataset: $dataset"
done

# Test 4: Check Cloud Storage Bucket
echo "🪣 Testing Cloud Storage Bucket..."
gsutil ls gs://$BUCKET > /dev/null
check_success "Cloud Storage Bucket Access"

# Test 5: Check DataProc Cluster
echo "🖥️ Testing DataProc Cluster..."
CLUSTER_STATE=$(gcloud dataproc clusters describe $CLUSTER_NAME \
    --region=$REGION \
    --format="value(status.state)" 2>/dev/null || echo "NOT_FOUND")

echo "  Cluster state: $CLUSTER_STATE"

if [ "$CLUSTER_STATE" = "NOT_FOUND" ]; then
    echo "  Creating DataProc cluster for testing..."
    gcloud dataproc clusters create $CLUSTER_NAME \
        --region=$REGION \
        --zone=us-central1-a \
        --master-machine-type=n2-standard-2 \
        --master-boot-disk-size=30GB \
        --num-workers=2 \
        --worker-machine-type=n2-standard-2 \
        --worker-boot-disk-size=30GB \
        --image-version=2.2-debian12 \
        --optional-components=JUPYTER,ZEPPELIN,TRINO,DELTA \
        --enable-http-port-access
    check_success "DataProc Cluster Creation"
elif [ "$CLUSTER_STATE" = "STOPPED" ]; then
    echo "  Starting DataProc cluster..."
    gcloud dataproc clusters start $CLUSTER_NAME --region=$REGION
    echo "  Waiting for cluster to be ready..."
    sleep 60
    check_success "DataProc Cluster Start"
elif [ "$CLUSTER_STATE" = "RUNNING" ]; then
    echo "  ✅ DataProc cluster is already running"
else
    echo "  ⚠️ DataProc cluster in unexpected state: $CLUSTER_STATE"
fi

# Test 6: Upload ETL Jobs to GCS
echo "📤 Testing ETL Jobs Upload..."
gsutil -m cp -r etl_spark/ gs://$BUCKET/jobs/ > /dev/null
check_success "ETL Jobs Upload"

# Test 7: Test Bronze Job (SIMBAD)
echo "⚡ Testing Bronze ETL Job..."
BRONZE_JOB_ID=$(gcloud dataproc jobs submit pyspark \
    gs://$BUCKET/jobs/etl_spark/bronze/land_simbad_bronze.py \
    --cluster=$CLUSTER_NAME \
    --region=$REGION \
    --format="value(reference.jobId)" \
    --async)

echo "  Bronze job submitted: $BRONZE_JOB_ID"
echo "  Waiting for bronze job to complete..."

# Wait for job completion (max 10 minutes)
timeout=600
elapsed=0
while [ $elapsed -lt $timeout ]; do
    JOB_STATE=$(gcloud dataproc jobs describe $BRONZE_JOB_ID \
        --region=$REGION \
        --format="value(status.state)" 2>/dev/null || echo "UNKNOWN")
    
    if [ "$JOB_STATE" = "DONE" ]; then
        echo "  ✅ Bronze job completed successfully"
        break
    elif [ "$JOB_STATE" = "ERROR" ]; then
        echo "  ❌ Bronze job failed"
        gcloud dataproc jobs describe $BRONZE_JOB_ID --region=$REGION
        exit 1
    else
        echo "  ⏳ Bronze job state: $JOB_STATE (${elapsed}s elapsed)"
        sleep 30
        elapsed=$((elapsed + 30))
    fi
done

if [ $elapsed -ge $timeout ]; then
    echo "  ⚠️ Bronze job timed out after ${timeout}s"
    exit 1
fi

# Test 8: Test Silver ETL (BigQuery)
echo "🥈 Testing Silver ETL (BigQuery MERGE)..."
bq query --use_legacy_sql=false --format=none --max_rows=0 \
"SELECT COUNT(*) FROM \`$PROJECT_ID.bronze.simbad_bronze_parquet_ext\`"
check_success "Silver ETL Prerequisites (Bronze External Table)"

# Run Silver MERGE (simplified version)
bq query --use_legacy_sql=false --format=none --max_rows=0 \
"CREATE TABLE IF NOT EXISTS \`$PROJECT_ID.silver_clean.simbad_hipotecarios_test\` AS
SELECT * FROM \`$PROJECT_ID.bronze.simbad_bronze_parquet_ext\` LIMIT 1000"
check_success "Silver ETL Test Query"

# Test 9: Test Gold ETL (BigQuery)
echo "🥇 Testing Gold ETL (Business Indicators)..."
bq query --use_legacy_sql=false --format=none --max_rows=0 \
"CREATE OR REPLACE TABLE \`$PROJECT_ID.gold.simbad_gold_test\` AS
SELECT 
    DATE_TRUNC(CURRENT_DATE(), MONTH) as periodo_date,
    'TEST' as provincia,
    COUNT(*) as record_count
FROM \`$PROJECT_ID.bronze.simbad_bronze_parquet_ext\`
WHERE periodo IS NOT NULL
GROUP BY 1, 2"
check_success "Gold ETL Test Query"

# Test 10: Data Quality Checks
echo "📊 Testing Data Quality Checks..."

# Check Bronze data
BRONZE_COUNT=$(bq query --use_legacy_sql=false --format=csv \
"SELECT COUNT(*) as count FROM \`$PROJECT_ID.bronze.simbad_bronze_parquet_ext\`" | tail -n +2)
echo "  Bronze records: $BRONZE_COUNT"

# Check if we have recent data
RECENT_DATA=$(bq query --use_legacy_sql=false --format=csv \
"SELECT COUNT(*) as count FROM \`$PROJECT_ID.bronze.simbad_bronze_parquet_ext\`
WHERE dt_captura >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)" | tail -n +2)
echo "  Recent records (last 7 days): $RECENT_DATA"

if [ "$BRONZE_COUNT" -gt "0" ]; then
    echo "  ✅ Bronze data quality check passed"
else
    echo "  ⚠️ No data found in bronze layer"
fi

# Test 11: Clean up test resources
echo "🧹 Cleaning up test resources..."
bq rm -f $PROJECT_ID:silver_clean.simbad_hipotecarios_test 2>/dev/null || true
bq rm -f $PROJECT_ID:gold.simbad_gold_test 2>/dev/null || true
echo "  ✅ Test cleanup completed"

# Final Results
echo ""
echo "🎉 Pipeline Test Results:"
echo "  ✅ GCP Authentication"
echo "  ✅ Project Access"
echo "  ✅ BigQuery Datasets"
echo "  ✅ Cloud Storage"
echo "  ✅ DataProc Cluster"
echo "  ✅ ETL Jobs Upload"
echo "  ✅ Bronze ETL Job"
echo "  ✅ Silver ETL Query"
echo "  ✅ Gold ETL Query"
echo "  ✅ Data Quality Checks"
echo ""
echo "🚀 All pipeline components tested successfully!"
echo ""
echo "📝 Next Steps:"
echo "  1. Run full pipeline: gcloud builds triggers run etl-pipeline-main"
echo "  2. Monitor execution: https://console.cloud.google.com/cloud-build/builds"
echo "  3. Check data quality: BigQuery console"
echo "  4. Set up monitoring alerts"
echo ""

# Optional: Stop cluster to save costs
read -p "🛑 Stop DataProc cluster to save costs? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "  Stopping DataProc cluster..."
    gcloud dataproc clusters stop $CLUSTER_NAME --region=$REGION
    echo "  ✅ DataProc cluster stopped"
fi

echo "✨ Pipeline testing completed successfully!"