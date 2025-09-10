#!/usr/bin/env python
# coding: utf-8
"""
Silver ETL Job: SIMBAD Bronze to Silver
Implements incremental MERGE logic based on sp_silver_upsert.sql
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import sys
from datetime import datetime

def create_spark_session():
    """Create Spark session with BigQuery connector"""
    return (SparkSession.builder
            .appName("SimbadSilverETL")
            .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.2")
            .getOrCreate())

def get_max_periodo_ym(spark, project_id):
    """Get max periodo_ym from existing silver table"""
    try:
        max_df = spark.read.format("bigquery") \
            .option("table", f"{project_id}.silver_clean.simbad_hipotecarios") \
            .load() \
            .select(F.max("periodo_ym").alias("max_ym"))
        
        max_ym = max_df.collect()[0]["max_ym"]
        return max_ym if max_ym is not None else 0
    except Exception as e:
        print(f"Silver table doesn't exist yet or error: {e}")
        return 0

def process_bronze_to_silver(spark, project_id, bucket_name):
    """
    Process bronze data to silver with deduplication and data quality
    Based on sp_silver_upsert.sql logic
    """
    
    # Get current max periodo_ym from silver
    max_ym = get_max_periodo_ym(spark, project_id)
    print(f"Max periodo_ym in silver: {max_ym}")
    
    # Read from bronze external table
    bronze_df = spark.read.format("bigquery") \
        .option("table", f"{project_id}.bronze.simbad_bronze_parquet_ext") \
        .load()
    
    # Data normalization and casting (equivalent to src_raw CTE)
    src_raw = bronze_df.select(
        # String fields - normalize whitespace
        F.regexp_replace(F.trim(F.col("periodo")), r'\s+', ' ').alias("periodo"),
        F.regexp_replace(F.trim(F.col("tipoCliente")), r'\s+', ' ').alias("tipoCliente"),
        F.regexp_replace(F.trim(F.col("actividad")), r'\s+', ' ').alias("actividad"),
        F.regexp_replace(F.trim(F.col("entidad")), r'\s+', ' ').alias("entidad"),
        F.regexp_replace(F.trim(F.col("sector")), r'\s+', ' ').alias("sector"),
        F.regexp_replace(F.trim(F.col("moneda")), r'\s+', ' ').alias("moneda"),
        F.regexp_replace(F.trim(F.col("provincia")), r'\s+', ' ').alias("provincia"),
        F.regexp_replace(F.trim(F.col("residencia")), r'\s+', ' ').alias("residencia"),
        F.regexp_replace(F.trim(F.col("genero")), r'\s+', ' ').alias("genero"),
        F.regexp_replace(F.trim(F.col("persona")), r'\s+', ' ').alias("persona"),
        
        # Numeric fields - safe casting
        F.col("deudaCapital").cast("double").alias("deudaCapital"),
        F.col("deudaVencida").cast("double").alias("deudaVencida"),
        F.col("deudaVencidaDe31A90Dias").cast("double").alias("deudaVencidaDe31A90Dias"),
        F.col("cantidadCredito").cast("int").alias("cantidadCredito"),
        F.col("valorDesembolso").cast("double").alias("valorDesembolso"),
        F.col("valorGarantia").cast("double").alias("valorGarantia"),
        F.col("valorProvisionCapitalYRendimiento").cast("double").alias("valorProvisionCapitalYRendimiento"),
        F.col("deuda").cast("double").alias("deuda"),
        
        # Date processing
        F.coalesce(
            F.col("periodo_date"), 
            F.to_date(F.trim(F.col("periodo")), "yyyy-MM")
        ).alias("periodo_date"),
        
        F.year(F.coalesce(
            F.col("periodo_date"), 
            F.to_date(F.trim(F.col("periodo")), "yyyy-MM")
        )).alias("anio"),
        
        F.month(F.coalesce(
            F.col("periodo_date"), 
            F.to_date(F.trim(F.col("periodo")), "yyyy-MM")
        )).alias("mes"),
        
        F.date_format(F.coalesce(
            F.col("periodo_date"), 
            F.to_date(F.trim(F.col("periodo")), "yyyy-MM")
        ), "yyyyMM").cast("int").alias("periodo_ym"),
        
        F.to_date(F.col("dt_captura"), "yyyy-MM-dd").alias("dt_captura"),
        F.col("archivo_origen")
    )
    
    # Deduplication (equivalent to src CTE)
    # Keep latest record per natural key
    window_spec = Window.partitionBy(
        "periodo", "entidad", "provincia", "moneda", 
        "tipoCliente", "actividad", "sector", "residencia"
    ).orderBy(F.desc("dt_captura"), F.desc("archivo_origen"))
    
    src_deduped = src_raw.withColumn("rn", F.row_number().over(window_spec)) \
                         .filter(F.col("rn") == 1) \
                         .drop("rn")
    
    # Filter for incremental processing
    src_incremental = src_deduped.filter(F.col("periodo_ym") >= max_ym)
    
    print(f"Records to process: {src_incremental.count()}")
    
    if src_incremental.count() == 0:
        print("No new records to process")
        return
    
    # Write to silver table (this will trigger MERGE in BigQuery)
    # Using overwrite mode with SaveMode.Append equivalent
    src_incremental.write.format("bigquery") \
        .option("table", f"{project_id}.silver_clean.simbad_hipotecarios") \
        .option("writeMethod", "direct") \
        .option("temporaryGcsBucket", bucket_name) \
        .mode("append") \
        .save()
    
    print(f"Successfully processed {src_incremental.count()} records to silver")

def main():
    """Main ETL execution"""
    if len(sys.argv) < 3:
        print("Usage: silver_simbad_job.py <project_id> <temp_bucket>")
        sys.exit(1)
    
    project_id = sys.argv[1]
    temp_bucket = sys.argv[2]
    
    spark = create_spark_session()
    
    try:
        print(f"Starting Silver ETL for project: {project_id}")
        process_bronze_to_silver(spark, project_id, temp_bucket)
        print("Silver ETL completed successfully")
    except Exception as e:
        print(f"Error in Silver ETL: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()