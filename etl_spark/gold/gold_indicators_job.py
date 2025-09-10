#!/usr/bin/env python
# coding: utf-8
"""
Gold ETL Job: Create business indicators table
Implements aggregation logic based on sp_gold_create.sql
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import sys
from datetime import datetime

def create_spark_session():
    """Create Spark session with BigQuery connector"""
    return (SparkSession.builder
            .appName("SimbadGoldIndicators")
            .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.2")
            .getOrCreate())

def create_gold_indicators(spark, project_id, temp_bucket):
    """
    Create gold indicators table with business metrics
    Based on sp_gold_create.sql logic
    """
    
    # A) Base SIMBAD (filter Asalariado Privado)
    simbad_df = spark.read.format("bigquery") \
        .option("table", f"{project_id}.silver_clean.simbad_hipotecarios") \
        .load() \
        .filter(F.col("tipoCliente") == "Asalariado Privado") \
        .select(
            "anio", "mes", "provincia", "tipoCliente", "actividad",
            "deudaCapital", "deudaVencida", "deudaVencidaDe31A90Dias", "cantidadCredito",
            "valorDesembolso", "valorGarantia", "valorProvisionCapitalYRendimiento", "deuda",
            "genero", "persona", "moneda", "sector", "entidad", "residencia"
        )
    
    # B) Aggregation (replica notebook logic)
    agg_df = simbad_df.groupBy("anio", "mes", "provincia", "tipoCliente", "actividad") \
        .agg(
            # Sum metrics
            F.sum("deudaCapital").alias("DEUDACAPITAL"),
            F.sum("deudaVencida").alias("DEUDAVENCIDA"),
            F.sum("deudaVencidaDe31A90Dias").alias("DEUDAVENCIDADE31A90DIAS"),
            F.sum("cantidadCredito").alias("CANTIDADCREDITO"),
            F.sum("valorGarantia").alias("VALORGARANTIA"),
            F.sum("valorProvisionCapitalYRendimiento").alias("VALORPROVISIONCAPITALYRENDIMIENTO"),
            F.sum("valorDesembolso").alias("VALORDESEMBOLSO"),
            F.sum("deuda").alias("DEUDA"),
            
            # Categorical averages (proportion of 1s)
            F.avg(
                F.when(F.col("genero").isin("Género femenino", "Genero femenino"), 1).otherwise(0)
            ).alias("GENERO"),
            F.avg(
                F.when(F.col("persona").isin("Persona jurídica", "Persona juridica"), 1).otherwise(0)
            ).alias("PERSONA"),
            F.avg(
                F.when(F.col("moneda").isin("Moneda Extranjera", "MONEDA EXTRANJERA"), 1).otherwise(0)
            ).alias("MONEDA"),
            F.avg(
                F.when(F.col("sector").isin("b) Privado", "B) Privado", "B) PRIVADO"), 1).otherwise(0)
            ).alias("SECTOR"),
            F.avg(
                F.when(F.col("entidad") == "APAP", 1).otherwise(0)
            ).alias("ENTIDAD"),
            F.avg(
                F.when(F.col("residencia").isin("a) Residente", "A) Residente"), 1).otherwise(0)
            ).alias("RESIDENCIA")
        ) \
        .withColumnRenamed("anio", "ANIO") \
        .withColumnRenamed("mes", "MES") \
        .withColumnRenamed("provincia", "PROVINCIA") \
        .withColumnRenamed("tipoCliente", "TIPOCLIENTE") \
        .withColumnRenamed("actividad", "ACTIVIDAD")
    
    # C) Derived metrics
    agg2_df = agg_df.withColumn(
        "TASA_MORA", 
        F.when(F.col("DEUDACAPITAL") > 0, F.col("DEUDAVENCIDADE31A90DIAS") / F.col("DEUDACAPITAL")).otherwise(0)
    ).withColumn(
        "COBERTURA_GARANTIA",
        F.when(F.col("DEUDACAPITAL") > 0, F.col("VALORGARANTIA") / F.col("DEUDACAPITAL")).otherwise(0)
    ).withColumn(
        "PROPORCION_PROVISIONADA",
        F.when(F.col("DEUDA") > 0, F.col("VALORPROVISIONCAPITALYRENDIMIENTO") / F.col("DEUDA")).otherwise(0)
    ).withColumn(
        "PD_AGREGADA",
        F.when(
            (F.col("DEUDAVENCIDA") > 0) & 
            (F.when(F.col("DEUDACAPITAL") > 0, F.col("DEUDAVENCIDADE31A90DIAS") / F.col("DEUDACAPITAL")).otherwise(0) >= 0.005),
            1
        ).otherwise(0)
    )
    
    # D) Inflation data (external table)
    try:
        inflacion_df = spark.read.format("bigquery") \
            .option("table", f"{project_id}.bronze.bronze_inflacion_12m_ext") \
            .load() \
            .select(
                F.year(F.to_date(F.col("FECHA"), "yyyy-MM")).alias("ANIO"),
                F.month(F.to_date(F.col("FECHA"), "yyyy-MM")).alias("MES"),
                F.regexp_replace(F.col("INFLACION"), ",", ".").cast("double").alias("INFLACION")
            )
    except Exception as e:
        print(f"Warning: Could not load inflation data: {e}")
        # Create empty DataFrame with proper schema
        schema = StructType([
            StructField("ANIO", IntegerType(), True),
            StructField("MES", IntegerType(), True),
            StructField("INFLACION", DoubleType(), True)
        ])
        inflacion_df = spark.createDataFrame([], schema)
    
    # E) Exchange rate data (external table)
    try:
        tipo_cambio_df = spark.read.format("bigquery") \
            .option("table", f"{project_id}.bronze.bronze_tipo_cambio_ext") \
            .load() \
            .select(
                F.year(F.to_date(F.col("FECHA"), "yyyy-MM-dd")).alias("ANIO"),
                F.month(F.to_date(F.col("FECHA"), "yyyy-MM-dd")).alias("MES"),
                F.regexp_replace(F.col("TC_VENTA"), ",", ".").cast("double").alias("TC_VENTA_RAW"),
                F.regexp_replace(F.col("TC_COMPRA"), ",", ".").cast("double").alias("TC_COMPRA_RAW")
            ) \
            .groupBy("ANIO", "MES") \
            .agg(
                F.avg("TC_VENTA_RAW").alias("TC_VENTA"),
                F.avg("TC_COMPRA_RAW").alias("TC_COMPRA")
            )
    except Exception as e:
        print(f"Warning: Could not load exchange rate data: {e}")
        # Create empty DataFrame with proper schema
        schema = StructType([
            StructField("ANIO", IntegerType(), True),
            StructField("MES", IntegerType(), True),
            StructField("TC_VENTA", DoubleType(), True),
            StructField("TC_COMPRA", DoubleType(), True)
        ])
        tipo_cambio_df = spark.createDataFrame([], schema)
    
    # F) Final result (equivalent to df_final)
    result_df = agg2_df.alias("a") \
        .join(inflacion_df.alias("i"), ["ANIO", "MES"], "left") \
        .join(tipo_cambio_df.alias("t"), ["ANIO", "MES"], "left") \
        .select(
            F.date_add(F.to_date(F.concat_ws("-", F.col("ANIO"), F.col("MES"), F.lit("01")), "yyyy-M-d"), 0).alias("periodo_date"),
            F.col("ANIO"),
            F.col("MES"),
            F.col("PROVINCIA"),
            F.col("DEUDACAPITAL"),
            F.col("DEUDAVENCIDA"),
            F.col("DEUDAVENCIDADE31A90DIAS"),
            F.col("CANTIDADCREDITO"),
            F.col("GENERO"),
            F.col("PERSONA"),
            F.col("MONEDA"),
            F.col("SECTOR"),
            F.col("ENTIDAD"),
            F.col("RESIDENCIA"),
            F.col("VALORGARANTIA"),
            F.col("VALORPROVISIONCAPITALYRENDIMIENTO"),
            F.col("VALORDESEMBOLSO"),
            F.col("DEUDA"),
            F.col("TASA_MORA"),
            F.col("COBERTURA_GARANTIA"),
            F.col("PROPORCION_PROVISIONADA"),
            F.col("PD_AGREGADA"),
            F.col("i.INFLACION"),
            F.col("t.TC_VENTA"),
            F.col("t.TC_COMPRA")
        )
    
    # Write to Gold table (overwrite mode)
    result_df.write.format("bigquery") \
        .option("table", f"{project_id}.gold.simbad_gold") \
        .option("writeMethod", "direct") \
        .option("temporaryGcsBucket", temp_bucket) \
        .option("createDisposition", "CREATE_IF_NEEDED") \
        .option("partitionField", "periodo_date") \
        .option("partitionType", "MONTH") \
        .option("clusteredFields", "PROVINCIA") \
        .mode("overwrite") \
        .save()
    
    print(f"Successfully created gold table with {result_df.count()} records")

def main():
    """Main ETL execution"""
    if len(sys.argv) < 3:
        print("Usage: gold_indicators_job.py <project_id> <temp_bucket>")
        sys.exit(1)
    
    project_id = sys.argv[1]
    temp_bucket = sys.argv[2]
    
    spark = create_spark_session()
    
    try:
        print(f"Starting Gold Indicators ETL for project: {project_id}")
        create_gold_indicators(spark, project_id, temp_bucket)
        print("Gold Indicators ETL completed successfully")
    except Exception as e:
        print(f"Error in Gold Indicators ETL: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()