# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "05e2c9a4-9967-41fc-b5eb-58157421e19a",
# META       "default_lakehouse_name": "lh_buildmate",
# META       "default_lakehouse_workspace_id": "7b7fba46-2d9f-483f-a5f8-bb538f95967b",
# META       "known_lakehouses": [
# META         {
# META           "id": "05e2c9a4-9967-41fc-b5eb-58157421e19a"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F, Window

SOURCE = "Files/buildmate_raw_data/raw"

def to_bronze(df):
    """Add lineage columns. Nothing else. No cleaning happens in Bronze."""
    return (
            df.withColumn("ingested_at", F.current_timestamp())
              .withColumn("source_file", F.input_file_name())
            )

def read_raw_csv(path):
    return (spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(path))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

to_bronze(read_raw_csv(f"{SOURCE}/billing/billing_export.csv")) \
    .write.mode("overwrite").saveAsTable("bronze.bronze_billing")

to_bronze(read_raw_csv(f"{SOURCE}/customer_master/customer_master_export.csv")) \
    .write.mode("overwrite").saveAsTable("bronze.bronze_customers")

to_bronze(read_raw_csv(f"{SOURCE}/depots/depot_master.csv")) \
    .write.mode("overwrite").saveAsTable("bronze.bronze_depots")

to_bronze(read_raw_csv(f"{SOURCE}/rentals/rentals_*.csv")) \
    .write.mode("overwrite").saveAsTable("bronze.bronze_rentals")
    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

schema = "bronze"
tables = ["bronze_billing", 
          "bronze_customers", 
          "bronze_depots", 
          "bronze_rentals"]
for table in tables:
    full_table_name = f"{schema}.{table}"
    count = spark.table(full_table_name).count()
    print(f"{table:20s} {count:>10,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

(spark.table("bronze.bronze_rentals")
   .groupBy(F.regexp_extract("source_file", r"([^/]+)$", 1).alias("file"))
   .count()
   .filter(F.col("file").contains("2026-06-10"))
   .show(truncate=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
