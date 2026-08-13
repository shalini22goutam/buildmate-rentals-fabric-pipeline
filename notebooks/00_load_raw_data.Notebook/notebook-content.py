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

import zipfile, os
src = "/lakehouse/default/Files/buildmate_raw_data.zip"
dst = "/lakehouse/default/Files/buildmate_raw_data/"
with zipfile.ZipFile(src) as z:
    z.extractall(dst)
print("expanded")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

base = "Files/buildmate_raw_data/raw"
def n(path):
    return spark.read.option("header", True).csv(path).count()
print("customers", n(f"{base}/customer_master/customer_master_export.csv"))
print("depots ", n(f"{base}/depots/depot_master.csv"))
print("rentals ", n(f"{base}/rentals/rentals_*.csv"))
print("billing ", n(f"{base}/billing/billing_export.csv"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC --test

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
