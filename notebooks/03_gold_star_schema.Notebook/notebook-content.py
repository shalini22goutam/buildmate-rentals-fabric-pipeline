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

from pyspark.sql import functions as F

WINDOW_START = "2026-06-01"
WINDOW_END   = "2026-07-01"   # exclusive edge of the 30-day reporting window

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Load Silver tables
# ============================================================

silver_customers_df = spark.table("silver.silver_customers")
silver_rentals_df = spark.table("silver.silver_rentals")
silver_billing_df = spark.table("silver.silver_billing")
silver_depots_df = spark.table("silver.silver_depots")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 1. Create Date Dimension
# ============================================================

gold_dim_date_df = spark.sql(
    """
    SELECT
        CAST(d AS DATE)        AS date_key,
        YEAR(d)               AS year,
        MONTH(d)              AS month,
        DATE_FORMAT(d, 'MMMM') AS month_name,
        DAY(d)                AS day_of_month,
        DATE_FORMAT(d, 'EEEE') AS day_name,
        CASE
            WHEN DAYOFWEEK(d) IN (1, 7)
                THEN true
            ELSE false
        END AS is_weekend
    FROM (
        SELECT EXPLODE(
            SEQUENCE(
                DATE'2026-06-01',
                DATE'2026-06-30',
                INTERVAL 1 DAY
            )
        ) AS d
    )
    """
)


# ============================================================
# 2. Write Gold table
# ============================================================

gold_dim_date_df.write \
    .mode("overwrite") \
    .saveAsTable("gold.gold_dim_date")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 1. Write Gold table
# ============================================================

gold_dim_depots_df = silver_depots_df

gold_dim_depots_df.write \
    .mode("overwrite") \
    .saveAsTable("gold.gold_dim_depots")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 1. Select final Gold schema
# ============================================================

gold_fact_billing_df = (
    silver_billing_df
    .select(
        "bill_id",
        "rental_id",
        "bill_date",
        "amount_inr",
        "payer_type"
    )
)


# ============================================================
# 2. Write Gold table
# ============================================================

gold_fact_billing_df.write \
    .mode("overwrite") \
    .saveAsTable("gold.gold_fact_billing")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 1. Calculate customer metrics
# ============================================================

gold_dim_customer_df = (
    silver_customers_df
    .withColumn(
        "tenure",
        F.floor(
            F.datediff(
                F.lit(WINDOW_END),
                F.col("registered_on")
            ) / 365.25
        ).cast("int")
    )
    .withColumn(
        "tenure_band",
        F.when(F.col("tenure") < 18, "0-17")
        .when(F.col("tenure") < 40, "18-39")
        .when(F.col("tenure") < 60, "40-59")
        .otherwise("60+")
    )
)


# ============================================================
# 2. Select final Gold schema
# ============================================================

gold_dim_customer_df = (
    gold_dim_customer_df
    .select(
        "customer_id",
        "customer_name",
        "registered_on",
        "tenure",
        "tenure_band",
        "kyc_verified_on",
        "customer_type",
        "city"
    )
)


# ============================================================
# 3. Write Gold table
# ============================================================

gold_dim_customer_df.write \
    .mode("overwrite") \
    .saveAsTable("gold.gold_dim_customer")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 1. Calculate rental metrics
# ============================================================

gold_fact_rental_df = (
    silver_rentals_df
    .withColumn(
        "checkout_date", 
            F.to_date("checkout_ts")
            )
    .withColumn(
        "checkin_date", 
            F.to_date("checkin_ts")
            )
    .withColumn(
        "rental_duration_days",
        F.round(
            (
                F.col("checkin_ts").cast("long")
                - F.col("checkout_ts").cast("long")
            ) / 86400.0,
            2
        )
    )
    .withColumn(
        "is_returned",
        F.col("checkin_ts").isNotNull().cast("int")
    )
    .withColumn(
        "_end",
        F.least(
            F.coalesce(
                F.col("checkin_ts"),
                F.lit(WINDOW_END).cast("timestamp")
            ),
            F.lit(WINDOW_END).cast("timestamp")
        )
    )
    .withColumn(
        "_start",
        F.greatest(
            F.col("checkout_ts"),
            F.lit(WINDOW_START).cast("timestamp")
        )
    )
    .withColumn(
        "asset_days_in_window",
        F.round(
            (
                F.col("_end").cast("long")
                - F.col("_start").cast("long")
            ) / 86400.0,
            4
        )
    )
)


# ============================================================
# 2. Select final Gold schema
# ============================================================

gold_fact_rental_df = (
    gold_fact_rental_df
    .select(
        "rental_id",
        "customer_id",
        "depot_code",
        "asset_id",
        "checkin_date",
        "checkout_date",
        "checkout_ts",
        "checkin_ts",
        "rental_type",
        "rental_duration_days",
        "is_returned",
        "asset_days_in_window"
    )
)


# ============================================================
# 3. Write Gold table
# ============================================================

gold_fact_rental_df.write \
    .mode("overwrite") \
    .saveAsTable("gold.gold_fact_rental")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Rentals - Gold Validation
# ============================================================

gold_rental_df = spark.table("gold.gold_fact_rental")

still_out_count = (
    gold_rental_df
    .filter(F.col("checkin_ts").isNull())
    .count()
)

null_duration_count = (
    gold_rental_df
    .filter(F.col("rental_duration_days").isNull())
    .count()
)

not_returned_count = (
    gold_rental_df
    .filter(F.col("is_returned") == 0)
    .count()
)

print(
    f"Still-out rentals: {still_out_count:,}"
)

print(
    f"NULL rental duration: {null_duration_count:,}"
)

print(
    f"Not returned (is_returned = 0): {not_returned_count:,}"
)


# ============================================================
# Asset days validation
# ============================================================

max_asset_days = (
    gold_rental_df
    .select(
        F.max("asset_days_in_window").alias("max_asset_days")
    )
    .first()["max_asset_days"]
)

print(
    f"Maximum asset days in window: {max_asset_days}"
)

print(
    f"No asset_days_in_window exceeds 30: "
    f"{max_asset_days <= 30 if max_asset_days is not None else True}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

schema = "gold"
tables = ["gold_fact_billing", 
          "gold_dim_customer", 
          "gold_dim_depots", 
          "gold_fact_rental",
          "gold_dim_date"]
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

# MAGIC %%sql
# MAGIC OPTIMIZE gold.gold_fact_rental ZORDER BY (depot_code, checkout_ts);
# MAGIC OPTIMIZE gold.gold_fact_billing ZORDER BY (rental_id);

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DESCRIBE HISTORY gold.gold_fact_rental;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Why did you not partition this table?**
# 
# Partitioning is useful for large tables with millions of rows. Since this table is small, partitioning would not make much difference.
# 
# 
# **Why duration is null rather than zero?**
# 
# Example:
# 
# checkout_ts = 2026-08-01 10:00:00 and checkin_ts  = NULL
# 
# checkin_ts.cast("long") -> NULL
# 
# NULL - checkout_ts -> NULL
# 
# NULL / 86400.0 -> NULL
# 
# round(NULL, 2) -> NULL
# 
# So, rental_duration_days is NULL and not 0 which is scenario-wise correct also.

