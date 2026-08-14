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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Load Bronze tables
# ============================================================

bronze_customers_df = spark.table("bronze.bronze_customers")
bronze_rentals_df = spark.table("bronze.bronze_rentals")
bronze_billing_df = spark.table("bronze.bronze_billing")
bronze_depots_df = spark.table("bronze.bronze_depots")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Customer variants in bronze:", bronze_customers_df.select("CUSTOMER_TYPE").distinct().count())
bronze_customers_df.select("CUSTOMER_TYPE").distinct().show()

customer_window = (
    Window
    .partitionBy("CUSTOMER_ID")
    .orderBy(F.col("ingested_at").desc())
)

duplicate_customers = (
    bronze_customers_df
    .withColumn("_rn", F.row_number().over(customer_window))
    .filter(F.col("_rn") > 1)
    .count()
)

print(f"Duplicate customers count: {duplicate_customers}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 1. Select the final Silver schema
# ============================================================

silver_depots_df = (
    bronze_depots_df
    .selectExpr(
        "DEPOT_CODE as depot_code",
        "DEPOT_NAME as depot_name",
        "ZONE as zone",
        "cast(FLEET_SIZE as int) as fleet_size"
    )
)

# ============================================================
# 2. Write Silver table
# ============================================================

silver_depots_df.write \
    .mode("overwrite") \
    .saveAsTable("silver.silver_depots")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
#  Validation
# ============================================================

bronze_depots_count = bronze_depots_df.count()
silver_depots_count = spark.table("silver.silver_depots").count()

print(f"Bronze depots count : {bronze_depots_count}")
print(f"Silver depots count : {silver_depots_count}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 1. Deduplication
# ============================================================

customer_window = (
    Window
    .partitionBy("CUSTOMER_ID")
    .orderBy(F.col("ingested_at").desc())
)

customers_dedup_df = (
    bronze_customers_df
    .withColumn("_rn", F.row_number().over(customer_window))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)


# ============================================================
# 2. Data cleansing / standardization
# ============================================================

silver_customers_df = (
    customers_dedup_df
    .withColumn(
        "customer_name", 
        F.initcap
            (F.trim(F.col("CUSTOMER_NAME")))
        )
    .withColumn(
        "customer_type",
        F.when(
            F.lower(F.col("CUSTOMER_TYPE")).isin("individual", "ind"),
            "individual"
        )
        .when(
            F.lower(F.col("CUSTOMER_TYPE")).isin("contractor", "contr."),
            "contractor"
        )
        .when(
            F.lower(F.col("CUSTOMER_TYPE")).isin(
                "company", "corporate", "corp"
            ),
            "company"
        )
        .otherwise(None)
    )
    .withColumn(
        "registered_on",
        F.coalesce(
            F.to_date(F.col("REGISTERED_ON"), "dd-MM-yyyy"),
            F.to_date(F.col("REGISTERED_ON"), "yyyy/MM/dd")
        )
    )
    .withColumn(
        "kyc_verified_on",
        F.to_date(
            F.col("KYC_VERIFIED_ON"),
            "yyyy-MM-dd"
        )
    )
)


# ============================================================
# 3. Select the final Silver schema
# ============================================================

silver_customers_df = (
    silver_customers_df
    .selectExpr(
        "CUSTOMER_ID as customer_id",
        "customer_name",
        "registered_on",
        "kyc_verified_on",
        "customer_type",
        "CITY as city"
    )
)


# ============================================================
# 4. Write Silver table
# ============================================================

silver_customers_df.write \
    .mode("overwrite") \
    .saveAsTable("silver.silver_customers")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Customers - Validation
# ============================================================

duplicate_cust_count = (bronze_customers_df
                .withColumn("_rn", F.row_number().over(customer_window))
                .filter(F.col("_rn") > 1)
                .count())
    
print(f"Bronze customers :  {bronze_customers_df.count()}")
print(f"Silver customers : {spark.table('silver.silver_customers').count()}")
print(f"Duplicate records removed: {duplicate_cust_count}")

# ============================================================
# Customer type standardization validation
# ============================================================

raw_customer_types = (
    bronze_customers_df
    .select("CUSTOMER_TYPE")
    .distinct()
    .count()
)

silver_customer_types = (
    spark.table("silver.silver_customers")
    .select("customer_type")
    .distinct()
    .count()
)

print(f"Customer type variants: {raw_customer_types} -> {silver_customer_types}")

# ============================================================
# Registered date format validation
# ============================================================

dedup_count = customers_dedup_df.count()

naive_1 = customers_dedup_df.withColumn(
    "registered_on",
    F.to_date(
        F.col("REGISTERED_ON"),
        "dd-MM-yyyy"
    )
)

lost_1 = naive_1.filter(
    F.col("registered_on").isNull()
).count()

print(
    f"Single-format (dd-MM-yyyy) would have nulled "
    f"{lost_1} of {dedup_count} dates"
)


naive_2 = customers_dedup_df.withColumn(
    "registered_on",
    F.to_date(
        F.col("REGISTERED_ON"),
        "yyyy/MM/dd"
    )
)

lost_2 = naive_2.filter(
    F.col("registered_on").isNull()
).count()

print(
    f"Single-format (yyyy/MM/dd) would have nulled "
    f"{lost_2} of {dedup_count} dates"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 1. Deduplication
# ============================================================

rental_window = (
    Window
    .partitionBy("RENTAL_ID")
    .orderBy(F.col("ingested_at").desc())
)

rentals_dedup_df = (
    bronze_rentals_df
    .withColumn("_rn", F.row_number().over(rental_window))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

# ============================================================
# 2. Data cleansing / standardization
# ============================================================

silver_rentals_df = (
    rentals_dedup_df
    .withColumn(
        "checkout_ts",
        F.to_timestamp("checkout_ts", "yyyy-MM-dd HH:mm:ss")
    )
    .withColumn(
        "checkin_ts",
        F.to_timestamp("checkin_ts", "yyyy-MM-dd HH:mm:ss")
    )
    .withColumn(
        "rental_type",
        F.upper(F.col("RENTAL_TYPE"))
    )
)


# ============================================================
# 3. Identify invalid records
# ============================================================

bad = (
    F.col("checkin_ts") < F.col("checkout_ts")
)


# ============================================================
# 4. Quarantine invalid records
# ============================================================

careless_rentals_df = silver_rentals_df.filter(~bad)

silver_rentals_quarantine_df = (
    silver_rentals_df
    .filter(bad)
    .withColumn(
        "quarantine_reason",
        F.lit("check-in recorded before a check-out")
    )
)

silver_rentals_quarantine_df.write \
    .mode("overwrite") \
    .saveAsTable("silver.silver_rentals_quarantine")


# ============================================================
# 5. Keep valid records for Silver
# ============================================================

silver_rentals_df = (
    silver_rentals_df
    .filter(~F.coalesce(bad, F.lit(False)))
)


# ============================================================
# 6. Select final Silver schema
# ============================================================

silver_rentals_df = (
    silver_rentals_df
    .selectExpr(
        "rental_id",
        "customer_id",
        "depot_code",
        "asset_id",
        "checkout_ts",
        "checkin_ts",
        "rental_type"
    )
)


# ============================================================
# 7. Write Silver table
# ============================================================

silver_rentals_df.write \
    .mode("overwrite") \
    .saveAsTable("silver.silver_rentals")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Validation
# ============================================================

duplicate_rentals_count =  (
    bronze_rentals_df
                .withColumn("_rn", F.row_number().over(rental_window))
                .filter(F.col("_rn") > 1)
                .count()
                )
                
print(f"Bronze rentals: {bronze_rentals_df.count()}")

print(f"Duplicates: {duplicate_rentals_count}")
   
print(f"Quarantined: {spark.table('silver.silver_rentals_quarantine').count()}")
   
print(f"Silver rentals: {spark.table('silver.silver_rentals').count()}")

print(f"Rental type variants: ", bronze_rentals_df.select('RENTAL_TYPE').distinct().count(),
      "-> ", silver_rentals_df.select('rental_type').distinct().count())
   

still_out_for_rent = (
    spark.table("silver.silver_rentals")
    .filter(F.col("checkin_ts").isNull())
    .count()
)

print(f"Still out for rent: {still_out_for_rent}")
   
correct = silver_rentals_df.count()
careless =  careless_rentals_df.count()

print(f"Filter without NULL handling         -> {careless:,} rows")
print(f"Filter with NULL-safe condition      -> {correct:,} rows")
print(f"Still-out rentals lost without NULL handling -> {correct - careless:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 1. Data cleansing / standardization
# ============================================================

silver_billing_df = (
    bronze_billing_df
    .withColumn(
        "amount_inr",
        F.regexp_replace(
            F.regexp_replace(
                F.col("AMOUNT_INR"),
                r"Rs.\s*",
                ""
            ),
            ",",
            ""
        ).cast("decimal(12,2)")
    )
    .withColumn(
        "payer_type",
        F.upper(F.trim(F.col("PAYER_TYPE")))
    )
    .withColumn(
        "bill_date",
        F.to_date(
            F.col("BILL_DATE"),
            "yyyy-MM-dd"
        )
    )
)


# ============================================================
# 2. Select the final Silver schema
# ============================================================

silver_billing_df = (
    silver_billing_df
    .selectExpr(
        "BILL_ID as bill_id",
        "RENTAL_ID as rental_id",
        "bill_date",
        "amount_inr",
        "payer_type"
    )
)


# ============================================================
# 3. Build known rental IDs
# ============================================================

known_rentals_df = (
    silver_rentals_df
    .select("rental_id")
    .union(
        silver_rentals_quarantine_df
        .select("rental_id")
    )
    .distinct()
    .withColumn("_match", F.lit(1))
)


# ============================================================
# 4. Validate billing against known rentals
# ============================================================

joined_df = (
    silver_billing_df
    .join(
        known_rentals_df,
        on="rental_id",
        how="left"
    )
)


# ============================================================
# 5. Write unmatched billing records
# ============================================================

(
    joined_df
    .filter(F.col("_match").isNull())
    .drop("_match")
    .write
    .mode("overwrite")
    .saveAsTable("silver.silver_billing_unmatched")
)


# ============================================================
# 6. Write matched billing records to Silver
# ============================================================

(
    joined_df
    .filter(F.col("_match").isNotNull())
    .drop("_match")
    .write
    .mode("overwrite")
    .saveAsTable("silver.silver_billing")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Billing - Validation
# ============================================================

unmatched_bills = (
    spark.table("silver.silver_billing_unmatched")
    .count()
)

silver_bills = (
    spark.table("silver.silver_billing")
    .count()
)

print(f"Unmatched bills : {unmatched_bills:,}")
print(f"Silver bills    : {silver_bills:,}")


# ============================================================
# Payer type standardization
# ============================================================

raw_payer_types = (
    bronze_billing_df
    .select("PAYER_TYPE")
    .distinct()
    .count()
)

silver_payer_types = (
    spark.table("silver.silver_billing")
    .select("payer_type")
    .distinct()
    .count()
)

print(
    f"Payer type variants: {raw_payer_types} -> {silver_payer_types}"
)


# ============================================================
# Amount parsing validation
# ============================================================

amount_parse_failures = (
    spark.table("silver.silver_billing")
    .filter(F.col("amount_inr").isNull())
    .count()
)

bill_date_parse_failures = (
    spark.table("silver.silver_billing")
    .filter(F.col("bill_date").isNull())
    .count()
)

print(f"Amount parse failures: {amount_parse_failures:,}")
print(f"Bill date parse failures: {bill_date_parse_failures:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

schema = "silver"
tables = ["silver_billing", 
          "silver_billing_unmatched",
          "silver_customers", 
          "silver_depots", 
          "silver_rentals",
          "silver_rentals_quarantine"]
for table in tables:
    full_table_name = f"{schema}.{table}"
    count = spark.table(full_table_name).count()
    print(f"{table:30} {count:>10,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Alternative approach for coalese in rentals.
"""

bad = (
    F.col("checkin_ts").isNotNull()
    & F.col("checkout_ts").isNotNull()
    & (F.col("checkin_ts") < F.col("checkout_ts"))
)

# Quarantine bad records
silver_rentals_quarantine_df = (
    silver_rentals_df
    .filter(bad)
    .withColumn(
        "quarantine_reason",
        F.lit("check-in recorded before a check-out")
    )
)

# Keep valid records
silver_rentals_df = silver_rentals_df.filter(~bad)

"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
