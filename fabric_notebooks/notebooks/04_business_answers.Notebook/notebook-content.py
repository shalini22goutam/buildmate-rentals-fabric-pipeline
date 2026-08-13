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

# MAGIC %%sql
# MAGIC 
# MAGIC /*
# MAGIC (a) Operations wants to know which machine types tie up the longest. Average rental
# MAGIC duration by asset type, returned rentals only.
# MAGIC Required output: asset_type, avg_duration_days (2 dp), rentals.
# MAGIC */
# MAGIC 
# MAGIC SELECT
# MAGIC     depot_name AS asset_type,
# MAGIC     ROUND(AVG(rental_duration_days), 2) AS avg_duration_days,
# MAGIC     COUNT(*) AS rentals
# MAGIC FROM gold.gold_fact_rental fr
# MAGIC JOIN gold.gold_dim_depots d ON d.depot_code = fr.depot_code
# MAGIC WHERE is_returned = 1
# MAGIC GROUP BY asset_type
# MAGIC ORDER BY avg_duration_days DESC;
# MAGIC 
# MAGIC 
# MAGIC   

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC /*
# MAGIC (b) Rhea wants to see which depots are running hot. Fleet utilisation by depot, using
# MAGIC asset_days_in_window over fleet_size times thirty.
# MAGIC Required output: depot_name, fleet_size, asset_days_used (1 dp), utilisation_pct (1 dp).
# MAGIC */
# MAGIC 
# MAGIC SELECT
# MAGIC     dd.depot_name,
# MAGIC     dd.fleet_size,
# MAGIC     ROUND(SUM(fr.asset_days_in_window), 1) AS asset_days_used,
# MAGIC     ROUND(
# MAGIC         SUM(fr.asset_days_in_window)
# MAGIC         / NULLIF(dd.fleet_size * 30, 0) * 100,
# MAGIC         1
# MAGIC     ) AS utilisation_pct
# MAGIC FROM gold.gold_dim_depots AS dd
# MAGIC JOIN gold.gold_fact_rental AS fr
# MAGIC     ON dd.depot_code = fr.depot_code
# MAGIC GROUP BY
# MAGIC     dd.depot_name,
# MAGIC     dd.fleet_size
# MAGIC ORDER BY utilisation_pct DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC /*
# MAGIC (c) Finance wants the revenue split by how customers pay. Revenue by payer type.
# MAGIC Required output: payer_type, bills, revenue (0 dp), avg_bill (0 dp).
# MAGIC */
# MAGIC 
# MAGIC SELECT
# MAGIC     payer_type,
# MAGIC     COUNT(bill_id) AS bills,
# MAGIC     ROUND(SUM(amount_inr), 0) AS revenue,
# MAGIC     ROUND(AVG(amount_inr), 0) AS avg_bill
# MAGIC FROM gold.gold_fact_billing
# MAGIC GROUP BY payer_type
# MAGIC ORDER BY revenue DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC /*
# MAGIC (d) Arun wants revenue by depot, the number no single system holds. Join billing to rentals
# MAGIC to the depot dimension.
# MAGIC Required output: depot_name, bills, revenue (0 dp), revenue_per_rental (0 dp).
# MAGIC */
# MAGIC 
# MAGIC SELECT
# MAGIC     dd.depot_name,
# MAGIC     COUNT(fb.bill_id) AS bills,
# MAGIC     ROUND(SUM(fb.amount_inr), 0) AS revenue,
# MAGIC     ROUND(SUM(fb.amount_inr)/COUNT(fr.rental_id), 0) AS revenue_per_rental
# MAGIC FROM gold.gold_fact_billing fb
# MAGIC JOIN gold.gold_fact_rental fr
# MAGIC     ON fb.rental_id = fr.rental_id
# MAGIC JOIN gold.gold_dim_depots dd
# MAGIC     ON fr.depot_code = dd.depot_code
# MAGIC GROUP BY dd.depot_name
# MAGIC ORDER BY revenue DESC;
# MAGIC 
# MAGIC -- The total revenue across all depots is 49,775,401. This is roughly 4.98 crore.

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC /*
# MAGIC (e) The board wants the priority mix. Share of rentals by rental type.
# MAGIC Required output: rental_type, rentals, pct (1 dp).
# MAGIC */
# MAGIC 
# MAGIC SELECT
# MAGIC     rental_type,
# MAGIC     COUNT(*) AS rentals,
# MAGIC     ROUND(
# MAGIC         COUNT(rental_id) * 100.0 / SUM(COUNT(rental_id)) OVER (),
# MAGIC         1
# MAGIC     ) AS pct
# MAGIC FROM gold.gold_fact_rental
# MAGIC GROUP BY rental_type;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC /*
# MAGIC (f) Rhea wants to know what is out right now. Machines currently out by depot.
# MAGIC Required output: depot_name, currently_out.
# MAGIC */
# MAGIC 
# MAGIC SELECT
# MAGIC     dd.depot_name,
# MAGIC     COUNT(*) AS currently_out
# MAGIC FROM gold.gold_dim_depots dd
# MAGIC JOIN gold.gold_fact_rental fr
# MAGIC     ON dd.depot_code = fr.depot_code
# MAGIC WHERE fr.is_returned = 0
# MAGIC GROUP BY dd.depot_name
# MAGIC ORDER BY currently_out DESC;
# MAGIC 
# MAGIC -- Sum of currently_out across all depots = 175
# MAGIC -- Silver layer also shows same count in results where checkin_is is NULL

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
