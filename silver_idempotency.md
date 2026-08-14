## Idempotency - Make the rebuild safe to run again

The Silver build is designed to be **safe to run multiple times** by using **overwrite"" writemode.

### How Overwrite Mode Guarantees Idempotency

- Silver uses **overwrite mode** instead of append mode.
- Each run rebuilds the Silver table from the Bronze data.
- The same **deduplication-by-latest** rule is applied on every run.
- Running the same build again replaces the existing data with the same result.
- Therefore, rerunning the pipeline does **not create duplicate records**.
- Invalid records remain in the **quarantine table** and are not added back to Silver.
- When a new day's file is added to Bronze, the Silver rebuild includes the existing data plus only the new day's **valid rentals**.
