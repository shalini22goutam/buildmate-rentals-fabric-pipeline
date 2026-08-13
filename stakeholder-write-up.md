
### Business Interpretation for Arun and Rhea

Hadapsar depot earns the most revenue across all six depots and also has the highest revenue per rental. 

The total revenue across all six depots is **₹49,775,401**, which is roughly **₹4.98 crore**. 

There are currently **175 machines out across all depots**. 

During data cleansing, null safety was handled using `filter(~coalesce(bad, False))` instead of `filter(~bad)`, ensuring that records with a null `bad` flag were retained. If this null-safety check had been missed, the currently-out machines could have been incorrectly classified as bad records and excluded from the Gold table, which would have resulted in a **currently-out figure of 0**.


