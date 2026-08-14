# Shared Capacity Discipline in Microsoft Fabric

## Question 1: Write a short note on shared-capacity discipline

Shared-capacity discipline means **using Fabric resources responsibly** because the capacity is shared by multiple users and workloads.

### Good Practices

- Run jobs only when they are needed.
- Avoid unnecessary or repeated job runs.
- Optimize notebooks, queries, and pipelines.
- Avoid running multiple heavy jobs at the same time.
- Stop or reduce workloads that are not required.

> **In simple words:** Do not use more Fabric resources than necessary, so other users can work smoothly.

---

## Question 2: Explain how Fabric bills on one shared capacity, how a heavy job can slow everything else, and what you would watch

Fabric uses **one shared capacity for multiple workloads**. This means notebooks, pipelines, SQL queries, and Power BI workloads can use the same pool of resources.

If a **heavy job** uses a large amount of capacity, fewer resources are available for other workloads. As a result, other jobs may become **slow or delayed**.

### What I Would Watch

- **Capacity usage** – Is the capacity getting close to its limit?
- **Heavy jobs** – Which jobs are consuming the most resources?
- **Long-running jobs** – Which jobs are taking too long?
- **Concurrent jobs** – Are many heavy jobs running at the same time?
- **Performance** – Are other workloads becoming slower?

> **In simple words:** One heavy job can affect everyone because the resources are shared. I would monitor capacity usage, heavy jobs, and workload performance to identify problems early.
