# Notebook sections (owners: C, D, E)

Each member writes their part as `<task>_<topic>.py` with `# %%` cell markers, for example
`task2_features.py`, `task2_splits.py`, `task3_model.py`, `task4_analytics.py`.
For the final run they are pasted (or converted) in order into one EMR notebook, which is exported as the
submitted `.ipynb`. Nobody edits a shared `.ipynb` directly.

Rules:
- First cell: parameters only (HDFS paths, `ACQ`, `RUN_ID`, seeds).
- Read inputs from the Hive tables / paths in `docs/contracts.md`; write outputs only to your own paths.
- Keep processing distributed: no `toPandas()` / `collect()` on full tables. Only small aggregates for plots.
- Print the Spark application ID and every output path, so the report can cite them.
