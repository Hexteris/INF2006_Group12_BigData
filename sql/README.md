# Hive queries (owner: B)

Files run in numeric order with `hive --hivevar ACQ=<YYYY-MM-DD> -f sql/NN_<name>.sql`.
Use `${hivevar:ACQ}` for the acquisition date instead of hard-coding it.

Planned:
- `01_raw_tables.sql`: `CREATE DATABASE g12`; external `g12.raw_prices` over the raw CSV, all columns `STRING`.
- `02_clean_prices.sql`: build `g12.cleaned_prices`, `g12.rejected_prices`, `g12.out_of_scope_prices`.
- `03_reconcile.sql`: row-count reconciliation (targets in `docs/contracts.md`).
- `04_sec_tables.sql`: external `g12.sec_filings` over the Streaming output; `g12.sec_price_link`.

Save each run's console output (it contains the Hive query IDs) as evidence.
