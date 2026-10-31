
# Hive Queries (Owner: B)

These scripts implement Role B's Hive SQL processing.

Run the scripts in numeric order after the required input files have been staged in HDFS and the Hadoop Streaming SEC output is available.

## Execution order

| Script | Purpose | Main output |
|---|---|---|
| `01_raw_tables.sql` | Create the raw prices external table | `g12.raw_prices` |
| `02_clean_prices.sql` | Clean price records and separate rejected/out-of-scope records | `g12.cleaned_prices`, `g12.rejected_prices`, `g12.out_of_scope_prices` |
| `03_reconcile.sql` | Check row counts and reconciliation | PASS/FAIL reconciliation results |
| `04_sec_tables.sql` | Create SEC filing tables and link filings to stock prices | `g12.sec_filings`, `g12.sec_price_overlap`, `g12.sec_price_link` |

## Required Hive variables

### Acquisition date

`ACQ` identifies the raw price dataset acquisition.

Example: `2026-10-28`

Used by `01_raw_tables.sql`.

### SEC metadata path

`SEC_PARSED_PATH` is the HDFS directory containing the Hadoop Streaming SEC metadata output in tab-separated format.

Used by `04_sec_tables.sql`.

### SEC overlap path

`SEC_OVERLAP_PATH` is the HDFS directory containing the SEC-to-price overlap CSV, including its header row.

Used by `04_sec_tables.sql`.

## Example execution

Run from the project root, replacing the SEC paths with the actual HDFS locations supplied by the team.

```bash
hive --hivevar ACQ=2026-10-28 -f sql/01_raw_tables.sql

hive --hivevar ACQ=2026-10-28 -f sql/02_clean_prices.sql

hive --hivevar ACQ=2026-10-28 -f sql/03_reconcile.sql

hive --hivevar SEC_PARSED_PATH=/corpus/staged/sec_parsed/run_<RUN> \
  --hivevar SEC_OVERLAP_PATH=/corpus/overlap/sec_price_overlap \
  -f sql/04_sec_tables.sql
```

Replace `<RUN>` and the overlap path with the actual staged HDFS locations. The example paths must be confirmed before execution.

## Expected reconciliation targets

Refer to `docs/contracts.md` for the proposed record-count targets and reconciliation rules.

These are expected targets, not locally verified Hive results.

## Evidence

Save each Hive execution's console output as project evidence. Include the query IDs, execution results and any PASS/FAIL checks.

## Important notes

- Run scripts in numeric order because later scripts depend on earlier tables.
- The SEC tables depend on the Hadoop Streaming output being available in HDFS.
- `04_sec_tables.sql` creates `g12.sec_price_link` as a Parquet table.
- Confirm the actual SEC output schema and HDFS paths with the team before running the SEC script.
- If an existing table was created with an older schema, check its definition before rerunning `CREATE TABLE IF NOT EXISTS`.
