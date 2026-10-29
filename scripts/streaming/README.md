
# Hadoop Streaming jobs (owner: B)

Each job uses one mapper and one reducer:
`<job>_mapper.py` and `<job>_reducer.py`.

Python 3 standard library only. Mappers read input from stdin and emit
tab-separated `key\tvalue` pairs. Hadoop sorts mapper output before
passing it to reducers.

## `price_audit`

Files:
- `price_audit_mapper.py`
- `price_audit_reducer.py`

The mapper audits price records and emits counts for:
- Rows by ticker and year
- Rejection reasons
- Out-of-scope records
- Malformed records
- Accepted/rejected candidate statuses

The reducer aggregates counts for identical keys.

### Local test

```bash
cat /tmp/price_audit_test.csv \
  | python3 scripts/streaming/price_audit_mapper.py \
  | LC_ALL=C sort \
  | python3 scripts/streaming/price_audit_reducer.py
```

### Syntax check

```bash
python3 -m py_compile \
  scripts/streaming/price_audit_mapper.py \
  scripts/streaming/price_audit_reducer.py
```

The local test validates the scripts on synthetic data only.
The full Hadoop Streaming job must still be tested on EMR.

## Planned jobs

- `sec_parse`: parse SEC filing headers and selected document/XBRL metadata.
  Confirm the final output schema with the team before implementation.
