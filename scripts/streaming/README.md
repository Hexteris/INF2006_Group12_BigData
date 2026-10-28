# Hadoop Streaming jobs (owner: B)

One mapper + one reducer per job: `<job>_mapper.py`, `<job>_reducer.py`. Python 3 standard library only.
Read input from stdin, write tab-separated `key\tvalue` lines to stdout.

Planned jobs (see `docs/contracts.md` for output paths):
- `price_audit`: rows per file/ticker/year and malformed-line counts by reason; cross-checks the Hive counts.
- `sec_parse`: one row per filing from the EDGAR header (CIK, accession, form, dates, acceptance time),
  plus document and XBRL facts. Split each file on `<DOCUMENT>`, keep blocks by `<TYPE>`, skip
  uuencoded binaries and `R*.htm` pages (`docs/corpus_inventory.md` L10).

Test locally in Git Bash:
`cat <input> | python <job>_mapper.py | LC_ALL=C sort | python <job>_reducer.py`
