#!/usr/bin/env python3
"""Measure how many inventory filings link to a price series (Task 1 decision gate).

Link used: inventory CIK -> current ticker(s) in SEC company_tickers.json -> price
CSV ticker. company_tickers.json lists CURRENT tickers only, so issuers that were
renamed, acquired or delisted after 2017 can be missed. Add such links by hand in an
--aliases CSV (columns: price_ticker,cik,note); every alias is reported as such.

Outputs under --out:
  sec_price_overlap.csv          one row per inventory filing with its link status
  sec_price_overlap_summary.txt  counts + the decision implied by the agreed rule

Decision rule (agreed in the project plan), on issuers with >=1 filing inside the
linked ticker's price date range:
  >= 30 issuers  -> SEC used as model features
  5 - 29         -> SEC used for Hadoop Streaming + Task 4 analysis only
  < 5            -> drop SEC or keep only as a Streaming showcase

Example:
  python3 check_sec_price_overlap.py --inventory SEC_Filing_Inventory.csv \
      --prices all_stocks_5yr.csv --tickers company_tickers.json --out overlap/
"""
import argparse
import csv
import json
import os
import sys
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from acquire_sec_filings import load_inventory  # noqa: E402
from acquire_url import sha256_file, utc_now  # noqa: E402

PRICE_END = "2017-12-29"  # last bar in the project's accepted price scope


def norm_ticker(t):
    """Price CSV uses e.g. BRK.B; SEC uses BRK-B. Compare on one form."""
    return t.strip().upper().replace(".", "-")


def load_price_ranges(path, key_col, date_col):
    rng = {}
    with open(path, newline="", encoding="utf-8-sig") as f:
        r = csv.DictReader(f)
        for c in (key_col, date_col):
            if c not in r.fieldnames:
                sys.exit(f"{path}: column '{c}' not in {r.fieldnames}")
        for row in r:
            t, d = norm_ticker(row[key_col]), row[date_col]
            if not t or not d:
                continue
            lo, hi = rng.get(t, (d, d))
            rng[t] = (min(lo, d), max(hi, d))
    return rng


def load_company_tickers(path):
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    rows = data.values() if isinstance(data, dict) else data
    by_cik = defaultdict(set)
    for e in rows:
        by_cik[str(e["cik_str"]).zfill(10)].add(norm_ticker(e["ticker"]))
    return by_cik


def load_aliases(path):
    by_cik = defaultdict(set)
    if path:
        with open(path, newline="", encoding="utf-8-sig") as f:
            for r in csv.DictReader(f):
                by_cik[r["cik"].strip().zfill(10)].add(norm_ticker(r["price_ticker"]))
    return by_cik


def decide(n):
    if n >= 30:
        return "SEC as model features (>=30 issuers)"
    if n >= 5:
        return "SEC for Hadoop Streaming + Task 4 analysis only (5-29 issuers)"
    return "drop SEC or keep only as a Streaming showcase (<5 issuers)"


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--inventory", required=True, help="course inventory .csv or .xlsx")
    p.add_argument("--prices", required=True, help="raw price CSV")
    p.add_argument("--tickers", required=True, help="SEC company_tickers.json")
    p.add_argument("--aliases", help="optional CSV price_ticker,cik,note for manual links")
    p.add_argument("--key-col", default="Name")
    p.add_argument("--date-col", default="date")
    p.add_argument("--out", required=True)
    a = p.parse_args()
    os.makedirs(a.out, exist_ok=True)

    recs = load_inventory(a.inventory)["records"]
    prices = load_price_ranges(a.prices, a.key_col, a.date_col)
    sec = load_company_tickers(a.tickers)
    alias = load_aliases(a.aliases)

    out_rows = []
    for r in recs:
        cik = r["cik"]
        current = sorted(sec.get(cik, set()))
        via_sec = [t for t in current if t in prices]
        via_alias = [t for t in sorted(alias.get(cik, set())) if t in prices and t not in via_sec]
        matched = (via_sec + via_alias)[:1]
        t = matched[0] if matched else ""
        if via_sec:
            method = "company_tickers"
        elif via_alias:
            method = "alias"
        elif current:
            method = "none:current_ticker_not_in_prices"
        else:
            method = "none:cik_not_in_company_tickers"
        first, last = prices.get(t, ("", ""))
        in_range = bool(t) and first <= r["filing_date"] <= min(last, PRICE_END)
        out_rows.append({
            "source_row": r["source_row"], "cik": cik, "issuer": r["issuer"], "form": r["form"],
            "filing_date": r["filing_date"], "accession": r["accession"],
            "sec_current_tickers": "|".join(current), "price_ticker": t,
            "all_price_matches": "|".join(via_sec + via_alias), "match_method": method,
            "price_first_date": first, "price_last_date": last,
            "filing_in_price_range": "yes" if in_range else "no",
        })

    path = os.path.join(a.out, "sec_price_overlap.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(out_rows[0]))
        w.writeheader()
        w.writerows(out_rows)

    matched = [r for r in out_rows if r["price_ticker"]]
    inrange = [r for r in matched if r["filing_in_price_range"] == "yes"]
    inrange_issuers = {r["cik"] for r in inrange}
    summary = [
        f"generated_at_utc={utc_now()}",
        f"inventory={os.path.basename(a.inventory)} sha256={sha256_file(a.inventory)}",
        f"prices={os.path.basename(a.prices)} sha256={sha256_file(a.prices)} price_tickers={len(prices)}",
        f"company_tickers={os.path.basename(a.tickers)} sha256={sha256_file(a.tickers)} ciks={len(sec)}",
        f"aliases={os.path.basename(a.aliases) if a.aliases else 'none'}",
        f"inventory_filings={len(out_rows)} inventory_issuers={len({r['cik'] for r in out_rows})}",
        f"match_method_counts={dict(Counter(r['match_method'] for r in out_rows))}",
        f"matched_filings={len(matched)} matched_issuers={len({r['cik'] for r in matched})}",
        f"matched_filings_in_price_range={len(inrange)} issuers_in_price_range={len(inrange_issuers)}",
        f"in_range_by_form={dict(Counter(r['form'] for r in inrange))}",
        f"in_range_by_year={dict(sorted(Counter(r['filing_date'][:4] for r in inrange).items()))}",
        f"multi_ticker_issuers={sorted({r['cik'] for r in matched if '|' in r['all_price_matches']})}",
        f"decision_rule_result={decide(len(inrange_issuers))}",
        "limitation=company_tickers.json holds current tickers; renamed/delisted issuers need --aliases",
    ]
    with open(os.path.join(a.out, "sec_price_overlap_summary.txt"), "w", encoding="utf-8") as f:
        f.write("\n".join(summary) + "\n")
    print("\n".join(summary))
    print(f"-> {path}")


if __name__ == "__main__":
    main()
