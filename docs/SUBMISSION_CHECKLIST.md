# Submission checklist (due 22 Nov 2026, 23:59 SGT)

Each line is a requirement from the brief's "Submission requirements" and report sections. Tick it only when the
"How to check" column passes.

## Build the ZIP

1. Export the report from Google Docs as PDF (File → Download → PDF) and save it as `report.pdf`. Also save a
   plain-text copy (File → Download → Plain text) as `report.txt`.
2. Sync the timed-run evidence: `aws s3 sync s3://<bucket>/evidence/task2_runs/ evidence/task2_runs/`, and copy the
   executed driver notebooks from `~/task2_runs/executed_notebooks/` into `evidence/task2_runs/executed_notebooks/`.
3. From the repo root:
   `python3 scripts/package/build_submission_zip.py --report report.pdf --report-text report.txt --out INF2006_P2_G12.zip`
4. Every line must say PASS. Fix any FAIL and rebuild.

## Contents

| Requirement | How to check | Owner |
|---|---|---|
| `report.pdf`, the `.ipynb` notebooks exported from EMR, the ML models, helper scripts and assets | ZIP builder: "required files present", "exported notebooks have saved outputs" | everyone |
| Executed Task 2 driver notebooks (timed runs) | `evidence/task2_runs/executed_notebooks/` exists and opens with outputs | Task 2 |
| No credentials, tokens, raw datasets or machine-specific paths | ZIP builder: "no credentials", "no machine-specific paths"; data folders are left out on purpose | everyone |
| Every referenced file opens from the ZIP root | ZIP builder: "report cites only files that exist" | everyone |

## Report

| Requirement | How to check | Owner |
|---|---|---|
| Required numbered headings 1–7, in order; References; Appendices A–D | Read the headings in the PDF | Damien |
| Main text ≤ 3,000 words, count stated on the cover | Count Sections 1–7 without captions, references or appendices; update the cover line | Damien |
| Section 1 labelled data-flow diagram | Figure 1.1 present (`docs/figures/data_flow.png`) | Damien |
| Section 6: all repetitions, medians and ranges, dev vs full, reference vs alternative, memory and disk, failures | No `[X]` or `[fill]` left; numbers match `evidence/task2_runs/aggregate_report.md` | Task 2 + whoever runs the matrix |
| No `[TBD]`, `[CHECK]`, `[X]` or "[… owners: …]" placeholders anywhere | Search the Doc for `[` | everyone |
| Appendix A: setup, data access, dependencies, configuration, notebook filenames and run order | A teammate follows it on a fresh cluster | Daniel |
| Appendix A: data versions, training cutoff, software versions, random seeds, saved-model locations | All five are stated | Daniel, Xin Huei |
| Appendix A: dated evidence of a fresh-session rerun | Task 1 row in Appendix B filled; Task 3 reload and Task 4 rerun already dated | Daniel |
| Every verification record states check, steps or command, expected, actual, evidence location | Appendix B tables for Tasks 1–4 all have the five columns | everyone |
| Failed checks reported honestly | Any FAIL from the runs appears in Section 6 or Appendix B | everyone |
| Data sources, reused code and AI assistance declared | References list versions; Appendix C has a row per tool per member | everyone |
| Contributions: concrete work per member, percentages total 100% | Appendix D | everyone |

Note on appendix letters: the brief's report section uses Appendix C for AI assistance and D for contributions, while
its checklist calls the contributions table "C". The report follows the report section (A, B, C = AI, D =
contributions).
