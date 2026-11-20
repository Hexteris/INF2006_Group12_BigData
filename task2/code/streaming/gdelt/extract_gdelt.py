#!/usr/bin/env python3
"""Extract TSV/text members from all GDELT ZIP archives.

Example:
  python3 extract_gdelt.py --input-dir /data/gdelt_zip \
      --output-dir /data/gdelt_extracted

Original ZIP files are not modified. Every extracted data line is written
unchanged to extracted_gdelt.tsv. A manifest records archive/member counts
and failures. Review member names before running if archives contain
non-GDELT files.
"""
import argparse
import json
import zipfile
from pathlib import Path

TEXT_SUFFIXES = {".tsv", ".txt", ".csv"}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()

    src = Path(args.input_dir)
    dst = Path(args.output_dir)
    dst.mkdir(parents=True, exist_ok=True)
    output_path = dst / "extracted_gdelt.tsv"
    manifest_path = dst / "extract_manifest.json"

    archives = sorted(src.rglob("*.zip"))
    manifest = {
        "archives_found": len(archives),
        "archives_opened": 0,
        "archives_failed": [],
        "members_extracted": 0,
        "records_written": 0,
        "member_details": [],
    }

    with output_path.open("w", encoding="utf-8", newline="") as out:
        for archive in archives:
            try:
                with zipfile.ZipFile(archive) as zf:
                    manifest["archives_opened"] += 1
                    for info in zf.infolist():
                        if info.is_dir() or Path(info.filename).suffix.lower() not in TEXT_SUFFIXES:
                            continue
                        member_records = 0
                        with zf.open(info, "r") as raw:
                            for raw_line in raw:
                                line = raw_line.decode("utf-8", errors="replace").rstrip("\r\n")
                                if not line:
                                    continue
                                out.write(line + "\n")
                                member_records += 1
                        manifest["members_extracted"] += 1
                        manifest["records_written"] += member_records
                        manifest["member_details"].append({
                            "archive": str(archive),
                            "member": info.filename,
                            "records": member_records,
                        })
            except (zipfile.BadZipFile, OSError, RuntimeError) as exc:
                manifest["archives_failed"].append({
                    "archive": str(archive), "error": str(exc)
                })

    manifest["output_file"] = str(output_path)
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(json.dumps(manifest, indent=2))

if __name__ == "__main__":
    main()
