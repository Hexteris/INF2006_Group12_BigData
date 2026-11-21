#!/usr/bin/env python3

import argparse
import glob
import json
import os
import zipfile


parser = argparse.ArgumentParser()

parser.add_argument("--input-dir", required=True)
parser.add_argument("--output", required=True)
parser.add_argument("--manifest", required=True)
parser.add_argument("--start", type=int, default=0)
parser.add_argument("--count", type=int, default=10)

args = parser.parse_args()

archives = sorted(
    glob.glob(
        os.path.join(args.input_dir, "*", "*.zip")
    )
)

selected = archives[
    args.start:args.start + args.count
]

if not selected:
    raise SystemExit("No archives selected.")

manifest = {
    "start_index": args.start,
    "archive_count": len(selected),
    "archives": []
}

total_bytes = 0
total_lines = 0

with open(args.output, "wb") as output_file:

    for archive_path in selected:

        with zipfile.ZipFile(archive_path) as archive:

            members = [
                name for name in archive.namelist()
                if name.lower().endswith((".csv", ".tsv"))
            ]

            if len(members) != 1:
                raise RuntimeError(
                    f"Expected one TSV/CSV member: {archive_path}"
                )

            member = members[0]
            archive_bytes = 0
            archive_lines = 0

            with archive.open(member) as source:

                while True:
                    chunk = source.read(1024 * 1024)

                    if not chunk:
                        break

                    output_file.write(chunk)

                    archive_bytes += len(chunk)
                    archive_lines += chunk.count(b"\n")

            total_bytes += archive_bytes
            total_lines += archive_lines

            manifest["archives"].append({
                "archive": os.path.basename(archive_path),
                "member": member,
                "bytes": archive_bytes,
                "lines": archive_lines
            })

            print(
                f"Processed {os.path.basename(archive_path)}: "
                f"{archive_lines:,} lines"
            )

manifest["total_uncompressed_bytes"] = total_bytes
manifest["total_lines"] = total_lines

with open(args.manifest, "w") as f:
    json.dump(manifest, f, indent=2)

print("\nBATCH SUMMARY")
print("Archives:", len(selected))
print("Total lines:", f"{total_lines:,}")
print("Uncompressed size:", f"{total_bytes / 1024**2:.2f} MiB")
print("Manifest:", args.manifest)
