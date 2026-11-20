#!/usr/bin/env python3
"""Hadoop Streaming mapper (map-only) for the Task 2 GDELT stage.

Input : lines holding HDFS paths of GDELT 1.0 daily archives (*.export.CSV.zip),
        delivered by NLineInputFormat. Each archive is read straight from HDFS
        inside the map task, so unzipping, structural validation and projection
        all run in parallel on the worker nodes.
Output: tab-separated, tagged lines (the Hive table gdelt_stream reads them):
  V <19 projected fields>                   valid event row
  R <reason> <archive> <raw line>           rejected row (tabs in the raw line -> 0x1f)
  A <archive> <rows> <valid> <rejected> <zip_bytes>   one audit line per archive

Structural rules applied here (value-level rules are applied later in Hive):
  - exactly GDELT_COLS (58) tab-separated fields        else wrong_column_count:<n>
  - valid UTF-8                                          else invalid_utf8
  - GLOBALEVENTID not repeated inside the same archive   else duplicate_event_id_in_archive
  - archive must be a readable zip                       else unreadable_archive
Duplicates across archives are not detected here.

Projected fields (0-based GDELT 1.0 indexes):
  0 GLOBALEVENTID, 1 SQLDATE, 26 EventCode, 27 EventBaseCode, 28 EventRootCode,
  29 QuadClass, 30 GoldsteinScale, 31 NumMentions, 32 NumSources, 33 NumArticles,
  34 AvgTone, 6 Actor1Name, 7 Actor1CountryCode, 16 Actor2Name, 17 Actor2CountryCode,
  51 ActionGeo_CountryCode, 50 ActionGeo_FullName, 56 DATEADDED, 57 SOURCEURL
"""
import io
import os
import shlex
import subprocess
import sys
import zipfile

HDFS_CAT = os.environ.get("HDFS_CAT", "hdfs dfs -cat")
EXPECTED_COLS = int(os.environ.get("GDELT_COLS", "58"))
KEEP = [0, 1, 26, 27, 28, 29, 30, 31, 32, 33, 34, 6, 7, 16, 17, 51, 50, 56, 57]
MAX_RAW = 1500


def emit(*fields):
    sys.stdout.write("\t".join(str(f) for f in fields) + "\n")


def read_zip(path):
    p = subprocess.run(shlex.split(HDFS_CAT) + [path],
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if p.returncode != 0:
        raise IOError(p.stderr.decode("utf-8", "replace")[:150])
    return p.stdout


def reject(reason, name, raw):
    raw = raw[:MAX_RAW].replace("\t", "\x1f").replace("\r", "").replace("\n", " ")
    emit("R", reason, name, raw)


for line_in in sys.stdin:
    path = line_in.rstrip("\n").split("\t")[-1].strip()
    if not path:
        continue
    name = os.path.basename(path)
    sys.stderr.write("reporter:status:" + name + "\n")
    rows = valid = rejected = 0
    nbytes = 0
    try:
        data = read_zip(path)
        nbytes = len(data)
        zf = zipfile.ZipFile(io.BytesIO(data))
        seen = set()
        for member in zf.namelist():
            with zf.open(member) as fh:
                for b in fh:
                    rows += 1
                    try:
                        text = b.decode("utf-8").rstrip("\r\n")
                    except UnicodeDecodeError:
                        rejected += 1
                        reject("invalid_utf8", name, b.decode("utf-8", "replace"))
                        continue
                    f = text.split("\t")
                    if len(f) != EXPECTED_COLS:
                        rejected += 1
                        reject("wrong_column_count:%d" % len(f), name, text)
                        continue
                    eid = f[0]
                    if eid.isdigit():
                        if eid in seen:
                            rejected += 1
                            reject("duplicate_event_id_in_archive", name, text)
                            continue
                        seen.add(eid)
                    valid += 1
                    emit("V", *[f[i] for i in KEEP])
    except Exception as e:
        rejected += 1
        reject("unreadable_archive", name, str(e))
        sys.stderr.write("reporter:counter:GDELT,unreadable_archives,1\n")
    emit("A", name, rows, valid, rejected, nbytes)
    sys.stderr.write("reporter:counter:GDELT,rows_read,%d\n" % rows)
    sys.stderr.write("reporter:counter:GDELT,valid_rows,%d\n" % valid)
    sys.stderr.write("reporter:counter:GDELT,rejected_rows,%d\n" % rejected)
    sys.stderr.write("reporter:counter:GDELT,archives,1\n")
