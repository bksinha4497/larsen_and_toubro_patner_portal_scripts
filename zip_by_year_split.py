#!/usr/bin/env python3
"""
zip_by_year_split.py
Zip each financial-year folder into independent parts ≤ 23 MB
(≈ 24 115 200 bytes).

Output structure
----------------
base_zips/
    2018-2019_part1.zip
    2018-2019_part2.zip
    ...
    2019-2020.zip           # single part because total ≤ 23 MB
    …

Usage
-----
python zip_by_year_split.py  /path/to/LNT_Partner_Downloads  [--out OUTDIR]
"""

import argparse
import os
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED

MAX_PART = 23 * 1024 * 1024            # 23 MiB limit
COMPRESSION = ZIP_DEFLATED             # widely supported

def iter_files(root: Path):
    """Yield all files under *root* with paths relative to *root*."""
    for path in root.rglob("*"):
        if path.is_file():
            yield path, path.relative_to(root)

def zip_year_folder(src_dir: Path, dest_root: Path) -> None:
    fy = src_dir.name
    part = 1
    zip_path = dest_root / f"{fy}_part{part}.zip"
    zf = ZipFile(zip_path, "w", compression=COMPRESSION, compresslevel=6)
    bytes_written = 0

    for abs_path, rel_path in iter_files(src_dir):
        size_guess = abs_path.stat().st_size
        # Start new part if adding this file would exceed MAX_PART
        if bytes_written and bytes_written + size_guess > MAX_PART:
            zf.close()
            part += 1
            zip_path = dest_root / f"{fy}_part{part}.zip"
            zf = ZipFile(zip_path, "w", compression=COMPRESSION, compresslevel=6)
            bytes_written = 0
        zf.write(abs_path, rel_path)
        zf.fp.flush()
        bytes_written = zf.fp.tell()

    # Rename single-part archive to drop “_part1”
    zf.close()
    if part == 1:
        final = dest_root / f"{fy}.zip"
        zip_path.rename(final)

def main():
    ap = argparse.ArgumentParser(description="Create ≤23 MB zip parts per FY")
    ap.add_argument("base_folder", type=Path,
                    help="Root folder arranged as FY/Site/WOD (output of restructure script)")
    ap.add_argument("--out", type=Path, default=None,
                    help="Destination directory for zips (default: sibling *_zips)")
    args = ap.parse_args()

    base = args.base_folder
    if not base.is_dir():
        ap.error(f"{base} is not a directory")

    out_dir = args.out or base.with_name(f"{base.name}_zips")
    out_dir.mkdir(exist_ok=True)

    for fy_dir in sorted(d for d in base.iterdir() if d.is_dir()):
        zip_year_folder(fy_dir, out_dir)
        print(f"✓ Zipped {fy_dir.name}")

    print(f"🎉  All zips saved in {out_dir}")

if __name__ == "__main__":
    main()
