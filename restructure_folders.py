#!/usr/bin/env python3
"""
restructure_folders.py
Re-arranges an existing LNT download tree from
    Site → FY → WOD
to
    FY → Site → WOD

Usage
-----
python restructure_folders.py  /path/to/LNT_Partner_Downloads
"""

from pathlib import Path
import shutil
import argparse
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s"
)

def migrate_tree(base: Path) -> None:
    """
    Walk   base/SITE/FY/WOD[…]
    Move → base/FY/SITE/WOD[…]
    """
    # Find every WOD folder three levels deep
    pattern = "*/*/*"           # site / year / WOD
    for wod_dir in (d for d in base.glob(pattern) if d.is_dir()):
        site, fy_year, wod = wod_dir.parts[-3:]
        src = wod_dir
        dst = base / fy_year / site / wod
        if dst.exists():
            logging.warning("Skip – destination already exists: %s", dst)
            continue
        dst.parent.mkdir(parents=True, exist_ok=True)
        logging.info("Move %s  →  %s", src.relative_to(base), dst.relative_to(base))
        shutil.move(str(src), str(dst))

    # Remove now-empty “site/year” holders
    for empty in sorted(base.glob("*/*"), reverse=True):
        if empty.is_dir() and not any(empty.iterdir()):
            empty.rmdir()

def main():
    ap = argparse.ArgumentParser(description="Restructure LNT download tree")
    ap.add_argument("base_folder", type=Path,
                    help="Root of existing downloads (e.g. ~/Desktop/LNT_Partner_Downloads)")
    args = ap.parse_args()

    if not args.base_folder.is_dir():
        ap.error(f"{args.base_folder} is not a directory")

    migrate_tree(args.base_folder)
    logging.info("✅ Restructuring complete")

if __name__ == "__main__":
    main()
