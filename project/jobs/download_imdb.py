"""Download the IMDb non-commercial datasets into data/raw.

Usage (inside the pyspark container):
    python /home/jovyan/jobs/download_imdb.py [--force]

Files already present are skipped unless --force is given.
Source: https://developer.imdb.com/non-commercial-datasets/
"""

import argparse
import os
import shutil
import sys
import urllib.request

from common import IMDB_BASE_URL, IMDB_TABLES, RAW_DIR


def download(url: str, dest: str) -> None:
    tmp = dest + ".part"
    with urllib.request.urlopen(url) as resp, open(tmp, "wb") as out:
        total = int(resp.headers.get("Content-Length", 0))
        done = 0
        while True:
            chunk = resp.read(1024 * 1024)
            if not chunk:
                break
            out.write(chunk)
            done += len(chunk)
            if total:
                pct = 100 * done / total
                sys.stdout.write(f"\r  {done / 1e6:8.1f} MB ({pct:5.1f}%)")
                sys.stdout.flush()
    print()
    shutil.move(tmp, dest)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true",
                        help="re-download files that already exist")
    args = parser.parse_args()

    os.makedirs(RAW_DIR, exist_ok=True)
    for table in IMDB_TABLES:
        name = f"{table}.tsv.gz"
        dest = os.path.join(RAW_DIR, name)
        if os.path.exists(dest) and not args.force:
            print(f"[skip] {name} already downloaded")
            continue
        print(f"[get ] {name}")
        download(f"{IMDB_BASE_URL}/{name}", dest)
    print("done")


if __name__ == "__main__":
    main()
