"""
One-off reconciliation between the on-disk Plex library and the fl_movies table.

Two drifts accumulated and are fixed here. Idempotent: safe to re-run.

  D1  Folders exist on disk with no fl_movies row (4858 as of 2026-10-19). They are real
      entries -- every sampled folder contains a .mkv -- so they are inserted with
      file_created=True. Without this, the rows are invisible to the app forever.

  D2  fl_movies rows whose folder is missing (13 as of 2026-10-19). These are exactly the
      titles containing '/', which the old os.makedirs turned into nested directories.
      Their file_created flag is reset to False so symlink_routine recreates them with the
      sanitized name, and the broken artefacts are MOVED (never deleted) to a quarantine dir.

Nothing is ever deleted. Run with --apply to write; the default is a dry run.
"""
import argparse
import os
import re
import shutil
import time

from loguru import logger
from sqlalchemy import create_engine, select, update
from sqlalchemy.dialects.postgresql import insert

from db import DB_URI, FilelistMovies

LIBRARY_PATH = os.getenv('PLEX_MOVIES_LIBRARY_FOLDER_MAP')
TCONST_RE = re.compile(r' (tt\d+)$')


def scan_disk(library_path):
    """Return {tconst: folder_name} for parseable folders, plus the unparseable leftovers."""
    by_tconst = {}
    unparseable = []
    for entry in sorted(os.listdir(library_path)):
        full = os.path.join(library_path, entry)
        if not os.path.isdir(full):
            if entry != 'placeholder.mkv':
                unparseable.append(entry)
            continue
        m = TCONST_RE.search(entry)
        if m:
            by_tconst[m.group(1)] = entry
        else:
            unparseable.append(entry)
    return by_tconst, unparseable


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--apply', action='store_true', help='write changes (default: dry run)')
    args = ap.parse_args()

    if not LIBRARY_PATH:
        raise SystemExit("PLEX_MOVIES_LIBRARY_FOLDER_MAP is not set")
    if not os.path.isdir(LIBRARY_PATH):
        raise SystemExit(f"library path does not exist in this container: {LIBRARY_PATH}")

    engine = create_engine(DB_URI, echo=False, isolation_level="AUTOCOMMIT")

    disk, unparseable = scan_disk(LIBRARY_PATH)
    with engine.connect() as conn:
        db_rows = {r[0] for r in conn.execute(select(FilelistMovies.tconst)).fetchall()}

    missing_in_db = sorted(set(disk) - db_rows)
    missing_on_disk = sorted(db_rows - set(disk))

    logger.info(f"library path           : {LIBRARY_PATH}")
    logger.info(f"folders on disk        : {len(disk)}")
    logger.info(f"fl_movies rows         : {len(db_rows)}")
    logger.info(f"D1 on disk, not in DB  : {len(missing_in_db)}")
    logger.info(f"D2 in DB, not on disk  : {len(missing_on_disk)}")
    logger.info(f"unparseable artefacts  : {len(unparseable)} -> {unparseable}")

    if not args.apply:
        logger.warning("DRY RUN -- nothing written. Re-run with --apply to commit.")
        return

    # ---- D1: back-fill rows for folders that already exist -------------------------------
    if missing_in_db:
        with engine.connect() as conn:
            for i in range(0, len(missing_in_db), 1000):
                batch = missing_in_db[i:i + 1000]
                stmt = insert(FilelistMovies.__table__).values(
                    [{'tconst': t, 'file_created': True} for t in batch])
                stmt = stmt.on_conflict_do_nothing(index_elements=['tconst'])
                conn.execute(stmt)
                logger.info(f"D1 inserted {min(i + 1000, len(missing_in_db))}/{len(missing_in_db)}")
    else:
        logger.info("D1 nothing to insert")

    # ---- D2: quarantine broken artefacts and queue the rows for recreation ---------------
    if missing_on_disk or unparseable:
        quarantine = os.path.join(LIBRARY_PATH, f".broken-{time.strftime('%Y%m%d-%H%M%S')}")
        moved = 0
        for entry in unparseable:
            src = os.path.join(LIBRARY_PATH, entry)
            if not os.path.exists(src):
                continue
            os.makedirs(quarantine, exist_ok=True)
            shutil.move(src, os.path.join(quarantine, entry))
            moved += 1
            logger.info(f"D2 quarantined {entry!r}")
        logger.info(f"D2 moved {moved} artefacts into {quarantine if moved else '(nothing)'}")

        if missing_on_disk:
            with engine.connect() as conn:
                conn.execute(
                    update(FilelistMovies)
                    .where(FilelistMovies.tconst.in_(missing_on_disk))
                    .values(file_created=False))
            logger.info(f"D2 reset file_created=False for {len(missing_on_disk)} rows; "
                        f"symlink_routine will recreate them with sanitized names")
    else:
        logger.info("D2 nothing to repair")

    with engine.connect() as conn:
        total = conn.execute(select(FilelistMovies.tconst)).fetchall()
    logger.info(f"done. fl_movies now has {len(total)} rows")


if __name__ == '__main__':
    main()
