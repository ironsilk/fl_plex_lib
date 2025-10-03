# fl_plex_lib

Tool to cross-check IMDB titles against the Filelist torrent API slowly to avoid rate limits.

Key components
- db_builder/db.py: ImdbMovies, FilelistMovies, [python.update_fl_database()](db_builder/db.py:237), [python.update_imdb_database()](db_builder/db.py:190), [python.populate_imdb_database()](db_builder/db.py:147)
- db_builder/run.py: [python.schedule_tasks()](db_builder/run.py:7)

Continuous cycle behavior
- On each scheduler cycle, [python.update_fl_database()](db_builder/db.py:237) computes rows-to-check = IMDB unchecked minus fl_movies.
- If rows-to-check is empty, it triggers [python.update_imdb_database()](db_builder/db.py:190) to fetch newer IMDB IDs and insert them.
- It repeats this IMDB update until either:
  - rows-to-check becomes non-empty (so there is something to check against Filelist), or
  - update_imdb_database() inserts 0 new rows (IMDB exhausted).
- When rows-to-check exist, it checks each tconst against Filelist, spacing requests by 24 seconds.

Rate limiting
- Filelist lookups back off on HTTP 429 by sleeping 5 minutes, then retry.
- Per-title spacing remains 24 seconds.

Notes
- Newly inserted IMDB rows are marked checked=False to be picked up by the Filelist checking pass.
- [python.populate_imdb_database()](db_builder/db.py:147) seeds the table for new deployments (uses START_IMDB_ID).

Running
- Use [python.schedule_tasks()](db_builder/run.py:7) to run both the Filelist check and any other jobs:
  python -m db_builder.run
- Alternatively call [python.update_fl_database()](db_builder/db.py:237) manually in a Python/REPL environment.

Environment
- Ensure .env contains DB creds, Filelist API details, and START_IMDB_ID.

Exit conditions per cycle
- Found rows to check (proceeds to Filelist checking).
- IMDB exhausted (no new rows inserted), nothing to check, cycle ends.