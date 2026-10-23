import os
import time
from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler
from loguru import logger
from sqlalchemy import create_engine, text

from db import DB_URI, update_fl_database, update_ratings_database
from file_mocks import symlink_routine

# How long to wait for Postgres before giving up. The container shares a host with
# MOVIELIB_postgres and Docker gives no ordering guarantee across compose projects, so on a
# reboot (or whenever the DB is restarted) this process can easily start first. Dying here is
# what left the container dead for two weeks on 2026-10-03, so wait instead of exiting.
DB_WAIT_ATTEMPTS = int(os.getenv('DB_WAIT_ATTEMPTS', '120'))
DB_WAIT_INTERVAL = int(os.getenv('DB_WAIT_INTERVAL', '5'))


def wait_for_db(attempts=DB_WAIT_ATTEMPTS, interval=DB_WAIT_INTERVAL):
    """
    Block until the database answers a trivial query. Returns True on success.

    Raises RuntimeError only after the full budget is exhausted, so a genuinely
    misconfigured DSN still surfaces as a failure instead of hanging forever.
    """
    last_error = None
    for attempt in range(1, attempts + 1):
        try:
            probe = create_engine(DB_URI, echo=False, isolation_level="AUTOCOMMIT")
            with probe.connect() as conn:
                conn.execute(text("SELECT 1"))
            probe.dispose()
            logger.info(f"Database reachable (attempt {attempt}/{attempts})")
            return True
        except Exception as e:
            last_error = e
            logger.warning(
                f"Database not reachable yet (attempt {attempt}/{attempts}): {e.__class__.__name__}; "
                f"retrying in {interval}s")
            time.sleep(interval)
    raise RuntimeError(
        f"Database still unreachable after {attempts} attempts "
        f"({attempts * interval}s). Last error: {last_error!r}")


def schedule_tasks(symlink_time=5, update_db_time=5):
    scheduler = BackgroundScheduler()

    # Schedule symlink_routine to run every 5 minutes
    scheduler.add_job(symlink_routine, 'interval', minutes=symlink_time, id='symlink_job')

    # Schedule update_fl_database; ensure no overlapping runs while it may sleep for 24h
    scheduler.add_job(
        update_fl_database,
        'interval',
        minutes=update_db_time,
        id='update_db_job',
        max_instances=1,
        coalesce=True,
        replace_existing=True
    )

    # Refresh the IMDB ratings dump weekly, and once immediately on startup (next_run_time=now)
    # so a fresh deploy populates title_ratings without waiting a week. Runs in the scheduler's
    # threadpool, so a download/DB hiccup is contained and never blocks the keep-alive loop.
    scheduler.add_job(
        update_ratings_database,
        'interval',
        weeks=1,
        id='update_ratings_job',
        max_instances=1,
        coalesce=True,
        replace_existing=True,
        next_run_time=datetime.now()
    )

    scheduler.start()

    try:
        # Keep the main thread alive to allow scheduled jobs to run
        while True:
            time.sleep(2)
    except (KeyboardInterrupt, SystemExit):
        # Not strictly necessary if daemonic mode is enabled but should be done if running a non-daemonic scheduler
        scheduler.shutdown()


if __name__ == '__main__':
    wait_for_db()
    schedule_tasks()
