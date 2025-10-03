from apscheduler.schedulers.background import BackgroundScheduler
from file_mocks import symlink_routine
from db import update_fl_database
import time


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

    scheduler.start()

    try:
        # Keep the main thread alive to allow scheduled jobs to run
        while True:
            time.sleep(2)
    except (KeyboardInterrupt, SystemExit):
        # Not strictly necessary if daemonic mode is enabled but should be done if running a non-daemonic scheduler
        scheduler.shutdown()


if __name__ == '__main__':
    schedule_tasks()
