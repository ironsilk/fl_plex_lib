import time

from sqlalchemy import Column, Integer, BigInteger, String, DateTime, Boolean, ForeignKey, ARRAY, \
    Float, MetaData, create_engine, select, desc, delete, inspect, func, or_, UniqueConstraint, ForeignKeyConstraint, \
    update, Inspector
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy import create_engine
import os
import pandas as pd
import os
import requests
import gzip
import shutil
from dotenv import load_dotenv
from loguru import logger
load_dotenv()


# ENV variables
POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
DB_URI = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
START_IMDB_ID = os.getenv('START_IMDB_ID')

API_URL = os.getenv('API_URL')
FL_USER = os.getenv('FL_USER')
FL_PASSKEY = os.getenv('FL_PASSKEY')
MOVIE_HDRO = os.getenv('MOVIE_HDRO')
MOVIE_4K = os.getenv('MOVIE_4K')

# ENV variables
IMDB_TSV_URL = "https://datasets.imdbws.com/title.basics.tsv.gz"
IMDB_TSV_FILE = "file.tar.gz"

engine = create_engine(DB_URI, echo=False, isolation_level="AUTOCOMMIT")

# declarative base class
Base = declarative_base()

# MetaData
META_DATA = MetaData()
META_DATA.bind = engine
META_DATA.reflect(bind=engine)


class FilelistMovies(Base):
    """
    Filelist movies table
    """
    __tablename__ = 'fl_movies'

    tconst = Column(String, primary_key=True)
    file_created = Column(Boolean, default=False)


class ImdbMovies(Base):
    """
    IMDB movies table
    """
    __tablename__ = 'imdb_movies'

    tconst = Column(String, primary_key=True)
    titleType = Column(String)
    primaryTitle = Column(String)
    originalTitle = Column(String)
    startYear = Column(Integer)
    endYear = Column(Integer)
    runtimeMinutes = Column(Integer)
    genres = Column(ARRAY(String))
    checked = Column(Boolean)


def _create_tables():
    """
    Create tables
    """
    Base.metadata.create_all(engine)


def _check_table_exists(table_name):
    """
    Check if table exists
    """
    inspector = Inspector.from_engine(engine)
    if table_name in inspector.get_table_names():
        return True
    return False


def _get_imdb_db(engine):
    """
    Get the imdb database
    """
    # download the tsv file
    response = requests.get(IMDB_TSV_URL)
    with open(IMDB_TSV_FILE, 'wb') as f:
        f.write(response.content)

    # use gzip to extract the tsv file
    with gzip.open(IMDB_TSV_FILE, 'rb') as f_in:
        with open('title.basics.tsv', 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    # read the tsv file
    df = pd.read_csv('title.basics.tsv', sep='\t', chunksize=100_000)

    return df

def _clean_files():
    # clean up files
    os.remove(IMDB_TSV_FILE)
    os.remove('title.basics.tsv')


def _get_filelist_movie(tconst):
    r = requests.get(
        url=API_URL,
        params={
            'username': FL_USER,
            'passkey': FL_PASSKEY,
            'action': 'search-torrents',
            'type': 'imdb',
            'query': tconst,
        })
    if r.status_code == 200:
        results = r.json()
        results = [x for x in results if x['category'] == 'Filme HD-RO' or x['category'] == 'Filme 4K Blu-Ray']
        logger.debug(f"Got {len(results)} results for tconst {tconst}")
        return results
    elif r.status_code == 429:
        logger.error(f"Rate limited, status code: {r.status_code}, response: {r.text}, sleeping for 5 minutes and trying again")
        time.sleep(300)
        return _get_filelist_movie(tconst)
    else:
        logger.error(f"Error fetching tconst {tconst} from filelist, status code: {r.status_code}, response: {r.text}")

def _exists_on_fl(tconst):
    fl_results = _get_filelist_movie(tconst)
    if fl_results:
        return True
    return False

def populate_imdb_database(engine=None, overwrite=False):
    """
    Method that fetches the .tar.gz tsv file from https://datasets.imdbws.com/title.basics.tsv.gz
    and populates the imdb_movies table with the data.
    Uses pandas, works in batches ok 10k rows and only inserts rows where titleType is 'movie' and
    'tconst' is > START_IMDB_ID.
    Only called if the table is empty.
    """

    engine = engine or create_engine(DB_URI, echo=False, isolation_level="AUTOCOMMIT")

    # check if table exists and is empty
    if _check_table_exists('imdb_movies'):
        with engine.connect() as conn:
            result = conn.execute(select([func.count()]).select_from(ImdbMovies))
            count = result.scalar()
            if count > 0 and not overwrite:
                return
    else:
        _create_tables()

    df = _get_imdb_db(engine)

    # insert the data into the database.

    for chunk in df:
        chunk = chunk[chunk['titleType'] == 'movie']
        chunk = chunk[chunk['tconst'] > START_IMDB_ID]
        if chunk.empty:
            continue
        # limit to the columns we need
        chunk = chunk[['tconst', 'titleType', 'primaryTitle', 'originalTitle', 'startYear', 'endYear', 'runtimeMinutes',
                       'genres']]
        chunk['checked'] = False
        # replace "\N" with None
        chunk = chunk.replace(r'\N', None)
        # for column genres split the string by comma
        chunk['genres'] = chunk['genres'].str.split(',')
        chunk.to_sql('imdb_movies', engine, if_exists='append', index=False)

    _clean_files()


def update_imdb_database(engine=None):
    """
    Method that fetches the .tar.gz tsv file from https://datasets.imdbws.com/title.basics.tsv.gz
    and updates the imdb_movies table with new tconst rows.

    Returns:
        int: number of newly inserted IMDB movie rows.
    """
    engine = engine or create_engine(DB_URI, echo=False, isolation_level="AUTOCOMMIT")

    df = _get_imdb_db(engine)

    # get the last tconst
    with engine.connect() as conn:
        result = conn.execute(select(func.max(ImdbMovies.tconst)))
        last_tconst = result.scalar()

    inserted_count = 0
    # insert the data into the database.
    for chunk in df:
        chunk = chunk[chunk['titleType'] == 'movie']
        # filter only new rows
        chunk = chunk[chunk['tconst'] > last_tconst]
        if chunk.empty:
            continue
        # limit to the columns we need
        chunk = chunk[['tconst', 'titleType', 'primaryTitle', 'originalTitle', 'startYear', 'endYear', 'runtimeMinutes', 'genres']]
        # ensure new IMDB rows are marked as unchecked so they get picked up for Filelist checks
        chunk['checked'] = False
        # replace "\N" with None
        chunk = chunk.replace(r'\N', None)
        # for column genres split the string by comma
        chunk['genres'] = chunk['genres'].str.split(',')

        rows_to_insert = len(chunk)
        if rows_to_insert == 0:
            continue

        try:
            chunk.to_sql('imdb_movies', engine, if_exists='append', index=False)
            inserted_count += rows_to_insert
        except Exception as e:
            logger.error(f"Error inserting IMDB chunk: {e}")

    _clean_files()
    logger.info(f"IMDB update complete. Inserted {inserted_count} new rows since last tconst={last_tconst}.")
    return inserted_count


def update_fl_database(engine=None, check_again=False):

    engine = engine or create_engine(DB_URI, echo=False, isolation_level="AUTOCOMMIT")

    if not _check_table_exists('fl_movies'):
        _create_tables()

    # get rows from imdb_movies
    with engine.connect() as conn:
        if not check_again:
            result = conn.execute(select(ImdbMovies.tconst).where(ImdbMovies.checked == False).order_by(desc(ImdbMovies.tconst)))
        else:
            result = conn.execute(select(ImdbMovies.tconst).order_by(desc(ImdbMovies.tconst)))
        rows = result.fetchall()

    logger.info(f"Got {len(rows)} rows from imdb_movies")
    # exclude rows already in fl_movies
    with engine.connect() as conn:
        result = conn.execute(select(FilelistMovies.tconst))
        fl_rows = result.fetchall()

    logger.info(f"Got {len(fl_rows)} rows from fl_movies")
    rows = [x[0] for x in rows]
    fl_rows = [x[0] for x in fl_rows]
    rows = [x for x in rows if x not in fl_rows]

    logger.info(f"Initial rows to check: {len(rows)}")

    # Pre-loop: if there is nothing to check, try to pull newer IMDB IDs until either rows exist or IMDB is exhausted
    if len(rows) == 0:
        loop_iter = 0
        while True:
            loop_iter += 1
            logger.info(f"No rows to check. Triggering IMDB update iteration {loop_iter} ...")
            inserted_count = update_imdb_database(engine=engine)
            logger.info(f"IMDB update iteration {loop_iter} inserted {inserted_count} rows.")

            # recompute rows-to-check
            with engine.connect() as conn:
                if not check_again:
                    result = conn.execute(select(ImdbMovies.tconst).where(ImdbMovies.checked == False).order_by(desc(ImdbMovies.tconst)))
                else:
                    result = conn.execute(select(ImdbMovies.tconst).order_by(desc(ImdbMovies.tconst)))
                rows = result.fetchall()

            with engine.connect() as conn:
                result = conn.execute(select(FilelistMovies.tconst))
                fl_rows = result.fetchall()

            rows = [x[0] for x in rows]
            fl_rows = [x[0] for x in fl_rows]
            rows = [x for x in rows if x not in fl_rows]

            logger.info(f"Rows to check after IMDB update iteration {loop_iter}: {len(rows)}")

            if rows:
                logger.info("Exiting pre-loop: found rows to check against Filelist.")
                break

            if inserted_count == 0:
                logger.info("Exiting pre-loop: IMDB dataset exhausted (no new rows inserted). Nothing to check this cycle.")
                return

    logger.info(f"Proceeding with {len(rows)} rows to check")
    for idx, tconst in enumerate(rows):
        if _exists_on_fl(tconst):
            with engine.connect() as conn:
                stmt = insert(FilelistMovies.__table__).values(tconst=tconst, file_created=False)
                stmt = stmt.on_conflict_do_nothing(index_elements=['tconst'])
                conn.execute(stmt)
        logger.debug("Sleeping for 24 seconds")
        time.sleep(24)  # sleep 24 seconds to avoid rate limiting
        with engine.connect() as conn:
            conn.execute(update(ImdbMovies).where(ImdbMovies.tconst == tconst).values(checked=True))
        if idx % 100 == 0:
            logger.info(f"Checked {idx} rows")
            logger.info(f"ETA: {len(rows) - idx} rows left, {((len(rows) - idx) * 24) / 60} minutes")


if __name__ == '__main__':
    populate_imdb_database()