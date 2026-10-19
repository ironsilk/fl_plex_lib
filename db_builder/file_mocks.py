from sqlalchemy import create_engine, select, update
from loguru import logger
from dotenv import load_dotenv
import os
import re
import shutil

load_dotenv()

from db import DB_URI, FilelistMovies, ImdbMovies

LIBRARY_PATH = os.getenv('PLEX_MOVIES_LIBRARY_FOLDER_MAP')


def sanitize_name(name):
    """
    Make a movie name safe to use as a single directory name.

    Only characters that genuinely break the on-disk layout are touched, so names that are
    already fine come back byte-identical -- renaming existing entries would orphan the
    15k folders already in the library.

    The important case is the path separator: 'Face/Off 1997 tt0119094' used to be passed
    straight to os.makedirs, which created a nested 'Face/Off .../' tree instead of one
    folder. 13 such entries exist in the library (Face/Off, Frost/Nixon, V/H/S, Self/less,
    50/50, Naked Gun 33 1/3, Fahrenheit 9/11, ...).
    """
    cleaned = re.sub(r'[/\\]', '-', name)
    cleaned = cleaned.replace('\0', '')
    # collapse whitespace runs and strip characters that are illegal/awkward as a trailing char
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    cleaned = cleaned.rstrip('. ')
    return cleaned


def create_symlinks(dummy_file_path, target_directory, movie_names):
    """
    Creates a folder per movie containing a copy of the placeholder file.

    Parameters:
    - dummy_file_path: The path to the single dummy file.
    - target_directory: The directory where the movie folders will be created.
    - movie_names: A list of names for the movies to create entries for.
    """
    # Ensure the target directory exists (create if it doesn't)
    os.makedirs(target_directory, exist_ok=True)

    # Iterate over the list of movie names
    for movie_name in movie_names:
        movie_name = sanitize_name(movie_name)
        if not movie_name:
            logger.warning("Skipping a movie with an empty name after sanitization")
            continue

        # Full path for the movie folder
        movie_folder = os.path.join(target_directory, movie_name)

        # Check if the folder already exists to avoid overwriting
        if os.path.exists(movie_folder):
            logger.warning(f"Entry already exists for {movie_name}")
            continue

        os.makedirs(movie_folder, exist_ok=True)
        # NOTE: plex can't be fooled with symlinks so easily, so we copy the placeholder.
        # This used to be os.system(f'cp "{dummy}" "{folder}"') followed by an os.rename.
        # Passing titles through a shell mangled them: 'The First $20 Million Is Always the
        # Hardest' became 'The First 0 Million ...' because the shell expanded $2, and the
        # result landed as a stray file next to the library instead of inside the folder.
        # shutil.copy2 takes the destination path literally -- no shell, no rename step.
        dest = os.path.join(movie_folder, f"{movie_name}.mkv")
        try:
            shutil.copy2(dummy_file_path, dest)
        except OSError as e:
            logger.error(f"Failed to create placeholder for {movie_name}: {e!r}")
            continue
        logger.debug(f"Created entry for {movie_name}")


def _prepare_symlink_creation():
    # check if placeholder.mkv file exists and is in library path
    dummy_file_path = os.path.join(LIBRARY_PATH, 'placeholder.mkv')
    if not os.path.exists(dummy_file_path):
        logger.info(f"Copying placeholder.mkv to {LIBRARY_PATH}")
        shutil.copy2('placeholder.mkv', dummy_file_path)
    else:
        logger.info(f"placeholder.mkv already exists in {LIBRARY_PATH}")
    return dummy_file_path


def symlink_routine():

    engine = create_engine(DB_URI, echo=False, isolation_level="AUTOCOMMIT")

    # get fl entries that still need a file on disk
    with engine.connect() as conn:
        result = conn.execute(select(FilelistMovies.tconst).where(FilelistMovies.file_created == False))
        pending_tconsts = [row[0] for row in result.fetchall()]

    if not pending_tconsts:
        logger.info("Got 0 movies to create symlinks for")
        return

    # get info on these rows from imdb_movies
    with engine.connect() as conn:
        result = conn.execute(
            select(ImdbMovies.tconst, ImdbMovies.primaryTitle, ImdbMovies.startYear)
            .where(ImdbMovies.tconst.in_(pending_tconsts)))
        rows = result.fetchall()

    file_names = [f"{row[1]} {row[2]} {row[0]}" for row in rows]
    tconst_s = [row[0] for row in rows]
    logger.info(f"Got {len(file_names)} movies to create symlinks for")
    dummy_file_path = _prepare_symlink_creation()
    create_symlinks(dummy_file_path, LIBRARY_PATH, file_names)
    # update db in bulk
    if tconst_s:
        with engine.connect() as conn:
            conn.execute(update(FilelistMovies).where(FilelistMovies.tconst.in_(tconst_s)).values(file_created=True))


if __name__ == '__main__':
    symlink_routine()
