from sqlalchemy import create_engine, select, update
from loguru import logger
from dotenv import load_dotenv
import os

load_dotenv()

from db import DB_URI, FilelistMovies, ImdbMovies

LIBRARY_PATH = os.getenv('PLEX_MOVIES_LIBRARY_FOLDER_MAP')


def create_symlinks(dummy_file_path, target_directory, movie_names):
    """
    Creates symlinks for a list of movie names pointing to a single dummy file.

    Parameters:
    - dummy_file_path: The path to the single dummy file.
    - target_directory: The directory where symlinks will be created.
    - movie_names: A list of names for the movies to create symlinks for.
    """
    import os

    # Ensure the target directory exists (create if it doesn't)
    os.makedirs(target_directory, exist_ok=True)

    # Iterate over the list of movie names
    for movie_name in movie_names:
        # Create a file name for the symlink
        symlink_name = f"{movie_name}.mkv"

        # Full path for the new symlink
        symlink_path = os.path.join(target_directory, symlink_name)

        # Check if symlink already exists to avoid overwriting
        if not os.path.exists(symlink_path):
            # Create the symlink inside a folder with the same name as the movie
            movie_folder = os.path.join(target_directory, movie_name)
            os.makedirs(movie_folder, exist_ok=True)
            # create symlink
            os.symlink(dummy_file_path, symlink_path)
            logger.debug(f"Created symlink for {movie_name}")
        else:
            logger.warning(f"Symlink already exists for {movie_name}")


def _prepare_symlink_creation():
    # check if placeholder.mkv file exists and is in library path
    dummy_file_path = os.path.join(LIBRARY_PATH, 'placeholder.mkv')
    if not os.path.exists(dummy_file_path):
        # copy it there
        logger.info(f"Copying placeholder.mkv to {LIBRARY_PATH}")
        os.system(f"cp placeholder.mkv {LIBRARY_PATH}")
    else:
        logger.info(f"placeholder.mkv already exists in {LIBRARY_PATH}")
    return dummy_file_path


def symlink_routine():

    engine = create_engine(DB_URI, echo=False, isolation_level="AUTOCOMMIT")

    # get fl entries
    with engine.connect() as conn:
        result = conn.execute(select(FilelistMovies.tconst).where(FilelistMovies.file_created == False))
        rows = result.fetchall()
    rows = [rows[0] for rows in rows]
    # get info on these rows from imdb_movies
    with engine.connect() as conn:
        result = conn.execute(select(ImdbMovies.tconst, ImdbMovies.primaryTitle, ImdbMovies.startYear).where(ImdbMovies.tconst.in_(rows)))
        rows = result.fetchall()
    file_names = [f"{row[1]} ({row[2]}) {row[0]}" for row in rows]
    logger.info(f"Got {len(file_names)} movies to create symlinks for")
    dummy_file_path = _prepare_symlink_creation()
    create_symlinks(dummy_file_path, LIBRARY_PATH, file_names)
    # update db in bulk
    with engine.connect() as conn:
        conn.execute(update(FilelistMovies).where(FilelistMovies.tconst.in_(rows)).values(file_created=True))


if __name__ == '__main__':
    symlink_routine()