"""
Regression tests for the 2026-10-19 db_builder fixes.

No database and no network: db.py / file_mocks.py are imported with a dummy DSN and the
requests call is monkeypatched. This is only possible because the module-level
MetaData().reflect() was removed -- importing db.py no longer touches Postgres.
"""
import os
import sys

import pytest

os.environ.setdefault('POSTGRES_DB', 'test')
os.environ.setdefault('POSTGRES_HOST', '127.0.0.1')
os.environ.setdefault('POSTGRES_PORT', '5432')
os.environ.setdefault('POSTGRES_USER', 'test')
os.environ.setdefault('POSTGRES_PASSWORD', 'test')
os.environ.setdefault('PLEX_MOVIES_LIBRARY_FOLDER_MAP', '/tmp/fl_test_library')

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db  # noqa: E402
import file_mocks  # noqa: E402


class _Resp:
    def __init__(self, status_code, payload=None, text=''):
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self):
        if self._payload is None:
            raise ValueError("no json")
        return self._payload


# --------------------------------------------------------------------------- import safety

def test_importing_db_does_not_require_a_database():
    """The module must be import-safe; this is what killed the container on 2026-10-03."""
    assert hasattr(db, 'update_fl_database')
    assert not hasattr(db, 'META_DATA')


# ------------------------------------------------------------------- _exists_on_fl tri-state

def test_exists_on_fl_returns_none_on_http_error(monkeypatch):
    monkeypatch.setattr(db.requests, 'get', lambda *a, **k: _Resp(500, text='boom'))
    assert db._exists_on_fl('tt0000001') is None


def test_exists_on_fl_returns_false_when_api_says_zero_matches(monkeypatch):
    monkeypatch.setattr(db.requests, 'get', lambda *a, **k: _Resp(200, payload=[]))
    assert db._exists_on_fl('tt0000001') is False


def test_exists_on_fl_returns_true_on_match(monkeypatch):
    payload = [{'category': 'Filme HD-RO'}, {'category': 'Serii'}]
    monkeypatch.setattr(db.requests, 'get', lambda *a, **k: _Resp(200, payload=payload))
    assert db._exists_on_fl('tt0000001') is True


def test_exists_on_fl_returns_none_on_network_error(monkeypatch):
    def boom(*a, **k):
        raise db.requests.RequestException("connection reset")
    monkeypatch.setattr(db.requests, 'get', boom)
    assert db._exists_on_fl('tt0000001') is None


def test_rate_limit_retries_are_bounded(monkeypatch):
    calls = {'n': 0}

    def always_429(*a, **k):
        calls['n'] += 1
        return _Resp(429, text='slow down')

    monkeypatch.setattr(db.requests, 'get', always_429)
    monkeypatch.setattr(db.time, 'sleep', lambda *_: None)
    assert db._get_filelist_movie('tt0000001') is None
    assert calls['n'] == db.RATE_LIMIT_MAX_RETRIES


# ------------------------------------------------------------------------------- sanitizer

@pytest.mark.parametrize("raw,expected", [
    ("Face/Off 1997 tt0119094", "Face-Off 1997 tt0119094"),
    ("Frost/Nixon 2008 tt0870111", "Frost-Nixon 2008 tt0870111"),
    ("V/H/S 2012 tt2105044", "V-H-S 2012 tt2105044"),
    ("The Naked Gun 33 1/3 1994 tt0110622", "The Naked Gun 33 1-3 1994 tt0110622"),
])
def test_sanitize_name_removes_path_separators(raw, expected):
    assert file_mocks.sanitize_name(raw) == expected


@pytest.mark.parametrize("name", [
    "03.ByPass 2016 tt3624344",
    "10,000 BC 2008 tt0443649",
    "The First $20 Million Is Always the Hardest 2002 tt0280674",
    'Good Night, and Good Luck. 2005 tt0433383',
])
def test_sanitize_name_leaves_valid_names_untouched(name):
    """Regression guard: renaming existing entries would orphan the 15k-folder library."""
    assert file_mocks.sanitize_name(name) == name.rstrip('. ')


# ------------------------------------------------------------------------ file creation

def test_create_symlinks_handles_shell_metacharacters(tmp_path):
    """'$20' and quotes must land literally -- the old os.system let the shell eat them."""
    dummy = tmp_path / "placeholder.mkv"
    dummy.write_bytes(b"x" * 16)
    library = tmp_path / "lib"
    name = 'The First $20 Million Is Always the Hardest 2002 tt0280674'

    file_mocks.create_symlinks(str(dummy), str(library), [name])

    folder = library / name
    assert folder.is_dir(), f"expected folder {name!r}, got {[p.name for p in library.iterdir()]}"
    assert (folder / f"{name}.mkv").is_file()
    # nothing may be created outside the intended folder
    assert [p.name for p in library.iterdir()] == [name]


def test_create_symlinks_does_not_nest_on_slash(tmp_path):
    dummy = tmp_path / "placeholder.mkv"
    dummy.write_bytes(b"x" * 16)
    library = tmp_path / "lib"

    file_mocks.create_symlinks(str(dummy), str(library), ["Face/Off 1997 tt0119094"])

    entries = [p.name for p in library.iterdir()]
    assert entries == ["Face-Off 1997 tt0119094"]
    assert (library / "Face-Off 1997 tt0119094" / "Face-Off 1997 tt0119094.mkv").is_file()


def test_create_symlinks_skips_existing(tmp_path):
    dummy = tmp_path / "placeholder.mkv"
    dummy.write_bytes(b"x" * 16)
    library = tmp_path / "lib"
    name = "Already There 2001 tt0000002"
    (library / name).mkdir(parents=True)

    file_mocks.create_symlinks(str(dummy), str(library), [name])

    # must not have dropped a placeholder into an already-populated folder
    assert list((library / name).iterdir()) == []
