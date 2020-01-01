import functools
import os
import sqlite3
import sys
import zlib

from datetime import datetime
from enum import Enum, auto
from pathlib import Path


class Compression(Enum):
    SQLAR_STORED = auto()
    SQLAR_DEFLATED = auto()


SQLAR_STORED = Compression.SQLAR_STORED
SQLAR_DEFLATED = Compression.SQLAR_DEFLATED


def _get_deflated_decompressor():
    return zlib.decompressobj(wbits=-zlib.MAX_WBITS)


def _get_deflated_compressor(level=-1):
    return zlib.compressobj(level=level, method=zlib.DEFLATED, wbits=-zlib.MAX_WBITS)


def compress_data(data, level=None):
    level = level if level else -1

    compressed_data = zlib.compress(data, level=level)
    return compressed_data if len(compressed_data) < len(data) else data


def decompress_data(data, size):
    if size == len(data):
        return data
    else:
        return zlib.decompress(data)

def _decompress_row(path, row):
    name, mode, mtime, size, data = row
    complete_path = path / name
    complete_path.parent.mkdir(parents=True, exist_ok=True)

    with complete_path.open("wb") as f:
        f.write(decompress_data(data, size))
    
    complete_path.chmod(mode)
    info = complete_path.stat()
    os.utime(complete_path, times=(info.st_atime, mtime))


class SQLiteArchive():
    def __init__(self, filename, create=False, compression=SQLAR_STORED, compress_level=None):
        self.filename = filename
        self._conn = self._init_archive() if create else None
        self._compression = compression
        self._compress_level = compress_level
    
    def _ensure_conn(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            if not self._conn:
                self._conn = self._init_archive()
            return func(self, *args, **kwargs)
        return wrapper
    
    def _init_archive(self):
        conn = self._conn or sqlite3.connect(self.filename)
        with conn as c:
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS sqlar(
                    name TEXT PRIMARY KEY,
                    mode INT,
                    mtime INT,
                    sz INT,
                    data BLOB
                );
                """
            )
        return conn

    def close(self):
        if self._conn:
            self._conn.close()
    
    @_ensure_conn
    def getinfo(self, name):
        with self._conn as c:
            row = c.execute(
                """
                SELECT * FROM sqlar WHERE name = ?;
                """,
                name
            ).fetchone()
        return row

    @_ensure_conn
    def infolist(self):
        with self._conn as c:
            rows = c.execute(
                """
                SELECT name, mode, mtime, sz FROM sqlar;
                """
            ).fetchall()
        return rows

    @_ensure_conn
    def namelist(self):
        with self._conn as c:
            rows = c.execute(
                """
                SELECT name FROM sqlar;
                """
            ).fetchall()
        return rows

    @_ensure_conn
    def open(self, name, mode="r"):
        raise NotImplementedError()

    @_ensure_conn
    def extract(self, member, path=None):
        path = Path(path) if path else Path()

        with self._conn as c:
            row = c.execute(
                """
                SELECT * FROM sqlar WHERE name = ?;
                """,
                member
            ).fetchone()
        if row:
            _decompress_row(path, row)

    @_ensure_conn
    def extractall(self, path=None, members=None):
        if members and len(members) > 1000:
            raise ValueError("can only extract 999 or less named members.")
        path = Path(path) if path else Path()

        with self._conn as c:
            if members:
                cur = c.execute(
                    f"""
                    SELECT * FROM sqlar WHERE name IN ({'?,'.join('' for _ in members)});
                    """,
                    members
                )
            else:
                cur = c.execute(
                    """
                    SELECT * FROM sqlar;
                    """
                )
            for row in cur:
                _decompress_row(path, row)

    @_ensure_conn
    def read(self, name):
        with self._conn as c:
            row = c.execute(
                """
                SELECT * FROM sqlar WHERE name = ?;
                """,
                member
            ).fetchone()

        _, _, _, size, data = row
        return decompress_data(data. size)

    @_ensure_conn
    def sql(self, query, *args):
        with self._conn as c:
            rows = c.execute(query, args).fetchall()
        return rows

    @_ensure_conn
    def testsqlar(self):
        raise NotImplementedError()

    @_ensure_conn
    def write(self, filename, arcname=None, compression=None, compress_level=None):
        arcname = arcname or filename

        compression = compression or self._compression
        level = compress_level or self._compress_level

        path = Path(filename)

        info = path.stat()
        mode = info.st_mode & 0o777
        mtime = int(info.st_mtime_ns * 1e-9)
        size = info.st_size

        if path.is_symlink():
            data = str(path.resolve().as_posix())
            size = -1
        elif path.is_file():
            with open(path, "rb") as f:
                if compression == SQLAR_DEFLATED:
                    data = compress_data(f.read(), level)
                else:
                    data = f.read()
        elif path.is_dir():
            data = None
            size = 0

        with self._conn as c:
            c.execute(
                """
                INSERT INTO sqlar(name, mode, mtime, sz, data)
                VALUES (?, ?, ?, ?, ?)
                """,
                (str(Path(arcname).as_posix()), mode, mtime, size, data)
            )

    @_ensure_conn
    def writestr(self, arcname, data, unix_mode=0o777, mtime=int(datetime.utcnow().timestamp()), compression=None, compress_level=None):
        if isinstance(data, str):
            data = data.encode("utf-8")
        
        compress_type = compression or self._compression
        level = compress_level or self._compress_level
        
        compressed_data = compress_data(data, level) if compress_type == SQLAR_DEFLATED else data

        with self._conn as c:
            c.execute(
                """
                INSERT INTO sqlar(name, mode, mtime, sz, data)
                VALUES (?, ?, ?, ?, ?)
                """,
                (str(Path(arcname).as_posix()), unix_mode, mtime, len(data), compressed_data)
            )

    def __enter__(self):
        return self

    def __exit__(self, *details):
        self.close()
