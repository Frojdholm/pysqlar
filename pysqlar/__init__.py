import functools
import os
import sqlite3
import sys
import zlib

from datetime import datetime
from pathlib import Path


def _get_deflated_decompressor():
    return zlib.decompressobj(wbits=-zlib.MAX_WBITS)


def _get_deflated_compressor(self, level=-1):
    return zlib.compressobj(level=level, method=zlib.DEFLATED, wbits=-zlib.MAX_WBITS)


def _compress_data(data, level=None):
    if level:
        compressor = _get_deflated_compressor(level)
        return compressor.compress(data) + compressor.flush()
    else:
        return data


def _decompress_data(data, size):
    if size == len(data):
        return data
    else:
        decompressor = _get_deflated_decompressor()
        return decompressor.decompress(data)

def _decompress_row(path, row):
    name, mode, mtime, size, data = row
    complete_path = path / name
    complete_path.parent.mkdir(parents=True, exist_ok=True)

    with complete_path.open("wb") as f:
        f.write(_decompress_data(data, size))
    
    complete_path.chmod(mode)
    info = complete_path.stat()
    os.utime(complete_path, times=(info.st_atime, mtime))


class SQLiteArchive():
    def __init__(self, filename, create=False, compress_level=None):
        self.filename = filename
        self._conn = self._init_archive() if create else None
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
                rows = c.execute(
                    f"""
                    SELECT * FROM sqlar WHERE name IN ({'?,'.join('' for _ in members)});
                    """,
                    members
                )
            else:
                rows = c.execute(
                    """
                    SELECT * FROM sqlar;
                    """
                ).fetchall()

        for row in rows:
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
        
        return _decompress_data(row[4], row[3])

    @_ensure_conn
    def testsqlar(self):
        raise NotImplementedError()

    @_ensure_conn
    def write(self, filename, arcname=None, compress_level=None):
        arcname = arcname or filename

        with open(filename, "rb") as f:
            data = _compress_data(f.read(), compress_level or self._compress_level)

        info = os.stat(filename)

        with self._conn as c:
            c.execute(
                """
                INSERT INTO sqlar(name, mode, mtime, sz, data)
                VALUES (?, ?, ?, ?, ?)
                """,
                (str(Path(arcname).as_posix()), info.st_mode & 0o777, int(info.st_mtime_ns * 1e-9), info.st_size, data)
            )

    @_ensure_conn
    def writestr(self, arcname, data, compress_level=None):
        if isinstance(data, str):
            data = data.encode("utf-8")
        
        compressed_data = _compress_data(data, compress_level or self._compress_level)

        with self._conn as c:
            c.execute(
                """
                INSERT INTO sqlar(name, mode, mtime, sz, data)
                VALUES (?, ?, ?, ?, ?)
                """,
                (str(Path(arcname).as_posix()), 0o777, int(datetime.utcnow().timestamp()), len(data), compressed_data)
            )

    def __enter__(self):
        return self

    def __exit__(self, *details):
        self.close()
