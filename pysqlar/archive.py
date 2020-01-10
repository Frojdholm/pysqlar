import logging
import os
import sqlite3
import sys
import zlib

from datetime import datetime
from enum import Enum, auto
from pathlib import Path


logger = logging.getLogger(__name__)


class Compression(Enum):
    """Constants for choosing compression type."""
    SQLAR_STORED = auto()
    SQLAR_DEFLATED = auto()


SQLAR_STORED = Compression.SQLAR_STORED
"""Alias for `Compression.SQLAR_STORED`."""

SQLAR_DEFLATED = Compression.SQLAR_DEFLATED
"""Alias for `Compression.SQLAR_DEFLATED`."""

SQLAR_TABLE_SCHEMA = " ".join("""
CREATE TABLE sqlar(
    name TEXT PRIMARY KEY,
    mode INT,
    mtime INT,
    sz INT,
    data BLOB
)""".split()) # Normalize whitespace in the statement
"""The table definition for the SQLite Archive table."""


def _get_deflated_decompressor():
    return zlib.decompressobj(wbits=-zlib.MAX_WBITS)


def _get_deflated_compressor(level=-1):
    return zlib.compressobj(level=level, method=zlib.DEFLATED, wbits=-zlib.MAX_WBITS)


def compress_data(data, level=None):
    """Compress data for storage in archive.

    The behaviour is the same as
    [*sqlar_compress*](https://sqlite.org/sqlar/doc/trunk/README.md). The data
    is compressed using the zlib compress convenience function, adding the file
    header and CRC footer. If the compressed data is smaller than the original
    it is returned otherwise the original data is returned.

    Args:
        data: The data to compress.
        level (optional): The level of compression, see the *zlib* documentation
            for allowed values. If it is `None`, `zlib.Z_DEFALUT_COMPRESSION`
            is used.

    Returns:
        The compressed data if it is smaller than the original, otherwise the
        original data.
    """
    level = level if level else zlib.Z_DEFAULT_COMPRESSION

    compressed_data = zlib.compress(data, level=level)
    return compressed_data if len(compressed_data) < len(data) else data


def decompress_data(data, size):
    """Decompress data compressed with `compress_data`.

    If the size of the data is the same as *size* the data is assumed to be
    uncompressed and is returned directly.

    Args:
        data: The data to be decompressed.
        size: The original size of the data.

    Returns:
        The decompressed data.
    """
    if size == len(data):
        return data
    else:
        return zlib.decompress(data)

def _decompress_row(path, row):
    name, mode, mtime, size, data = row
    complete_path = path / name
    complete_path.parent.mkdir(parents=True, exist_ok=True)

    with open(complete_path, "wb") as f:
        f.write(decompress_data(data, size))
    
    complete_path.chmod(mode)
    info = complete_path.stat()
    os.utime(complete_path, times=(info.st_atime, mtime))


def _init_archive(filename, mode):
    if filename == ":memory:":
        conn = sqlite3.connect(filename)
        mode = "rwc"
    else:
        if mode == "memory":
            uri = "file:{}".format(filename)
        else:
            uri = Path(filename).absolute().as_uri()

        conn = sqlite3.connect("{}?mode={}".format(uri, mode), uri=True)

    if "w" in mode or mode == "memory":
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
    return conn, mode


def is_sqlar(filename):
    """Checks if *filename* is a SQLite Archive.

    Checks whether the table *sqlar* with the following schema exists in the
    database:
    ```sql
    CREATE TABLE sqlar(
                    name TEXT PRIMARY KEY,
                    mode INT,
                    mtime INT,
                    sz INT,
                    data BLOB
                )
    ```

    Args:
        filename: Filename or path-like object to the file to check.
    Returns:
        `True` if *filename* is a SQLite Archive, `False`
        otherwise.
    """
    if filename == ":memory:":
        return True

    if not os.path.exists(filename):
        return False

    flag = False
    try:
        conn, _ = _init_archive(filename, mode="ro")
        cur = conn.cursor()
        row = cur.execute(
            """
            SELECT sql FROM sqlite_master
            WHERE tbl_name = 'sqlar' AND type = 'table';
            """
        ).fetchone()
        if row:
            sql = row[0]
            # Normalize whitespace
            sql = " ".join(sql.split())
            logger.debug(sql)
            if sql == SQLAR_TABLE_SCHEMA:
                flag = True
    except sqlite3.OperationalError:
        # if we there is an error the file is not a SQLite Archive
        flag = False
    finally:
        conn.close()
    return flag



class SQLiteArchive():
    """An SQLite Archive.

    SQLite Archive is an archiving format that utilises an SQLite database to
    store data. Data is optionally compressed using the zlib deflated
    compression.

    The archive table *sqlar* has the following schema:
    ```sql
    CREATE TABLE sqlar(
                    name TEXT PRIMARY KEY,
                    mode INT,
                    mtime INT,
                    sz INT,
                    data BLOB
                )
    ```

    Files are stored as binary blobs. Both directories and empty files has a
    size (`sz`) field set to 0 in the database, but directories also have
    `data = NULL`. Symbolic links get their size set to -1 and `data` to
    their original targets.

    Additional tables can be stored in the database to store additional metadata
    for the files.

    Attributes:
        filename: The filename of the SQLite Archive.
        mode: The current mode of the opened database.
    """

    def __init__(self,
                 filename,
                 mode="ro",
                 compression=SQLAR_STORED,
                 compress_level=None):
        """Open a SQLite Archive.

        Args:
            filename: The path to the archive. The special name `:memory:`
                opens a memory-only database, in this case the *mode* parameter
                is ignored.
            mode (optional): The SQLite *mode* to open the archive with. Allowed
                values are: `"ro"` Read-Only, `"rw"` Read-Write, `"rwc"`
                Read-Write-Create and `"memory"` opens a memory-only database.
                See [SQLite URI documentation](https://www.sqlite.org/uri.html)
                for more information
            compression (optional): Controls the compression of the archive.
                Allowed values are `SQLAR_STORED` which stores the data
                uncompressed in the archive and `SQLAR_DEFLATED` which stores
                data in zlib deflated compressed format.
            compress_level (optional): The compression level to use, see *zlib*
                documentation for allowed values. If compression is
                `SQLAR_DEFLATED` the default is
                `zlib.Z_DEFAULT_COMPRESSION`.
        """
        self.filename = filename
        self._conn, self.mode = _init_archive(filename, mode)
        self._compression = compression
        self._compress_level = compress_level

    def close(self):
        """Close the database."""
        self._conn.close()
    
    def getinfo(self, name):
        """Return metadata about a file in the archive.

        Args:
            name: Name of the file in the archive.

        Returns:
            Metadata for file *name* or `None` if there is no such file in the
            archive.
        """
        with self._conn as c:
            row = c.execute(
                """
                SELECT name, mode, mtime, sz FROM sqlar WHERE name = ?;
                """,
                (name,)
            ).fetchone()
        return row

    def infolist(self):
        """Returns a list of metadata for all files in the archive."""
        with self._conn as c:
            rows = c.execute(
                """
                SELECT name, mode, mtime, sz FROM sqlar;
                """
            ).fetchall()
        return rows

    def namelist(self):
        """Returns a list of all files in the archive."""
        with self._conn as c:
            rows = c.execute(
                """
                SELECT name FROM sqlar;
                """
            ).fetchall()
        return list(*zip(*rows)) # unpack [(item1,), (item2,), ...] to [item1, item2, ...]

    def open(self, name, mode="r"):
        raise NotImplementedError()

    def extract(self, member, path=None):
        """Extract a single member of the archive.

        Defaults to extracting in *cwd*.

        Args:
            member: The archive member to extract.
            path (optional): The root path to extract the archive to.
        """
        path = Path(path) if path else Path()

        with self._conn as c:
            row = c.execute(
                """
                SELECT * FROM sqlar WHERE name = ?;
                """,
                (member,)
            ).fetchone()
        if row:
            _decompress_row(path, row)

    def extractall(self, path=None, members=None):
        """Extract the entire archive.

        Defaults to extracting in *cwd*.

        Args:
            path (optional): The root path to extract the archive to.
            members (optional): A list of member files to extract. Has to be
                shorter than 999 members.

        Raises:
            ValueError: If `len(members) > 1000`.
        """
        if members and len(members) > 1000:
            raise ValueError("can only extract 999 or less named members.")
        path = Path(path) if path else Path()

        with self._conn as c:
            if members:
                cur = c.execute(
                    """
                    SELECT * FROM sqlar WHERE name IN ({});
                    """.format('?,'.join('' for _ in members)),
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

    def read(self, name):
        """Returns a decompressed bytes-object from the archive.

        Args:
            name: The name of the file to extract.

        Returns:
            A bytes-object with the decompressed file.
        """
        with self._conn as c:
            row = c.execute(
                """
                SELECT * FROM sqlar WHERE name = ?;
                """,
                (name,)
            ).fetchone()
        if row:
            _, _, _, size, data = row
            return decompress_data(data, size)

    def sql(self, query, *args):
        """Execute raw SQL statements against the database.

        Args:
            query: The SQL statement.
            *args: Arguments that are substituted into query.

        Returns:
            The results of the query.
        """
        with self._conn as c:
            rows = c.execute(query, args).fetchall()
        return rows

    def testsqlar(self):
        raise NotImplementedError()

    def write(self, filename, arcname=None, compression=None, compress_level=None):
        """Write the file pointed to by *filename* to the archive.

        Writes the file into the archive with the archive name *arcname*, which
        by default is the same as filename.

        Both directories and empty files has a size (`sz`) field set to 0 in
        the database, but directories also have `data = NULL`. Symbolic links
        get their size set to -1 and `data` to their original targets.

        Args:
            filename: Filename or path-like object to the file to be written
                into the archive.
            arcname (optional): The name of the file in the archive.
            compression (optional): Override the *compression* chosen when
                opening the archive.
            compress_level (optional): Override the *compress_level* chosen when
                opening the archive.

        Raises:
            ValueError: *filename* does not represent a file, directory or
                symlink.
        """
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
        else:
            raise ValueError("path is not a file, directory or symlink")

        logger.debug(
            "Write: arcname={}, mode={}, mtime={}, size={}".format(
                arcname,
                mode,
                mtime,
                size
            )
        )
        with self._conn as c:
            c.execute(
                """
                INSERT INTO sqlar(name, mode, mtime, sz, data)
                VALUES (?, ?, ?, ?, ?)
                """,
                (str(Path(arcname).as_posix()), mode, mtime, size, data)
            )

    def writestr(self,
                 arcname,
                 data,
                 unix_mode=0o777,
                 mtime=int(datetime.utcnow().timestamp()),
                 compression=None,
                 compress_level=None):
        """Write the string into the archive with name *arcname*.

        If *data* is a *str* it is first encoded as utf-8 before writing.

        Args:
            arcname: The name of the file in the archive.
            data: The *bytes* or *str* to write to the archive.
            unix_mode (optional): The unix file permissions.
            mtime (optional): The modification time in unix epoch time (seconds).
            compression (optional): Override the *compression* chosen when
                opening the archive.
            compress_level (optional): Override the *compress_level* chosen when
                opening the archive.
        """
        if isinstance(data, str):
            data = data.encode("utf-8")
        
        compress_type = compression or self._compression
        level = compress_level or self._compress_level
        
        if compress_type == SQLAR_DEFLATED:
            compressed_data = compress_data(data, level)
        else:
            compressed_data = data

        with self._conn as c:
            c.execute(
                """
                INSERT INTO sqlar(name, mode, mtime, sz, data)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    str(Path(arcname).as_posix()),
                    unix_mode,
                    mtime,
                    len(data),
                    compressed_data
                )
            )

    def __enter__(self):
        return self

    def __exit__(self, *details):
        self.close()
