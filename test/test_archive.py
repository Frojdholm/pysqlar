import unittest
from unittest.mock import patch, mock_open, call

import binascii
import sqlite3
from collections import namedtuple
from datetime import datetime, timezone
from pathlib import Path

from pysqlar import archive


class ArchiveTestCase(unittest.TestCase):

    def test_memory_archive(self):
        self.assertTrue(
            archive.is_sqlar(":memory:"),
            "in-memory archive not created with sqlar table"
        )


SQLITE_ARCHIVE_RETURN = ('CREATE TABLE sqlar(\n                    name TEXT PRIMARY KEY,\n                    mode INT,\n                    mtime INT,\n                    sz INT,\n                    data BLOB\n                )',)
SQLITE_ARCHIVE_INCORRECT_RETURN = ('CREATE TABLE sqlar(\n                    name TEXT PRIMARY KEY\n                )',)


class ArchiveMockedSQLiteTestCase(unittest.TestCase):

    def setUp(self):
        sqlpatcher = patch("pysqlar.archive.sqlite3")
        self.mocksql = sqlpatcher.start()
        self.mocksql.OperationalError = sqlite3.OperationalError

        ospatcher = patch("pysqlar.archive.os")
        mockos = ospatcher.start()
        mockos.path.exists.return_value = True

        self.addCleanup(ospatcher.stop)
        self.addCleanup(sqlpatcher.stop)
    
    def test_is_sqlar_archive(self):
        self.mocksql.connect().cursor().execute().fetchone.return_value = SQLITE_ARCHIVE_RETURN
        self.assertTrue(
            archive.is_sqlar("example.sqlar"),
            "archive with correct schema not identified"
        )

    def test_is_sqlar_archive_incorrect_schema(self):
        self.mocksql.connect().cursor().execute().fetchone.return_value = SQLITE_ARCHIVE_INCORRECT_RETURN
        self.assertFalse(
            archive.is_sqlar("example.sqlar"),
            "archive with incorrect schema not identified"
        )
    
    def test_is_sqlar_none_return(self):
        self.mocksql.connect().cursor().execute().fetchone.return_value = None
        self.assertFalse(
            archive.is_sqlar("example.sqlar"),
            "archive without sqlar table not identified"
        )
    
    def test_is_sqlar_sqlite_raise(self):
        # Set the return value so that test will fail if error is not dealt with
        self.mocksql.connect().cursor().execute().fetchone.return_value = SQLITE_ARCHIVE_RETURN
        # Make sqlite3.Cursor.execute raise
        self.mocksql.connect().cursor().execute.side_effect = sqlite3.OperationalError
        self.assertFalse(
            archive.is_sqlar("example.sqlar"),
            "wrong return when sqlite3.Cursor.execute raises"
        )


class SQLiteArchiveWithDataTestCase(unittest.TestCase):

    def setUp(self):
        self.sqlar = archive.SQLiteArchive(":memory:")
        with self.sqlar._conn as conn:
            conn.execute(
                """
                INSERT INTO sqlar
                VALUES ('example/python.py',438,1578096131,22,X'7072696e74282248656c6c6f20576f726c642122290a'),
                       ('example/text.txt',438,1578096145,16,X'46616e7461737469632070726f73650a');
                """
            )

    def tearDown(self):
        self.sqlar.close()
    
    def test_close(self):
        self.sqlar.close()
        with self.assertRaises(archive.sqlite3.ProgrammingError, msg="Cannot operate on a closed database."):
            self.sqlar._conn.execute("SELECT * FROM sqlite_master;")

    def test_getinfo(self):
        res = self.sqlar.getinfo("example/python.py")
        self.assertEqual(res, ('example/python.py', 438, 1578096131, 22))

    def test_infolist(self):
        res = self.sqlar.infolist()
        self.assertSequenceEqual(
            res,
            [
                ('example/python.py', 438, 1578096131, 22),
                ('example/text.txt', 438, 1578096145, 16)
            ]
        )

    def test_namelist(self):
        res = self.sqlar.namelist()
        self.assertSequenceEqual(
            res,
            ['example/python.py', 'example/text.txt']
        )

    def test_open(self):
        with self.assertRaises(NotImplementedError):
            self.sqlar.open("filename.txt")

    def test_extract(self):
        with patch("pysqlar.archive._decompress_row") as decompress_row:

            self.sqlar.extract("example/python.py")

            decompress_row.assert_called_with(
                Path(),
                ('example/python.py', 438, 1578096131, 22, binascii.unhexlify("7072696e74282248656c6c6f20576f726c642122290a"))
            )
    
    def test_extract_with_path(self):
        with patch("pysqlar.archive._decompress_row") as decompress_row:

            self.sqlar.extract("example/python.py", "folder")

            decompress_row.assert_called_with(
                Path("folder"),
                ('example/python.py', 438, 1578096131, 22, binascii.unhexlify("7072696e74282248656c6c6f20576f726c642122290a"))
            )

    def test_extractall(self):
        with patch("pysqlar.archive._decompress_row") as decompress_row:

            self.sqlar.extractall()
            
            decompress_row.assert_has_calls([
                call(
                    Path(),
                    ('example/python.py', 438, 1578096131, 22, binascii.unhexlify("7072696e74282248656c6c6f20576f726c642122290a"))
                ),
                call(
                    Path(),
                    ('example/text.txt', 438, 1578096145, 16, binascii.unhexlify("46616e7461737469632070726f73650a"))
                )
            ])
    
    def test_extractall_with_path(self):
        with patch("pysqlar.archive._decompress_row") as decompress_row:

            self.sqlar.extractall("folder")

            decompress_row.assert_has_calls([
                call(
                    Path("folder"),
                    ('example/python.py', 438, 1578096131, 22, binascii.unhexlify("7072696e74282248656c6c6f20576f726c642122290a"))
                ),
                call(
                    Path("folder"),
                    ('example/text.txt', 438, 1578096145, 16, binascii.unhexlify("46616e7461737469632070726f73650a"))
                )
            ])

    def test_read(self):
        res = self.sqlar.read("example/python.py")
        self.assertEqual(res, b'print("Hello World!")\n')

    def test_sql(self):
        res = self.sqlar.sql(
            "SELECT datetime(mtime, 'unixepoch') FROM sqlar WHERE name = ?;",
            "example/text.txt"
        )

        self.assertEqual(
            res[0][0],
            datetime.utcfromtimestamp(1578096145).isoformat(sep=" ")
        )

    def test_testsqlar(self):
        with self.assertRaises(NotImplementedError):
            self.sqlar.testsqlar()


class TestException(Exception):
    pass


class SQLiteArchiveNoDataTestCase(unittest.TestCase):

    def test_write(self):
        filename = "test.txt"
        msg = b"Hello World!"
        with patch("pysqlar.archive.open", mock_open(read_data=msg)) as mockopen, patch("pysqlar.archive.Path") as mockpath:
            mockpath().is_file.return_value = True
            mockpath().is_symlink.return_value = False
            mockpath().is_dir.return_value = False
            mockpath().as_posix.return_value = filename
            
            statinfo_mock = namedtuple(
                "statinfo_mock",
                ["st_size", "st_mode", "st_mtime_ns"]
            )
            mockpath().stat.return_value = statinfo_mock(len(msg), 0o777, 0)

            with archive.SQLiteArchive(":memory:") as ar:
                ar.write(filename)
                self.assertEqual(
                    ar.read(filename),
                    b"Hello World!"
                )

    def test_writestr(self):
        with archive.SQLiteArchive(":memory:") as ar:
            ar.writestr("test.txt", "Hello World!")
            self.assertEqual(
                ar.read("test.txt"),
                b"Hello World!"
            )

    def test_context_manager(self):
        try:
            with archive.SQLiteArchive(":memory:") as ar:
                raise TestException()
        except TestException:
            pass
        # Make sure that the context manager closed it
        with self.assertRaises(archive.sqlite3.ProgrammingError, msg="Cannot operate on a closed database."):
            ar._conn.execute("SELECT * FROM sqlite_master;")
