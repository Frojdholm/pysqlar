# pysqlar

Module for working with [SQLite Archive files](https://www.sqlite.org/sqlar.html)
with an API mimicking the zipfile module.

The module requires zlib and sqlite3 support, but has no external dependencies.

# Installation

`$ pip install pysqlar`

# Usage

To open an archive for reading

```python
from pysqlar import SQLiteArchive

ar = SQLiteArchive("filename.sqlar")

print(ar.read("file.txt"))

ar.close()
```

or as a context manager

```python
from pysqlar import SQLiteArchive

with SQLiteArchive("filename.sqlar") as ar:
    print(ar.read("file.txt"))
```

To be able to write into the archive you need to specify the mode as "rwc"
(Read-Write-Create)

```python
from pysqlar import SQLiteArchive

with SQLiteArchive("filename.sqlar", mode="rwc") as ar:
    ar.writestr("file.txt", "Hello World!")
    print(ar.read("file.txt"))
```

Note that this will create a new archive if the file does not exist.
