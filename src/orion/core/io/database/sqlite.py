# -*- coding: utf-8 -*-
"""
:mod:`orion.core.io.database.sqlite` -- Wrapper for Sqlite database
===================================================================

.. module:: database
   :platform: Unix
   :synopsis: Implement :class:`orion.core.io.database.AbstractDB` for Sqlite.

"""
import functools
import operator
import pickle

import sqlite3

from orion.core.io.database import (
    AbstractDB, DatabaseError, DuplicateKeyError)


def sqlite_exception_wrapper(method):
    """Convert sqlite3 exceptions to orion.core.io.database ones."""
    @functools.wraps(method)
    def _inner(*args, **kwargs):
        try:
            rval = method(*args, **kwargs)
        except Exception:
            raise
        return rval
    return _inner


def _mongo_binary_op(f):
    def _inner(v):
        return functools.partial(f, v)
    return _inner


_mongo_ops = {
    "$eq": _mongo_binary_op(operator.eq),
    "$lt": _mongo_binary_op(operator.lt),
    "$gt": _mongo_binary_op(operator.gt),
    "$ne": _mongo_binary_op(operator.ne),
    "$in": _mongo_binary_op(lambda a, b: a in b)
}


def _parse_mongo_statement(field, query):
    if field.startswith("$"):
        return _mongo_ops[field]
    else:
        field_parts = field.split(".")
        f = _parse_mongo_query(query)
        def _inner(o):
            for p in field_parts:
                o = o[p]
            return f(o)
        return _inner


def _parse_mongo_query(query):
    """Parse a MongoDB query into a function that evaluates on a python dict
    whether the query is a match or not.
    
    Limited operators are available.
    """
    if query is None:
        return lambda o: True
    elif isinstance(query, dict):
        functions = [
            _parse_mongo_statement(k, v) for k, v in query.items()
        ]
    elif isinstance(query, list):
        functions = [
            _parse_mongo_query(v) for v in query
        ]
    else:
        functions = [_mongo_ops["$eq"](query)]
    def _inner(o):
        return all(f(o) for f in functions)
    return _inner


class Document(dict):
    @staticmethod
    def adapter(document):
        return pickle.dumps(document, protocol=4)

    @staticmethod
    def converter(s):
        return pickle.loads(s)


# Register the type in the sqlite database
sqlite3.register_adapter(Document, Document.adapter)
sqlite3.register_converter("pickle", Document.converter)


class Sqlite(AbstractDB):
    """Implement the AbstractDB interface with an sqlite database.

    Attributes
    ----------
    host : str
        The file that contains the sqlite database or ':memory:' to create a
        temporary in memory database.
    """
    def __init__(self, host=':memory:', name=None,
                 port=None, username=None, password=None):
        """Create an Sqlite instance, see also :class:`AbstractDB`"""
        super(Sqlite, self).__init__(host, name, port, username, password)

    @sqlite_exception_wrapper
    def initiate_connection(self):
        """Open the database file unless `is_connected`."""
        if self.is_connected:
            return

        # Open the database
        self._conn = sqlite3.connect(
            self.host,
            detect_types=sqlite3.PARSE_DECLTYPES
        )

        # Make sure all the tables are available
        self._setup_db()

    def _setup_db(self):
        """Create the necessary tables and load metadata"""
        # Create the documents table
        with self._conn as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS documents"
                         "(id INTEGER PRIMARY KEY, collection TEXT, "
                         "document PICKLE)")
            conn.execute("CREATE INDEX IF NOT EXISTS data_idx ON documents"
                         "(collection)")

        # Create the indexes table
        # with self._conn as conn:
        #     conn.execute("CREATE TABLE IF NOT EXISTS indexes"
        #                  "(collection TEXT, field TEXT)")
        #     conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS"
        #                  "indexes_idx ON indexes (collection, field)")


    @property
    def is_connected(self):
        """True if a usable db connection is available"""
        if self._conn is None:
            return False
        try:
            self._conn.execute("SELECT name from sqlite_master")
            return True
        except sqlite3.Error:
            return False

    def close_connection(self):
        """Disconnect from the database."""
        self._conn.close()

    def ensure_index(self, collection_name, keys, unique=False):
        # TODO: Actually enforce uniqueness and maybe index stuff
        pass

    def _get_documents(self, conn, collection):
        return conn.execute(
            "SELECT id, document FROM documents WHERE collection=?",
            (collection,)
        )

    def _find(self, conn, collection, query):
        # TODO: Use the indexes to speed up things
        match = _parse_mongo_query(query)
        for d in self._get_documents(conn, collection):
            if match(d):
                yield d

    def _find_one(self, conn, collection, query):
        try:
            return next(self._find(conn, collection, query))
        except StopIteration:
            return None

    def _select(self, document, selection):
        return document

    def _update_document(self, conn, index, document):
        return conn.execute(
            "UPDATE documents SET document=? WHERE id=?",
            (document, index)
        )

    def _insert_many(self, conn, collection, documents):
        # TODO: Validate and update indexes
        for d in documents:
            conn.execute(
                "INSERT INTO documents (collection, document) VALUES (?, ?)",
                (collection, d)
            )

    def _delete_many(self, conn, indices):
        # TODO: Update indexes
        conn.executemany("DELETE FROM documents WHERE id=?", indices)

    @sqlite_exception_wrapper
    def write(self, collection_name, data, query=None):
        with self._conn as conn:
            # No query so insert
            if query is None:
                if not isinstance(data, (list, tuple)):
                    data = [data]
                self._insert_many(conn, collection_name, data)
            else:
                documents = list(self._find(conn, collection_name, query))
                # Nothing found so do the extremely weird things mongodb does
                # by combining the data and the query to make a new document
                if len(documents) == 0:
                    new_doc = copy.deepcopy(query)
                    new_doc.update(data)
                    self._insert_many(conn, collection_name, [new_doc])

                # We found stuff so update them
                else:
                    for d in documents:
                        d[1].update(data)
                        self._update_document(conn, d[0], d[1])

    @sqlite_exception_wrapper
    def read(self, collection_name, query=None, selection=None):
        with self._conn as conn:
            return [
                self._select(d, selection)
                for d in self._find(conn, collection_name, query)
            ]

    @sqlite_exception_wrapper
    def read_and_write(self, collection_name, query, data, selection=None):
        with self._conn as conn:
            rval = self._find_one(con, collection_name, query)
            if rval is None:
                return None
            index, doc = rval
            doc.update(data)
            self._update_document(conn, index, doc)

            return doc

    @sqlite_exception_wrapper
    def count(self, collection_name, query=None):
        with self._conn as conn:
            return sum(1 for d in self._find(conn, collection_name, query))

    @sqlite_exception_wrapper
    def remove(self, collection_name, query):
        with self._conn as conn:
            self._delete_many(
                conn,
                [d[0] for d in self._find(conn, collection_name, query)]
            )
