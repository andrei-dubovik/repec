# Copyright (c) 2019-2020, CPB Netherlands Bureau for Economic Policy Analysis
# Copyright (c) 2019-2020, Andrey Dubovik <andrei@dubovik.eu>

"""Miscellaneous routines."""

# Load global packages
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
import sqlite3


def iserror(err):
    """Check if err is an exception."""
    return isinstance(err, Exception)


def silent(func):
    """Catch errors and return them regularly."""
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as err:
            return err
    return wrapper


def meter(it, it_len):
    """Print progress information."""
    for i, el in enumerate(it):
        print('[{}/{}]'.format(i + 1, it_len), end='\r')
        yield el


def parallel(func, lst, threads):
    """Apply a function to a list in parallel."""
    with ThreadPoolExecutor(max_workers=threads) as executor:
        return list(meter(executor.map(func, lst), len(lst)))


def collect(lst):
    """Collect a list of key, value pairs into a dictionary."""
    dct = {}
    for k, v in lst:
        vv = dct.setdefault(k, [])
        vv.append(v)
    return dct


def dbconnection(database):
    """Establish a database connection."""
    def decorator(func):
        @wraps(func)
        def wrapper(**kwargs):
            conn = sqlite3.connect(database)
            try:
                rslt = func(conn=conn, **kwargs)
                conn.commit()
            except BaseException:
                conn.rollback()
                raise
            finally:
                conn.close()
            return rslt
        return wrapper
    return decorator
