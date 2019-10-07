# Load global packages
import sqlite3
import os

# Load local packages
import settings

SQL = '''
    CREATE TABLE repec (
        file text PRIMARY KEY,
        type text,
        ftpdate text,
        status integer DEFAULT 1,
        error text
    );
    CREATE TABLE series (
        file text,
        handle text,
        type text,
        url text,
        status integer DEFAULT 1,
        error text,
        PRIMARY KEY (file, handle)
    );
    CREATE TABLE remotes (
        url text PRIMARY KEY,
        status integer DEFAULT 1,
        error text
    );
    CREATE TABLE listings (
        url text PRIMARY KEY,
        remote text,
        status integer DEFAULT 1,
        error text
    );
    CREATE TABLE papers (
        pid integer PRIMARY KEY,
        handle text UNIQUE,
        url text,
        template text,
        title text,
        abstract text,
        journal text,
        year integer,
        volume integer,
        issue integer,
        pages text,
        redif blob
    );
    CREATE TABLE authors (
        pid integer REFERENCES papers ON DELETE CASCADE,
        name text
    );
    CREATE TABLE meta (
        parameter text PRIMARY KEY,
        value text
    );
    INSERT INTO meta VALUES
        ("version", 1);
'''

def prepare(path):
    '''Prepare a new SQLite database'''
    if os.path.exists(path):
        raise RuntimeError('Database already exists')
    conn = sqlite3.connect(path)
    c = conn.cursor()
    c.executescript(SQL)
    c.close()

# prepare(settings.database)
