# Copyright (c) 2019-2020, CPB Netherlands Bureau for Economic Policy Analysis
# Copyright (c) 2019-2020, Andrey Dubovik <andrei@dubovik.eu>

"""Database intialization routines."""

# Load global packages
import sqlite3
import os
import requests
from lxml import etree
from html import unescape

# Load local packages
import settings

# Define constants
DBVERSION = '7'

SQL = f"""
    CREATE TABLE repec (
        file text PRIMARY KEY,
        type text,
        ftpdate text,
        status integer DEFAULT 1,
        error text,
        replaced_at text DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE series (
        file text,
        handle text,
        type text,
        url text,
        status integer DEFAULT 1,
        error text,
        replaced_at text DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (file, handle)
    );
    CREATE TABLE remotes (
        url text PRIMARY KEY,
        status integer DEFAULT 1,
        error text,
        replaced_at text DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE listings (
        url text PRIMARY KEY,
        remote text,
        status integer DEFAULT 1,
        error text,
        replaced_at text DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE papers (
        pid integer PRIMARY KEY,
        handle text UNIQUE,
        url text,
        template text,
        language text,
        title text,
        abstract text,
        journal text,
        year integer,
        volume integer,
        issue integer,
        pages text,
        redif blob,
        replaced_at text DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE authors (
        pid integer REFERENCES papers ON DELETE CASCADE,
        name text,
        email text
    );
    CREATE INDEX authors_pid ON authors (pid);
    CREATE TABLE jel (
        code text PRIMARY KEY,
        parent text,
        description text
    );
    CREATE TABLE papers_jel (
        pid integer REFERENCES papers ON DELETE CASCADE,
        code text REFERENCES jel
    );
    CREATE INDEX papers_jel_pid ON papers_jel (pid);
    CREATE INDEX papers_jel_code ON papers_jel (code);
    CREATE TABLE meta (
        parameter text PRIMARY KEY,
        value text
    );
    INSERT INTO meta VALUES
        ('version', {DBVERSION});
"""


def jcode(item):
    """Get JEL code."""
    e = item.xpath('code/text()')
    return e[0] if e else None


def jdesc(item):
    """Get JEL description."""
    return unescape(item.xpath('description/text()')[0])


def import_level(c, element):
    """Recursively import JEL hierarchy."""
    sql = 'INSERT INTO jel (code, parent, description) VALUES (?, ?, ?)'
    parent = jcode(element)
    items = element.xpath('classification')
    rows = [(jcode(i), parent, jdesc(i)) for i in items]
    c.executemany(sql, rows)
    for item in items:
        import_level(c, item)


def populate_jel(conn):
    """Download and save official JEL classification."""
    page = requests.get(settings.jel)
    xml = etree.fromstring(page.content)
    with conn:
        c = conn.cursor()
        import_level(c, xml)
        c.close()


def prepare(path):
    """Prepare a new SQLite database."""
    if os.path.exists(path):
        raise RuntimeError('Database already exists')
    conn = sqlite3.connect(path)
    conn.executescript(SQL)
    populate_jel(conn)


def check_version():
    """Verify database version."""
    sql = "SELECT value FROM meta WHERE parameter = 'version'"
    conn = sqlite3.connect(settings.database)
    version, = conn.execute(sql).fetchone()
    if version != DBVERSION:
        raise RuntimeError('Incompatible database version')
