# Copyright (c) 2019-2020, CPB Netherlands Bureau for Economic Policy Analysis
# Copyright (c) 2019-2020, Andrey Dubovik <andrei@dubovik.eu>

"""Routines for downloading directory listings."""

# Load global packages
import re
from urllib.parse import urlparse, urljoin
from lxml import etree
import random
import threading
import sqlite3

# Load local packages
import settings
from misc import iserror, silent, parallel
from network import fetch, fetch_curl


def listing_ftp(url):
    """Download an FTP directory listing."""
    files = fetch_curl(url, ['-l']).decode().splitlines()
    prog = re.compile(r'.+\.(rdf|redif)$', flags=re.I)
    files = [f for f in files if prog.match(f)]
    return [url + f for f in files]


def listing_http(url):
    """Download an HTTP directory listing."""
    content, _ = fetch(url)
    html = etree.HTML(content)
    files = html.xpath('//a/@href')
    prog = re.compile(r'.+\.(rdf|redif)$', flags=re.I)
    files = [f for f in files if prog.match(f)]
    return [urljoin(url, f) for f in files]


@silent
def listing(url):
    """Get a list of ReDIF files for a given series."""
    scheme = urlparse(url)[0]
    if scheme == 'ftp':
        files = listing_ftp(url)
    elif scheme in ['http', 'https']:
        files = listing_http(url)
    else:
        raise RuntimeError('Unknown scheme {}'.format(scheme))
    if len(files) == 0:
        raise RuntimeError('Empty listing')
    return files


def update_listings_1(conn, lock, url):
    """Update remote listings for a single series."""
    files = listing(url)
    with lock:
        c = conn.cursor()
        if iserror(files):
            sql = 'UPDATE remotes SET status = 2, error = ? WHERE url = ?'
            c.execute(sql, (str(files), url))
        else:
            files = [(f, url) for f in files]
            sql = 'UPDATE remotes SET status = 0, error = NULL WHERE url = ?'
            c.execute(sql, (url, ))
            sql = 'REPLACE INTO listings (url, remote) VALUES (?, ?)'
            c.executemany(sql, files)
        c.close()
    return not iserror(files)


def update_listings(conn, lock, status=1):
    """Update remote listings for all series."""
    c = conn.cursor()
    c.execute('SELECT url FROM remotes WHERE status = ?', (status, ))
    urls = [r[0] for r in c.fetchall()]
    urls = random.sample(urls, k=len(urls))  # to redistribute load
    c.close()
    print('Updating remote listings...')

    def worker(u):
        return update_listings_1(conn, lock, u)

    status = parallel(worker, urls, settings.no_threads_www)
    print(f'{sum(status)} out of {len(urls)} records updated successfully')


def update():
    """Update remote listings (wrapper)."""
    conn = sqlite3.connect(settings.database, check_same_thread=False)
    lock = threading.Lock()
    try:
        update_listings(conn, lock)
    except BaseException:
        conn.rollback()
        raise
    else:
        conn.commit()
    conn.close()
