"""Routines for downloading data from the RePEc website."""

# Load global packages
import re
import subprocess
import threading
import sqlite3
from datetime import datetime

# Load local packages
import settings
import redif
from misc import iserror, silent, parallel, collect

# RePEc FTP is broken; using curl as a workaround


def ftp_datetime(month, day, tory):
    """Decode ftp modification datetime."""
    if tory.find(':') != -1:
        time, year = tory, str(datetime.now().year)
    else:
        time, year = '00:00', tory
    dt = ' '.join((year, month, day, time))
    return datetime.strptime(dt, '%Y %b %d %H:%M')


def ftp_ls(url):
    """Get file listing (with modification dates)."""
    rslt = subprocess.run([settings.curl, '-s', url], stdout=subprocess.PIPE)
    files = rslt.stdout.decode().splitlines()
    prog = re.compile(
        r'\S+\s+\S+\s+\S+\s+\S+\s+\S+\s+(\S+)\s+(\S+)\s+(\S+)\s+(.+)'
    )
    files = [prog.match(f).groups() for f in files]
    return [(f[3], ftp_datetime(*f[:3])) for f in files]


def ftp_get(url):
    """Get ascii file (ReDIF encoding conventions)."""
    rslt = subprocess.run([settings.curl, '-s', url], stdout=subprocess.PIPE)
    return rslt.stdout


def update_repec(conn):
    """Update the list of available archives."""
    files = ftp_ls(settings.repec_ftp)
    prog = re.compile(r'(...(arch|seri)\.(rdf|redif))$', flags=re.I)
    files = [(prog.match(f), d) for f, d in files]
    files = [(*m.groups(), d) for m, d in files if m]
    files = [(n, t.lower(), d.isoformat(sep=' ')) for n, t, e, d in files]
    c = conn.cursor()
    sql = 'REPLACE INTO repec (file, type, ftpdate) VALUES (?, ?, ?)'
    c.executemany(sql, files)
    c.close()


@silent
def load(file, cat):
    """Load a series or an archive file."""
    rdf = redif.load(redif.decode(ftp_get(settings.repec_ftp + file)))

    def key(r):
        r = dict(r)
        url = r['url'].rstrip('/') + '/' if 'url' in r else None
        return (file, cat, r['handle'], url)
    return [key(r) for r in rdf]


def update_series_1(conn, lock, file, cat):
    """Update the database for a series or an archive file."""
    r = load(file, cat)
    with lock:
        c = conn.cursor()
        if iserror(r):
            sql = 'UPDATE repec SET status = 2, error = ? WHERE file = ?'
            c.execute(sql, (str(r), file))
        else:
            sql = 'UPDATE repec SET status = 0, error = NULL WHERE file = ?'
            c.execute(sql, (file, ))
            sql = (
                'REPLACE INTO series (file, type, handle, url)'
                ' VALUES (?, ?, ?, ?)'
            )
            c.executemany(sql, r)
        c.close()
    return not iserror(r)


def update_series(conn, lock, status=1):
    """Update all archive and series files in the database."""
    c = conn.cursor()
    c.execute('SELECT file, type FROM repec WHERE status = ?', (status, ))
    files = c.fetchall()
    c.close()
    print('Updating archive and series files...')

    def worker(el):
        return update_series_1(conn, lock, *el)
    status = parallel(worker, files, threads=settings.no_threads_repec)
    print(f'{sum(status)} out of {len(files)} records updated successfully')


def update_remotes(conn, status=1):
    """Update the list of remotes in the database."""
    c = conn.cursor()

    # Archives
    sql = 'SELECT handle, url FROM series, repec USING (file)'
    sql += ' WHERE series.type = "arch" ORDER BY ftpdate DESC'
    c.execute(sql)
    archives = [(h.lower(), u) for h, u in c.fetchall()]
    archives = {k: v[0] for k, v in collect(archives).items()}
    c.execute('UPDATE series SET status = 0 WHERE type = "arch"')

    # Series
    def lookup(s):
        a, sep, f = s[1].rpartition(':')
        a = archives.get(a.lower())
        if a:
            return (a + f + '/', 0, None, *s)
        else:
            return (None, 2, 'Archive not found', *s)

    sql = 'SELECT file, handle FROM series WHERE type = "seri" AND status = ?'
    c.execute(sql, (status, ))
    series = [lookup(s) for s in c.fetchall()]
    sql = (
        'UPDATE series SET url = ?, status = ?, error = ?'
        ' WHERE file = ? AND handle = ?'
    )
    c.executemany(sql, series)

    # Remotes
    remotes = list(set((s[0],) for s in series if s[0]))
    c.executemany('REPLACE INTO remotes (url) VALUES (?)', remotes)
    c.close()


def update():
    """Update the database on the basis of RePEc information."""
    conn = sqlite3.connect(settings.database, check_same_thread=False)
    lock = threading.Lock()
    try:
        update_repec(conn)
        update_series(conn, lock)
        update_remotes(conn)
    except BaseException:
        conn.rollback()
        raise
    else:
        conn.commit()
    conn.close()
