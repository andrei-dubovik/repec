# Load global packages
import requests
from requests.packages import urllib3
from urllib.parse import urlparse
import subprocess
import re
import sqlite3
import json
import zlib
import threading
import random
import math

# Load local packages
import settings
import redif
from misc import iserror, silent, parallel, collect

def load_ftp(url):
    cmd = ['curl', '-sm {}'.format(settings.timeout), url]
    rslt = subprocess.run(cmd, stdout = subprocess.PIPE)
    if rslt.returncode != 0:
        raise RuntimeError('CURL Error {}'.format(rslt.returncode))
    return redif.decode(rslt.stdout)

def load_http(url):
    try:
        headers = {'User-Agent': settings.user_agent}
        response = requests.get(url, timeout = settings.timeout, headers = headers)
    except requests.exceptions.ConnectionError as err:
        if type(err.args[0]) == urllib3.exceptions.MaxRetryError:
            err.args = ('Max retries exceeded', )
            raise
        else:
            raise
    if response.status_code != 200:
        raise RuntimeError('HTTP Error {}'.format(response.status_code))
    return redif.decode(response.content, hint = [response.encoding])

def ttype(record):
    '''Get template type'''
    tt = next(v for k, v in record if k == 'template-type')
    return re.match('redif-(\S*)', tt.lower()).group(1)

@silent
def load(url):
    '''Download ReDIF papers'''
    scheme = urlparse(url)[0]
    if scheme == 'ftp':
        papers = load_ftp(url)
    elif scheme in ['http', 'https']:
        papers = load_http(url)
    else:
        raise RuntimeError('Unknown scheme {}'.format(scheme))
    papers = redif.load(papers)
    for p in papers:
        fields = set(k for k, v in p)
        for f in ['handle', 'template-type']:
            if f not in fields:
                raise RuntimeError('{} is missing'.format(f))
    if len(papers) == 0:
        raise RuntimeError('Empty series')
    return papers

def replace_paper(c, paper, url):
    '''Update a single paper record'''
    blob = json.dumps(paper, ensure_ascii = False).encode(encoding = 'utf-8')
    paper = collect(paper)
    r = {}
    r['url'] = url
    r['handle'] = paper['handle'][0]
    r['template'] = paper['template-type'][0]
    for f in ['title', 'abstract', 'journal', 'volume', 'issue', 'pages']:
        r[f] = paper.get(f, [None])[0]
    date_fields = ['creation-date', 'revision-date', 'year']
    dates = [d[:4] for f in date_fields if f in paper for d in paper[f]]
    r['year'] = ([None] + dates)[-1]
    r['redif'] = zlib.compress(blob, level = 9)

    sql = 'REPLACE INTO papers (' + ', '.join(k for k in r.keys()) + ')'
    sql += ' VALUES (' + ', '.join(['?']*len(r)) + ')'
    c.execute(sql, list(r.values()))
    pid = c.lastrowid

    if 'author-name' in paper:
        authors = [(pid, n) for n in paper['author-name']]
        c.executemany('INSERT INTO authors (pid, name) VALUES (?, ?)', authors)

def update_papers_1(conn, lock, url):
    '''Update papers from a single ReDIF document'''
    papers = load(url)
    with lock:
        c = conn.cursor()
        if iserror(papers):
            c.execute('UPDATE listings SET status = 2, error = ? WHERE url = ?', (str(papers), url))
        else:
            c.execute('UPDATE listings SET status = 0, error = NULL WHERE url = ?', (url, ))
            for paper in papers:
                replace_paper(c, paper, url)
        c.close()
    return not iserror(papers)

def update_papers(conn, lock, status = 1):
    '''Update papers from all ReDIF documents'''
    c = conn.cursor()
    c.execute('SELECT url FROM listings WHERE status = ?', (status, ))
    urls = [r[0] for r in c.fetchall()]
    urls = random.sample(urls, k = len(urls)) # To redistribute load
    c.close()

    size = settings.batch_size
    no_batches = math.ceil(len(urls)/size)
    status = 0
    for i in range(no_batches):
        print('Downloading batch {}/{}...'.format(i+1, no_batches))
        batch = urls[i*size:(i+1)*size]
        worker = lambda u: update_papers_1(conn, lock, u)
        bs = sum(parallel(worker, batch, threads = settings.no_threads_www))
        status += bs
        conn.commit()
        print('{} out of {} records updated successfully'.format(bs, len(batch)))

    print('All batches: {} out of {} records updated successfully'.format(status, len(urls)))

def update():
    '''Update papers from all ReDIF documents (wrapper)'''

    conn = sqlite3.connect(settings.database, check_same_thread = False)
    c = conn.cursor()
    c.execute('PRAGMA foreign_keys = ON')
    c.close()

    lock = threading.Lock()
    try:
        update_papers(conn, lock)
    except:
        conn.rollback()
    else:
        conn.commit()
    conn.close()