# Load global packages
import re
import subprocess
from urllib.parse import urlparse, urljoin
import requests
from requests.packages import urllib3
from lxml import etree
import random
from threading import Lock

# Load local packages
import settings
from misc import iserror, silent, parallel

def listing_ftp(url):
    cmd = ['curl', '-lsm {}'.format(settings.timeout), url]
    rslt = subprocess.run(cmd, stdout = subprocess.PIPE)
    if rslt.returncode != 0:
        raise RuntimeError('CURL Error {}'.format(rslt.returncode))
    files = rslt.stdout.decode().splitlines()
    prog = re.compile('.+\.(rdf|redif)$', flags = re.I)
    files = [f for f in files if prog.match(f)]
    return [url + f for f in files]

def listing_http(url):
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
    html = etree.HTML(response.content)
    files = html.xpath('//a/@href')
    prog = re.compile('.+\.(rdf|redif)$', flags = re.I)
    files = [f for f in files if prog.match(f)]
    return [urljoin(url, f) for f in files]

@silent
def listing(url):
    '''Get a list of ReDIF files for a given series'''
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
    files = listing(url)
    with lock:
        c = conn.cursor()
        if iserror(files):
            c.execute('UPDATE remotes SET status = 2, error = ? WHERE url = ?', (str(files), url))
        else:
            files = [(f, url) for f in files]
            c.execute('UPDATE remotes SET status = 0, error = NULL WHERE url = ?', (url, ))
            c.executemany('REPLACE INTO listings (url, remote) VALUES (?, ?)', files)
        c.close()
    return not iserror(files)

def update_listings(conn, lock, status = 1):
    '''Update remote listings'''
    c = conn.cursor()
    c.execute('SELECT url FROM remotes WHERE status = ?', (status, ))
    urls = [r[0] for r in c.fetchall()]
    urls = random.sample(urls, k = len(urls)) # To redistribute load
    c.close()
    print('Updating remote listings...')
    status = parallel(lambda u: update_listings_1(conn, lock, u), urls)
    print('{} out of {} records updated successfully'.format(sum(status), len(urls)))

def update():
    '''Update remote listings (wrapper)'''

    conn = sqlite3.connect(settings.database, check_same_thread = False)
    lock = threading.Lock()
    try:
        update_listings(conn, lock)
    except:
        conn.rollback()
    else:
        conn.commit()
    conn.close()
