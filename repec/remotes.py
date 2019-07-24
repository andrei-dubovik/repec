# Load packages
import re
import subprocess
from urllib.parse import urlparse, urljoin
import requests
from lxml import etree
import random
from concurrent.futures import ThreadPoolExecutor
from collections import Counter
import pickle

# Masquerading as Chrome, otherwise some sites refuse connection
HTTP_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.157 Safari/537.36'
}

def silent(func):
    '''A wrapper that suppresses errors'''
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as err:
            return str(err)
    return wrapper

def listing_ftp(url):
    rslt = subprocess.run(['curl', '-lsm 300', url], stdout = subprocess.PIPE)
    if rslt.returncode != 0:
        raise RuntimeError('CURL Error {}'.format(rslt.returncode))
    files = rslt.stdout.decode().splitlines()
    prog = re.compile('.+\.(rdf|redif)$', flags = re.I)
    files = [f for f in files if prog.match(f)]
    return [url + f for f in files]

def listing_http(url):
    try:
        response = requests.get(url, timeout = 300, headers = HTTP_HEADERS)
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

def meter(it, it_len):
    '''Progress meter'''
    for i, el in enumerate(it):
        print('[{}/{}]'.format(i + 1, it_len), end = '\r')
        yield el

def update_listings(urls, threads = 32):
    '''Redownload missing urls'''
    old_count = sum(1 for v in urls.values() if type(v) == str)
    keys = list(k for k, v in urls.items() if type(v) == str)
    keys = random.sample(keys, k = len(keys)) # To redistribute load
    print('Updating listings')
    with ThreadPoolExecutor(max_workers = threads) as executor:
        listings = list(meter(executor.map(listing, keys), len(keys)))
    for k, v in zip(keys, listings):
        urls[k] = v
    new_count = sum(1 for v in urls.values() if type(v) == str)
    print('{} updated, {} missing'.format(old_count - new_count, new_count))

def all_listings(urls):
    '''Get a list of all available ReDIF files'''
    urls = {url:'Not processed' for url in urls}
    update_listings(urls)
    #errors = Counter(v for v in urls.values() if type(v) == str)
    return [u for f in urls.values() if type(f) == list for u in f]
