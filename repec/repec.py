# Load global packages
import re
import subprocess
from concurrent.futures import ThreadPoolExecutor

# Load local packages
import settings
import redif

# RePEc FTP is broken; using curl as a workaround

def ftp_ls(url):
    '''Get file listing'''
    rslt = subprocess.run(['curl', '-ls', url], stdout = subprocess.PIPE)
    return rslt.stdout.decode().splitlines()

def ftp_get(url):
    '''Get ascii file (ReDIF encoding conventions)'''
    rslt = subprocess.run(['curl', '-s', url], stdout = subprocess.PIPE)
    return rslt.stdout

def meter(it, it_len):
    '''Progress meter'''
    for i, el in enumerate(it):
        print('[{}/{}]'.format(i + 1, it_len), end = '\r')
        yield el

def collect(lst):
    '''Collect a list of key, value pairs into a dictionary'''
    dct = {}
    for k, v in lst:
        vv = dct.setdefault(k.lower(), [])
        vv.append(v)
    return dct

def print_errors(inlist, outlist):
    '''Drop and report empty results'''
    newlst = []
    for inel, outel in zip(inlist, outlist):
       if len(outel) > 0:
           newlst.append(outel)
       else:
           print('Skipping {}'.format(inel))
    return newlst

def archive_url(archive):
    '''Get a single archive url'''
    try:
        rdf = redif.load(redif.decode(ftp_get(settings.repec_ftp + archive)))
        rdf = dict(rdf[0])
        return (rdf['handle'], rdf['url'])
    except:
        return ()

def archive_all(files, threads = 32):
    '''Get a list of all archive urls'''
    prog = re.compile('...arch\.(rdf|redif)$', flags = re.I)
    files = [f for f in files if prog.match(f)]
    print('Downloading and processing archive files...')
    with ThreadPoolExecutor(max_workers = threads) as executor:
        archives = list(meter(executor.map(archive_url, files), len(files)))
    archives = print_errors(files, archives)
    archives = collect(archives)
    # Prefer https over http over ftp
    archives = {k:sorted(v)[-1] for k,v in archives.items()}
    return archives

def series_url(series, archives):
    '''Get series urls for a given archive'''
    def url(series):
        a, sep, s = series.rpartition(':')
        return archives[a.lower()].rstrip('/') + '/' + s + '/'
    try:
        rdf = redif.load(redif.decode(ftp_get(settings.repec_ftp + series)))
        links = list(set(dict(s)['handle'] for s in rdf))
        return [url(s) for s in links]
    except:
        return []

def series_all(files, archives, threads = 32):
    '''Get a list of all series urls'''
    def su_wrapper(series):
        return series_url(series, archives)
    prog = re.compile('...seri\.(rdf|redif)$', flags = re.I)
    files = [f for f in files if prog.match(f)]
    print('Downloading and processing series files...')
    with ThreadPoolExecutor(max_workers = threads) as executor:
        series = list(meter(executor.map(su_wrapper, files), len(files)))
    series = print_errors(files, series)
    series = [el for lst in series for el in lst]
    return series

def remotes():
    '''Get a list of all remote directories with ReDIF documents'''
    files = ftp_ls(settings.repec_ftp)
    archives = archive_all(files)
    series = series_all(files, archives)
    return series
