# Load packages
import re
import subprocess
from concurrent.futures import ThreadPoolExecutor

# Configure global parameters
REPEC_FTP = 'ftp://all.repec.org/RePEc/all/'

# ReDIF functions

def redif_decode(rdf):
    '''Decode ReDIF document'''
    def decode(encoding):
        rslt = rdf.decode(encoding)
        if rslt.lower().find('template-type') == -1:
            raise RuntimeError('Decoding Error')
        return rslt

    encodings = ['windows-1252', 'utf-8', 'utf-16', 'latin-1']
    if rdf[:3] == b'\xef\xbb\xbf':
        encodings = ['utf-8-sig'] + encodings
    for enc in encodings:
        try:
            return decode(enc)
        except:
            continue
    raise RuntimeError('Decoding Error')

def split(lst, sel):
    '''Split a list using a selector function'''
    group = []
    groups = [group]
    for el in lst:
        if sel(el):
            group = []
            groups.append(group)
        group.append(el)
    return groups

def redif_load(rdf):
    '''Load ReDIF document'''
    # Repair line endings
    rdf = re.sub('\r(?!\n)', '\r\n', rdf, flags = re.M)

    # Drop comments
    rdf = re.sub('^#.*\n?', '', rdf, flags = re.M)

    # Split fields
    rdf = re.split('(^[a-zA-Z0-9\-#]+:\s*)', rdf, flags = re.M)[1:]
    rdf = [l.strip() for l in rdf]
    rdf = [(rdf[i].rstrip(':').lower(), rdf[i+1]) for i in range(0, len(rdf), 2)]

    # Split templates
    rdf = split(rdf, lambda x: x[0] == 'template-type')[1:]
    return rdf

# RePEc FTP is broken; using curl as a workaround

def ftp_ls(url):
    '''Get file listing'''
    rslt = subprocess.run(['curl', '-ls', url], stdout = subprocess.PIPE)
    return rslt.stdout.decode().splitlines()

def ftp_get(url):
    '''Get ascii file (ReDIF encoding conventions)'''
    rslt = subprocess.run(['curl', '-s', url], stdout = subprocess.PIPE)
    return rslt.stdout

# RePEc functions
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
        rdf = redif_load(redif_decode(ftp_get(REPEC_FTP + archive)))
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
        rdf = redif_load(redif_decode(ftp_get(REPEC_FTP + series)))
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
    files = ftp_ls(REPEC_FTP)
    archives = archive_all(files)
    series = series_all(files, archives)
    return series
