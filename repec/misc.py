# Load global packages
from concurrent.futures import ThreadPoolExecutor

def iserror(err):
    '''Check if err is an exception'''
    return isinstance(err, Exception)

def silent(func):
    '''A wrapper that suppresses errors'''
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as err:
            return err
    return wrapper

def meter(it, it_len):
    '''Basic progress meter'''
    for i, el in enumerate(it):
        print('[{}/{}]'.format(i + 1, it_len), end = '\r')
        yield el

def parallel(func, lst, threads = 32):
    '''Apply a function to a list in parallel'''
    with ThreadPoolExecutor(max_workers = threads) as executor:
        return list(meter(executor.map(func, lst), len(lst)))
