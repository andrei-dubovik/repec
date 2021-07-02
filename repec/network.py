# Copyright (c) 2021, Andrey Dubovik <andrei@dubovik.eu>

"""Convenience wrappers around network functions."""

# Load global packages
import requests
from requests.packages import urllib3
import subprocess

# Load local packages
import settings


def fetch(url):
    """Fetch an URL using requests."""
    try:
        headers = {'User-Agent': settings.user_agent}
        response = requests.get(url, timeout=settings.timeout, headers=headers)
    except requests.exceptions.ConnectionError as err:
        if type(err.args[0]) == urllib3.exceptions.MaxRetryError:
            err.args = ('Max retries exceeded', )  # Simplify error message
            raise
        else:
            raise
    if response.status_code != 200:
        raise RuntimeError('HTTP Error {}'.format(response.status_code))
    return response.content, response.encoding


def fetch_curl(url, options=''):
    """Fetch an URL using curl."""
    cmd = ['curl', '-sm {}'.format(settings.timeout), options, url]
    rslt = subprocess.run(cmd, stdout=subprocess.PIPE)
    if rslt.returncode != 0:
        raise RuntimeError('CURL Error {}'.format(rslt.returncode))
    return rslt.stdout
