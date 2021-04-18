# Copyright (c) 2019-2020, CPB Netherlands Bureau for Economic Policy Analysis
# Copyright (c) 2019-2020, Andrey Dubovik <andrei@dubovik.eu>

"""Configuration parameters."""

# Load libraries
import os

# Default command line arguments
database = './repec.db'
timeout = 300  # seconds
batch_size = 10000  # records in each commit
no_threads_repec = 32
no_threads_www = 128
verbosity = 3

# Additional configuration
repec_ftp = 'ftp://all.repec.org/RePEc/all/'
jel = 'https://www.aeaweb.org/econlit/classificationTree.xml'
curl = 'curl' if os.name == 'nt' else '/usr/bin/curl'

# Masquerading as Chrome, otherwise some sites refuse connection
user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
user_agent += ' AppleWebKit/537.36 (KHTML, like Gecko)'
user_agent += ' Chrome/74.0.3729.157 Safari/537.36'
