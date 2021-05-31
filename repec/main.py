# Copyright (c) 2019-2020, CPB Netherlands Bureau for Economic Policy Analysis
# Copyright (c) 2019-2020, Andrey Dubovik <andrei@dubovik.eu>

"""Command line interface."""

# Load standard packages
import argparse

# Load local packages
import settings
import database
import repec
import remotes
import papers


def init(args):
    """Initialize the database."""
    settings.database = args.database
    database.prepare(settings.database)


def update(args):
    """Run full database update."""
    settings.database = args.database
    settings.timeout = args.timeout
    settings.batch_size = args.batchsize
    settings.no_threads_repec = args.threads_repec
    settings.no_threads_www = args.threads_www
    settings.verbosity = max(settings.verbosity - args.quiet, 0)

    if not args.repec and not args.listings and not args.papers:
        args.repec = args.listings = args.papers = True

    database.check_version()  # Abort on incompatible versions

    if args.repec:
        repec.update()
    if args.listings:
        remotes.update()
    if args.papers:
        papers.update()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='RePEc Database Manager')
    commands = parser.add_subparsers(title='Subcommands', required=True)

    # Initialize subcommand

    p_init = commands.add_parser(
        'init',
        help='Initialize the database',
        description='Initialize the database, download JEL codes',
    )
    p_init.set_defaults(func=init)
    p_init.add_argument(
        '--database',
        default=settings.database,
        help=f'SQLite database location (default: {settings.database})',
    )

    # Update subcommand

    p_update = commands.add_parser(
        'update',
        help='Run full database update',
        description=(
            'Run database update. If none of --repec, --listings, or --papers'
            ' is given, full update is performed. Otherwise, the selected'
            ' parts are updated.'
        ),
    )
    p_update.set_defaults(func=update)
    p_update.add_argument(
        '--database',
        default=settings.database,
        help=f'SQLite database location (default: {settings.database})',
    )
    p_update.add_argument(
        '--repec',
        action='store_true',
        help='Download RePEc website data',
    )
    p_update.add_argument(
        '--listings',
        action='store_true',
        help='Download website listings',
    )
    p_update.add_argument(
        '--papers',
        action='store_true',
        help='Download papers',
    )
    p_update.add_argument(
        '--timeout',
        type=int,
        default=settings.timeout,
        help=(
            'Timeout for individual requests, sec'
            f' (default: {settings.timeout})'
        ),
    )
    p_update.add_argument(
        '--batchsize',
        type=int,
        default=settings.batch_size,
        metavar='SIZE',
        help=(
            'Number of papers to commit in a single transaction'
            f' (default: {settings.batch_size:,})'
        ),
    )
    p_update.add_argument(
        '--threads-repec',
        type=int,
        default=settings.no_threads_repec,
        metavar='N',
        help=(
            'Number of threads when downloading from RePEc website'
            f' (default: {settings.no_threads_repec})'
        ),
    )
    p_update.add_argument(
        '--threads-www',
        type=int,
        default=settings.no_threads_repec,
        metavar='N',
        help=(
            'General number of threads used for downloading'
            f' (default: {settings.no_threads_www})'
        ),
    )
    p_update.add_argument(
        '-q', '--quiet',
        action='count',
        default=0,
        help='Decrease verbosity level (can be given multiple times)',
    )

    # Process command line arguments and dispatch on subcommand
    args = parser.parse_args()
    args.func(args)
