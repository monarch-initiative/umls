"""Command line interface for umls-ingest."""
import logging
from typing import Tuple

import click

from umls_ingest import __version__
from umls_ingest.constants import MAPPINGS_FILE
from umls_ingest.main import download, mappings, x_mappings

__all__ = [
    "main",
]

logger = logging.getLogger(__name__)


@click.group()
@click.option("-v", "--verbose", count=True)
@click.option("-q", "--quiet")
@click.version_option(__version__)
def main(verbose: int, quiet: bool):
    """
    CLI for umls-ingest.

    :param verbose: Verbosity while running.
    :param quiet: Boolean to be quiet or verbose.
    """
    if verbose >= 2:
        logger.setLevel(level=logging.DEBUG)
    elif verbose == 1:
        logger.setLevel(level=logging.INFO)
    else:
        logger.setLevel(level=logging.WARNING)
    if quiet:
        logger.setLevel(level=logging.ERROR)


@click.option("--umls-version", help="UMLS version to download.")
@main.command("get-tsv")
def get_tsv(umls_version: str):
    """Run the umls-ingest's demo command."""
    download(version=umls_version)


@main.command("get-mappings")
@click.option("--resource", help="UMLS is the default resource.", default="umls")
@click.option("--names", help="Get labels for CURIEs.", default=False)
@click.option("--subject-prefixes", help="Prefix of subject CURIEs.", multiple=True)
@click.option("--object-prefixes", help="Prefix of object CURIEs.", multiple=True)
@click.option("--output", help="Output file name.", default=MAPPINGS_FILE)
def get_mappings(resource: str, names: bool, subject_prefixes: Tuple[str], object_prefixes: Tuple[str], output: str):
    """Run mappings."""
    mappings(
        resource=resource,
        output_file=output,
        names=names,
        subject_prefixes=subject_prefixes,
        object_prefixes=object_prefixes,
    )


@main.command("get-x-mappings")
@click.option("--object-prefixes", help="Prefix of subject CURIEs.", multiple=True)
@click.option("--names", help="Get labels for CURIEs.", default=False)
def get_x_mappings(object_prefixes: Tuple[str], names: bool):
    """Get non-umls mappings."""
    x_mappings(object_prefixes=object_prefixes, names=names)


if __name__ == "__main__":
    main()
