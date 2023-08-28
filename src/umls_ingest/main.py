"""UMLS ingest code."""

from pathlib import Path
from typing import Optional
from zipfile import ZipFile

import pyobo
from umls_downloader import download_tgt_versioned

from umls_ingest.constants import (
    API_KEY,
    DATA_DIR,
    MRCONSO_COLUMN_HEADERS,
    MRMAP_COLUMN_NAMES,
    UMLS_URL,
)


def main():
    """."""
    pass


def download(version: Optional[str]):
    """Define API."""
    path = download_tgt_versioned(
        url_fmt=UMLS_URL,
        version=version,
        api_key=API_KEY,
        version_key="umls",
        module_key="umls",
    )

    with open(DATA_DIR / "mrconso.tsv", "w") as f:
        f.write("\t".join(MRCONSO_COLUMN_HEADERS) + "\n")

        mrconso = (
            line.decode("utf8").strip().strip("|").split("|")
            for lines in _open_file_from_zip(path, "MRCONSO.RRF")
            for line in lines
        )
        eng_rows = (row for row in mrconso if "ENG" in row)

        for row in eng_rows:
            f.write("\t".join(row) + "\n")

    with open(DATA_DIR / "mrmap.tsv", "w") as f:
        f.write("\t".join(MRMAP_COLUMN_NAMES) + "\n")

        mrmap = (
            line.decode("utf8").strip().strip("|").split("|")
            for lines in _open_file_from_zip(path, "MRMAP.RRF")
            for line in lines
        )
        for row in mrmap:
            f.write("\t".join(row) + "\n")


def _open_file_from_zip(path: Path, fn: str):
    """Get file information from within a zipfile."""
    with ZipFile(path) as zip_file:
        file_of_interest = [x for x in zip_file.namelist() if fn in x][0]
        with zip_file.open(file_of_interest, mode="r") as file:
            yield file


def mappings(resource: str, output_file: str, names: bool):
    """
    Map to other ontologies.

    Mapping diagram:
        https://www.nlm.nih.gov/research/umls/implementation_resources/query_diagrams/er9.html
    """
    df = pyobo.get_sssom_df(resource, names=names)
    df.to_csv(output_file, sep="\t", index=False)


if __name__ == "__main__":
    main()
