"""UMLS ingest code."""

import logging
from pathlib import Path
from typing import Optional, Tuple
from zipfile import ZipFile
import pandas as pd
import pyobo
from umls_downloader import download_tgt_versioned

from umls_ingest.constants import (
    API_KEY,
    DATA_DIR,
    MAPPINGS_DIR,
    MRCONSO_COLUMN_HEADERS,
    MRMAP_COLUMN_NAMES,
    OBJECT_ID,
    SUBJECT_ID,
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


def mappings(
    resource: str,
    names: bool,
    subject_prefixes: Tuple[str],
    object_prefixes: Tuple[str],
    output_file: str,
):
    """
    Map to other ontologies.

    Mapping diagram:
        https://www.nlm.nih.gov/research/umls/implementation_resources/query_diagrams/er9.html
    """
    df = pyobo.get_sssom_df(resource, names=names)
    if not output_file:
        output_file = "sssom.tsv"
    df.to_csv(MAPPINGS_DIR / output_file, sep="\t", index=False)

    if subject_prefixes:
        df_subject_subset = df[df[SUBJECT_ID].apply(lambda x: any(x.startswith(prefix) for prefix in subject_prefixes))]
        new_outfile = MAPPINGS_DIR.joinpath("_".join(subject_prefixes) + "." + output_file)
    if object_prefixes:
        df_object_subset = df[df[OBJECT_ID].apply(lambda x: any(x.startswith(prefix) for prefix in object_prefixes))]
        new_outfile = MAPPINGS_DIR.joinpath("_".join(object_prefixes) + "." + output_file)
    if subject_prefixes and object_prefixes:
        common_rows = pd.merge(df_subject_subset, df_object_subset, how="inner")
        new_outfile = MAPPINGS_DIR.joinpath("_".join(subject_prefixes + object_prefixes) + "." + output_file)

    if common_rows is not None:
        import pdb; pdb.set_trace()
        common_rows.to_csv(new_outfile, sep="\t", index=False)
    else:
        if subject_prefixes:
            df_subject_subset.to_csv(new_outfile, sep="\t", index=False)
        elif object_prefixes:
            df_object_subset.to_csv(new_outfile, sep="\t", index=False)
        else:
            logging.warning(f"Exported full mappings file at {output_file}")

    


if __name__ == "__main__":
    main()
