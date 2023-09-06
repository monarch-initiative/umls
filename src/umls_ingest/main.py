"""UMLS ingest code."""

import logging
from datetime import date
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
    MAPPINGS_FILE,
    MRCONSO_COLUMN_HEADERS,
    MRMAP_COLUMN_NAMES,
    OBJECT_ID,
    SUBJECT_ID,
    UMLS_SSSOM_TSV,
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
    output_file: str = MAPPINGS_FILE,
):
    """
    Map to other ontologies.

    Mapping diagram:
        https://www.nlm.nih.gov/research/umls/implementation_resources/query_diagrams/er9.html
    """
    if not UMLS_SSSOM_TSV.exists():
        df = pyobo.get_sssom_df(resource, names=names)
        df.to_csv(UMLS_SSSOM_TSV, sep="\t", index=False)
    else:
        df = pd.read_csv(UMLS_SSSOM_TSV, sep="\t", low_memory=False)

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
        common_rows.to_csv(new_outfile, sep="\t", index=False)
    else:
        if subject_prefixes:
            df_subject_subset.to_csv(new_outfile, sep="\t", index=False)
        elif object_prefixes:
            df_object_subset.to_csv(new_outfile, sep="\t", index=False)
        else:
            logging.warning(f"Exported full mappings file at {output_file}")


def x_mappings(object_prefixes: Tuple[str], names: bool = False):
    """
    Map all ontologies with UMLS as the common link.

    :param object_prefixes: Prefixes of ontologies of interest.
    :param names: Get names from UMLS True/False, defaults to False
    """
    if not UMLS_SSSOM_TSV.exists():
        df = pyobo.get_sssom_df("umls", names=names)
        df.to_csv(UMLS_SSSOM_TSV, sep="\t", index=False)
    else:
        df = pd.read_csv(UMLS_SSSOM_TSV, sep="\t", low_memory=False)

    # For each prefix in object_prefixes create dynamic variables - prefix_df
    prefix_dfs = {}
    for prefix in object_prefixes:
        prefix_dfs[prefix] = df[df["object_id"].str.startswith(prefix)]

    # Perform inner join of all dynamic variables with the key subject_id
    result_df = None
    for prefix_df in prefix_dfs.values():
        if result_df is None:
            result_df = prefix_df
        else:
            result_df = pd.merge(result_df, prefix_df, on="subject_id", how="inner")

    # Rename columns
    result_df = result_df.rename(
        columns={
            "subject_id": "match_string",
            "object_id_x": "subject_id",
            "object_id_y": "object_id",
            "predicate_id_x": "predicate_id",
            "mapping_justification_x": "mapping_justification",
        }
    )

    # Add new columns
    result_df["mapping_tool"] = "umls_ingest"
    result_df["mapping_date"] = date.today()

    # Reorder columns
    result_df = result_df[
        [
            "subject_id",
            "object_id",
            "predicate_id",
            "mapping_justification",
            "match_string",
            "mapping_tool",
            "mapping_date",
        ]
    ]

    new_outfile = MAPPINGS_DIR.joinpath("_".join(object_prefixes) + "." + MAPPINGS_FILE)
    result_df.to_csv(new_outfile, sep="\t", index=False)


if __name__ == "__main__":
    main()
