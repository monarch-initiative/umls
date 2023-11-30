"""UMLS ingest code."""

import logging
from datetime import date
from pathlib import Path
from typing import Optional, Tuple
from zipfile import ZipFile

import curies
import pandas as pd
import pyobo
from curies import get_bioregistry_converter, get_obo_converter
from sssom.constants import SEMAPV
from sssom.util import MappingSetDataFrame, get_converter
from sssom.writers import write_table
from umls_downloader import download_tgt_versioned

from umls_ingest.constants import (
    API_KEY,
    DATA_DIR,
    MAPPING_DATE,
    MAPPING_JUSTIFICATION,
    MAPPING_TOOL,
    MAPPINGS_DIR,
    MAPPINGS_FILE,
    MATCH_STRING,
    MRCONSO_COLUMN_HEADERS,
    MRMAP_COLUMN_NAMES,
    OBJECT_ID,
    PREDICATE_ID,
    SUBJECT_ID,
    UMLS_SSSOM_TSV,
    UMLS_URL,
)

CONVERTER = curies.chain([get_obo_converter(), get_bioregistry_converter(), get_converter()])


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


def _import_umls_via_pyobo(resource: str, names: bool) -> pd.DataFrame:
    # bioregistry_converter = get_bioregistry_converter()
    df = pyobo.get_sssom_df(resource, names=names)
    # In case of ba:na:na
    # df[OBJECT_ID] = df[OBJECT_ID].apply(lambda x: ":".join(x.split(":")[-2:]) if x.count(":") > 1 else x)
    CONVERTER.pd_standardize_curie(df, column=OBJECT_ID, passthrough=True)
    df[MAPPING_JUSTIFICATION] = SEMAPV.MappingChaining.value
    df.to_csv(UMLS_SSSOM_TSV, sep="\t", index=False)
    return df

def _make_mapping_set_from_df(df:pd.DataFrame) -> MappingSetDataFrame:
    msdf = MappingSetDataFrame(df=df, converter=CONVERTER)
    msdf.clean_prefix_map(strict=False)
    return msdf

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
        df = _import_umls_via_pyobo(resource=resource, names=names)
    else:
        df = pd.read_csv(UMLS_SSSOM_TSV, sep="\t", low_memory=False)

    if subject_prefixes:
        df_subject_subset = df[
            df[SUBJECT_ID].apply(
                lambda x: any(x.startswith((prefix.lower(), prefix.upper())) for prefix in subject_prefixes)
            )
        ]
        new_outfile = MAPPINGS_DIR.joinpath("_".join(subject_prefixes) + "." + output_file)
    if object_prefixes:
        df_object_subset = df[
            df[OBJECT_ID].apply(
                lambda x: any(str(x).startswith((prefix.lower(), prefix.upper())) for prefix in object_prefixes)
            )
        ]
        new_outfile = MAPPINGS_DIR.joinpath("_".join(object_prefixes) + "." + output_file)
    if subject_prefixes and object_prefixes:
        common_rows = pd.merge(df_subject_subset, df_object_subset, how="inner")
        new_outfile = MAPPINGS_DIR.joinpath("_".join(subject_prefixes + object_prefixes) + "." + output_file)

    if common_rows is not None:
        common_rows_msdf = _make_mapping_set_from_df(common_rows)
        with open(new_outfile, "w") as f:
            write_table(common_rows_msdf, f)
    else:
        if subject_prefixes:
            msdf_subject_subset = _make_mapping_set_from_df(df_subject_subset)
            with open(new_outfile, "w") as f:
                write_table(msdf_subject_subset, f)
        elif object_prefixes:
            msdf_object_subset = _make_mapping_set_from_df(df_object_subset)
            with open(new_outfile, "w") as f:
                write_table(msdf_object_subset, f)
        else:
            logging.warning(f"Exported full mappings file at {output_file}")


def x_mappings(object_prefixes: Tuple[str], names: bool = False):
    """
    Map all ontologies with UMLS as the common link.

    :param object_prefixes: Prefixes of ontologies of interest.
    :param names: Get names from UMLS True/False, defaults to False
    """
    if not UMLS_SSSOM_TSV.exists():
        df = _import_umls_via_pyobo(resource="umls", names=names)
    else:
        df = pd.read_csv(UMLS_SSSOM_TSV, sep="\t", low_memory=False)

    # For each prefix in object_prefixes create dynamic variables - prefix_df
    prefix_dfs = {}
    df.fillna("", inplace=True)
    for prefix in object_prefixes:
        prefix_dfs[prefix] = df[df[OBJECT_ID].str.startswith((prefix.lower(), prefix.upper()))]

    # Perform inner join of all dynamic variables with the key subject_id
    result_df = None
    for prefix_df in prefix_dfs.values():
        if result_df is None or result_df.empty:
            result_df = prefix_df
        else:
            result_df = pd.merge(result_df, prefix_df, on=SUBJECT_ID, how="inner", suffixes=('_x', '_y'))

    # Rename columns
    # Define the old and new column names
    column_mapping = {
        SUBJECT_ID: MATCH_STRING,
        "object_id_x": SUBJECT_ID,
        "object_id_y": OBJECT_ID,
        "predicate_id_x": PREDICATE_ID,
        "mapping_justification_x": MAPPING_JUSTIFICATION,
    }

    # Only keep the mappings for columns that exist in the DataFrame
    valid_column_mapping = {old: new for old, new in column_mapping.items() if old in result_df.columns}

    # Rename the columns
    result_df = result_df.rename(columns=valid_column_mapping)

    # Add new columns
    result_df[MAPPING_TOOL] = "umls_ingest"
    result_df[MAPPING_DATE] = date.today()

    # Reorder columns
    result_df = result_df[
        [
            SUBJECT_ID,
            OBJECT_ID,
            PREDICATE_ID,
            MAPPING_JUSTIFICATION,
            MATCH_STRING,
            MAPPING_TOOL,
            MAPPING_DATE,
        ]
    ]

    CONVERTER.pd_standardize_curie(result_df, column=PREDICATE_ID, passthrough=True)
    result_msdf = MappingSetDataFrame(df=result_df, converter=CONVERTER)
    result_msdf.clean_prefix_map()

    new_outfile = MAPPINGS_DIR.joinpath("_".join(object_prefixes) + "." + MAPPINGS_FILE)
    with open(new_outfile, "w") as o:
        write_table(result_msdf, o)


if __name__ == "__main__":
    main()
