import pandas as pd
import gzip
from config import predefined_ranks
from itertools import zip_longest

# function to extract ranks into separate columns
def extract_ranks(structure, values):
    # split structure and values into lists
    rank_list = [r.strip() for r in structure.split("|")]
    value_list = [v.strip() for v in values.split("|")]
    rank_dict = dict(zip_longest(rank_list, value_list, fillvalue=""))
    return {rank: rank_dict.get(rank, "") for rank in predefined_ranks}


# function to split taxon paths and rank names
def safe_extract_ranks(row):
    if pd.notna(row["TaxonPathName"]) and pd.notna(row["TaxonRankName"]):
        return extract_ranks(row["TaxonRankName"], row["TaxonPathName"])
    else:
        return pd.Series({rank: pd.NA for rank in predefined_ranks})


# clean files with abnormal ending of quotes - only use if absolutely necessary, not used anywhere in the main code so far
def clean_quotes_from_file(input_path, output_path):
    with open(input_path, 'r', encoding='iso-8859-1') as infile, open(output_path, 'w', encoding='utf-8') as outfile:
        for line in infile:
            # Count quotes in the line
            quote_count = line.count('"')
            if quote_count % 2 != 0:
                # Remove all double quotes if unbalanced
                line = line.replace('"', '')
            outfile.write(line)


# function to detect clear mappings with names and ids. Also sets 'Match_Status' value
def initialTaxMatchDfZ(verbatim_globi_df, id_map, id_map_WD):
    # create stripped versions of the relevant columns (TaxonID and TaxonName)
    taxon_id = verbatim_globi_df.iloc[:, 0].astype(str).str.strip()
    taxon_name = verbatim_globi_df.iloc[:, 1].astype(str).str.strip()
    # map TaxonID to corresponding TaxonName from id_map (returns NaN if not found)
    verbatim_globi_df["Mapped_Value"] = taxon_id.map(id_map)
    verbatim_globi_df["Mapped_ID_WD"] = taxon_id.map(id_map_WD)
    # Assign Mapped_ID (same as TaxonID if found in id_map, else None)
    verbatim_globi_df["Mapped_ID"] = taxon_id.where(verbatim_globi_df["Mapped_Value"].notna())
    # compare TaxonName with mapped value (case insensitive)
    verbatim_globi_df["Match_Status"] = (
        verbatim_globi_df["Mapped_Value"].str.lower() == taxon_name.str.lower()
    ).map({True: "NAME-MATCH-YES", False: "NAME-MATCH-NO"})
    # handle cases where TaxonID is missing in id_map
    verbatim_globi_df["Match_Status"] = verbatim_globi_df["Match_Status"].where(
        verbatim_globi_df["Mapped_Value"].notna(), "ID-NOT-FOUND"
    )
    # handle cases where TaxonID is NA or empty
    verbatim_globi_df["Match_Status"] = verbatim_globi_df["Match_Status"].where(
        taxon_id.notna() & (taxon_id != ""), "ID-NOT-PRESENT"
    )
    return verbatim_globi_df

