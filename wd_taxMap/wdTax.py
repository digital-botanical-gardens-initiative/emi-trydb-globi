import requests
import pandas as pd
from datetime import datetime
import sys
sys.path.append('./functions')  # Add the 'src' directory to the sys.path
import wdTaxUtil as utils



# Query 1: Mapping multiple external taxonomies
query_mapping = '''
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX wd: <http://www.wikidata.org/entity/>
SELECT ?WdID ?eol ?gbif ?ncbi ?ott ?itis ?irmng ?col ?nbn ?worms ?bold ?plazi ?apni ?msw3 ?iNat ?eppo ?WdName WHERE {
  ?WdID wdt:P31 wd:Q16521;
        wdt:P225 ?WdName .
  OPTIONAL { ?WdID wdt:P9157 ?ott . }
  OPTIONAL { ?WdID wdt:P685 ?ncbi . }
  OPTIONAL { ?WdID wdt:P846 ?gbif . }
  OPTIONAL { ?WdID wdt:P830 ?eol . }
  OPTIONAL { ?WdID wdt:P815 ?itis . }
  OPTIONAL { ?WdID wdt:P5055 ?irmng . }
  OPTIONAL { ?WdID wdt:P10585 ?col . }
  OPTIONAL { ?WdID wdt:P3240 ?nbn . }
  OPTIONAL { ?WdID wdt:P850 ?worms . }
  OPTIONAL { ?WdID wdt:P3606 ?bold . }
  OPTIONAL { ?WdID wdt:P1992 ?plazi . }
  OPTIONAL { ?WdID wdt:P5984 ?apni . }
  OPTIONAL { ?WdID wdt:P959 ?msw3 . }
  OPTIONAL { ?WdID wdt:P3151 ?iNat . }
  OPTIONAL { ?WdID wdt:P3031 ?eppo . }
}
'''

# Query 2: Taxonomic lineage
query_lineage = '''
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX wd: <http://www.wikidata.org/entity/>
SELECT ?WdID ?WdName ?hTax ?hTaxName ?hTaxRank WHERE {
  ?WdID wdt:P31 wd:Q16521;
        wdt:P225 ?WdName ;
        wdt:P171* ?hTax .
  ?hTax wdt:P225 ?hTaxName ;
        wdt:P105 ?hTaxRank .
}
'''

today_str = datetime.today().strftime('%Y%m%d') #date suffix


# explore mapping of wikidata to other taxonomic databases
"""file_mapping = f"wdTax_SPARQL_{today_str}.txt"
df_mapping = utils.querki_to_dataframe(query_mapping)
df_mapping.to_csv(file_mapping, index=False)
file_mappingC = utils.compress_and_remove(file_mapping)

# write full lineage of wikidata taxons
file_lineage = f"wdTax_SPARQL_lineage_{today_str}.txt"
utils.querki_write_file(query_lineage, file_lineage)
file_lineageC = utils.compress_and_remove(file_lineage)
"""

chunk_size = 22787142
predefined_ranks = ["http://www.wikidata.org/entity/Q35409", "http://www.wikidata.org/entity/Q34740", "http://www.wikidata.org/entity/Q36602", "http://www.wikidata.org/entity/Q38348", "http://www.wikidata.org/entity/Q37517", "http://www.wikidata.org/entity/Q36732", "http://www.wikidata.org/entity/Q7432"]

file_lineage_filtered = f"wdTax_SPARQL_lineage_filtered_{today_str}.txt.gz"

# extract only those rows, which have the predefined ranks
"""first_chunk = True
for chunk in pd.read_csv(file_lineageC, chunksize=chunk_size):
    required_columns = ["WdID", "WdName", "hTaxRank", "hTaxName"]
    print(chunk.columns)
#    if not required_columns.issubset(chunk.columns):
#        print(f"Error: Missing columns in chunk. Found: {list(chunk.columns)}")
#        continue
    filtered_chunk = chunk[chunk["hTaxRank"].isin(predefined_ranks)]
    mode = 'w' if first_chunk else 'a' # append to file instead of write afresh
    header = first_chunk
    filtered_chunk.to_csv(file_lineage_filtered, compression="gzip", mode=mode, header=header, index=False)
    first_chunk = False
"""

file_lineage_filtered_aligned = f"wdTax_SPARQL_lineage_filtered_aligned_{today_str}.txt.gz"

# operate on the filtered file to align data into columns with ranks as their header
chunk_iter = pd.read_csv(file_lineage_filtered, compression="gzip", chunksize=chunk_size)
first_chunk = True
"""for chunk in chunk_iter:
    chunk.columns = chunk.columns.str.strip()
    required_columns = ["WdID", "WdName", "hTaxRank", "hTaxName"]
    missing_columns = [col for col in required_columns if col not in chunk.columns]
    if missing_columns:
        print(f"Error: Missing columns in chunk: {', '.join(missing_columns)}")
        continue
    transformed_chunk = chunk[["WdID", "WdName"]].drop_duplicates().set_index(["WdID", "WdName"])
    for rank in predefined_ranks:
        transformed_chunk[rank] = ""
    # manually map values from hTaxRank to predefined columns
    for _, row in chunk.iterrows():
        if row["hTaxRank"] in predefined_ranks:
            transformed_chunk.at[(row["WdID"], row["WdName"]), row["hTaxRank"]] = row["hTaxName"]
    # reset index to flatten DataFrame
    transformed_chunk.reset_index(inplace=True)
    # write to file (header only for first chunk)
    mode = 'w' if first_chunk else 'a'
    header = first_chunk
    transformed_chunk.to_csv(file_lineage_filtered_aligned, compression="gzip", mode=mode, header=header, index=False)
    # after first write, switch to append mode
    first_chunk = False
"""

seen_keys = set()
duplicate_rows = []

for chunk in chunk_iter:
    chunk.columns = chunk.columns.str.strip()
    required_columns = ["WdID", "WdName", "hTaxRank", "hTaxName"]
    missing_columns = [col for col in required_columns if col not in chunk.columns]
    if missing_columns:
        print(f"Error: Missing columns in chunk: {', '.join(missing_columns)}")
        continue

    transformed_chunk = chunk[["WdID", "WdName"]].drop_duplicates().set_index(["WdID", "WdName"])
    for rank in predefined_ranks:
        transformed_chunk[rank] = ""

    # manually map values from hTaxRank to predefined columns
    for _, row in chunk.iterrows():
        if row["hTaxRank"] in predefined_ranks:
            transformed_chunk.at[(row["WdID"], row["WdName"]), row["hTaxRank"]] = row["hTaxName"]

    transformed_chunk.reset_index(inplace=True)
    mode = 'w' if first_chunk else 'a' #write to file (header only for first chunk)
    header = first_chunk
#    transformed_chunk.to_csv(file_lineage_filtered_aligned, compression="gzip", mode=mode, header=header, index=False)
    first_chunk = False # after first write, switch to append mode

    # precompute keys as tuples
    index_tuples = list(zip(transformed_chunk["WdID"], transformed_chunk["WdName"]))
    current_keys = set(index_tuples)

    # identify duplicates and new keys
    duplicates = current_keys & seen_keys
    new_keys = current_keys - seen_keys

    # mask rows
    is_duplicate = [key in duplicates for key in index_tuples]
    is_new = [key in new_keys for key in index_tuples]

    dup_df = transformed_chunk[is_duplicate]
    #write_chunk = transformed_chunk[is_new]

    if not dup_df.empty:
        duplicate_rows.append(dup_df)

    seen_keys.update(new_keys)



file_duplicates = f"wdTax_duplicates_{today_str}.txt"
if duplicate_rows:
    all_duplicates = pd.concat(duplicate_rows)
    # Group by WdID and WdName to sort them together
    all_duplicates.sort_values(by="WdName", inplace=True)
    all_duplicates.to_csv("duplicates_output.csv", index=False)
#if duplicate_rows:
#    pd.concat(duplicate_rows).to_csv(file_duplicates, index=False)

