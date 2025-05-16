import requests
import pandas as pd
from datetime import datetime
import argparse
import gzip
import shutil
import os

def compress_and_remove(file_path):
    gz_path = file_path + '.gz'
    with open(file_path, 'rb') as f_in, gzip.open(gz_path, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)
    os.remove(file_path)
    return gz_path


# Function to stream SPARQL query and write output to file line by line (CSV format)
def querki_write_file(query: str, output_file: str):
    headers = {"Accept": "text/csv"}
    params = {"query": query}
    with requests.get("https://qlever.cs.uni-freiburg.de/wikidata/sparql", headers=headers, params=params, stream=True) as response:
        response.raise_for_status()
        with open(output_file, 'w', encoding='utf-8') as f:
            for chunk in response.iter_content(chunk_size=8192, decode_unicode=True):
                f.write(chunk)

# Function to run SPARQL query and return DataFrame (JSON format)
def querki_to_dataframe(query: str) -> pd.DataFrame:
    endpoint_url = "https://qlever.cs.uni-freiburg.de/wikidata/sparql"
    response = requests.get(endpoint_url, params={"query": query, "format": "json"})
    response.raise_for_status()
    data = response.json()
    variables = data["head"]["vars"]
    rows = []
    for item in data["results"]["bindings"]:
        row = [item[var]["value"] if var in item else None for var in variables]
        rows.append(row)
    return pd.DataFrame(rows, columns=variables)
