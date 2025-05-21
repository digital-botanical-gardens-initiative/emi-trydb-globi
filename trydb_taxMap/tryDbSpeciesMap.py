import pandas as pd
import numpy as np
import gzip
import os
import argparse
import configparser
from datetime import datetime

# align directly by name - reliable because TRY-db has already standardized names
def process_row(row):
    if row['TRY_AccSpeciesName'] in wd_name_to_id_set:
        tempVar = wd_name_to_id.get((row['TRY_AccSpeciesName'],'Plantae'))
        if tempVar:
            best_wd_id = tempVar
            kingdomV = "Plantae"
        else: 
            best_wd_id = wd_name_to_id.get((row['TRY_AccSpeciesName'],np.nan))
            kingdomV = "None"
        status = "ID-MATCHED-BY-NAME-direct"
    else:
        best_wd_id = None
        kingdomV = None
        status = "NAME-NOT-MATCHED"
    row["WdID"] = best_wd_id
    row["Match_Status"] = status
    row["kingdom"] = kingdomV
    #print(row)
    return row



#main execution
configFile = "config.txt"
if os.path.exists(configFile):       #if config file is available
        config = configparser.ConfigParser()
        config.read(configFile)
        tryDbFile = config.get('input tsv files', 'tryDb_species_file')
        wd_lineage_file = config.get('input tsv files', 'wd_lineage_file')
        outputFile = config.get('input tsv files', 'output_file')
else:                               #else use command line arguments
        parser = argparse.ArgumentParser() #argument parser
        #add arguments
        parser.add_argument('inputFile', type=str, help="Enter the tryDb gzip file")
        parser.add_argument('wd_lineage_aligned_file', type=str, help="Enter the wd lineage file")
        parser.add_argument('outputFile', type=str, help="Enter the output file name")
        #parse the arguments
        args = parser.parse_args()
        tryDbFile = args.inputFile
        outputFile = args.outputFile
        wd_lineage_file = args.wd_lineage_aligned_file

# make a set and dict of wd_lineage
wd_lineage_df = pd.read_csv(wd_lineage_file, usecols=['WdID','WdName','kingdom'], sep=",", dtype=str)
wd_lineage_df["WdID"] = wd_lineage_df["WdID"].str.replace("http://www.wikidata.org/entity/", "", regex=False)
wd_lineage_df["kingdom"] = wd_lineage_df["kingdom"].replace({np.nan: None, pd.NA: None, "": None})
wd_name_to_id_set = set(wd_lineage_df["WdName"])
wd_name_to_id = (
    wd_lineage_df.set_index(["WdName", "kingdom"])["WdID"]
    .to_dict()
)

# process tryDb file to get wd Ids
tryDb_df = pd.read_csv(tryDbFile, usecols=['TRY_SpeciesName','TRY_AccSpeciesName'], sep=",", dtype=str, encoding="iso-8859-1") #read source
d = { 'TRY_AccSpeciesName' : list(set(tryDb_df['TRY_AccSpeciesName']))}
tryDb_df = pd.DataFrame(d)
tryDb_dfX = tryDb_df.apply(process_row, axis=1).apply(pd.Series) #same df

today_str = datetime.today().strftime('%Y%m%d') #date suffix
outputFileX = f"{outputFile}_{today_str}.txt.gz"
tryDb_dfX.to_csv(outputFileX, sep="\t", index=False, compression='gzip')




