import pandas as pd
import argparse
import configparser
import os


configFile = "config.txt"
if os.path.exists(configFile):       #if config file is available
    config = configparser.ConfigParser()
    config.read(configFile)
    bpFileName = config.get('accessory files', 'bp_fileName')
    lsFileName = config.get('accessory files', 'ls_fileName')

# Declare dictionaries to be used globally. It is best to combine bodyParts and lifeStages in one file in one file.
keyCol="InputTerm"
valCol1="BestMatch"
valCol2="URI"
# list of file paths - bodyPart (anatomicalEntity) and lifeStage (developmentalStage)
file_names = [bpFileName,lsFileName]
# read and combine all files into one DataFrame
mapDf = pd.concat([pd.read_csv(file, sep=",", quoting=0, dtype=str) for file in file_names], ignore_index=True)
eNames = mapDf.dropna(subset=[valCol1]).query(f"{valCol1}.str.strip() != ''", engine='python')
eNamesDict, eNamesSet = eNames.set_index(keyCol)[valCol1].to_dict(), set(eNames[keyCol])
eURI = mapDf.dropna(subset=[valCol2]).query(f"{valCol2}.str.strip() != ''", engine='python')
eURIDict, eURISet = eURI.set_index(keyCol)[valCol2].to_dict(), set(eURI[keyCol])

fungiTerms = {
    "dematiaceous anamorph": "dematiaceous anamorph",
    "coelomycetous anamorph": "coelomycetous anamorph",
    "anamorph": "anamorph",
    "anamoprh": "anamorph",
    "synnemata": "synnemata",
    "synnema": "synnemata",
    "teleomorph": "teleomorph"
}

traitNamesX = ["Leaf area (in case of compound leaves: leaflet, petiole excluded)",
"Leaf area (in case of compound leaves: leaflet, petiole included)",
"Leaf area (in case of compound leaves: leaflet, undefined if petiole is in- or excluded)",
"Leaf area (in case of compound leaves: leaf, petiole excluded)",
"Leaf area (in case of compound leaves: leaf, petiole included)",
"Leaf area (in case of compound leaves: leaf, undefined if petiole in- or excluded)",
"Leaf area (in case of compound leaves undefined if leaf or leaflet, undefined if petiole is in- or excluded)",
"Leaf area per leaf dry mass (specific leaf area, SLA or 1/LMA): petiole excluded",
"Leaf area per leaf dry mass (specific leaf area, SLA or 1/LMA): petiole included",
"Leaf area per leaf dry mass (specific leaf area, SLA or 1/LMA) petiole, rhachis and midrib excluded",
"Leaf area per leaf dry mass (specific leaf area, SLA or 1/LMA): undefined if petiole is in- or excluded",
"Leaf carbon (C) content per leaf dry mass",
"Leaf lifespan (longevity)",
"Leaf nitrogen (N) content per leaf dry mass",
"Leaf phosphorus (P) content per leaf dry mass",
"Leaf respiration rate in light per dry mass",
"Leaf respiration rate in light per leaf area",
"Leaf respiration rate in light per leaf respiration rate in the dark",
"Leaf respiration rate in the dark as fraction of photosynthetic carboxylation capacity (Vcmax)",
"Leaf respiration rate in the dark minus respiration in light, mass based",
"Leaf respiration rate in the dark per leaf area",
"Leaf respiration rate in the dark per leaf dry mass",
"Leaf respiration rate in the dark per leaf nitrogen (N) content",
"Leaf respiration rate in the dark temperature dependence",
"Photosynthesis rate per leaf area",
"Photosynthesis rate per leaf area: transition to TPU limited photosynthesis",
"Photosynthesis rate per leaf dry mass",
"Photosynthesis rate per leaf nitrogen (N) content (photosynthetic nitrogen use efficiency, PNUE)",
"Photosynthesis rate per leaf transpiration (photosynthetic water use effinciency: WUE)",
"Photosynthesis rate per stomatal conductance",
"Plant height generative",
"Plant height vegetative",
"Seed dry mass",
"Seed mass per fruit",
"Stem specific density (SSD, stem dry mass per stem fresh volume) or wood density",
"Stem specific density (SSD, stem dry mass per stem fresh volume) or wood density: heartwood"]

traitNames = set(traitNamesX)
