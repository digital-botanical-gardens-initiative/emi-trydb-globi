import argparse
import configparser
import os


configFile = "config.txt"
predefined_ranks = ["kingdom", "phylum", "class", "order", "family", "genus", "species"]
prefixes = {
    1: "EOL:", 2: "GBIF:", 3: "NCBI:", 4: "OTT:", 5: "ITIS:",
    6: "IRMNG:", 7: "COL:", 8: "NBN:", 9: "WORMS:", 10: "BOLD:",
    11: "PLAZI:", 12: "APNI:",  13: "msw3:", 14: "INAT_TAXON:", 15: "EPPO:"
}


