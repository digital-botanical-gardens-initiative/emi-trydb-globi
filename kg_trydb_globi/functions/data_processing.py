'''
    File name: data_processing.py
    Author: Disha Tandon
    Date created: 13/11/2024
    Python Version: 3
'''


import pandas as pd
import gzip
import rdflib
from rdflib import URIRef, Literal, Namespace, RDF, RDFS, XSD, DCTERMS, Graph, BNode
from urllib.parse import quote
import re

rdflib.plugin.register('turtle_custom', rdflib.plugin.Serializer, 'turtle_custom.serializer', 'TurtleSerializerCustom')


# Function for checking na/none/empty strings
def is_none_na_or_empty(value):
    return not (value is None or value == '' or value == "\\N" or value == "no:match" or pd.isna(value) or re.match(r"ĜLOBI:", value))


# Define a function for real-time filtering
def filter_file_runtime(file_path, filter_df, key_column):
    # Read the file in chunks for runtime processing
    cs = 10000  # Adjust chunk size as needed
    matching_rows = pd.DataFrame()  # To store filtered rows
    
    for chunk in pd.read_csv(file_path, compression="gzip", sep="\t", dtype=str, encoding="utf-8", chunksize=cs):
        # Filter rows where 'key_column' matches values in filter_df
        filtered_chunk = chunk[chunk['source_WD'].isin(filter_df[key_column]) | chunk['target_WD'].isin(filter_df[key_column])]
        
        # Append matching rows to the result DataFrame
        matching_rows = pd.concat([matching_rows, filtered_chunk], ignore_index=True)
    
    return matching_rows

# Define a function for real-time filtering by phylum name
def filter_file_runtime_taxonomy(file_path):
    # Here, neither filter_df nor key_column are bieng used.
    # Read the file in chunks for runtime processing
    cs = 10000  # Adjust chunk size as needed
    matching_rows = pd.DataFrame()  # To store filtered rows
    phylumName = ["Arthropoda", "Nematoda"]
    kingdomName = ["Archaeplastida"]
    for chunk in pd.read_csv(file_path, compression="gzip", sep="\t", dtype=str, encoding="utf-8", chunksize=cs):
        # Filter rows where 'key_column' matches values in filter_df
        filtered_chunk = chunk[chunk['targetTaxonKingdomName'].isin(kingdomName) | chunk['sourceTaxonKingdomName'].isin(kingdomName) | chunk['targetTaxonPhylumName'].isin(phylumName) | chunk['sourceTaxonPhylumName'].isin(phylumName)]
        
        # Append matching rows to the result DataFrame
        matching_rows = pd.concat([matching_rows, filtered_chunk], ignore_index=True)
    
    return matching_rows

# adds inverse relationships to the RDF graph based on predefined mappings.
def add_inverse_relationships(graph, tripCount):
    INVERSE_RELATIONS = {
    "http://purl.org/dc/terms/isPartOf": "http://purl.org/dc/terms/hasPart",
    "http://purl.org/dc/terms/hasFormat": "http://purl.org/dc/terms/isFormatOf",
    "http://purl.org/dc/terms/hasVersion": "http://purl.org/dc/terms/isVersionOf",
    "http://purl.org/dc/terms/references": "http://purl.org/dc/terms/isReferencedBy",
    "http://purl.org/dc/terms/replaces": "http://purl.org/dc/terms/isReplacedBy",
    "http://purl.org/dc/terms/requires": "http://purl.org/dc/terms/isRequiredBy",
    "http://www.w3.org/ns/sosa/isActedOnBy": "http://www.w3.org/ns/sosa/actsOnProperty",
	"http://www.w3.org/ns/sosa/isFeatureOfInterestOf": "http://www.w3.org/ns/sosa/hasFeatureOfInterest",
	"http://www.w3.org/ns/sosa/isResultOf": "http://www.w3.org/ns/sosa/hasResult",
	"http://www.w3.org/ns/sosa/isSampleOf": "http://www.w3.org/ns/sosa/hasSample",
	"http://www.w3.org/ns/sosa/isHostedBy": "http://www.w3.org/ns/sosa/hosts",
	"http://www.w3.org/ns/sosa/actsOnProperty": "http://www.w3.org/ns/sosa/isActedOnBy",
	"http://www.w3.org/ns/sosa/hasFeatureOfInterest": "http://www.w3.org/ns/sosa/isFeatureOfInterestOf",
	"http://www.w3.org/ns/sosa/hosts": "http://www.w3.org/ns/sosa/isHostedBy",
	"http://www.w3.org/ns/sosa/observes": "http://www.w3.org/ns/sosa/isObservedBy",
	"http://www.w3.org/ns/sosa/hasResult": "http://www.w3.org/ns/sosa/isResultOf",
	"http://www.w3.org/ns/sosa/hasSample": "http://www.w3.org/ns/sosa/isSampleOf",
	"http://www.w3.org/ns/sosa/madeByActuator": "http://www.w3.org/ns/sosa/madeActuation",
	"http://www.w3.org/ns/sosa/madeActuation": "http://www.w3.org/ns/sosa/madeByActuator",
	"http://www.w3.org/ns/sosa/madeSampling": "http://www.w3.org/ns/sosa/madeBySampler",
	"http://www.w3.org/ns/sosa/madeObservation": "http://www.w3.org/ns/sosa/madeBySensor",
	"http://www.w3.org/ns/sosa/madeBySensor": "http://www.w3.org/ns/sosa/madeObservation",
	"http://www.w3.org/ns/sosa/madeBySampler": "http://www.w3.org/ns/sosa/madeSampling",
	"http://www.w3.org/ns/sosa/isObservedBy": "http://www.w3.org/ns/sosa/observes",
    }
    
    new_triples = []
    for subj, pred, obj in graph:
        pred_str = str(pred)
        if pred_str in INVERSE_RELATIONS:
            inverse_pred = URIRef(INVERSE_RELATIONS[pred_str])
            if isinstance(obj, URIRef):  # Only create inverses for URI objects
                new_triples.append((obj, inverse_pred, subj))
    
    for triple in new_triples:
        graph.add(triple)
        tripCount += 1
    
    return tripCount

# Formats the URI part by replacing spaces with underscores.
def format_uri(uri_part):
    '''
    :param uri_part: The string part of the URI that may contain special chars.
    :return: The formatted URI part with spaces replaced by underscores.
    '''
    encoded_string = quote(uri_part, safe="")  # Encode all special characters
    return encoded_string


# Create a dictionary from two columns in a CSV file using pandas.
def create_dict_from_csv(csv_file1, key_column, value_column):
    """
    :param csv_file: Path to the CSV file.
    :param key_column: Column name to use as keys.
    :param value_column: Column name to use as values.
    :return: A dictionary where keys are from key_column and values are from value_column.
    """
    df1 = pd.read_csv(csv_file1, sep=",", dtype=str)
    #df2 = pd.read_csv(csv_file2, sep=",", dtype=str)
    #result = pd.concat([df1, df2], ignore_index=True)

    return dict(zip(df1[key_column], df1[value_column]))


