'''
    File name: makeTriples_taxonomy_rdf_v1.py
    Author: Disha Tandon
    Date created: 12/11/2024
    Python Version: 3
'''


import pandas as pd
from rdflib import URIRef, Literal, Namespace, RDF, RDFS, XSD, DCTERMS, Graph, BNode
import gzip
import rdflib
import argparse
import sys


sys.path.append('./functions')  # Add the 'src' directory to the sys.path
import data_processing as dp


rdflib.plugin.register('turtle_custom', rdflib.plugin.Serializer, 'turtle_custom.serializer', 'TurtleSerializerCustom')

# Namespace declarations
emi = Namespace("https://purl.org/emi#")
emiBox = Namespace("https://purl.org/emi/abox#")
sosa = Namespace("http://www.w3.org/ns/sosa/")
dcterms = Namespace("http://purl.org/dc/terms/")
wd = Namespace("http://www.wikidata.org/entity/")
#nTemp = Namespace("http://example.com/base-ns#")
otol = Namespace("https://tree.opentreeoflife.org/taxonomy/browse")
skos = Namespace("http://www.w3.org/2004/02/skos/core#")



def generate_rdf_in_batches(input_csv_gz, join_csv, output_file, join_column, batch_size=1000):
    """
    Generate RDF triples in compact Turtle format using batches of rows and rdflib for serialization.

    :param input_csv_gz: Path to the gzipped CSV input file.
    :param join_csv: Path to the secondary CSV file for joining.
    :param output_file: Path to the output Turtle file.
    :param join_column: Column name for joining the two CSVs.
    :param batch_size: The number of rows to process per batch.
    """
    # Load input data
    data1 = pd.read_csv(input_csv_gz, compression="gzip", sep="\t", dtype=str)
    data2 = pd.read_csv(join_csv, compression="gzip",  sep="\t", dtype=str)

    # Filter the taxonomy data to include only rows with WdID found in enpkg['wd_taxon_id']
    merged_data = data1[data1['WdID'].isin(data2[join_column])]
    print(merged_data.shape)

    
    # Open the output file for writing
    with gzip.open(output_file, "wt", encoding="utf-8") as out_file:
        # Write prefixes directly to the file
        out_file.write("@prefix emi: <https://purl.org/emi#> .\n")
        out_file.write("@prefix skos: <http://www.w3.org/2004/02/skos/core#> .\n")
        out_file.write("@prefix : <https://purl.org/emi/abox#> .\n")
        out_file.write("@prefix sosa: <http://www.w3.org/ns/sosa/> .\n")
        out_file.write("@prefix dcterms: <http://purl.org/dc/terms/> .\n")
        out_file.write("@prefix wd: <http://www.wikidata.org/entity/> .\n")
        out_file.write("@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n")
        out_file.write("@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n")
        out_file.write("@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n")
        out_file.write("@prefix otol: <https://tree.opentreeoflife.org/taxonomy/browse> .\n\n")


    # Process in batches
    for start_row in range(0, len(merged_data), batch_size):
        end_row = min(start_row + batch_size, len(merged_data))
        batch_data = merged_data[start_row:end_row]
        print(batch_data.shape)
        print(start_row)
        # Initialize a new graph for this batch
        graph = Graph()
        graph.bind("", emiBox)  # ":" will now map to "https://purl.org/emi/abox#"
        graph.bind("emi", emi)  # Bind the 'emi' prefix explicitly
        graph.bind("sosa", sosa)  # Bind the 'emi' prefix explicitly
        graph.bind("dcterms", dcterms)  # Bind the 'emi' prefix explicitly
        graph.bind("wd", wd)  # Bind the 'emi' prefix explicitly
        graph.bind("otol", otol)  # Bind the 'emi' prefix explicitly
        graph.bind("skos", skos)  # Bind the 'emi' prefix explicitly
        #graph.namespace_manager.bind("_", nTemp)

        # Process each row in the batch
        for _, row in batch_data.iterrows():
#            graph.namespace_manager.bind("_", nTemp)
            # Define URIs (ensure spaces are replaced with underscores)
            taxonomy_uri = otol[f"?id={row['ott']}"] if dp.is_none_na_or_empty(row['ott']) else None
            wikidata_uri = wd[f"{row['WdID']}"] if dp.is_none_na_or_empty(row['WdID']) else None

            # Add triples to the graph
            if dp.is_none_na_or_empty(row['ncbi.wd']):
                if dp.is_none_na_or_empty(wikidata_uri):
                    graph.add((wikidata_uri, skos.exactMatch, URIRef(f"http://purl.uniprot.org/taxonomy/{row['ncbi.wd']}")))
                graph.add((taxonomy_uri, skos.exactMatch, URIRef(f"http://purl.uniprot.org/taxonomy/{row['ncbi.wd']}")))
                graph.add((URIRef(f"http://purl.uniprot.org/taxonomy/{row['ncbi.wd']}"), dcterms.identifier, Literal(row['ncbi.wd'], datatype=XSD.string)))
                graph.add((URIRef(f"http://purl.uniprot.org/taxonomy/{row['ncbi.wd']}"), skos.inScheme, URIRef("http://purl.obolibrary.org/obo/ncbitaxon.owl")))

            if dp.is_none_na_or_empty(row['gbif.ott']):
                if dp.is_none_na_or_empty(wikidata_uri):
                    graph.add((wikidata_uri, skos.exactMatch, URIRef(f"https://www.gbif.org/species/{row['gbif.ott']}")))
                graph.add((taxonomy_uri, skos.exactMatch, URIRef(f"https://www.gbif.org/species/{row['gbif.ott']}")))
                graph.add((URIRef(f"https://www.gbif.org/species/{row['gbif.ott']}"), dcterms.identifier, Literal(row['gbif.ott'], datatype=XSD.int)))
                graph.add((URIRef(f"https://www.gbif.org/species/{row['gbif.ott']}"), skos.inScheme, URIRef("https://www.gbif.org/species")))

            if dp.is_none_na_or_empty(row['eol']):
                if dp.is_none_na_or_empty(wikidata_uri):
                    graph.add((wikidata_uri, skos.exactMatch, URIRef(f"https://www.eol.org/pages/{row['eol']}")))
                graph.add((taxonomy_uri, skos.exactMatch, URIRef(f"https://www.eol.org/pages/{row['eol']}")))
                graph.add((URIRef(f"https://www.eol.org/pages/{row['eol']}"), dcterms.identifier, Literal(row['eol'], datatype=XSD.int)))
                graph.add((URIRef(f"https://www.eol.org/pages/{row['eol']}"), skos.inScheme, URIRef("https://www.eol.org")))

            if dp.is_none_na_or_empty(row['itis']):
                if dp.is_none_na_or_empty(wikidata_uri):
                    graph.add((wikidata_uri, skos.exactMatch, URIRef(f"https://itis.gov/servlet/SingleRpt/SingleRpt?search_topic=TSN&search_value={row['itis']}")))
                graph.add((taxonomy_uri, skos.exactMatch, URIRef(f"https://itis.gov/servlet/SingleRpt/SingleRpt?search_topic=TSN&search_value={row['itis']}")))
                graph.add((URIRef(f"https://itis.gov/servlet/SingleRpt/SingleRpt?search_topic=TSN&search_value={row['itis']}"), dcterms.identifier, Literal(row['itis'], datatype=XSD.string)))
                graph.add((URIRef(f"https://itis.gov/servlet/SingleRpt/SingleRpt?search_topic=TSN&search_value={row['itis']}"), skos.inScheme, URIRef("https://itis.gov")))

            if dp.is_none_na_or_empty(row['irmng.wd']):
                if dp.is_none_na_or_empty(wikidata_uri):
                    graph.add((wikidata_uri, skos.exactMatch, URIRef(f"https://www.irmng.org/aphia.php?p=taxdetails&id={row['irmng.wd']}")))
                graph.add((taxonomy_uri, skos.exactMatch, URIRef(f"https://www.irmng.org/aphia.php?p=taxdetails&id={row['irmng.wd']}")))
                graph.add((URIRef(f"https://www.irmng.org/aphia.php?p=taxdetails&id={row['irmng.wd']}"), dcterms.identifier, Literal(row['irmng.wd'], datatype=XSD.string)))
                graph.add((URIRef(f"https://www.irmng.org/aphia.php?p=taxdetails&id={row['irmng.wd']}"), skos.inScheme, URIRef("https://www.irmng.org")))

            if dp.is_none_na_or_empty(row['worms.wd']):
                if dp.is_none_na_or_empty(wikidata_uri):
                    graph.add((wikidata_uri, skos.exactMatch, URIRef(f"https://www.marinespecies.org/aphia.php?p=taxdetails&id={row['worms.wd']}")))
                graph.add((taxonomy_uri, skos.exactMatch, URIRef(f"https://www.marinespecies.org/aphia.php?p=taxdetails&id={row['worms.wd']}")))
                graph.add((URIRef(f"https://www.marinespecies.org/aphia.php?p=taxdetails&id={row['worms.wd']}"), dcterms.identifier, Literal(row['worms.wd'], datatype=XSD.string)))
                graph.add((URIRef(f"https://www.marinespecies.org/aphia.php?p=taxdetails&id={row['worms.wd']}"), skos.inScheme, URIRef("https://www.marinespecies.org")))

            if dp.is_none_na_or_empty(row['col']):
                if dp.is_none_na_or_empty(wikidata_uri):
                    graph.add((wikidata_uri, skos.exactMatch, URIRef(f"https://www.catalogueoflife.org/data/taxon/{row['col']}")))
                graph.add((taxonomy_uri, skos.exactMatch, URIRef(f"https://www.catalogueoflife.org/data/taxon/{row['col']}")))
                graph.add((URIRef(f"https://www.catalogueoflife.org/data/taxon/{row['col']}"), dcterms.identifier, Literal(row['col'], datatype=XSD.string)))
                graph.add((URIRef(f"https://www.catalogueoflife.org/data/taxon/{row['col']}"), skos.inScheme, URIRef("https://www.catalogueoflife.org")))

            if dp.is_none_na_or_empty(row['nbn']):
                if dp.is_none_na_or_empty(wikidata_uri):
                    graph.add((wikidata_uri, skos.exactMatch, URIRef(f"https://species.nbnatlas.org/species/{row['nbn']}")))
                graph.add((taxonomy_uri, skos.exactMatch, URIRef(f"https://species.nbnatlas.org/species/{row['nbn']}")))
                graph.add((URIRef(f"https://species.nbnatlas.org/species/{row['nbn']}"), dcterms.identifier, Literal(row['nbn'], datatype=XSD.string)))
                graph.add((URIRef(f"https://species.nbnatlas.org/species/{row['nbn']}"), skos.inScheme, URIRef("https://species.nbnatlas.org")))

            if dp.is_none_na_or_empty(row['msw3']):
                if dp.is_none_na_or_empty(wikidata_uri):
                    graph.add((wikidata_uri, skos.exactMatch, URIRef(f"https://departments.bucknell.edu/biology/resources/msw3/browse.asp?id={row['msw3']}")))
                graph.add((taxonomy_uri, skos.exactMatch, URIRef(f"https://departments.bucknell.edu/biology/resources/msw3/browse.asp?id={row['msw3']}")))
                graph.add((URIRef(f"https://departments.bucknell.edu/biology/resources/msw3/browse.asp?id={row['msw3']}"), dcterms.identifier, Literal(row['msw3'], datatype=XSD.string)))
                graph.add((URIRef(f"https://departments.bucknell.edu/biology/resources/msw3/browse.asp?id={row['msw3']}"), skos.inScheme, URIRef("https://departments.bucknell.edu/biology/resources/msw3")))

            if dp.is_none_na_or_empty(row['WdID']):
                graph.add((taxonomy_uri, skos.exactMatch, URIRef(wd[f"{row['WdID']}"])))
                graph.add((URIRef(wd[f"{row['WdID']}"]), dcterms.identifier, Literal(row['WdID'], datatype=XSD.string)))
                graph.add((URIRef(wd[f"{row['WdID']}"]), RDF.type, emi.Taxon))
                graph.add((URIRef(wd[f"{row['WdID']}"]), skos.inScheme, URIRef(f"http://www.wikidata.org/entity")))

            # Add labels for all taxonomies
            graph.add((URIRef("http://purl.obolibrary.org/obo/ncbitaxon.owl"), RDFS.label, Literal("NCBI organismal classification", datatype=XSD.string)))
            graph.add((URIRef("https://www.gbif.org/species"), RDFS.label,Literal("The GBIF taxonomy", datatype=XSD.string)))
            graph.add((URIRef("https://www.eol.org"), RDFS.label,Literal("Encyclopedia of Life", datatype=XSD.string)))
            graph.add((URIRef("https://itis.gov"), RDFS.label,Literal("Integrated Taxonomic Information System", datatype=XSD.string)))
            graph.add((URIRef("https://www.irmng.org"), RDFS.label,Literal("Interim Register of Marine and Nonmarine Genera", datatype=XSD.string)))
            graph.add((URIRef("https://www.marinespecies.org"), RDFS.label,Literal("World Register of Marine Species", datatype=XSD.string)))
            graph.add((URIRef("https://www.catalogueoflife.org"), RDFS.label,Literal("Catalogue of Life", datatype=XSD.string)))
            graph.add((URIRef("https://species.nbnatlas.org"), RDFS.label,Literal("National Biodiversity Network", datatype=XSD.string)))
            graph.add((URIRef("https://departments.bucknell.edu/biology/resources/msw3"), RDFS.label,Literal("Mammal Species of the World", datatype=XSD.string)))
            graph.add((URIRef(f"http://www.wikidata.org/entity"), RDFS.label,Literal("Wikidata entities", datatype=XSD.string)))

        dp.add_inverse_relationships(graph)
        # Serialize the graph for the batch and write to the file
        with gzip.open(output_file, "at", encoding="utf-8") as out_file:  # Append mode
            out_file.write(graph.serialize(format="turtle_custom"))
        #out_file.flush()

        # Clear the graph to free memory
    #    graph.remove((None, None, None))
        del graph
        print(out_file)

    print(f"RDF triples saved to {output_file}")

# Main execution
if __name__ == "__main__":
    configFile = "config.txt"
    if os.path.exists(configFile):       #if config file is available
        config = configparser.ConfigParser()
        config.read(configFile)
        csv_file1 = config.get('tsv files', 'taxonomy_tsv')
        csv_file2 = config.get('accessory files', 'enpkg_wd')
        output_file = config.get('output files', 'taxonomy_ttl')
    else:                               #else use command line arguments
        # Create the argument parser
        parser = argparse.ArgumentParser()
    
        # Add arguments
        parser.add_argument('inputFile', type=str, help="Enter the file name for which you want the triples")
        parser.add_argument('joinFile', type=str, help="Enter the file name which will be used for filtering or joining the input_file")
        parser.add_argument('outputFile', type=str, help="Enter the output file name")
    
        # Parse the arguments
        args = parser.parse_args()
        csv_file1 = args.inputFile
        csv_file2 = args.joinFile
        output_file = args.outputFile

    generate_rdf_in_batches(csv_file1, csv_file2, output_file, join_column="wd_taxon_id", batch_size=10000)

