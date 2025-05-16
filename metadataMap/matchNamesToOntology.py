'''
    File name: matchNamesToOntology.py
    Author: Disha Tandon
    Date created: 12/13/2024
    Python Version: 3
'''


from owlready2 import get_ontology
from sentence_transformers import SentenceTransformer, util
import csv
import argparse

def load_ontologies(ontology_paths):
    ontologies = {}
    for ontology_name, path in ontology_paths.items():
        ontologies[ontology_name] = get_ontology(path).load()
    return ontologies

model = SentenceTransformer('all-MiniLM-L6-v2')

def extract_terms_from_ontology(ontology):
    ontology_terms = []
    for cls in ontology.classes():
        if cls.label:
            main_label = cls.label[0]
            synonyms = []
            
            if hasattr(cls, "hasExactSynonym"):
                synonyms.extend(cls.hasExactSynonym)
            if hasattr(cls, "hasBroadSynonym"):
                synonyms.extend(cls.hasBroadSynonym)
            if hasattr(cls, "hasRelatedSynonym"):
                synonyms.extend(cls.hasRelatedSynonym)
            
            all_labels = [main_label] + synonyms
            for label in all_labels:
                ontology_terms.append((label, cls.iri, main_label))
    return ontology_terms

def generate_ontology_embeddings(ontologies, model):
    all_ontology_terms = []
    for ontology_name, ontology in ontologies.items():
        terms = extract_terms_from_ontology(ontology)
        all_ontology_terms.extend(terms)
    
    ontology_labels = [term[0] for term in all_ontology_terms]
    ontology_embeddings = model.encode(ontology_labels, convert_to_tensor=True)
    return all_ontology_terms, ontology_embeddings

def find_best_match(input_term, ontology_terms, ontology_embeddings, model):
    input_embedding = model.encode(input_term, convert_to_tensor=True)
    similarities = util.pytorch_cos_sim(input_embedding, ontology_embeddings)
    best_match_idx = similarities.argmax().item()
    best_match_label = ontology_terms[best_match_idx][0]
    best_match_uri = ontology_terms[best_match_idx][1]
    best_match_main_label = ontology_terms[best_match_idx][2]
    best_match_score = similarities[0][best_match_idx].item()
    
    return best_match_label, best_match_main_label, best_match_uri, best_match_score

def process_terms(input_file, output_file, ontology_paths):
    ontologies = load_ontologies(ontology_paths)
    all_ontology_terms, ontology_embeddings = generate_ontology_embeddings(ontologies, model)
    
    with open(input_file, "r") as infile:
        input_terms = [line.strip() for line in infile if line.strip()]
    
    with open(output_file, "w", newline="") as outfile:
        csv_writer = csv.writer(outfile)
        csv_writer.writerow(["Input Term", "Matched Term", "Primary Label", "URI", "Similarity Score"])
        
        for terms in input_terms:
            tx = [term.strip() for term in terms.replace('/', ',').split(',')]
            for i in tx:
                best_label, main_label, best_uri, best_score = find_best_match(i, all_ontology_terms, ontology_embeddings, model)
                csv_writer.writerow([i, best_label, main_label, best_uri, f"{best_score:.4f}"])
                print(f"Processed: {i} -> {best_label} ({best_score:.4f})")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('inputFile', type=str, help="Enter the file name with terms to be matched")
    parser.add_argument('outputFile', type=str, help="Enter the output file name")
    args = parser.parse_args()
    
    ontology_paths = {
        "UBERON": "http://purl.obolibrary.org/obo/uberon.owl",
        "PO": "http://purl.obolibrary.org/obo/po.owl",
        "ENVO": "http://purl.obolibrary.org/obo/envo.owl",
        "PATO": "http://purl.obolibrary.org/obo/pato.owl",
        "FAO": "http://purl.obolibrary.org/obo/fao.owl",
        "HAO": "http://purl.obolibrary.org/obo/hao.owl",
        "BTO": "http://purl.obolibrary.org/obo/bto.owl",
        "NCIT": "http://purl.obolibrary.org/obo/ncit.owl",
    }
    
    process_terms(args.inputFile, args.outputFile, ontology_paths)

