import pandas as pd
import re
import sys
import os
from rdflib import URIRef, Literal, Namespace, RDF, RDFS, XSD, DCTERMS, Graph, BNode
from config import eURIDict, eURISet, eNamesDict, eNamesSet, bsFileName
import data_processing as dp


# add entity specific for body parts names, life stage names and biological sex
def add_entity(graph, subject, predicate, rdftype, entityX, entity_name, desigSet, fetchtype, termOr, tripCount):
    print(subject, termOr, entityX, fetchtype, sep="\t")
    graph.add((subject, predicate, entityX))
    tripCount += 1
    if entityX not in desigSet:
        graph.add((entityX, RDF.type, rdftype))
        graph.add((entityX, RDFS.label, Literal(entity_name, datatype=XSD.string)))
        tripCount += 2
        desigSet.add(entityX)
    return tripCount

def lookup_term(termOr, graph, subject, predicate, rdftype, ns, term, pre_post_fix, desigSet, tripCount):
    emiBox = Namespace("https://purl.org/emi/abox#")
    term = preprocess_term(term)
    if term in eURISet:
        modEntityURI = URIRef(eURIDict[term])
        modEntityName = eNamesDict[term]
#        print(term, ns, modEntityURI, "URI-FETCHED-1", sep="\t")
        tripCount = add_entity(graph, subject, predicate, rdftype, modEntityURI, modEntityName, desigSet, "URI-FETCHED-1", termOr, tripCount)
    elif term in eNamesSet:
        modEntityName = eNamesDict[term]
        ent = emiBox[f"{ns}-{dp.format_uri(modEntityName)}"]
#        print(term, ns, modEntityName, "URI-FETCHED-1a", sep="\t")
        tripCount = add_entity(graph, subject, predicate, rdftype, ent, modEntityName, desigSet, "URI-FETCHED-1a", termOr, tripCount)
    else:
        term = preprocess_term(pre_post_fix.sub('', term))
        if term in eURISet:
            modEntityURI = URIRef(eURIDict[term])
            modEntityName = eNamesDict[term]
#            print(term, ns, modEntityURI, "URI-FETCHED-1", sep="\t")
            tripCount = add_entity(graph, subject, predicate, rdftype, modEntityURI, modEntityName, desigSet, "URI-FETCHED-1", termOr, tripCount)
        elif term in eNamesSet:
            modEntityName = eNamesDict[term]
            ent = emiBox[f"{ns}-{dp.format_uri(modEntityName)}"]
#            print(term, ns, modEntityName, "URI-FETCHED-1a", sep="\t")
            tripCount = add_entity(graph, subject, predicate, rdftype, ent, modEntityName, desigSet, "URI-FETCHED-1a", termOr, tripCount)
        else:
            print(termOr, ns, term, "NOTHING-AVAILABLE", sep="\t")
    return tripCount


def listTerms(term, graph, subject, predicate, rdftype, ns, desigSet, tripCount):
    termOr = term
    conjunction_patterns1 = re.compile(r'\b(and|y)\b', re.IGNORECASE)
    conjunction_patterns2 = re.compile(r'\b(or)\b', re.IGNORECASE)
    pre_post_fix = re.compile(r"(adult[as]?|tortere|juvenil[e]?|maybe|\(?torete[s]?\)?)", re.IGNORECASE)
    delimiters_regex = re.compile(r"[,;/|&]+", re.IGNORECASE)
    delimiters_regex1 = re.compile(r"[\[\]\(\)\?\#:`]+", re.IGNORECASE)
    delimiters_regex2 = re.compile(r"[+.,]+", re.IGNORECASE)
    delimiters_regex3 = re.compile(r"\s\s", re.IGNORECASE)
    pattern = r"(\d+)\s*([\w-]+)|([\w-]+)\s*(\d+)"
    term = term.lower().strip()
    term = conjunction_patterns1.sub(',', term)
    term = conjunction_patterns2.sub('', term)
    term = delimiters_regex.sub(',', term)
    term = delimiters_regex1.sub(' ', term)
    term = delimiters_regex3.sub(' ', term)
    terms = delimiters_regex2.split(term)
    for term in terms:
        cleaned_row = re.sub(r"[+.,]", " ", term)
        matches = re.findall(pattern, cleaned_row)
        if matches:
            for match in matches:
                number1, term1, term2, number2 = match
                term = term1 if term1 else term2
                tripCount = lookup_term(termOr, graph, subject, predicate, rdftype, ns, term.strip(), pre_post_fix, desigSet, tripCount)
        else:
            for term in delimiters_regex2.split(term):
                tripCount = lookup_term(termOr, graph, subject, predicate, rdftype, ns, term.strip(), pre_post_fix, desigSet, tripCount)
    return tripCount


def countTerms(term,mapping_dict,mapping_set):
    records = []
    conjunction_patterns1 = re.compile(r'\b(and|y)\b', re.IGNORECASE)
    conjunction_patterns2 = re.compile(r'\b(or)\b', re.IGNORECASE)
    pre_post_fix = re.compile(r"(adult[as]?|tortere|juvenil[e]?|maybe|\(?torete[s]?\)?)", re.IGNORECASE)
    delimiters_regex = re.compile(r"[,;/|&]+", re.IGNORECASE)                          # Removed '-'
    delimiters_regex1 = re.compile(r"[\[\]\(\)\?\#:`]+", re.IGNORECASE)                # Removed '-'
    delimiters_regex2 = re.compile(r"[+.,]+", re.IGNORECASE)
    delimiters_regex3 = re.compile(r"\s\s", re.IGNORECASE)
    pattern = r"(\d+)\s*([\w-]+)|([\w-]+)\s*(\d+)"
    counts_template = {value: 0 for value in mapping_dict.values()}
    mapping_count = counts_template.copy()
    term = term.lower().strip()  # Convert to lowercase and remove extra spaces
    term=conjunction_patterns1.sub(',', term)  # Replace "and/or/y" with a comma
    term=conjunction_patterns2.sub('', term)  # Replace "and/or/y" with a comma
    term=delimiters_regex.sub(',', term)  # Replace "and/or/y" with a comma
    term=delimiters_regex1.sub(' ', term)  # Replace "and/or/y" with a comma
    term=delimiters_regex3.sub(' ', term)  # Replace "and/or/y" with a comma
    if term not in mapping_dict:
        #cleaned_row = re.sub(r"[+.,]", " ", term)
        terms = delimiters_regex2.split(term)
        for term in terms:
            cleaned_row = re.sub(r"[+.,]", " ", term)
            matches = re.findall(pattern, cleaned_row)
            if matches:
                for match in matches:
                    number1, term1, term2, number2 = match
                    term = term1 if term1 else term2  # Choose the non-empty term
                    term = preprocess_term(term.strip())  # Normalize to lowercase
                    if term in mapping_set:
                        termX = mapping_dict[term]
                    else:
                        term = preprocess_term(pre_post_fix.sub('', term))  # Replace "and/or/y" with a comma
                        if term in mapping_set:
                            termX = mapping_dict[term]
                        else:                                               # Unmapped 
                            termX = "unknown"
     #               print("\t".join([term,termX]))
                    records.append({"Term": term, "TermID": termX})
            else:
                terms = delimiters_regex2.split(term)
                for term in terms:
                    term = preprocess_term(term.strip())  # Normalize to lowercase
                    if term in mapping_set:
                        termX = mapping_dict[term]
                    else:
                        term = preprocess_term(pre_post_fix.sub('', term))  # Replace "and/or/y" with a comma
                        if term in mapping_set:
                            termX = mapping_dict[term]
                        else:                                               # Unmapped
                            termX = "unknown"
    #                print("\t".join([term,termX]))
                    records.append({"Term": term, "TermID": termX})
    else:
        termX = mapping_dict[term]
        #print("\t".join([term,termX]))
        records.append({"Term": term, "TermID": termX})
    return pd.DataFrame(records)

# Functions for mapping biological gender - Match the biological gender values
def map_terms_to_valuesX(term,mapping_dict):
    print(term)
    # conjunction patterns 
    conjunction_patterns1 = re.compile(r'\b(and|y)\b', re.IGNORECASE)
    conjunction_patterns2 = re.compile(r'\b(or)\b', re.IGNORECASE)
    # pre_post fixes 
    pre_post_fix = re.compile(r"(adult[as]?|tortere|juvenil[e]?|maybe|\(?torete[s]?\)?)", re.IGNORECASE)
    # delimiters to be considered
    delimiters_regex = re.compile(r"[,;/|&]+", re.IGNORECASE)                          # Removed '-'
    delimiters_regex1 = re.compile(r"[\[\]\(\)\?\#:`]+", re.IGNORECASE)                # Removed '-'
    delimiters_regex2 = re.compile(r"[+.,]+", re.IGNORECASE)
    delimiters_regex3 = re.compile(r"\s\s", re.IGNORECASE)
    # patterns for matching strings like "12 male, 3 female"
    pattern = r"(\d+)\s*([\w-]+)|([\w-]+)\s*(\d+)"
    # create a template dictionary from the values of mapping_dict
    counts_template = {value: 0 for value in mapping_dict.values()}
    mapping_count = counts_template.copy()
    term = term.lower().strip()  # Convert to lowercase and remove extra spaces
    term=conjunction_patterns1.sub(',', term)  # Replace "and/or/y" with a comma
    term=conjunction_patterns2.sub('', term)  # Replace "and/or/y" with a comma
    term=delimiters_regex.sub(',', term)  # Replace "and/or/y" with a comma
    term=delimiters_regex1.sub(' ', term)  # Replace "and/or/y" with a comma
    term=delimiters_regex3.sub(' ', term)  # Replace "and/or/y" with a comma
    # Replace delimiters (+, ., ,) with spaces for consistent splitting
    if term not in mapping_dict:
        #cleaned_row = re.sub(r"[+.,]", " ", term)
        terms = delimiters_regex2.split(term)
        for term in terms:
            cleaned_row = re.sub(r"[+.,]", " ", term)
            matches = re.findall(pattern, cleaned_row)
            if matches:
                for match in matches:
                    # Match groups: (number, term) or (term, number)
                    number1, term1, term2, number2 = match
                    term = term1 if term1 else term2  # Choose the non-empty term
                    count = number1 if number1 else number2  # Choose the non-empty number
                    term = preprocess_term(term.strip())  # Normalize to lowercase
                    count = int(count)  # Convert count to integer
                    #Find the corresponding key in reference_dict
                    if term in mapping_dict:
                        mapping_count[mapping_dict[term]]= mapping_count[mapping_dict[term]] + int(count)
                        termX = mapping_dict[term]
                        countX = mapping_count[mapping_dict[term]]
                    else:
                        term = preprocess_term(pre_post_fix.sub('', term))  # Replace "and/or/y" with a comma
                        if term in mapping_dict:
                            termX = mapping_dict[term]
                            countX = 1
                            mapping_count[mapping_dict[term]] = countX
                        else:                                               # Unmapped 
                            termX = "unknown"
                            countX = 1
                            if termX not in mapping_dict:
                                mapping_dict["unknown"] = 0
                                mapping_count[mapping_dict["unknown"]] = 0
                            else:
                                mapping_count[mapping_dict["unknown"]] = mapping_count[mapping_dict["unknown"]] + countX
            else:
                terms = delimiters_regex2.split(term)
                for term in terms:
                    term = preprocess_term(term.strip())  # Normalize to lowercase
                    if term in mapping_dict:
                        count = mapping_count[mapping_dict[term]]
                        mapping_count[mapping_dict[term]] = count + 1
                        termX = mapping_dict[term]
                        countX = mapping_count[mapping_dict[term]]
                    else:
                        term = preprocess_term(pre_post_fix.sub('', term))  # Replace "and/or/y" with a comma
                        if term in mapping_dict:
                            count = mapping_count[mapping_dict[term]]
                            mapping_count[mapping_dict[term]] = count + 1
                            termX = mapping_dict[term]
                            countX = mapping_count[mapping_dict[term]]
                        else:                                               # Unmapped
                            termX = "unknown"
                            countX = 1
                            if termX not in mapping_dict:
                                mapping_dict["unknown"] = 0
                                mapping_count[mapping_dict["unknown"]] = 0
                            else:
                                mapping_count[mapping_dict["unknown"]] = mapping_count[mapping_dict["unknown"]] + countX
            #print(termX, "\t", countX)
    else:
        mapping_count[mapping_dict[term]] = 1
        termX = mapping_dict[term]
        countX = 1
        mapping_count[mapping_dict[term]] = countX
        #print(termX, "\t", countX)
    # Use dictionary comprehension to filter out pairs with value 0
    filtered_dict = {k: v for k, v in mapping_count.items() if v != 0}
    print(filtered_dict)


# Preprocessing functions - Lowercase, autocorrect, and remove extra characters (plural)
def preprocess_term(term):
    term = term.lower().strip()  # Convert to lowercase and remove extra spaces
    if "mono" not in term and "auto" not in term:
        if term.endswith('s'):
            term = term[:-1]  # Remove trailing 's' to handle plurals
    return term


# Functions for mapping biological gender - Match the biological gender values
def map_terms_to_values(term):
    #dataFile = "../data/globi/correctedBiologicalSexNames.tsv"
    mapDf = pd.read_csv(bsFileName, sep="\t", quoting=3, dtype=str)
    mapping_dict = dict(zip(mapDf['input'].str.lower(), mapDf['output']))
    
    # conjunction patterns 
    conjunction_patterns1 = re.compile(r'\b(and|y)\b', re.IGNORECASE)
    conjunction_patterns2 = re.compile(r'\b(or)\b', re.IGNORECASE)

    # pre_post fixes 
    pre_post_fix = re.compile(r"(adult[as]?|tortere|juvenil[e]?|maybe|\(?torete[s]?\)?)", re.IGNORECASE)

    # delimiters to be considered
    delimiters_regex = re.compile(r"[,;/|&]+", re.IGNORECASE)                          # Removed '-'
    delimiters_regex1 = re.compile(r"[\[\]\(\)\?\#:`]+", re.IGNORECASE)                # Removed '-'
    delimiters_regex2 = re.compile(r"[+.,]+", re.IGNORECASE)
    delimiters_regex3 = re.compile(r"\s\s", re.IGNORECASE)

    # patterns for matching strings like "12 male, 3 female"
    pattern = r"(\d+)\s*([\w-]+)|([\w-]+)\s*(\d+)"

    # create a template dictionary from the values of mapping_dict
    counts_template = {value: 0 for value in mapping_dict.values()}

    mapping_count = counts_template.copy()
    term = term.lower().strip()  # Convert to lowercase and remove extra spaces
    term=conjunction_patterns1.sub(',', term)  # Replace "and/or/y" with a comma
    term=conjunction_patterns2.sub('', term)  # Replace "and/or/y" with a comma
    term=delimiters_regex.sub(',', term)  # Replace "and/or/y" with a comma
    term=delimiters_regex1.sub(' ', term)  # Replace "and/or/y" with a comma
    term=delimiters_regex3.sub(' ', term)  # Replace "and/or/y" with a comma

    # Replace delimiters (+, ., ,) with spaces for consistent splitting
    if term not in mapping_dict:
        #cleaned_row = re.sub(r"[+.,]", " ", term)
        terms = delimiters_regex2.split(term)
        for term in terms:
            cleaned_row = re.sub(r"[+.,]", " ", term)
            matches = re.findall(pattern, cleaned_row)
            if matches:
                for match in matches:
                    # Match groups: (number, term) or (term, number)
                    number1, term1, term2, number2 = match
                    term = term1 if term1 else term2  # Choose the non-empty term
                    count = number1 if number1 else number2  # Choose the non-empty number
                    term = preprocess_term(term.strip())  # Normalize to lowercase
                    count = int(count)  # Convert count to integer
                    #Find the corresponding key in reference_dict
                    if term in mapping_dict:
                        mapping_count[mapping_dict[term]]= mapping_count[mapping_dict[term]] + int(count)
                        termX = mapping_dict[term]
                        countX = mapping_count[mapping_dict[term]]
                    else:
                        term = preprocess_term(pre_post_fix.sub('', term))  # Replace "and/or/y" with a comma
                        if term in mapping_dict:
                            termX = mapping_dict[term]
                            countX = 1
                            mapping_count[mapping_dict[term]] = countX
                        else:                                               # Unmapped 
                            termX = "unknown"
                            countX = 1
                            mapping_count[mapping_dict["unknown"]] = mapping_count[mapping_dict["unknown"]] + countX
            else:
                terms = delimiters_regex2.split(term)
                for term in terms:
                    term = preprocess_term(term.strip())  # Normalize to lowercase
                    if term in mapping_dict:
                        count = mapping_count[mapping_dict[term]]
                        mapping_count[mapping_dict[term]] = count + 1
                        termX = mapping_dict[term]
                        countX = mapping_count[mapping_dict[term]]
                    else:
                        term = preprocess_term(pre_post_fix.sub('', term))  # Replace "and/or/y" with a comma
                        if term in mapping_dict:
                            count = mapping_count[mapping_dict[term]]
                            mapping_count[mapping_dict[term]] = count + 1
                            termX = mapping_dict[term]
                            countX = mapping_count[mapping_dict[term]]
                        else:                                               # Unmapped
                            termX = "unknown"
                            countX = 1
                            mapping_count[mapping_dict["unknown"]] = mapping_count[mapping_dict["unknown"]] + countX
            #print(termX, "\t", countX)
    else:
        mapping_count[mapping_dict[term]] = 1
        termX = mapping_dict[term]
        countX = 1
        mapping_count[mapping_dict[term]] = countX
        #print(termX, "\t", countX)
    # Use dictionary comprehension to filter out pairs with value 0
    filtered_dict = {k: v for k, v in mapping_count.items() if v != 0}
    #print(filtered_dict)
    return filtered_dict
