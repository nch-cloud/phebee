#!/usr/bin/env python3

import json
from phebee.utils.sparql import execute_query

def debug_subject_data(subject_iri):
    """Debug what data exists for a subject in Neptune"""
    
    # Query 1: What's directly connected to the subject?
    query1 = f"""
    PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>
    SELECT ?p ?o WHERE {{
        GRAPH <http://ods.nationwidechildrens.org/phebee/subjects> {{
            <{subject_iri}> ?p ?o .
        }}
    }}
    """
    
    print("=== Direct properties of subject ===")
    result1 = execute_query(query1)
    for row in result1["results"]["bindings"]:
        print(f"{row['p']['value']} -> {row['o']['value']}")
    
    # Query 2: What encounters exist for this subject?
    query2 = f"""
    PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>
    SELECT ?encounter WHERE {{
        GRAPH <http://ods.nationwidechildrens.org/phebee/subjects> {{
            <{subject_iri}> phebee:hasEncounter ?encounter .
        }}
    }}
    """
    
    print("\n=== Encounters for subject ===")
    result2 = execute_query(query2)
    encounters = []
    for row in result2["results"]["bindings"]:
        encounter = row['encounter']['value']
        encounters.append(encounter)
        print(f"Encounter: {encounter}")
    
    # Query 3: What notes exist for these encounters?
    if encounters:
        encounter_filter = " ".join([f"<{enc}>" for enc in encounters])
        query3 = f"""
        PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>
        SELECT ?note ?encounter WHERE {{
            GRAPH <http://ods.nationwidechildrens.org/phebee/subjects> {{
                ?note phebee:hasEncounter ?encounter .
                FILTER(?encounter IN ({encounter_filter}))
            }}
        }}
        """
        
        print("\n=== Notes for encounters ===")
        result3 = execute_query(query3)
        notes = []
        for row in result3["results"]["bindings"]:
            note = row['note']['value']
            encounter = row['encounter']['value']
            notes.append(note)
            print(f"Note: {note} -> Encounter: {encounter}")
        
        # Query 4: What term links exist for these notes?
        if notes:
            note_filter = " ".join([f"<{note}>" for note in notes])
            query4 = f"""
            PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>
            SELECT ?termlink ?note ?term WHERE {{
                GRAPH <http://ods.nationwidechildrens.org/phebee/subjects> {{
                    ?termlink a phebee:TermLink ;
                              phebee:sourceNode ?note ;
                              phebee:hasTerm ?term .
                    FILTER(?note IN ({note_filter}))
                }}
            }}
            """
            
            print("\n=== TermLinks for notes ===")
            result4 = execute_query(query4)
            for row in result4["results"]["bindings"]:
                termlink = row['termlink']['value']
                note = row['note']['value']
                term = row['term']['value']
                print(f"TermLink: {termlink}")
                print(f"  Source: {note}")
                print(f"  Term: {term}")

if __name__ == "__main__":
    # Use the subject IRI from the last test run
    subject_iri = "http://ods.nationwidechildrens.org/phebee/subjects/86560b0c-085e-4dbf-8ff5-cc3a6f687f9c"
    debug_subject_data(subject_iri)
