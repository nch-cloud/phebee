from phebee.utils.aws import get_current_timestamp
import io
import json
import zipfile

def subjects_to_phenopackets(subject_data: list[dict], project_iri: str, hpo_version: str, mondo_version: str) -> list[dict]:
    """
    Convert subject records to Phenopacket v2-compatible dictionaries.
    """
    metadata = _build_metadata(hpo_version, mondo_version)
    phenopackets = []

    for subject in subject_data:
        packet = {
            "id": subject["project_subject_id"],
            "subject": {
                "id": subject["project_subject_id"]
            },
            "phenotypicFeatures": [],
            "metaData": metadata
        }

        for term_link in subject.get("term_links", []):
            phenotypic_feature = {
                "type": {
                    "id": _compact_iri(term_link["term_iri"]),
                    "label": term_link.get("term_label"),
                },
            }

            evidence_list = []
            for evidence in term_link.get("evidence", []):
                ev = {}
                if "type" in evidence or "evidence_type" in evidence:
                    ev["type"] = {
                        "id": _compact_iri(evidence.get("evidence_type") or evidence.get("type"))
                    }
                if "created" in evidence:
                    ev["date"] = evidence["created"]
                if "creator" in evidence:
                    ev["reference"] = {
                        "id": evidence.get("creator", {}).get("creator_id") or evidence.get("creator", {}).get("name"),
                        "label": evidence.get("creator", {}).get("title") or evidence.get("creator", {}).get("type")
                    }
                if ev:
                    evidence_list.append(ev)

            if evidence_list:
                phenotypic_feature["evidence"] = evidence_list

            packet["phenotypicFeatures"].append(phenotypic_feature)

        phenopackets.append(packet)

    return phenopackets


def _compact_iri(iri: str | None) -> str:
    """
    Converts full IRI like http://purl.obolibrary.org/obo/HP_0001250 to HP:0001250.
    If not compactable, returns full IRI.
    """
    if not iri:
        return iri
    if iri.startswith("http://purl.obolibrary.org/obo/"):
        return iri.rsplit("/", 1)[-1].replace("_", ":")
    return iri

def _build_metadata(hpo_version: str, mondo_version: str) -> dict:
    return {
        "created": get_current_timestamp(),
        "createdBy": "PheBee",
        "resources": [
            {
                "id": "hp",
                "name": "Human Phenotype Ontology",
                "url": "http://purl.obolibrary.org/obo/hp.owl",
                "version": hpo_version,
                "namespacePrefix": "HP",
                "iriPrefix": "http://purl.obolibrary.org/obo/HP_"
            },
            {
                "id": "mondo",
                "name": "MONDO Disease Ontology",
                "url": "http://purl.obolibrary.org/obo/mondo.owl",
                "version": mondo_version,
                "namespacePrefix": "MONDO",
                "iriPrefix": "http://purl.obolibrary.org/obo/MONDO_"
            }
        ],
        "phenopacketSchemaVersion": "2.0"
    }

def zip_phenopackets(phenopacket_list: list):
    # Use BytesIO to store the ZIP file in memory
    zip_buffer = io.BytesIO()

    # Create a ZIP archive and add each phenopacket as a separate JSON file
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for phenopacket in phenopacket_list:
            # Use the subject ID as the filename for each phenopacket
            subject_id = phenopacket["subject"]["id"]
            json_data = json.dumps(phenopacket, indent=2)
            # Create a virtual file in the ZIP archive for each phenopacket
            zip_file.writestr(f"{subject_id}.json", json_data)

    # The final ZIP file is now stored in memory (in zip_buffer)
    zip_buffer.seek(0)
    return zip_buffer