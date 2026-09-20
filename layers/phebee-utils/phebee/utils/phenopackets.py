from phebee.utils.aws import get_current_timestamp
import io
import json
import zipfile

def subjects_to_phenopackets(subject_data: list[dict], project_iri: str, hpo_version: str, mondo_version: str) -> list[dict]:
    """
    Convert subject records to Phenopacket v2-compatible dictionaries.

    Expects the subject shape produced by query_subjects_by_project: each subject
    carries "project_subject_id" and a "phenotypes" list, and each phenotype carries
    a nested "term" ("iri", "id", "label") plus "qualifiers" as "type:value" strings.

    A "negated" qualifier becomes Phenopacket's "excluded": true, which is the inverse
    of the import mapping in functions/process_phenopacket.py.

    Features qualified "hypothetical" or "family" are omitted entirely. Phenopacket v2
    has no slot for either, and a phenotypicFeature asserts something about the subject,
    so emitting one would claim the subject has a term that was recorded as uncertain or
    as a relative's. Nothing exportable is lost: a TermLink is identified by subject,
    term and qualifiers, so when the subject does have the term there is a separate
    unqualified TermLink, and that one exports on its own.

    No "evidence" is emitted. The producer supplies aggregates (evidence_count,
    first/last_evidence_date) rather than individual evidence records, and Phenopacket
    evidence is optional, so synthesizing an entry from a count would assert more than
    is known.
    """
    metadata = _build_metadata(hpo_version, mondo_version)
    phenopackets = []

    for subject in subject_data:
        phenotypes = _subject_phenotypes(subject)

        packet = {
            "id": subject["project_subject_id"],
            "subject": {
                "id": subject["project_subject_id"]
            },
            "phenotypicFeatures": [],
            "metaData": metadata
        }

        for phenotype in phenotypes:
            qualifiers = phenotype.get("qualifiers")
            if not _is_exportable(qualifiers):
                continue

            term = phenotype.get("term") or {}
            phenotypic_feature = {
                "type": {
                    "id": _compact_iri(term.get("iri")),
                    "label": term.get("label"),
                },
            }

            if _is_negated(qualifiers):
                phenotypic_feature["excluded"] = True

            packet["phenotypicFeatures"].append(phenotypic_feature)

        phenopackets.append(packet)

    return phenopackets


# Qualifier types arrive in either serialization, the bare name or the fully-qualified
# PheBee qualifier IRI, so both are listed. query_subjects_by_project's own
# include_qualified filter matches the same pairs.
QUALIFIER_IRI_PREFIX = "http://ods.nationwidechildrens.org/phebee/qualifier/"

NEGATED_QUALIFIER_TYPES = (
    "negated",
    f"{QUALIFIER_IRI_PREFIX}negated",
)

# Qualifiers that make a term unsafe to state as a phenotypicFeature at all, because the
# feature would read as an observation about the subject. See the docstring above: the
# subject's own terms arrive as separate unqualified TermLinks.
UNEXPORTABLE_QUALIFIER_TYPES = (
    "hypothetical",
    f"{QUALIFIER_IRI_PREFIX}hypothetical",
    "family",
    f"{QUALIFIER_IRI_PREFIX}family",
)


def _subject_phenotypes(subject: dict) -> list[dict]:
    """
    Read the phenotype list, refusing to silently export an empty packet.

    This function previously read a "term_links" key that no live producer emits, and
    because a missing key yields an empty list it returned structurally valid packets
    with no phenotypicFeatures at all. Absent the expected key, fail loudly instead.
    """
    if "phenotypes" in subject:
        return subject["phenotypes"] or []
    if "term_links" in subject:
        raise ValueError(
            "Subject carries the legacy 'term_links' key. Phenopacket export expects "
            "the 'phenotypes' shape from query_subjects_by_project."
        )
    raise KeyError(
        f"Subject {subject.get('project_subject_id')!r} has no 'phenotypes' key; "
        "cannot build phenotypicFeatures."
    )


def _has_qualifier_type(qualifiers, qualifier_types) -> bool:
    """
    True when any of the given qualifier types is present.

    Qualifiers arrive as "type:value" strings already filtered to active values, and the
    type half may itself contain colons when it is an IRI, so match the type as a whole
    rather than splitting on the first colon.
    """
    for qualifier in qualifiers or []:
        for qualifier_type in qualifier_types:
            if qualifier == qualifier_type or qualifier.startswith(f"{qualifier_type}:"):
                return True
    return False


def _is_negated(qualifiers) -> bool:
    """True when an active negated qualifier is present."""
    return _has_qualifier_type(qualifiers, NEGATED_QUALIFIER_TYPES)


def _is_exportable(qualifiers) -> bool:
    """
    False when the term must not be stated as a phenotypicFeature.

    Checked before negation, so a negated family-history term is omitted rather than
    exported as excluded: "the relative does not have X" is not "the subject does not
    have X".
    """
    return not _has_qualifier_type(qualifiers, UNEXPORTABLE_QUALIFIER_TYPES)


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