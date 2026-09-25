"""
Pin the rebuild pipeline's rehash copy of the hash functions to the other two.

scripts/rebuild/rehash_evidence.py recomputes evidence_id and termlink_id for rows that
are already stored, so it must reproduce exactly the ids the API path
(phebee/utils/hash.py) and the bulk importer (scripts/bulk_evidence_processor.py)
wrote. It is a third copy that runs standalone on EMR, and unlike the bulk copy it was
not covered by any test, so a drift would only surface as a rebuild that silently
re-keys evidence.

These tests feed the rehash wrappers the qualifiers array exactly as each write path
stores it, and require the recomputed ids to match the ids that path produced.
"""

import importlib.util
import itertools
import sys
import types
from collections import namedtuple
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "layers" / "phebee-utils"))

from phebee.utils.hash import generate_evidence_hash, generate_termlink_hash  # noqa: E402
from phebee.utils.qualifier import Qualifier, normalize_qualifiers  # noqa: E402

_STUBBED_MODULES = (
    "pyspark",
    "pyspark.sql",
    "pyspark.sql.functions",
    "pyspark.sql.types",
    "pyspark.sql.utils",
    "pyspark.storagelevel",
    "boto3",
)


def _load_script(relative_path, module_name):
    """
    Import an EMR script with its AWS/Spark dependencies stubbed.

    Mirrors the loader in test_qualifier_canonicalization.py. The hash logic under test is
    pure Python; only the module-level imports need Spark and boto3.
    """
    stubbed = []
    for name in _STUBBED_MODULES:
        if name not in sys.modules:
            module = types.ModuleType(name)
            module.__getattr__ = lambda _name: (lambda *a, **kw: None)
            sys.modules[name] = module
            stubbed.append(name)
    sys.modules["pyspark.sql"].SparkSession = object
    sys.modules["pyspark.sql"].DataFrame = object
    sys.modules["pyspark"].StorageLevel = object
    sys.modules["pyspark.storagelevel"].StorageLevel = object

    spec = importlib.util.spec_from_file_location(module_name, REPO_ROOT / relative_path)
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    finally:
        for name in stubbed:
            sys.modules.pop(name, None)
    return module


rehash = _load_script("scripts/rebuild/rehash_evidence.py", "rehash_for_parity")
bulk = _load_script("scripts/bulk_evidence_processor.py", "bulk_for_rehash_parity")

# The stored qualifiers column is an array of structs; Spark hands the UDF Row objects,
# which expose the fields as attributes.
StoredQualifier = namedtuple("StoredQualifier", "qualifier_type qualifier_value")

SUBJECT_ID = "3f2a9c1e-7b4d-4e8a-9c6f-1d2e3f4a5b6c"
SUBJECT_IRI = f"http://ods.nationwidechildrens.org/phebee/subjects/{SUBJECT_ID}"
TERM_IRI = "http://purl.obolibrary.org/obo/HP_0001250"

# Every spelling a producer or API client might send for a boolean context flag.
SPELLINGS = [True, False, "true", "True", "false", "False", 1, 0, 1.0, 0.0, "1", "0", None]


def _stored_by_api(qualifiers):
    """The qualifiers column as create_evidence_record writes it: active, canonical, sorted."""
    normalized = normalize_qualifiers(qualifiers)
    if not normalized:
        return None
    return [StoredQualifier(**q.to_storage_dict()) for q in normalized]


def _stored_by_bulk(negated, family, hypothetical):
    """
    The qualifiers column as the bulk importer writes it: all three flags, always present.

    stored_context_flag renders a set flag as "true" and everything else, including null
    and an absent key, as "false"; the hash side agrees through normalize_qualifier_value.
    """
    def flag(value):
        return "true" if bulk.normalize_qualifier_value(value) else "false"

    return [
        StoredQualifier("negated", flag(negated)),
        StoredQualifier("family", flag(family)),
        StoredQualifier("hypothetical", flag(hypothetical)),
    ]


def test_rehash_reproduces_api_ids():
    mismatches = []
    for negated, family, hypothetical in itertools.product(SPELLINGS, repeat=3):
        qualifiers = [
            q for q in (
                Qualifier.from_raw("negated", negated),
                Qualifier.from_raw("family", family),
                Qualifier.from_raw("hypothetical", hypothetical),
            ) if q is not None
        ]
        normalized = normalize_qualifiers(qualifiers)
        stored = _stored_by_api(qualifiers)

        termlink_matches = rehash.create_termlink_hash_wrapper(SUBJECT_ID, TERM_IRI, stored) == \
            generate_termlink_hash(SUBJECT_IRI, TERM_IRI, normalized)
        evidence_matches = rehash.create_evidence_hash_wrapper(
            SUBJECT_ID, "note-1", "enc-1", TERM_IRI, 3, 9, stored, "creator-1"
        ) == generate_evidence_hash(
            clinical_note_id="note-1", encounter_id="enc-1", term_iri=TERM_IRI,
            span_start=3, span_end=9, qualifiers=normalized,
            subject_id=SUBJECT_ID, creator_id="creator-1",
        )
        if not (termlink_matches and evidence_matches):
            mismatches.append((negated, family, hypothetical))

    assert not mismatches, f"rehash disagrees with the API path for (negated, family, hypothetical): {mismatches}"


def test_rehash_normalizes_legacy_prefixed_qualifier_types():
    """
    Rows stored with PheBee-prefixed qualifier types rehash to the id of the short form.

    Neither write path stores the prefixed form today, but the rehash wrapper normalizes it
    so older rows land on the same termlink_id the API computes for the same assertion.
    """
    prefix = "http://ods.nationwidechildrens.org/phebee/qualifier/"
    stored = [StoredQualifier(f"{prefix}negated", "true"), StoredQualifier(f"{prefix}family", "false")]
    normalized = normalize_qualifiers([Qualifier(type="negated", value="true")])

    assert rehash.create_termlink_hash_wrapper(SUBJECT_ID, TERM_IRI, stored) == \
        generate_termlink_hash(SUBJECT_IRI, TERM_IRI, normalized)


def test_rehash_reproduces_api_ids_for_domain_qualifiers():
    """Non-boolean qualifier values keep their spelling through storage and rehash."""
    qualifiers = [
        q for q in (
            Qualifier.from_raw("severity", "Mild"),
            Qualifier.from_raw("http://purl.obolibrary.org/obo/HP_0012823", "present"),
            Qualifier.from_raw("negated", True),
        ) if q is not None
    ]
    assert len(qualifiers) == 3
    normalized = normalize_qualifiers(qualifiers)
    stored = _stored_by_api(qualifiers)

    assert rehash.create_termlink_hash_wrapper(SUBJECT_ID, TERM_IRI, stored) == \
        generate_termlink_hash(SUBJECT_IRI, TERM_IRI, normalized)


def test_rehash_reproduces_bulk_ids():
    mismatches = []
    for negated, family, hypothetical in itertools.product(SPELLINGS, repeat=3):
        stored = _stored_by_bulk(negated, family, hypothetical)

        termlink_matches = rehash.create_termlink_hash_wrapper(SUBJECT_ID, TERM_IRI, stored) == \
            bulk.create_termlink_hash_wrapper(SUBJECT_ID, TERM_IRI, family, hypothetical, negated)
        evidence_matches = rehash.create_evidence_hash_wrapper(
            SUBJECT_ID, "note-1", "enc-1", TERM_IRI, 3, 9, stored, "creator-1"
        ) == bulk.create_evidence_hash_wrapper(
            SUBJECT_ID, "note-1", "enc-1", TERM_IRI, 3, 9, negated, family, hypothetical, "creator-1"
        )
        if not (termlink_matches and evidence_matches):
            mismatches.append((negated, family, hypothetical))

    assert not mismatches, f"rehash disagrees with the bulk importer for (negated, family, hypothetical): {mismatches}"
