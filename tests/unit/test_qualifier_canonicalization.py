"""
Unit tests for qualifier value canonicalization, and for API/bulk hash agreement.

Qualifier values arrive untyped from JSON: the same intent shows up as a bool, an
int, a float or a string. The bulk importer has always folded every truthy spelling
onto the single string "true", while the API handlers passed str(value) straight
into the hash. str(True) is "True", so the identical assertion got one evidence_id
when it arrived through the API and a different one when it arrived through bulk
import - and nothing surfaced the disagreement, because both ids look equally valid.

canonical_qualifier_value is now the single truth table both sides use. The bulk
importer runs standalone on EMR and cannot import the phebee layer, so its copy
(normalize_qualifier_value in scripts/bulk_evidence_processor.py) has to agree by
inspection. test_bulk_and_api_agree_on_every_spelling below imports the real bulk
wrapper and compares the two, so the pair cannot drift silently.
"""

import importlib.util
import sys
import types
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "layers" / "phebee-utils"))

from phebee.utils.qualifier import (  # noqa: E402
    Qualifier,
    canonical_qualifier_value,
    normalize_qualifiers,
)

_STUBBED_MODULES = (
    "pyspark",
    "pyspark.sql",
    "pyspark.sql.functions",
    "pyspark.sql.types",
    "pyspark.sql.utils",
    "pyspark.storagelevel",
    "boto3",
)


def _load_bulk_processor():
    """
    Import bulk_evidence_processor with its AWS/Spark dependencies stubbed.

    Mirrors the loader in test_bulk_context_null_handling.py. The qualifier logic
    under test is pure Python; only the module-level imports need Spark and boto3.
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

    script = REPO_ROOT / "scripts" / "bulk_evidence_processor.py"
    spec = importlib.util.spec_from_file_location("bulk_processor_for_parity", script)
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    finally:
        for name in stubbed:
            sys.modules.pop(name, None)
    return module


bulk = _load_bulk_processor()

# Every spelling a producer or API client might send for a boolean context flag.
# The capitalized strings are what f"negated:{flag}" produces from a Python bool -
# an easy client-side mistake that used to hash differently from a JSON boolean.
TRUTHY_SPELLINGS = [True, "true", "True", "TRUE", 1, 1.0, "1"]
FALSEY_SPELLINGS = [False, "false", "False", "FALSE", 0, 0.0, "0", None]

# Values that are not boolean spellings and must survive untouched, case included.
DOMAIN_VALUES = ["mild", "Mild", "HP:0003593", "Y"]


@pytest.mark.parametrize("value", TRUTHY_SPELLINGS)
def test_truthy_spellings_canonicalize_to_true(value):
    assert canonical_qualifier_value(value) == "true", \
        f"{value!r} should canonicalize to the string 'true'"


@pytest.mark.parametrize("value", FALSEY_SPELLINGS)
def test_falsey_spellings_are_inactive(value):
    assert canonical_qualifier_value(value) is None, \
        f"{value!r} should be dropped, not hashed"


def test_null_is_inactive_rather_than_the_string_none():
    """Regression: str(None) is "None", which read as an active qualifier."""
    assert canonical_qualifier_value(None) is None
    assert Qualifier.from_raw("negated", None) is None


@pytest.mark.parametrize("value", DOMAIN_VALUES)
def test_domain_values_pass_through_with_case_intact(value):
    """
    Canonicalization must not flatten non-boolean qualifiers.

    Only boolean spellings are matched case-insensitively. A severity of "Mild"
    is a distinct value, not a spelling of something else, so lowercasing it would
    change hashes for no reason.
    """
    assert canonical_qualifier_value(value) == value


def test_from_raw_normalizes_the_type_too():
    q = Qualifier.from_raw(
        "http://ods.nationwidechildrens.org/phebee/qualifier/negated", True
    )
    assert q == Qualifier(type="negated", value="true")


def test_from_raw_preserves_external_iri_types():
    q = Qualifier.from_raw("http://purl.obolibrary.org/obo/HP_0012823", "present")
    assert q == Qualifier(type="http://purl.obolibrary.org/obo/HP_0012823",
                          value="present")


SUBJECT_ID = "test-subject-123"
TERM_IRI = "http://purl.obolibrary.org/obo/HP_0001250"
SUBJECT_IRI = f"http://ods.nationwidechildrens.org/phebee/subjects/{SUBJECT_ID}"


def _api_termlink_hash(negated):
    """termlink_id as the API handlers now compute it."""
    from phebee.utils.hash import generate_termlink_hash

    q = Qualifier.from_raw("negated", negated)
    return generate_termlink_hash(
        SUBJECT_IRI, TERM_IRI, normalize_qualifiers([q] if q else [])
    )


def _bulk_termlink_hash(negated):
    """termlink_id as the shipped bulk UDF wrapper computes it."""
    return bulk.create_termlink_hash_wrapper(
        SUBJECT_ID, TERM_IRI, None, None, negated  # family, hypothetical, negated
    )


@pytest.mark.parametrize("value", TRUTHY_SPELLINGS + FALSEY_SPELLINGS + DOMAIN_VALUES)
def test_bulk_and_api_agree_on_every_spelling(value):
    """
    Identical input must yield an identical termlink_id from both paths.

    Both sides run their real shipped code - the bulk UDF wrapper and the API's
    hash path - rather than a reimplementation, so this fails if either drifts.

    If it fails, the same assertion gets a different id depending on whether bulk
    import or the API loaded it, so the API's existence check will not find
    bulk-loaded rows and will write duplicates instead of deduplicating.
    """
    assert _bulk_termlink_hash(value) == _api_termlink_hash(value), \
        f"termlink_id differs between bulk and API for negated={value!r}"


def test_truthy_spellings_all_reach_the_same_hash():
    """true, "true", 1, 1.0 and "1" describe one assertion, so one id."""
    hashes = {_api_termlink_hash(v) for v in TRUTHY_SPELLINGS}
    assert len(hashes) == 1, "truthy spellings produced more than one termlink_id"


def test_falsey_spellings_all_reach_the_same_hash():
    """false, "false", 0, 0.0, "0" and null describe one assertion, so one id."""
    hashes = {_api_termlink_hash(v) for v in FALSEY_SPELLINGS}
    assert len(hashes) == 1, "falsey spellings produced more than one termlink_id"


def test_truthy_and_falsey_remain_distinct():
    """Canonicalization must not collapse a set flag into an unset one."""
    assert _api_termlink_hash(True) != _api_termlink_hash(False)
