"""
Unit tests for null handling in the bulk processor's context qualifiers.

A null or absent context flag means the same as false. This is a load-bearing
guarantee, not a tolerance: it is what keeps the qualifier set forward-compatible.

Only non-false qualifiers enter the hash - normalize_qualifiers drops false values
rather than encoding them. So when a new context is added in future, evidence loaded
before it existed keeps its original evidence_id: the new flag is absent, absent
means false, and false contributes nothing. If nulls hashed differently from false,
adding a context would re-hash the entire historical corpus.

It also has to hold for records written today, since the stored qualifiers array
writes "false" for a null. If the hash disagreed, the same assertion loaded with
nulls would get a different evidence_id than one loaded with explicit false values,
the two rows would never deduplicate, and the data would look identical while the
hashes diverged - a silent failure.

Unlike test_bulk_hash_consistency.py, this imports the real wrappers from
bulk_evidence_processor.py rather than copying them, so it cannot drift. PySpark is
stubbed because the wrappers don't use it - only the module-level imports do.
"""

import importlib.util
import sys
import types
from pathlib import Path

import pytest


_PYSPARK_MODULES = (
    "pyspark",
    "pyspark.sql",
    "pyspark.sql.functions",
    "pyspark.sql.types",
    "pyspark.sql.utils",
    "pyspark.storagelevel",
)


def _load_bulk_processor():
    """
    Import bulk_evidence_processor with PySpark stubbed out.

    The hash wrappers under test are pure Python. Stubbing lets this run in the
    unit suite (no Spark install) while still exercising the shipped code.
    """
    stubbed = []
    if "pyspark" not in sys.modules:
        for name in _PYSPARK_MODULES:
            module = types.ModuleType(name)
            # Any attribute resolves to a no-op callable, so `from pyspark.sql.functions
            # import <anything>` succeeds without enumerating every name used.
            module.__getattr__ = lambda _name: (lambda *a, **kw: None)
            sys.modules[name] = module
            stubbed.append(name)
        sys.modules["pyspark.sql"].SparkSession = object
        sys.modules["pyspark.sql"].DataFrame = object
        sys.modules["pyspark"].StorageLevel = object
        sys.modules["pyspark.storagelevel"].StorageLevel = object

    script = Path(__file__).resolve().parents[2] / "scripts" / "bulk_evidence_processor.py"
    spec = importlib.util.spec_from_file_location("bulk_evidence_processor_under_test", script)
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    finally:
        for name in stubbed:
            sys.modules.pop(name, None)

    return module


bulk = _load_bulk_processor()

TERM_IRI = "http://purl.obolibrary.org/obo/HP_0001250"
SUBJECT_ID = "test-subject-123"

# Values that must all mean "this qualifier does not apply"
FALSEY = [None, False, "false", 0, 0.0]


def evidence_hash(negated, family, hypothetical):
    """Evidence hash for a note-less curated record with the given contexts."""
    return bulk.create_evidence_hash_wrapper(
        SUBJECT_ID,
        None,  # clinical_note_id
        None,  # encounter_id
        TERM_IRI,
        None,  # span_start
        None,  # span_end
        negated,
        family,
        hypothetical,
        "jsmith@example.org",
    )


def termlink_hash(negated, family, hypothetical):
    return bulk.create_termlink_hash_wrapper(
        SUBJECT_ID, TERM_IRI, family, hypothetical, negated
    )


@pytest.mark.parametrize("falsey", FALSEY)
def test_evidence_hash_treats_falsey_contexts_alike(falsey):
    """Every false-y context representation yields the same evidence_id."""
    baseline = evidence_hash(False, False, False)
    assert evidence_hash(falsey, falsey, falsey) == baseline, \
        f"contexts of {falsey!r} should hash the same as False"


@pytest.mark.parametrize("falsey", FALSEY)
def test_termlink_hash_treats_falsey_contexts_alike(falsey):
    """Every false-y context representation yields the same termlink_id."""
    baseline = termlink_hash(False, False, False)
    assert termlink_hash(falsey, falsey, falsey) == baseline, \
        f"contexts of {falsey!r} should hash the same as False"


def test_null_contexts_do_not_hash_as_the_string_none():
    """
    Regression: None once fell through to str(value) and was hashed as "None".

    That made a null context read as an *active* qualifier, so a record with null
    contexts got a different evidence_id than the identical all-false record.
    """
    assert evidence_hash(None, None, None) == evidence_hash(False, False, False)
    assert termlink_hash(None, None, None) == termlink_hash(False, False, False)


def test_true_contexts_still_change_the_hash():
    """The null fix must not collapse true into false."""
    baseline = evidence_hash(False, False, False)

    assert evidence_hash(True, False, False) != baseline
    assert evidence_hash(False, True, False) != baseline
    assert evidence_hash(False, False, True) != baseline

    assert termlink_hash(True, False, False) != termlink_hash(False, False, False)


def test_null_and_false_are_interchangeable_alongside_an_active_qualifier():
    """A negated assertion hashes the same whether its other contexts are null or false."""
    assert evidence_hash(True, None, None) == evidence_hash(True, False, False)
    assert termlink_hash(True, None, None) == termlink_hash(True, False, False)


def test_absent_contexts_hash_the_same_as_explicit_false():
    """
    A record that omits contexts entirely matches one that states all three false.

    This is the case Spark produces for evidence written before a context existed:
    the column is there, the value is null.
    """
    assert evidence_hash(None, None, None) == evidence_hash(False, False, False)
    # Partially stated - only negated given - must also match
    assert evidence_hash(False, None, None) == evidence_hash(False, False, False)
    assert termlink_hash(False, None, None) == termlink_hash(False, False, False)


def test_adding_a_new_false_context_does_not_change_the_hash():
    """
    Forward compatibility: introducing a new qualifier must not re-hash history.

    normalize_qualifiers drops false-valued qualifiers instead of encoding them, so a
    context added later and defaulted to false contributes nothing. Evidence loaded
    before that context existed keeps its original evidence_id.

    If this breaks, every historical record silently acquires a new identity the next
    time it is loaded, and nothing deduplicates against what is already stored.
    """
    today = bulk.normalize_qualifiers(["negated:false", "family:false", "hypothetical:false"])
    with_new_context = bulk.normalize_qualifiers(
        ["negated:false", "family:false", "hypothetical:false", "severity:false"]
    )

    def hash_for(qualifiers):
        return bulk.generate_evidence_hash(
            None, None, TERM_IRI, None, None, qualifiers, SUBJECT_ID, "jsmith@example.org"
        )

    assert hash_for(today) == hash_for(with_new_context), \
        "adding a new false-valued context changed the hash of an existing record"

    # But a new context that is actually set must still discriminate
    genuinely_set = bulk.normalize_qualifiers(
        ["negated:false", "family:false", "hypothetical:false", "severity:true"]
    )
    assert hash_for(today) != hash_for(genuinely_set)


def test_truthy_representations_agree():
    """1, 1.0, "true" and True are equivalent, mirroring the false-y set."""
    baseline = evidence_hash(True, False, False)
    for truthy in [True, "true", 1, 1.0]:
        assert evidence_hash(truthy, False, False) == baseline, \
            f"negated={truthy!r} should hash the same as True"
