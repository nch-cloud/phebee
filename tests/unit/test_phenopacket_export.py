"""
Unit tests for Phenopacket export, and for what survives an export/import round trip.

The export function had no tests, and it was reading a "term_links" key that no live
producer emits. Because a missing key yields an empty list, every exported packet came
out structurally valid with phenotypicFeatures: [] -- no phenotype content at all. The
producer shape used here (PRODUCER_SUBJECT) is copied from what
query_subjects_by_project actually returns, so these tests fail if the two drift apart
again.
"""
import json
import sys
import os
import zipfile

import pytest

# Mirror test_phenopacket_processing.py: the import-side module lives under functions/
# and pulls in aws_lambda_powertools at module scope.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../functions'))
from unittest.mock import MagicMock
sys.modules.setdefault('aws_lambda_powertools', MagicMock())

import phebee.utils.phenopackets as phenopackets_module  # noqa: E402
from phebee.utils.phenopackets import (  # noqa: E402
    subjects_to_phenopackets,
    zip_phenopackets,
)
from process_phenopacket import create_evidence_payload  # noqa: E402


HPO_VERSION = "v2026-01-08"
MONDO_VERSION = "v2026-01-06"
PROJECT_IRI = "http://ods.nationwidechildrens.org/phebee/projects/proj1"

SEIZURE_IRI = "http://purl.obolibrary.org/obo/HP_0001250"
DIABETES_IRI = "http://purl.obolibrary.org/obo/MONDO_0005148"


def make_phenotype(iri, term_id, label, qualifiers=None):
    """One phenotype in the shape query_subjects_by_project emits (iceberg.py)."""
    return {
        "term": {"iri": iri, "id": term_id, "label": label},
        "qualifiers": qualifiers if qualifiers is not None else [],
        "termlink_id": "abc123",
        "evidence_count": 2,
        "first_evidence_date": "2024-01-01",
        "last_evidence_date": "2024-02-01",
    }


def make_subject(project_subject_id="S1", phenotypes=None):
    return {
        "subject_iri": "http://ods.nationwidechildrens.org/phebee/subjects/uuid-1",
        "project_subject_iri": f"{PROJECT_IRI}#{project_subject_id}",
        "project_subject_id": project_subject_id,
        "phenotypes": phenotypes if phenotypes is not None else [
            make_phenotype(SEIZURE_IRI, "HP:0001250", "Seizure")
        ],
    }


def export(subjects):
    return subjects_to_phenopackets(subjects, PROJECT_IRI, HPO_VERSION, MONDO_VERSION)


class TestProducerShape:
    """The export must consume what query_subjects_by_project produces."""

    def test_producer_phenotypes_reach_the_packet(self):
        """Regression: two phenotypes in, two phenotypicFeatures out."""
        subject = make_subject(phenotypes=[
            make_phenotype(SEIZURE_IRI, "HP:0001250", "Seizure"),
            make_phenotype(DIABETES_IRI, "MONDO:0005148", "type 2 diabetes mellitus"),
        ])
        packet = export([subject])[0]
        assert len(packet["phenotypicFeatures"]) == 2

    def test_term_is_compacted_to_curie(self):
        packet = export([make_subject()])[0]
        assert packet["phenotypicFeatures"][0]["type"]["id"] == "HP:0001250"

    def test_label_is_carried(self):
        packet = export([make_subject()])[0]
        assert packet["phenotypicFeatures"][0]["type"]["label"] == "Seizure"

    def test_subject_ids_are_set(self):
        packet = export([make_subject("PT-7")])[0]
        assert packet["id"] == "PT-7"
        assert packet["subject"]["id"] == "PT-7"

    def test_metadata_versions(self):
        packet = export([make_subject()])[0]
        versions = {r["id"]: r["version"] for r in packet["metaData"]["resources"]}
        assert versions == {"hp": HPO_VERSION, "mondo": MONDO_VERSION}

    def test_no_evidence_key_emitted(self):
        """The producer supplies counts, not evidence records, so none is invented."""
        packet = export([make_subject()])[0]
        assert "evidence" not in packet["phenotypicFeatures"][0]

    def test_multiple_subjects(self):
        packets = export([make_subject("S1"), make_subject("S2")])
        assert [p["id"] for p in packets] == ["S1", "S2"]
        assert all(len(p["phenotypicFeatures"]) == 1 for p in packets)

    def test_mixed_hp_and_mondo(self):
        subject = make_subject(phenotypes=[
            make_phenotype(SEIZURE_IRI, "HP:0001250", "Seizure"),
            make_phenotype(DIABETES_IRI, "MONDO:0005148", "type 2 diabetes mellitus"),
        ])
        packet = export([subject])[0]
        ids = [f["type"]["id"] for f in packet["phenotypicFeatures"]]
        assert ids == ["HP:0001250", "MONDO:0005148"]


class TestNegation:
    """A negated assertion must not export as a present phenotype."""

    def test_negated_sets_excluded(self):
        subject = make_subject(phenotypes=[
            make_phenotype(SEIZURE_IRI, "HP:0001250", "Seizure", ["negated:true"])
        ])
        packet = export([subject])[0]
        assert packet["phenotypicFeatures"][0]["excluded"] is True

    def test_negated_as_full_iri(self):
        qualifier = "http://ods.nationwidechildrens.org/phebee/qualifier/negated:true"
        subject = make_subject(phenotypes=[
            make_phenotype(SEIZURE_IRI, "HP:0001250", "Seizure", [qualifier])
        ])
        packet = export([subject])[0]
        assert packet["phenotypicFeatures"][0]["excluded"] is True

    def test_unqualified_omits_excluded(self):
        """Phenopacket treats a missing 'excluded' as present; don't write false."""
        packet = export([make_subject()])[0]
        assert "excluded" not in packet["phenotypicFeatures"][0]

    def test_other_qualifiers_do_not_set_excluded(self):
        subject = make_subject(phenotypes=[
            make_phenotype(SEIZURE_IRI, "HP:0001250", "Seizure", ["onset:HP:0003593"])
        ])
        packet = export([subject])[0]
        assert "excluded" not in packet["phenotypicFeatures"][0]

    def test_negated_alongside_other_qualifiers(self):
        subject = make_subject(phenotypes=[
            make_phenotype(SEIZURE_IRI, "HP:0001250", "Seizure",
                           ["onset:HP:0003593", "negated:true"])
        ])
        packet = export([subject])[0]
        assert packet["phenotypicFeatures"][0]["excluded"] is True

    @pytest.mark.parametrize("qualifiers,expected", [
        (None, False),
        ([], False),
        (["negated"], True),
        (["negated:true"], True),
        (["negatedly:true"], False),
        (["family:true"], False),
        (["hypothetical:true"], False),
    ])
    def test_is_negated(self, qualifiers, expected):
        assert phenopackets_module._is_negated(qualifiers) is expected

    def test_hypothetical_and_family_are_unmarked(self):
        """
        Documented limitation: Phenopacket v2 has no slot for these, so they export
        without a marker. Callers that must not emit them pass include_qualified=False.
        """
        subject = make_subject(phenotypes=[
            make_phenotype(SEIZURE_IRI, "HP:0001250", "Seizure", ["family:true"])
        ])
        feature = export([subject])[0]["phenotypicFeatures"][0]
        assert "excluded" not in feature
        assert feature["type"]["id"] == "HP:0001250"


class TestOptionalAndMissingFields:
    def test_no_phenotypes_yields_empty_features(self):
        packet = export([make_subject(phenotypes=[])])[0]
        assert packet["phenotypicFeatures"] == []
        assert packet["metaData"]["phenopacketSchemaVersion"] == "2.0"

    def test_null_phenotypes_treated_as_empty(self):
        subject = make_subject()
        subject["phenotypes"] = None
        assert export([subject])[0]["phenotypicFeatures"] == []

    def test_missing_label_is_none(self):
        phenotype = make_phenotype(SEIZURE_IRI, "HP:0001250", None)
        del phenotype["term"]["label"]
        packet = export([make_subject(phenotypes=[phenotype])])[0]
        assert packet["phenotypicFeatures"][0]["type"]["label"] is None

    def test_missing_qualifiers_key(self):
        phenotype = make_phenotype(SEIZURE_IRI, "HP:0001250", "Seizure")
        del phenotype["qualifiers"]
        packet = export([make_subject(phenotypes=[phenotype])])[0]
        assert "excluded" not in packet["phenotypicFeatures"][0]

    def test_empty_term_dict(self):
        packet = export([make_subject(phenotypes=[{"term": {}, "qualifiers": []}])])[0]
        assert packet["phenotypicFeatures"][0]["type"] == {"id": None, "label": None}

    def test_null_term(self):
        packet = export([make_subject(phenotypes=[{"term": None, "qualifiers": []}])])[0]
        assert packet["phenotypicFeatures"][0]["type"]["id"] is None

    def test_non_obo_iri_passes_through(self):
        iri = "http://example.org/ontology/term123"
        phenotype = make_phenotype(iri, "term123", "Custom term")
        packet = export([make_subject(phenotypes=[phenotype])])[0]
        assert packet["phenotypicFeatures"][0]["type"]["id"] == iri

    def test_empty_subject_list(self):
        assert export([]) == []

    def test_aggregate_fields_are_not_emitted(self):
        """evidence_count and termlink_id are internal, not Phenopacket fields."""
        feature = export([make_subject()])[0]["phenotypicFeatures"][0]
        assert set(feature) <= {"type", "excluded"}


class TestInvalidInput:
    def test_legacy_term_links_shape_is_rejected(self):
        """
        The precise bug this guards: the old shape used to yield an empty packet
        silently. It must now fail loudly.
        """
        legacy = {
            "project_subject_id": "S1",
            "term_links": [{"term_iri": SEIZURE_IRI, "term_label": "Seizure"}],
        }
        with pytest.raises(ValueError, match="legacy 'term_links'"):
            export([legacy])

    def test_missing_phenotypes_key_raises(self):
        with pytest.raises(KeyError):
            export([{"project_subject_id": "S1"}])

    def test_missing_project_subject_id_raises(self):
        subject = make_subject()
        del subject["project_subject_id"]
        with pytest.raises(KeyError):
            export([subject])

    def test_one_bad_subject_does_not_pass_silently(self):
        with pytest.raises((KeyError, ValueError)):
            export([make_subject("S1"), {"project_subject_id": "S2"}])


class TestZipOutput:
    def test_zip_contains_one_file_per_subject_with_features(self):
        subjects = [
            make_subject("S1"),
            make_subject("S2", phenotypes=[
                make_phenotype(DIABETES_IRI, "MONDO:0005148", "type 2 diabetes mellitus")
            ]),
        ]
        buffer = zip_phenopackets(export(subjects))
        with zipfile.ZipFile(buffer) as archive:
            assert sorted(archive.namelist()) == ["S1.json", "S2.json"]
            packet = json.loads(archive.read("S2.json"))
        assert packet["phenotypicFeatures"][0]["type"]["id"] == "MONDO:0005148"


class TestRoundTrip:
    """
    Export, then feed the packet back through the import mapper and compare. This is
    only meaningful once the export emits features, which is why it could not be
    written before the shape fix.
    """

    def reimport(self, packet):
        return create_evidence_payload("proj1", packet["subject"]["id"], packet)

    def test_term_iris_preserved(self):
        subject = make_subject(phenotypes=[
            make_phenotype(SEIZURE_IRI, "HP:0001250", "Seizure"),
            make_phenotype(DIABETES_IRI, "MONDO:0005148", "type 2 diabetes mellitus"),
        ])
        records = self.reimport(export([subject])[0])
        assert [r["term_iri"] for r in records] == [SEIZURE_IRI, DIABETES_IRI]

    def test_feature_count_preserved(self):
        subject = make_subject(phenotypes=[
            make_phenotype(SEIZURE_IRI, "HP:0001250", "Seizure"),
            make_phenotype(DIABETES_IRI, "MONDO:0005148", "type 2 diabetes"),
        ])
        assert len(self.reimport(export([subject])[0])) == 2

    def test_negation_preserved(self):
        subject = make_subject(phenotypes=[
            make_phenotype(SEIZURE_IRI, "HP:0001250", "Seizure", ["negated:true"])
        ])
        records = self.reimport(export([subject])[0])
        assert records[0]["qualifiers"] == ["negated"]

    def test_absence_of_negation_preserved(self):
        records = self.reimport(export([make_subject()])[0])
        assert records[0]["qualifiers"] == []

    def test_negation_preserved_from_iri_form(self):
        qualifier = "http://ods.nationwidechildrens.org/phebee/qualifier/negated:true"
        subject = make_subject(phenotypes=[
            make_phenotype(SEIZURE_IRI, "HP:0001250", "Seizure", [qualifier])
        ])
        records = self.reimport(export([subject])[0])
        assert records[0]["qualifiers"] == ["negated"]

    def test_subject_id_preserved(self):
        packet = export([make_subject("PT-42")])[0]
        assert packet["subject"]["id"] == "PT-42"
        assert self.reimport(packet)[0]["subject_id"] == "proj1#PT-42"

    def test_empty_subject_round_trips_to_no_evidence(self):
        records = self.reimport(export([make_subject(phenotypes=[])])[0])
        assert records == []

    def test_creator_comes_from_metadata(self):
        """Documented boundary: import attributes to metaData.createdBy, not per-record."""
        records = self.reimport(export([make_subject()])[0])
        assert records[0]["creator_id"] == "PheBee"
        assert records[0]["creator_type"] == "system"

    def test_evidence_type_is_not_round_tripped(self):
        """
        Documented loss: import stamps every feature as ECO_0000311 (imported
        information) rather than recovering the original evidence type, which the
        export does not carry either.
        """
        records = self.reimport(export([make_subject()])[0])
        assert records[0]["evidence_type"] == "http://purl.obolibrary.org/obo/ECO_0000311"

    def test_labels_are_not_round_tripped(self):
        """Import keeps IRIs only; labels are resolved from the ontology, not the packet."""
        records = self.reimport(export([make_subject()])[0])
        assert "term_label" not in records[0]
