"""The Athena recompute of a subject's (or project's) materialized terms.

materialize_subject_terms runs whenever an existing subject is linked to
another project. Two defects there went unnoticed because the link handler
logged the failure as non-critical:
- evidence_count was COUNT(*) after the qualifier UNNEST, so each evidence row
  counted once per qualifier (x3 on the benchmark data);
- the by_project_term INSERT covered every (project, term) pair at once, and
  Athena fails an INSERT that opens more than 100 partitions. That ran after
  the DELETE, so a subject with more than 50 terms linked to a second project
  lost its by_project_term rows in both.

These tests pin the query shapes: distinct counting, INSERTs sized to the
partition limit, and nothing deleted unless the inserts can follow.
"""
import json
import os
import re
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.modules.setdefault("aws_lambda_powertools", MagicMock())


SID = "sid-1"


@pytest.fixture(autouse=True)
def iceberg_env():
    with patch.dict(os.environ, {
        "ICEBERG_DATABASE": "phebee",
        "ICEBERG_EVIDENCE_TABLE": "evidence",
        "ICEBERG_SUBJECT_TERMS_BY_SUBJECT_TABLE": "subject_terms_by_subject",
        "ICEBERG_SUBJECT_TERMS_BY_PROJECT_TERM_TABLE": "subject_terms_by_project_term",
        "PheBeeDynamoTable": "t",
    }):
        yield


class _Athena:
    """Stands in for _execute_athena_query and query_iceberg_evidence."""

    def __init__(self, n_terms, fail_on=None):
        self.n_terms = n_terms
        self.fail_on = fail_on
        self.statements = []

    def execute(self, sql, wait_for_completion=True):
        self.statements.append(sql)
        if self.fail_on and self.fail_on in sql:
            raise Exception(f"Athena query failed: {self.fail_on}")
        return "qid"

    def query(self, sql):
        if "$snapshots" in sql:
            return [{"snapshot_id": "42"}]
        if "COUNT(DISTINCT term_iri)" in sql:
            if self.fail_on == "COUNT(DISTINCT term_iri)":
                raise Exception("Athena query failed")
            return [{"n": str(self.n_terms)}]
        return [{"projects": "1", "terms": "1", "subjects": "1"}]

    def of_kind(self, prefix):
        return [s for s in self.statements if s.strip().startswith(prefix)]

    def inserts_into(self, table):
        return [s for s in self.of_kind("INSERT INTO") if f"phebee.{table} (" in s]


def _ranks(sql):
    return tuple(int(x) for x in re.search(r"term_rank BETWEEN (\d+) AND (\d+)", sql).groups())


def _mapping_rows(sql):
    return re.findall(r"\('([^']+)', '" + SID + r"', '([^']+)'\)", sql)


def _dynamo(items):
    resource = MagicMock()
    resource.Table.return_value.query.return_value = {"Items": items}
    return resource


def _subject_items(n_projects):
    return [{"PK": f"SUBJECT#{SID}", "SK": f"PROJECT#p{i:03d}#SUBJECT#ps{i}"} for i in range(n_projects)]


def _materialize_subject(athena, n_projects):
    from phebee.utils import iceberg
    resource = _dynamo(_subject_items(n_projects))
    with patch.object(iceberg, "_execute_athena_query", athena.execute), \
            patch.object(iceberg, "query_iceberg_evidence", athena.query), \
            patch("boto3.resource", return_value=resource):
        result = iceberg.materialize_subject_terms(SID)
    return result, resource


class TestEvidenceIsCountedOncePerRow:
    def test_every_insert_counts_distinct_evidence(self):
        athena = _Athena(n_terms=10)
        _materialize_subject(athena, n_projects=2)

        inserts = athena.of_kind("INSERT INTO")
        assert inserts
        for sql in inserts:
            assert "COUNT(*)" not in sql
            assert re.search(r"COUNT\(DISTINCT (e\.)?evidence_id\) as evidence_count", sql)


class TestByProjectTermInsertsStayUnderThePartitionLimit:
    def test_two_projects_split_the_terms_into_ranges_of_fifty(self):
        """The failing case from dev: 477 terms, 2 projects, 954 partitions in one INSERT."""
        athena = _Athena(n_terms=477)
        _materialize_subject(athena, n_projects=2)

        inserts = athena.inserts_into("subject_terms_by_project_term")
        ranges = [_ranks(s) for s in inserts]
        assert ranges[0] == (1, 50) and ranges[-1] == (451, 477)
        assert len(inserts) == 10
        for sql, (lo, hi) in zip(inserts, ranges):
            assert len(_mapping_rows(sql)) * (hi - lo + 1) <= 100

    def test_ranges_cover_every_term_exactly_once(self):
        athena = _Athena(n_terms=477)
        _materialize_subject(athena, n_projects=3)

        covered = []
        for sql in athena.inserts_into("subject_terms_by_project_term"):
            lo, hi = _ranks(sql)
            covered.extend(range(lo, hi + 1))
        assert covered == list(range(1, 478))

    def test_more_than_100_projects_are_grouped(self):
        athena = _Athena(n_terms=3)
        _materialize_subject(athena, n_projects=150)

        inserts = athena.inserts_into("subject_terms_by_project_term")
        groups = {}
        for sql in inserts:
            lo, hi = _ranks(sql)
            n = len(_mapping_rows(sql))
            assert n * (hi - lo + 1) <= 100
            groups.setdefault(tuple(_mapping_rows(sql)), []).append((lo, hi))
        assert sorted(len(g) for g in groups) == [50, 100]
        assert sum(len(g) for g in groups) == 150

    def test_by_subject_is_one_insert(self):
        """by_subject is partitioned by subject_id: one subject is one partition."""
        athena = _Athena(n_terms=477)
        _materialize_subject(athena, n_projects=2)

        assert len(athena.inserts_into("subject_terms_by_subject")) == 1

    def test_subject_without_evidence_deletes_and_inserts_no_project_rows(self):
        athena = _Athena(n_terms=0)
        _materialize_subject(athena, n_projects=2)

        assert len(athena.of_kind("DELETE FROM")) == 2
        assert athena.inserts_into("subject_terms_by_project_term") == []


class TestEveryReadUsesOneSnapshot:
    def test_evidence_reads_are_pinned(self):
        """Term ranks must not shift between the INSERTs if evidence arrives meanwhile."""
        athena = _Athena(n_terms=120)
        _materialize_subject(athena, n_projects=2)

        for sql in athena.of_kind("INSERT INTO"):
            reads = re.findall(r"FROM phebee\.evidence(?: FOR VERSION AS OF (\d+))?", sql)
            assert reads and all(r == "42" for r in reads), sql


class TestNothingIsDeletedUnlessTheInsertsCanFollow:
    def test_a_failed_delete_stops_before_inserting(self):
        """An INSERT after a failed DELETE would duplicate the rows left behind."""
        from phebee.utils import iceberg
        athena = _Athena(n_terms=10, fail_on="DELETE FROM phebee.subject_terms_by_project_term")
        with pytest.raises(Exception):
            _materialize_subject(athena, n_projects=2)
        assert athena.of_kind("INSERT INTO") == []

    def test_a_failed_term_count_deletes_nothing(self):
        athena = _Athena(n_terms=10, fail_on="COUNT(DISTINCT term_iri)")
        with pytest.raises(Exception):
            _materialize_subject(athena, n_projects=2)
        assert athena.statements == []

    def test_mappings_are_read_consistently(self):
        """The link handler calls this straight after writing the new mapping."""
        athena = _Athena(n_terms=1)
        _, resource = _materialize_subject(athena, n_projects=1)

        assert resource.Table.return_value.query.call_args.kwargs["ConsistentRead"] is True


class TestProjectMaterialization:
    def _run(self, athena, n_subjects, batch_size=100):
        from phebee.utils import iceberg
        items = [{"PK": "PROJECT#p1", "SK": f"SUBJECT#ps{i}", "subject_id": f"s{i}"} for i in range(n_subjects)]
        with patch.object(iceberg, "_execute_athena_query", athena.execute), \
                patch.object(iceberg, "query_iceberg_evidence", athena.query), \
                patch("boto3.resource", return_value=_dynamo(items)):
            iceberg.materialize_project("p1", batch_size=batch_size)

    def test_counts_distinct_evidence(self):
        athena = _Athena(n_terms=5)
        self._run(athena, n_subjects=3)

        for sql in athena.of_kind("INSERT INTO"):
            assert "COUNT(*)" not in sql
            assert re.search(r"COUNT\(DISTINCT (e\.)?evidence_id\) as evidence_count", sql)

    def test_by_project_term_is_split_into_ranges_of_100_terms(self):
        """A batch of 100 subjects easily covers more than 100 distinct terms."""
        athena = _Athena(n_terms=250)
        self._run(athena, n_subjects=3)

        ranges = [_ranks(s) for s in athena.inserts_into("subject_terms_by_project_term")]
        assert ranges == [(1, 100), (101, 200), (201, 250)]

    def test_a_failed_delete_stops_before_inserting(self):
        athena = _Athena(n_terms=5, fail_on="DELETE FROM phebee.subject_terms_by_subject")
        with pytest.raises(Exception):
            self._run(athena, n_subjects=3)
        assert athena.of_kind("INSERT INTO") == []


class TestLinkRollsBackWhenMaterializationFails:
    """A failed recompute leaves the subject missing from its projects' queries.

    Returning success hid that. The handler now undoes the link so that a retry
    links afresh (it would otherwise find the mapping and return early), then
    recomputes the subject for the projects it already had.
    """

    def _link(self, materialize):
        import create_subject
        table = MagicMock()
        table.batch_writer.return_value.__enter__.return_value = batch = MagicMock()
        event = {"body": json.dumps({
            "project_id": "new", "project_subject_id": "ps-new",
            "known_subject_iri": f"http://ods.nationwidechildrens.org/phebee/subjects/{SID}",
        })}
        with patch.object(create_subject, "project_exists", return_value=True), \
                patch.object(create_subject, "get_subject_id", return_value=None), \
                patch.object(create_subject, "_get_table_name", return_value="t"), \
                patch.object(create_subject, "link_subject_to_project"), \
                patch.object(create_subject, "execute_update") as update, \
                patch.object(create_subject, "fire_event") as fire, \
                patch.object(create_subject, "materialize_subject_terms", materialize), \
                patch.object(create_subject.time, "sleep"), \
                patch.object(create_subject.boto3, "resource") as resource:
            resource.return_value.Table.return_value = table
            result = create_subject.lambda_handler(event, None)
        return result, batch, update, fire

    def test_transient_failure_is_retried(self):
        materialize = MagicMock(side_effect=[Exception("commit conflict"), {"projects_affected": 2}])
        result, batch, _, fire = self._link(materialize)

        assert result["statusCode"] == 200
        assert materialize.call_count == 2
        batch.delete_item.assert_not_called()
        fire.assert_called_once()

    def test_persistent_failure_undoes_the_link_and_reports_it(self):
        attempts = []

        def materialize(subject_id):
            attempts.append(subject_id)
            if len(attempts) <= 3:
                raise Exception("ICEBERG_TOO_MANY_OPEN_PARTITIONS")
            return {"projects_affected": 1}

        result, batch, update, fire = self._link(materialize)

        assert result["statusCode"] == 500
        deleted = [c.kwargs["Key"] for c in batch.delete_item.call_args_list]
        assert {"PK": "PROJECT#new", "SK": "SUBJECT#ps-new"} in deleted
        assert {"PK": f"SUBJECT#{SID}", "SK": "PROJECT#new#SUBJECT#ps-new"} in deleted
        assert any("projects/new/ps-new" in c.args[0] for c in update.call_args_list)
        assert len(attempts) == 4, "three attempts, then one recompute for the remaining projects"
        fire.assert_not_called()
