"""Which page-reading strategy query_subjects_by_project picks, and why.

The function has two ways to read a page, and the choice is mostly a
performance decision that nothing else in the suite would notice regressing. It
has regressed once: the two-pass rewrite in 94710a4 applied unconditionally,
which added a second *sequential* Athena round trip to every term-filtered
query. Athena has a ~1.5 s per-query floor, so that cost the three
term-filtered benchmark workloads ~1.2-1.6 s each at every cohort size.

These tests pin the query shapes rather than the timings:
- A term filter reads from subject_terms_by_project_term: one aggregate plus a
  concurrent count.
- Without a term filter, the page comes from the project's DynamoDB membership
  items, and one query reads that page's phenotypes from
  subject_terms_by_subject.
"""
import os
from unittest.mock import patch

import pytest


# Athena renders an anonymous ROW positionally, without field names, so the
# parser maps values to field_names by index. Writing these as "term_id=..."
# parses without error and silently puts "term_id=HP:0001627" in the term_id
# field, which is why the fixtures are bare values.
TERMS_ONE = (
    "[{HP:0001627, http://purl.obolibrary.org/obo/HP_0001627, "
    "Abnormal heart morphology, [], 3, abc123, 2020-01-01, 2021-01-01}]"
)

PROJECTS = "http://ods.nationwidechildrens.org/phebee/projects"
SUBJECTS = "http://ods.nationwidechildrens.org/phebee/subjects"


def _row(subject_id, terms=TERMS_ONE):
    """A subject_terms_by_project_term aggregate row."""
    return {
        "subject_id": subject_id,
        "project_subject_id": f"perf-subj-{subject_id}",
        "subject_iri": f"{SUBJECTS}/{subject_id}",
        "project_subject_iri": f"{PROJECTS}/p1/perf-subj-{subject_id}",
        "terms": terms,
    }


def _by_subject_row(subject_id, terms=TERMS_ONE):
    """A subject_terms_by_subject aggregate row: no project columns."""
    return {"subject_id": subject_id, "terms": terms}


@pytest.fixture(autouse=True)
def iceberg_env():
    """The function reads its table names from the environment and fails without them."""
    with patch.dict(os.environ, {
        "ICEBERG_DATABASE": "phebee",
        "ICEBERG_SUBJECT_TERMS_BY_PROJECT_TERM_TABLE": "subject_terms_by_project_term",
        "ICEBERG_SUBJECT_TERMS_BY_SUBJECT_TABLE": "subject_terms_by_subject",
    }):
        yield


class _Recorder:
    """Stands in for query_iceberg_evidence, recording SQL and replaying answers.

    Keyed by query shape rather than call order, because the term-filtered path
    runs its two queries concurrently and their completion order is not fixed.
    """

    def __init__(self, aggregate=None, count=None, by_subject=None):
        self.sql = []
        self.all_pages = []
        self._aggregate = aggregate if aggregate is not None else []
        self._count = count if count is not None else []
        self._by_subject = by_subject if by_subject is not None else []

    def __call__(self, sql, all_pages=False):
        self.sql.append(sql)
        self.all_pages.append(all_pages)
        if "COUNT(DISTINCT subject_id)" in sql:
            return self._count
        if "subject_terms_by_subject" in sql:
            return self._by_subject
        return self._aggregate

    @property
    def count_queries(self):
        return [s for s in self.sql if "COUNT(DISTINCT subject_id)" in s]

    @property
    def aggregate_queries(self):
        return [s for s in self.sql if "subject_terms_by_project_term" in s and "ARRAY_AGG(" in s]

    @property
    def by_subject_queries(self):
        return [s for s in self.sql if "subject_terms_by_subject" in s]


class _Members:
    """Stands in for the three DynamoDB membership helpers.

    members is the project's full membership as (project_subject_id,
    subject_id), in project_subject_id order, as DynamoDB returns it.
    """

    def __init__(self, members):
        self.members = sorted(members)
        self.page_calls = []
        self.id_lookups = []

    def count(self, project_id):
        return len(self.members)

    def page(self, project_id, offset, limit):
        self.page_calls.append((offset, limit))
        return self.members[offset:offset + limit]

    def by_ids(self, project_id, project_subject_ids):
        self.id_lookups.append(list(project_subject_ids))
        wanted = set(project_subject_ids)
        return [m for m in self.members if m[0] in wanted]

    def patches(self):
        return [
            patch("phebee.utils.dynamodb.count_project_subjects", self.count),
            patch("phebee.utils.dynamodb.get_project_subjects_page", self.page),
            patch("phebee.utils.dynamodb.get_project_subjects_by_ids", self.by_ids),
        ]


def _call(**kwargs):
    from phebee.utils.iceberg import query_subjects_by_project
    return query_subjects_by_project(project_id="p1", **kwargs)


def _call_with(rec, members=None, **kwargs):
    members = members or _Members([])
    ps = members.patches()
    with patch("phebee.utils.iceberg.query_iceberg_evidence", rec), ps[0], ps[1], ps[2]:
        return _call(**kwargs)


class TestTermFilterUsesOneAggregateAndAConcurrentCount:
    """A term filter prunes on term_id, so one aggregate is cheaper than two passes."""

    def test_term_filter_runs_one_aggregate_and_one_count(self):
        members = _Members([("a", "s1")])
        rec = _Recorder(aggregate=[_row("s1"), _row("s2")], count=[{"total": "2"}])
        result = _call_with(rec, members, term_ids=["HP:0001627"], include_child_terms=False, limit=25)

        assert len(rec.aggregate_queries) == 1
        assert len(rec.count_queries) == 1
        assert len(rec.sql) == 2, f"expected exactly 2 queries, got {len(rec.sql)}"
        assert rec.by_subject_queries == []
        assert members.page_calls == [], "a term-filtered query must not page membership"
        assert result["pagination"]["total_count"] == 2
        assert len(result["subjects"]) == 2

    def test_aggregate_carries_the_pagination_and_the_term_filter(self):
        rec = _Recorder(aggregate=[_row("s1")], count=[{"total": "1"}])
        _call_with(rec, term_ids=["HP:0001627"], include_child_terms=False, limit=25, offset=50)

        sql = rec.aggregate_queries[0]
        assert "OFFSET 50 LIMIT 25" in sql
        assert "term_id IN ('HP:0001627')" in sql
        assert "GROUP BY subject_id" in sql

    def test_aggregate_reads_every_result_page(self):
        """GetQueryResults caps a page at 999 data rows; the default limit is 1000."""
        rec = _Recorder(aggregate=[_row("s1")], count=[{"total": "1"}])
        _call_with(rec, term_ids=["HP:0001627"], include_child_terms=False, limit=1000)

        assert rec.all_pages[rec.sql.index(rec.aggregate_queries[0])] is True

    def test_term_filter_with_subject_list_stays_on_the_aggregate(self):
        rec = _Recorder(aggregate=[_row("s1")], count=[{"total": "1"}])
        _call_with(rec, term_ids=["HP:0001627"], include_child_terms=False,
                   project_subject_ids=["perf-subj-s1"], limit=10)

        assert "project_subject_id IN ('perf-subj-s1')" in rec.aggregate_queries[0]
        assert rec.by_subject_queries == []

    def test_empty_result_still_reports_the_counted_total(self):
        """No page rows needs no special case: the total is counted independently."""
        rec = _Recorder(aggregate=[], count=[{"total": "7"}])
        result = _call_with(rec, term_ids=["HP:0001627"], include_child_terms=False,
                            limit=10, offset=100)

        assert result["subjects"] == []
        assert result["pagination"]["total_count"] == 7
        assert result["pagination"]["has_more"] is False


class TestRequestValuesAreEscaped:
    """Request values are inlined into SQL, so a quote must not end the literal."""

    def test_subject_ids_with_quotes_are_escaped(self):
        rec = _Recorder(aggregate=[], count=[{"total": "0"}])
        _call_with(rec, term_ids=["HP:0001627"], include_child_terms=False,
                   project_subject_ids=["x') OR 1=1 --"], limit=10)

        assert "project_subject_id IN ('x'') OR 1=1 --')" in rec.aggregate_queries[0]

    def test_term_ids_with_quotes_are_escaped(self):
        rec = _Recorder(aggregate=[], count=[{"total": "0"}])
        _call_with(rec, term_ids=["HP:1' OR '1'='1"], include_child_terms=False, limit=10)

        assert "term_id IN ('HP:1'' OR ''1''=''1')" in rec.aggregate_queries[0]


class TestUnfilteredPathPagesMembership:
    """Without a term filter the page comes from DynamoDB, the phenotypes from by_subject."""

    def test_page_from_membership_then_one_by_subject_query(self):
        members = _Members([(f"p{i:03d}", f"s{i:03d}") for i in range(30)])
        rec = _Recorder(by_subject=[_by_subject_row("s010"), _by_subject_row("s011")])
        result = _call_with(rec, members, limit=2, offset=10)

        assert members.page_calls == [(10, 2)]
        assert rec.aggregate_queries == [] and rec.count_queries == []
        assert len(rec.by_subject_queries) == 1
        sql = rec.by_subject_queries[0]
        assert "subject_id IN ('s010', 's011')" in sql
        assert "project_id" not in sql, "phenotypes are subject-wide, not per project"
        assert [s["project_subject_id"] for s in result["subjects"]] == ["p010", "p011"]
        assert result["pagination"] == {
            "limit": 2, "cursor": "10", "next_cursor": "12", "has_more": True, "total_count": 30,
        }

    def test_iris_are_built_from_the_membership(self):
        members = _Members([("MRN-1", "uuid-1")])
        rec = _Recorder(by_subject=[_by_subject_row("uuid-1")])
        subject = _call_with(rec, members, limit=10)["subjects"][0]

        assert subject["subject_iri"] == f"{SUBJECTS}/uuid-1"
        assert subject["project_subject_iri"] == f"{PROJECTS}/p1/MRN-1"
        assert subject["phenotypes"][0]["term"]["id"] == "HP:0001627"

    def test_members_without_evidence_are_listed_with_no_phenotypes(self):
        members = _Members([("a", "s1"), ("b", "s2"), ("c", "s3")])
        rec = _Recorder(by_subject=[_by_subject_row("s2")])
        result = _call_with(rec, members, limit=10)

        assert [(s["project_subject_id"], len(s["phenotypes"])) for s in result["subjects"]] == [
            ("a", 0), ("b", 1), ("c", 0)]
        assert result["pagination"]["total_count"] == 3

    def test_phenotype_query_reads_every_result_page(self):
        """A page over 999 subjects would otherwise list the rest with no phenotypes."""
        members = _Members([("a", "s1")])
        rec = _Recorder(by_subject=[_by_subject_row("s1")])
        _call_with(rec, members, limit=10)

        assert rec.all_pages == [True]

    def test_empty_page_runs_no_athena_query(self):
        """An empty IN list is a syntax error."""
        members = _Members([("a", "s1")])
        rec = _Recorder()
        result = _call_with(rec, members, limit=10, offset=5)

        assert rec.sql == []
        assert result["subjects"] == []
        assert result["pagination"]["total_count"] == 1
        assert result["pagination"]["has_more"] is False

    def test_qualifier_filter_drops_phenotypes_not_subjects(self):
        members = _Members([("a", "s1"), ("b", "s2")])
        rec = _Recorder(by_subject=[_by_subject_row("s1")])
        result = _call_with(rec, members, include_qualified=False, limit=10)

        assert "ANY_MATCH(qualifiers" in rec.by_subject_queries[0]
        assert len(result["subjects"]) == 2, "a member with only qualified phenotypes is still listed"
        assert result["pagination"]["total_count"] == 2


class TestSubjectListWithoutTermUsesMembership:
    """by_project_term cannot prune on project_subject_id, so the lookup goes to DynamoDB."""

    def test_ids_are_looked_up_and_non_members_dropped(self):
        members = _Members([("a", "s1"), ("b", "s2"), ("c", "s3")])
        rec = _Recorder(by_subject=[_by_subject_row("s1"), _by_subject_row("s3")])
        result = _call_with(rec, members, project_subject_ids=["c", "a", "zz"], limit=10)

        assert members.id_lookups == [["c", "a", "zz"]]
        assert members.page_calls == []
        assert rec.aggregate_queries == []
        assert [s["project_subject_id"] for s in result["subjects"]] == ["a", "c"]
        assert result["pagination"]["total_count"] == 2

    def test_the_found_members_are_paginated(self):
        members = _Members([(f"p{i}", f"s{i}") for i in range(5)])
        rec = _Recorder(by_subject=[_by_subject_row("s2"), _by_subject_row("s3")])
        result = _call_with(rec, members, project_subject_ids=[f"p{i}" for i in range(5)],
                            limit=2, offset=2)

        assert [s["project_subject_id"] for s in result["subjects"]] == ["p2", "p3"]
        assert result["pagination"]["next_cursor"] == "4"


class TestBothPathsReturnTheSameShape:
    """Callers read both paths' subjects with the same code."""

    def test_phenotypes_are_parsed_identically(self):
        rec = _Recorder(aggregate=[_row("s1")], count=[{"total": "1"}])
        filtered = _call_with(rec, term_ids=["HP:0001627"], include_child_terms=False, limit=10)

        members = _Members([("perf-subj-s1", "s1")])
        rec = _Recorder(by_subject=[_by_subject_row("s1")])
        unfiltered = _call_with(rec, members, limit=10)

        assert filtered["subjects"] == unfiltered["subjects"]
        assert filtered["pagination"] == unfiltered["pagination"]

    def test_full_phenotype_list_survives_both_paths(self):
        """The truncation bug in 2c4ea1a collapsed every list to one term.

        The benchmark dataset carries 150-499 terms per subject, so a regression
        here is invisible in the response shape but changes every latency number.
        """
        many = "[" + ", ".join(
            "{HP:%07d, http://purl.obolibrary.org/obo/HP_%07d, Term %d, [], 1, tl%d, "
            "2020-01-01, 2020-01-02}" % (i, i, i, i)
            for i in range(200)
        ) + "]"

        rec = _Recorder(aggregate=[_row("s1", terms=many)], count=[{"total": "1"}])
        filtered = _call_with(rec, term_ids=["HP:0001627"], include_child_terms=False, limit=10)

        rec = _Recorder(by_subject=[_by_subject_row("s1", terms=many)])
        unfiltered = _call_with(rec, _Members([("a", "s1")]), limit=10)

        assert len(filtered["subjects"][0]["phenotypes"]) == 200
        assert len(unfiltered["subjects"][0]["phenotypes"]) == 200
