"""Which page-reading strategy query_subjects_by_project picks, and why.

The function has two ways to read a page and the choice is a performance
decision with no visible effect on the response, so nothing else in the suite
would notice it regressing. It has regressed once: the two-pass rewrite in
94710a4 applied unconditionally, which added a second *sequential* Athena round
trip to every term-filtered query. Athena has a ~1.5 s per-query floor, so that
cost the three term-filtered benchmark workloads ~1.2-1.6 s each at every cohort
size, for no reduction in what they scanned -- those workloads were already flat
from 1k to 100k subjects.

These tests pin the query shapes rather than the timings: one aggregate plus a
concurrent count when the filter is selective, two sequential passes when it is
not.
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


def _row(subject_id, terms=TERMS_ONE):
    return {
        "subject_id": subject_id,
        "project_subject_id": f"perf-subj-{subject_id}",
        "subject_iri": f"http://example.org/subject/{subject_id}",
        "project_subject_iri": f"http://example.org/project/p1/{subject_id}",
        "terms": terms,
    }


@pytest.fixture(autouse=True)
def iceberg_env():
    """The function reads its table names from the environment and fails without them."""
    with patch.dict(os.environ, {
        "ICEBERG_DATABASE": "phebee",
        "ICEBERG_SUBJECT_TERMS_BY_PROJECT_TERM_TABLE": "subject_terms_by_project_term",
    }):
        yield


class _Recorder:
    """Stands in for query_iceberg_evidence, recording SQL and replaying answers.

    Keyed by query shape rather than call order, because the selective path runs
    its two queries concurrently and their completion order is not fixed.
    """

    def __init__(self, page=None, aggregate=None, count=None, second_pass=None):
        self.sql = []
        self._page = page if page is not None else []
        self._aggregate = aggregate if aggregate is not None else []
        self._count = count if count is not None else []
        self._second_pass = second_pass if second_pass is not None else []

    # Matched against the second pass's own clause rather than "subject_id IN (",
    # which is a substring of the project_subject_id filter.
    SECOND_PASS_CLAUSE = "\n        AND subject_id IN ('"

    def __call__(self, sql):
        self.sql.append(sql)
        if "COUNT(DISTINCT subject_id)" in sql:
            return self._count
        if "COUNT(*) OVER ()" in sql:
            return self._page
        if self.SECOND_PASS_CLAUSE in sql:
            return self._second_pass
        return self._aggregate

    # -- query shape predicates ------------------------------------------------
    @property
    def page_queries(self):
        return [s for s in self.sql if "COUNT(*) OVER ()" in s]

    @property
    def count_queries(self):
        return [s for s in self.sql if "COUNT(DISTINCT subject_id)" in s]

    @property
    def second_pass_queries(self):
        return [s for s in self.sql if self.SECOND_PASS_CLAUSE in s]

    @property
    def aggregate_queries(self):
        return [s for s in self.sql
                if "ARRAY_AGG(" in s and self.SECOND_PASS_CLAUSE not in s]


def _call(**kwargs):
    from phebee.utils.iceberg import query_subjects_by_project
    return query_subjects_by_project(project_id="p1", **kwargs)


class TestSelectivePathUsesOneAggregateAndAConcurrentCount:
    """A term filter prunes on term_id, so one aggregate is cheaper than two passes."""

    def test_term_filter_skips_the_page_pass(self):
        rec = _Recorder(aggregate=[_row("s1"), _row("s2")], count=[{"total": "2"}])
        with patch("phebee.utils.iceberg.query_iceberg_evidence", rec):
            result = _call(term_ids=["HP:0001627"], include_child_terms=False, limit=25)

        assert rec.page_queries == [], "term-filtered query should not run the page pass"
        assert len(rec.aggregate_queries) == 1
        assert len(rec.count_queries) == 1
        assert len(rec.sql) == 2, f"expected exactly 2 queries, got {len(rec.sql)}"
        assert result["pagination"]["total_count"] == 2
        assert len(result["subjects"]) == 2

    def test_aggregate_carries_the_pagination_and_the_term_filter(self):
        rec = _Recorder(aggregate=[_row("s1")], count=[{"total": "1"}])
        with patch("phebee.utils.iceberg.query_iceberg_evidence", rec):
            _call(term_ids=["HP:0001627"], include_child_terms=False, limit=25, offset=50)

        sql = rec.aggregate_queries[0]
        assert "OFFSET 50 LIMIT 25" in sql
        assert "term_id IN ('HP:0001627')" in sql
        assert "GROUP BY subject_id" in sql

    def test_explicit_subject_list_is_also_selective(self):
        rec = _Recorder(aggregate=[_row("s1")], count=[{"total": "1"}])
        with patch("phebee.utils.iceberg.query_iceberg_evidence", rec):
            _call(project_subject_ids=["perf-subj-000001"], limit=10)

        assert rec.page_queries == []
        assert len(rec.aggregate_queries) == 1

    def test_qualifier_filter_alone_is_not_selective(self):
        """include_qualified is a row predicate over an array; it prunes nothing.

        Treating it as selective would put the unfiltered cohort workloads back on
        the single aggregate that scans the whole project.
        """
        rec = _Recorder(page=[{"subject_id": "s1", "total_count": "1"}],
                        second_pass=[_row("s1")])
        with patch("phebee.utils.iceberg.query_iceberg_evidence", rec):
            _call(include_qualified=False, limit=10)

        assert len(rec.page_queries) == 1, "should still take the two-pass path"

    def test_empty_result_still_reports_the_counted_total(self):
        """No page rows on the selective path needs no special case."""
        rec = _Recorder(aggregate=[], count=[{"total": "7"}])
        with patch("phebee.utils.iceberg.query_iceberg_evidence", rec):
            result = _call(term_ids=["HP:0001627"], include_child_terms=False,
                           limit=10, offset=100)

        assert result["subjects"] == []
        assert result["pagination"]["total_count"] == 7
        assert result["pagination"]["has_more"] is False


class TestUnfilteredPathUsesTwoPasses:
    """Unfiltered, a single aggregate reads the whole project before the limit."""

    def test_page_pass_then_full_rows_by_literal_id(self):
        rec = _Recorder(
            page=[{"subject_id": "s1", "total_count": "1000"},
                  {"subject_id": "s2", "total_count": "1000"}],
            second_pass=[_row("s1"), _row("s2")],
        )
        with patch("phebee.utils.iceberg.query_iceberg_evidence", rec):
            result = _call(limit=10)

        assert len(rec.page_queries) == 1
        assert len(rec.second_pass_queries) == 1
        assert rec.count_queries == [], "the window count makes a count query unnecessary"
        # Ordering matters: the IN list is built from the page pass's rows.
        assert rec.sql.index(rec.page_queries[0]) < rec.sql.index(rec.second_pass_queries[0])
        assert "subject_id IN ('s1', 's2')" in rec.second_pass_queries[0]
        assert result["pagination"]["total_count"] == 1000
        assert result["pagination"]["has_more"] is True

    def test_page_pass_reads_only_subject_id(self):
        """Reading full rows in the page pass is what made the scan 2.9 GB."""
        rec = _Recorder(page=[{"subject_id": "s1", "total_count": "1"}],
                        second_pass=[_row("s1")])
        with patch("phebee.utils.iceberg.query_iceberg_evidence", rec):
            _call(limit=10)

        sql = rec.page_queries[0]
        assert "SELECT DISTINCT subject_id" in sql
        assert "ARRAY_AGG(" not in sql

    def test_empty_page_at_offset_zero_skips_the_second_pass(self):
        """An empty IN list is a syntax error, so the second pass must not run."""
        rec = _Recorder(page=[])
        with patch("phebee.utils.iceberg.query_iceberg_evidence", rec):
            result = _call(limit=10)

        assert rec.second_pass_queries == []
        assert rec.count_queries == [], "nothing matched, so no need to count"
        assert result["subjects"] == []
        assert result["pagination"]["total_count"] == 0

    def test_empty_page_past_the_end_counts_the_total_directly(self):
        """Past the last page the total is still nonzero and has no window row to read."""
        rec = _Recorder(page=[], count=[{"total": "1000"}])
        with patch("phebee.utils.iceberg.query_iceberg_evidence", rec):
            result = _call(limit=10, offset=5000)

        assert rec.second_pass_queries == []
        assert len(rec.count_queries) == 1
        assert result["subjects"] == []
        assert result["pagination"]["total_count"] == 1000
        assert result["pagination"]["has_more"] is False


class TestBothPathsReturnTheSameShape:
    """The strategy is an implementation detail; callers must not be able to tell."""

    def test_phenotypes_are_parsed_identically(self):
        selective = _Recorder(aggregate=[_row("s1")], count=[{"total": "1"}])
        with patch("phebee.utils.iceberg.query_iceberg_evidence", selective):
            filtered = _call(term_ids=["HP:0001627"], include_child_terms=False, limit=10)

        two_pass = _Recorder(page=[{"subject_id": "s1", "total_count": "1"}],
                             second_pass=[_row("s1")])
        with patch("phebee.utils.iceberg.query_iceberg_evidence", two_pass):
            unfiltered = _call(limit=10)

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
        with patch("phebee.utils.iceberg.query_iceberg_evidence", rec):
            result = _call(term_ids=["HP:0001627"], include_child_terms=False, limit=10)

        assert len(result["subjects"][0]["phenotypes"]) == 200
