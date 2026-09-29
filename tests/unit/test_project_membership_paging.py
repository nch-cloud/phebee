"""DynamoDB paging over a project's membership items.

DynamoDB has no offset. get_project_subjects_page walks to one with
Select=COUNT queries, then resumes from the key where the walk stopped. The
fake table below mimics the two properties the walk depends on: a query stops
at Limit items or at a per-call size cap, whichever comes first, and it returns
LastEvaluatedKey only when it stopped early.
"""
import os
from unittest.mock import patch

import pytest

from phebee.utils import dynamodb


class _FakeTable:
    def __init__(self, members, per_call_cap=7):
        # Other projects' items and non-member items share the partition space;
        # the key condition must keep them out.
        self.items = sorted(
            [{"PK": "PROJECT#p1", "SK": f"SUBJECT#{psid}", "subject_id": sid} for psid, sid in members]
            + [{"PK": "PROJECT#p2", "SK": "SUBJECT#other", "subject_id": "x"}],
            key=lambda it: (it["PK"], it["SK"]),
        )
        self.per_call_cap = per_call_cap
        self.calls = []

    def query(self, KeyConditionExpression, ExpressionAttributeValues, Select=None,
              Limit=None, ExclusiveStartKey=None):
        self.calls.append({"Select": Select, "Limit": Limit, "start": ExclusiveStartKey})
        pk = ExpressionAttributeValues[":pk"]
        prefix = ExpressionAttributeValues[":sk_prefix"]
        matching = [it for it in self.items if it["PK"] == pk and it["SK"].startswith(prefix)]
        if ExclusiveStartKey:
            matching = [it for it in matching if it["SK"] > ExclusiveStartKey["SK"]]

        take = min(len(matching), self.per_call_cap, Limit if Limit is not None else len(matching))
        batch = matching[:take]
        response = {"Count": len(batch)}
        if Select != "COUNT":
            response["Items"] = batch
        if take < len(matching):
            last = batch[-1]
            response["LastEvaluatedKey"] = {"PK": last["PK"], "SK": last["SK"]}
        return response


MEMBERS = [(f"psid-{i:03d}", f"sid-{i:03d}") for i in range(20)]


@pytest.fixture
def table():
    fake = _FakeTable(MEMBERS)
    with patch.object(dynamodb, "_get_table", return_value=fake):
        yield fake


class TestPage:
    def test_first_page(self, table):
        assert dynamodb.get_project_subjects_page("p1", 0, 3) == MEMBERS[:3]

    def test_offset_walk_crosses_several_calls(self, table):
        """An offset past the per-call cap takes several COUNT calls to reach."""
        assert dynamodb.get_project_subjects_page("p1", 15, 3) == MEMBERS[15:18]
        skip_calls = [c for c in table.calls if c["Select"] == "COUNT"]
        assert len(skip_calls) == 3  # 7 + 7 + 1

    def test_page_larger_than_one_call(self, table):
        assert dynamodb.get_project_subjects_page("p1", 2, 12) == MEMBERS[2:14]

    def test_last_partial_page(self, table):
        assert dynamodb.get_project_subjects_page("p1", 18, 10) == MEMBERS[18:]

    @pytest.mark.parametrize("offset", [20, 21, 500])
    def test_offset_at_or_past_the_end_is_empty(self, table, offset):
        """Without a key to resume from, a page query would restart at the top."""
        assert dynamodb.get_project_subjects_page("p1", offset, 5) == []

    def test_offset_on_a_call_boundary(self, table):
        assert dynamodb.get_project_subjects_page("p1", 7, 2) == MEMBERS[7:9]

    def test_psid_containing_hash_is_kept_whole(self):
        fake = _FakeTable([("a#b", "sid-1")])
        with patch.object(dynamodb, "_get_table", return_value=fake):
            assert dynamodb.get_project_subjects_page("p1", 0, 5) == [("a#b", "sid-1")]


class TestCount:
    def test_counts_only_this_projects_members(self, table):
        assert dynamodb.count_project_subjects("p1") == 20
        assert all(c["Select"] == "COUNT" for c in table.calls)


class _FakeResource:
    """batch_get_item that leaves some keys unprocessed on the first call."""

    def __init__(self, members, table_name, unprocessed_first=0):
        self.by_sk = {f"SUBJECT#{psid}": sid for psid, sid in members}
        self.table_name = table_name
        self.unprocessed_first = unprocessed_first
        self.requests = []

    def batch_get_item(self, RequestItems):
        keys = RequestItems[self.table_name]["Keys"]
        self.requests.append(len(keys))
        if len(keys) != len({k["SK"] for k in keys}):
            raise ValueError("Provided list of item keys contains duplicates")
        held, keys = keys[:self.unprocessed_first], keys[self.unprocessed_first:]
        self.unprocessed_first = 0
        found = [{"PK": k["PK"], "SK": k["SK"], "subject_id": self.by_sk[k["SK"]]}
                 for k in keys if k["SK"] in self.by_sk]
        response = {"Responses": {self.table_name: found}}
        if held:
            response["UnprocessedKeys"] = {self.table_name: {"Keys": held}}
        return response


class TestById:
    def _run(self, resource, ids):
        with patch.dict(os.environ, {"PheBeeDynamoTable": "t"}), \
                patch.object(dynamodb.boto3, "resource", return_value=resource), \
                patch.object(dynamodb.time, "sleep"):
            return dynamodb.get_project_subjects_by_ids("p1", ids)

    def test_found_ids_in_psid_order_and_non_members_dropped(self):
        resource = _FakeResource(MEMBERS, "t")
        assert self._run(resource, ["psid-005", "nope", "psid-001"]) == [MEMBERS[1], MEMBERS[5]]

    def test_duplicates_are_sent_once(self):
        resource = _FakeResource(MEMBERS, "t")
        assert self._run(resource, ["psid-001", "psid-001"]) == [MEMBERS[1]]

    def test_requests_are_chunked_at_100_keys(self):
        many = [(f"m{i:04d}", f"s{i}") for i in range(250)]
        resource = _FakeResource(many, "t")
        assert len(self._run(resource, [psid for psid, _ in many])) == 250
        assert resource.requests == [100, 100, 50]

    def test_unprocessed_keys_are_retried(self):
        resource = _FakeResource(MEMBERS, "t", unprocessed_first=2)
        ids = [psid for psid, _ in MEMBERS[:5]]
        assert self._run(resource, ids) == MEMBERS[:5]
        assert resource.requests == [5, 2]
