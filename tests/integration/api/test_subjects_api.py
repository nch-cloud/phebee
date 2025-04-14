import uuid
import pytest
import requests

pytestmark = [pytest.mark.integration, pytest.mark.api]


def test_list_subjects_by_project(api_base_url, sigv4_auth, test_project_id):
    subject_id = f"subject-{uuid.uuid4().hex[:8]}"

    # Create a subject to query
    create_resp = requests.post(
        f"{api_base_url}/subject",
        json={"project_id": test_project_id, "project_subject_id": subject_id},
        auth=sigv4_auth,
    )
    assert create_resp.status_code == 200

    # Call /subjects/phenotypes
    query_payload = {
        "project_id": test_project_id,
        "term_source": "hpo",
        "include_terms": False,
    }

    resp = requests.post(
        f"{api_base_url}/subjects/phenotypes", json=query_payload, auth=sigv4_auth
    )

    assert resp.status_code == 200
    results = resp.json()
    print(f"results: {results}")
    subjects_dict = results["body"]
    # Check that we've returned a dict with only one subject
    assert isinstance(subjects_dict, dict)
    assert len(subjects_dict) == 1
    subject_projects = next(iter(subjects_dict.values()))
    # Check that our subject is only in one project
    assert len(subject_projects) == 1
    assert subject_projects[0]["projectSubjectId"].endswith(f"#{subject_id}")


def test_query_subjects_flat(api_base_url, sigv4_auth, test_project_id):
    subject_id = f"subject-{uuid.uuid4().hex[:8]}"

    # Create subject
    create_resp = requests.post(
        f"{api_base_url}/subject",
        json={"project_id": test_project_id, "project_subject_id": subject_id},
        auth=sigv4_auth,
    )
    assert create_resp.status_code == 200

    # Query with flat SPARQL-style results
    query_payload = {"project_id": test_project_id, "return_raw_json": True}

    resp = requests.post(
        f"{api_base_url}/subjects/query", json=query_payload, auth=sigv4_auth
    )

    assert resp.status_code == 200
    subject_list = resp.json()
    print(subject_list)
    assert isinstance(subject_list, list)
    assert len(subject_list) == 1
    subject = subject_list[0]
    assert "iri" in subject
    assert subject["project_subject_id"].endswith(f"#{subject_id}")
