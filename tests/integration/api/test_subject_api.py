import uuid
import requests
import pytest

pytestmark = [pytest.mark.integration, pytest.mark.api]


def test_create_new_subject(api_base_url, sigv4_auth, test_project_id):
    subject_id = f"subject-{uuid.uuid4().hex[:8]}"
    payload = {"project_id": test_project_id, "project_subject_id": subject_id}

    resp = requests.post(f"{api_base_url}/subject", json=payload, auth=sigv4_auth)
    assert resp.status_code == 200
    body = resp.json()
    assert body.get("subject_created") is True
    assert "subject" in body
    assert (
        f"http://ods.nationwidechildrens.org/phebee/projects/{test_project_id}"
        in body["subject"]["projects"]
    )


def test_get_subject_details(api_base_url, sigv4_auth, test_project_id):
    subject_id = f"subject-{uuid.uuid4().hex[:8]}"
    payload = {"project_id": test_project_id, "project_subject_id": subject_id}

    # Create subject first
    create_resp = requests.post(
        f"{api_base_url}/subject", json=payload, auth=sigv4_auth
    )
    assert create_resp.status_code == 200

    # Then fetch subject details
    detail_resp = requests.post(
        f"{api_base_url}/subject/details", json=payload, auth=sigv4_auth
    )
    assert detail_resp.status_code == 200
    body = detail_resp.json()
    print(f"body: {body}")
    assert "iri" in body
    assert "terms" in body
    assert isinstance(body["terms"], dict)


def test_remove_subject(api_base_url, sigv4_auth, test_project_id):
    subject_id = f"subject-{uuid.uuid4().hex[:8]}"
    payload = {"project_id": test_project_id, "project_subject_id": subject_id}

    # Create subject first
    create_resp = requests.post(
        f"{api_base_url}/subject", json=payload, auth=sigv4_auth
    )
    assert create_resp.status_code == 200

    # Delete subject
    delete_resp = requests.post(
        f"{api_base_url}/subject/remove", json=payload, auth=sigv4_auth
    )
    assert delete_resp.status_code == 200
    body = delete_resp.json()
    assert "message" in body
    assert "Subject removed" in body["message"]

    # TODO Try fetching the subject again, confirm that it's gone


@pytest.mark.parametrize(
    "payload, expected_error",
    [
        (
            {
                "project_id": "some-project",
                "project_subject_id": "some-id",
                "known_project_id": "proj-123",
            },
            "known_project_id' is provided, 'known_project_subject_id",
        ),
        (
            {
                "project_id": "some-project",
                "project_subject_id": "some-id",
                "known_project_id": "proj-123",
                "known_project_subject_id": "proj-123-subj-a",
                "known_subject_iri": "http://example.org",
            },
            "cannot both be provided",
        ),
    ],
)
def test_subject_validation_errors(api_base_url, sigv4_auth, payload, expected_error):
    resp = requests.post(f"{api_base_url}/subject", json=payload, auth=sigv4_auth)
    assert resp.status_code == 400
    body = resp.json()
    print(f"response body: {body}")
    assert "error" in body
    assert expected_error in body["error"]
