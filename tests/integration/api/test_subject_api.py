import uuid
import pytest
import requests

pytestmark = [pytest.mark.integration, pytest.mark.api]


def test_create_subject(api_base_url, sigv4_auth, test_project):
    project_id = test_project
    project_subject_id = f"test-subj-{uuid.uuid4().hex[:6]}"
    project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}/{project_subject_id}"

    # --- Create the subject ---
    payload = {"project_id": project_id, "project_subject_id": project_subject_id}

    resp = requests.post(
        f"{api_base_url}/subject/create", json=payload, auth=sigv4_auth
    )

    assert resp.status_code == 200, (
        f"Unexpected status: {resp.status_code} - {resp.text}"
    )

    body = resp.json()
    assert body["subject_created"] is True
    assert "subject" in body
    assert "iri" in body["subject"]
    assert body["subject"]["projects"][project_id] == project_subject_id

    subject_iri = body["subject"]["iri"]
    subject_id = subject_iri.split("/")[-1]

    # --- Verify DynamoDB mapping works by retrieving via get_subject ---
    resp = requests.post(
        f"{api_base_url}/subject",
        json={"project_subject_iri": project_subject_iri},
        auth=sigv4_auth,
    )
    assert resp.status_code == 200, "Should be able to retrieve newly created subject via DynamoDB mapping"
    body = resp.json()
    assert body["subject_iri"] == subject_iri
    assert body["subject_id"] == subject_id
    assert body["project_subject_iri"] == project_subject_iri


def test_get_subject(api_base_url, sigv4_auth, test_project):
    project_id = test_project
    project_subject_id = f"test-subj-{uuid.uuid4().hex[:6]}"
    project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}/{project_subject_id}"

    # --- Create subject ---
    resp = requests.post(
        f"{api_base_url}/subject/create",
        json={"project_id": project_id, "project_subject_id": project_subject_id},
        auth=sigv4_auth,
    )
    assert resp.status_code == 200
    subject_iri = resp.json()["subject"]["iri"]
    subject_id = subject_iri.split("/")[-1]

    # --- Get newly created subject via project_subject_iri (should have empty terms) ---
    resp = requests.post(
        f"{api_base_url}/subject",
        json={"project_subject_iri": project_subject_iri},
        auth=sigv4_auth,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["project_subject_iri"] == project_subject_iri
    assert body["subject_iri"] == subject_iri
    assert body["subject_id"] == subject_id
    assert body["terms"] == [], "Newly created subject should have no terms"

    # --- Add a term link ---
    test_term_iri = "http://purl.obolibrary.org/obo/HP_0001234"
    resp = requests.post(
        f"{api_base_url}/term-link/create",
        json={
            "subject_id": subject_id,
            "term_iri": test_term_iri,
            "creator_id": "test-creator"
        },
        auth=sigv4_auth,
    )
    assert resp.status_code == 200

    # --- Get subject via project_subject_iri (should have term data) ---
    resp = requests.post(
        f"{api_base_url}/subject",
        json={"project_subject_iri": project_subject_iri},
        auth=sigv4_auth,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["project_subject_iri"] == project_subject_iri
    assert body["subject_iri"] == subject_iri
    assert len(body["terms"]) == 1, "Subject should have one term"
    assert body["terms"][0]["term_iri"] == test_term_iri

    # --- Get subject via subject_id (tests Iceberg direct path) ---
    resp = requests.post(
        f"{api_base_url}/subject",
        json={"subject_id": subject_id},
        auth=sigv4_auth,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["subject_id"] == subject_id
    assert body["subject_iri"] == subject_iri
    assert "project_subject_iri" not in body, "Direct subject_id lookup should not include project_subject_iri"
    assert len(body["terms"]) == 1, "Subject should have one term"
    assert body["terms"][0]["term_iri"] == test_term_iri

    # --- Test 404 for non-existent project_subject_iri ---
    resp = requests.post(
        f"{api_base_url}/subject",
        json={"project_subject_iri": f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}/nonexistent"},
        auth=sigv4_auth,
    )
    assert resp.status_code == 404

    # --- Test 404 for non-existent subject_id ---
    resp = requests.post(
        f"{api_base_url}/subject",
        json={"subject_id": "nonexistent-subject-id"},
        auth=sigv4_auth,
    )
    assert resp.status_code == 404


def test_remove_subject_without_terms(api_base_url, sigv4_auth, test_project):
    """Test removing a newly created subject that has no term links yet"""
    project_id = test_project
    project_subject_id = f"test-subj-{uuid.uuid4().hex[:6]}"
    project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}/{project_subject_id}"

    # --- Create subject without adding any terms ---
    resp = requests.post(
        f"{api_base_url}/subject/create",
        json={"project_id": project_id, "project_subject_id": project_subject_id},
        auth=sigv4_auth,
    )
    assert resp.status_code == 200
    subject_iri = resp.json()["subject"]["iri"]

    # --- Verify subject exists ---
    resp = requests.post(
        f"{api_base_url}/subject",
        json={"project_subject_iri": project_subject_iri},
        auth=sigv4_auth,
    )
    assert resp.status_code == 200
    assert resp.json()["subject_iri"] == subject_iri
    assert resp.json()["terms"] == []

    # --- Remove subject ---
    resp = requests.post(
        f"{api_base_url}/subject/remove",
        json={"project_subject_iri": project_subject_iri},
        auth=sigv4_auth,
    )
    assert resp.status_code == 200

    # --- Verify subject is gone ---
    resp = requests.post(
        f"{api_base_url}/subject",
        json={"project_subject_iri": project_subject_iri},
        auth=sigv4_auth,
    )
    assert resp.status_code == 404


def test_remove_subject(api_base_url, sigv4_auth, test_project):
    project_id = test_project
    project_subject_id = f"test-subj-{uuid.uuid4().hex[:6]}"
    project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}/{project_subject_id}"

    # --- Create subject ---
    resp = requests.post(
        f"{api_base_url}/subject/create",
        json={"project_id": project_id, "project_subject_id": project_subject_id},
        auth=sigv4_auth,
    )
    assert resp.status_code == 200
    subject_iri = resp.json()["subject"]["iri"]
    subject_id = subject_iri.split("/")[-1]

    # --- Add a term link to ensure Iceberg data exists ---
    resp = requests.post(
        f"{api_base_url}/term-link/create",
        json={
            "subject_id": subject_id,
            "term_iri": "http://purl.obolibrary.org/obo/HP_0001234",
            "creator_id": "test-creator"
        },
        auth=sigv4_auth,
    )
    assert resp.status_code == 200

    # --- Verify subject exists via project_subject_iri ---
    resp = requests.post(
        f"{api_base_url}/subject",
        json={"project_subject_iri": project_subject_iri},
        auth=sigv4_auth,
    )
    assert resp.status_code == 200
    assert resp.json()["subject_iri"] == subject_iri

    # --- Verify subject exists via subject_id (tests Iceberg path) ---
    resp = requests.post(
        f"{api_base_url}/subject",
        json={"subject_id": subject_id},
        auth=sigv4_auth,
    )
    assert resp.status_code == 200
    assert resp.json()["subject_id"] == subject_id
    assert len(resp.json()["terms"]) > 0, "Subject should have term links"

    # --- Remove subject via project_subject_iri ---
    resp = requests.post(
        f"{api_base_url}/subject/remove",
        json={"project_subject_iri": project_subject_iri},
        auth=sigv4_auth,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["message"].startswith("Subject removed")

    # --- Confirm subject is gone via project_subject_iri (tests DynamoDB + Iceberg) ---
    resp = requests.post(
        f"{api_base_url}/subject",
        json={"project_subject_iri": project_subject_iri},
        auth=sigv4_auth,
    )
    assert resp.status_code == 404
    assert resp.json()["message"] == "Subject not found"

    # --- Confirm subject is gone via subject_id (tests Iceberg directly) ---
    resp = requests.post(
        f"{api_base_url}/subject",
        json={"subject_id": subject_id},
        auth=sigv4_auth,
    )
    assert resp.status_code == 404
    assert resp.json()["message"] == "Subject not found"


def test_create_duplicate_subject(api_base_url, sigv4_auth, test_project):
    """Test that creating the same subject twice returns the existing subject"""
    project_id = test_project
    project_subject_id = f"test-subj-{uuid.uuid4().hex[:6]}"

    # --- Create subject first time ---
    resp1 = requests.post(
        f"{api_base_url}/subject/create",
        json={"project_id": project_id, "project_subject_id": project_subject_id},
        auth=sigv4_auth,
    )
    assert resp1.status_code == 200
    body1 = resp1.json()
    assert body1["subject_created"] is True
    subject_iri_1 = body1["subject"]["iri"]

    # --- Create same subject again ---
    resp2 = requests.post(
        f"{api_base_url}/subject/create",
        json={"project_id": project_id, "project_subject_id": project_subject_id},
        auth=sigv4_auth,
    )
    assert resp2.status_code == 200
    body2 = resp2.json()
    assert body2["subject_created"] is False, "Should return existing subject"
    subject_iri_2 = body2["subject"]["iri"]

    # --- Should be the same subject ---
    assert subject_iri_1 == subject_iri_2, "Both calls should return the same subject IRI"


def test_link_existing_subject(api_base_url, sigv4_auth, test_project):
    project_id = test_project
    subj_id_1 = f"test-subj-{uuid.uuid4().hex[:6]}"
    subj_id_2 = f"test-subj-{uuid.uuid4().hex[:6]}"
    project_subject_iri_1 = f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}/{subj_id_1}"
    project_subject_iri_2 = f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}/{subj_id_2}"

    # --- First subject creation ---
    create_resp_1 = requests.post(
        f"{api_base_url}/subject/create",
        json={"project_id": project_id, "project_subject_id": subj_id_1},
        auth=sigv4_auth,
    )
    assert create_resp_1.status_code == 200
    first_subject = create_resp_1.json()["subject"]
    subject_iri = first_subject["iri"]
    subject_id = subject_iri.split("/")[-1]

    # --- Link existing subject to a new project_subject_iri ---
    create_resp_2 = requests.post(
        f"{api_base_url}/subject/create",
        json={
            "project_id": project_id,
            "project_subject_id": subj_id_2,
            "known_subject_iri": subject_iri,
        },
        auth=sigv4_auth,
    )

    assert create_resp_2.status_code == 200
    body = create_resp_2.json()
    assert body["subject_created"] is False
    assert body["subject"]["iri"] == subject_iri
    assert body["subject"]["projects"][project_id] == subj_id_2

    # --- Verify both project_subject_iris resolve to the same subject ---
    resp1 = requests.post(
        f"{api_base_url}/subject",
        json={"project_subject_iri": project_subject_iri_1},
        auth=sigv4_auth,
    )
    assert resp1.status_code == 200
    body1 = resp1.json()
    assert body1["subject_iri"] == subject_iri
    assert body1["subject_id"] == subject_id
    assert body1["project_subject_iri"] == project_subject_iri_1

    resp2 = requests.post(
        f"{api_base_url}/subject",
        json={"project_subject_iri": project_subject_iri_2},
        auth=sigv4_auth,
    )
    assert resp2.status_code == 200
    body2 = resp2.json()
    assert body2["subject_iri"] == subject_iri
    assert body2["subject_id"] == subject_id
    assert body2["project_subject_iri"] == project_subject_iri_2

    # --- Verify both resolve to the same underlying subject ---
    assert body1["subject_id"] == body2["subject_id"], "Both project_subject_iris should map to the same subject_id"


def test_link_via_known_project_id(api_base_url, sigv4_auth, test_project):
    """Test linking a subject using known_project_id and known_project_subject_id"""
    project_id = test_project
    subj_id_1 = f"test-subj-{uuid.uuid4().hex[:6]}"
    subj_id_2 = f"test-subj-{uuid.uuid4().hex[:6]}"

    # --- Create first subject ---
    resp1 = requests.post(
        f"{api_base_url}/subject/create",
        json={"project_id": project_id, "project_subject_id": subj_id_1},
        auth=sigv4_auth,
    )
    assert resp1.status_code == 200
    subject_iri = resp1.json()["subject"]["iri"]

    # --- Link to second project_subject_id using known_project_id path ---
    resp2 = requests.post(
        f"{api_base_url}/subject/create",
        json={
            "project_id": project_id,
            "project_subject_id": subj_id_2,
            "known_project_id": project_id,
            "known_project_subject_id": subj_id_1,
        },
        auth=sigv4_auth,
    )
    assert resp2.status_code == 200
    body = resp2.json()
    assert body["subject_created"] is False
    assert body["subject"]["iri"] == subject_iri

    # --- Verify both project_subject_ids resolve to same subject ---
    project_subject_iri_1 = f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}/{subj_id_1}"
    project_subject_iri_2 = f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}/{subj_id_2}"

    resp1 = requests.post(
        f"{api_base_url}/subject",
        json={"project_subject_iri": project_subject_iri_1},
        auth=sigv4_auth,
    )
    assert resp1.status_code == 200
    assert resp1.json()["subject_iri"] == subject_iri

    resp2 = requests.post(
        f"{api_base_url}/subject",
        json={"project_subject_iri": project_subject_iri_2},
        auth=sigv4_auth,
    )
    assert resp2.status_code == 200
    assert resp2.json()["subject_iri"] == subject_iri


def test_unlink_subject_with_multiple_mappings(api_base_url, sigv4_auth, test_project):
    """Test that removing one mapping doesn't delete the subject if other mappings exist"""
    project_id = test_project
    subj_id_1 = f"test-subj-{uuid.uuid4().hex[:6]}"
    subj_id_2 = f"test-subj-{uuid.uuid4().hex[:6]}"
    project_subject_iri_1 = f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}/{subj_id_1}"
    project_subject_iri_2 = f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}/{subj_id_2}"

    # --- Create subject ---
    resp = requests.post(
        f"{api_base_url}/subject/create",
        json={"project_id": project_id, "project_subject_id": subj_id_1},
        auth=sigv4_auth,
    )
    assert resp.status_code == 200
    subject_iri = resp.json()["subject"]["iri"]
    subject_id = subject_iri.split("/")[-1]

    # --- Add term links so Iceberg has data ---
    resp = requests.post(
        f"{api_base_url}/term-link/create",
        json={
            "subject_id": subject_id,
            "term_iri": "http://purl.obolibrary.org/obo/HP_0001234",
            "creator_id": "test-creator"
        },
        auth=sigv4_auth,
    )
    assert resp.status_code == 200

    # --- Link to second project_subject_id ---
    resp = requests.post(
        f"{api_base_url}/subject/create",
        json={
            "project_id": project_id,
            "project_subject_id": subj_id_2,
            "known_subject_iri": subject_iri,
        },
        auth=sigv4_auth,
    )
    assert resp.status_code == 200

    # --- Verify both mappings work ---
    resp1 = requests.post(
        f"{api_base_url}/subject",
        json={"project_subject_iri": project_subject_iri_1},
        auth=sigv4_auth,
    )
    assert resp1.status_code == 200
    assert resp1.json()["subject_id"] == subject_id

    resp2 = requests.post(
        f"{api_base_url}/subject",
        json={"project_subject_iri": project_subject_iri_2},
        auth=sigv4_auth,
    )
    assert resp2.status_code == 200
    assert resp2.json()["subject_id"] == subject_id

    # --- Remove first mapping ---
    resp = requests.post(
        f"{api_base_url}/subject/remove",
        json={"project_subject_iri": project_subject_iri_1},
        auth=sigv4_auth,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert "unlinked" in body["message"].lower(), "Should indicate unlinking, not full removal"

    # --- First mapping should be gone ---
    resp = requests.post(
        f"{api_base_url}/subject",
        json={"project_subject_iri": project_subject_iri_1},
        auth=sigv4_auth,
    )
    assert resp.status_code == 404

    # --- Second mapping should STILL work (subject not deleted) ---
    resp = requests.post(
        f"{api_base_url}/subject",
        json={"project_subject_iri": project_subject_iri_2},
        auth=sigv4_auth,
    )
    assert resp.status_code == 200
    assert resp.json()["subject_id"] == subject_id

    # --- Subject should still have terms in Iceberg ---
    resp = requests.post(
        f"{api_base_url}/subject",
        json={"subject_id": subject_id},
        auth=sigv4_auth,
    )
    assert resp.status_code == 200
    assert len(resp.json()["terms"]) > 0, "Subject data should still exist in Iceberg"

    # --- Now remove the LAST mapping ---
    resp = requests.post(
        f"{api_base_url}/subject/remove",
        json={"project_subject_iri": project_subject_iri_2},
        auth=sigv4_auth,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert "removed" in body["message"].lower(), "Should indicate full removal"

    # --- Now subject should be completely gone ---
    resp = requests.post(
        f"{api_base_url}/subject",
        json={"subject_id": subject_id},
        auth=sigv4_auth,
    )
    assert resp.status_code == 404, "Subject should be fully deleted from Iceberg after last mapping removed"


def test_link_already_linked_subject_via_known_iri(api_base_url, sigv4_auth, test_project):
    """Test that attempting to link an already-linked subject returns existing mapping without duplicates"""
    project_id = test_project
    subj_id = f"test-subj-{uuid.uuid4().hex[:6]}"

    # --- Create subject ---
    resp = requests.post(
        f"{api_base_url}/subject/create",
        json={"project_id": project_id, "project_subject_id": subj_id},
        auth=sigv4_auth,
    )
    assert resp.status_code == 200
    result = resp.json()
    assert result["subject_created"] is True
    subject_iri = result["subject"]["iri"]

    # --- Attempt to link the SAME subject to the SAME project_subject_id via known_subject_iri ---
    resp = requests.post(
        f"{api_base_url}/subject/create",
        json={
            "project_id": project_id,
            "project_subject_id": subj_id,
            "known_subject_iri": subject_iri,
        },
        auth=sigv4_auth,
    )
    assert resp.status_code == 200
    result = resp.json()
    # Should return False since it already existed
    assert result["subject_created"] is False
    # Should return the same subject IRI
    assert result["subject"]["iri"] == subject_iri
    # Should have the same project mapping
    assert result["subject"]["projects"][project_id] == subj_id

    # --- Verify subject is still accessible and not duplicated ---
    project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{project_id}/{subj_id}"
    resp = requests.post(
        f"{api_base_url}/subject",
        json={"project_subject_iri": project_subject_iri},
        auth=sigv4_auth,
    )
    assert resp.status_code == 200
    assert resp.json()["subject_iri"] == subject_iri


def test_link_different_subject_to_used_project_subject_id(api_base_url, sigv4_auth, test_project):
    """Test that attempting to link a different subject to an already-used project_subject_id fails"""
    project_id = test_project
    subj_id = f"test-subj-{uuid.uuid4().hex[:6]}"

    # --- Create first subject ---
    resp = requests.post(
        f"{api_base_url}/subject/create",
        json={"project_id": project_id, "project_subject_id": subj_id},
        auth=sigv4_auth,
    )
    assert resp.status_code == 200
    first_subject_iri = resp.json()["subject"]["iri"]

    # --- Create second subject with different project_subject_id ---
    subj_id_2 = f"test-subj-{uuid.uuid4().hex[:6]}"
    resp = requests.post(
        f"{api_base_url}/subject/create",
        json={"project_id": project_id, "project_subject_id": subj_id_2},
        auth=sigv4_auth,
    )
    assert resp.status_code == 200
    second_subject_iri = resp.json()["subject"]["iri"]
    assert second_subject_iri != first_subject_iri

    # --- Attempt to link second subject to first subject's project_subject_id ---
    resp = requests.post(
        f"{api_base_url}/subject/create",
        json={
            "project_id": project_id,
            "project_subject_id": subj_id,  # Already used by first_subject
            "known_subject_iri": second_subject_iri,
        },
        auth=sigv4_auth,
    )
    # Should fail with 400 error
    assert resp.status_code == 400
    assert "already linked to a different subject" in resp.json()["error"]


def test_cross_project_subject_linking(api_base_url, sigv4_auth):
    """Test linking a subject across multiple projects and removing mappings"""
    # --- Create two test projects ---
    project_id_a = f"test-proj-{uuid.uuid4().hex[:6]}"
    project_id_b = f"test-proj-{uuid.uuid4().hex[:6]}"

    for proj_id in [project_id_a, project_id_b]:
        resp = requests.post(
            f"{api_base_url}/project/create",
            json={"project_id": proj_id},
            auth=sigv4_auth,
        )
        assert resp.status_code == 200

    # --- Create subject in Project A ---
    subj_id_a = f"test-subj-{uuid.uuid4().hex[:6]}"
    project_subject_iri_a = f"http://ods.nationwidechildrens.org/phebee/projects/{project_id_a}/{subj_id_a}"

    resp = requests.post(
        f"{api_base_url}/subject/create",
        json={"project_id": project_id_a, "project_subject_id": subj_id_a},
        auth=sigv4_auth,
    )
    assert resp.status_code == 200
    subject_iri = resp.json()["subject"]["iri"]
    subject_id = subject_iri.split("/")[-1]

    # --- Add term links ---
    resp = requests.post(
        f"{api_base_url}/term-link/create",
        json={
            "subject_id": subject_id,
            "term_iri": "http://purl.obolibrary.org/obo/HP_0001234",
            "creator_id": "test-creator"
        },
        auth=sigv4_auth,
    )
    assert resp.status_code == 200

    # --- Link same subject to Project B (cross-project) ---
    subj_id_b = f"test-subj-{uuid.uuid4().hex[:6]}"
    project_subject_iri_b = f"http://ods.nationwidechildrens.org/phebee/projects/{project_id_b}/{subj_id_b}"

    resp = requests.post(
        f"{api_base_url}/subject/create",
        json={
            "project_id": project_id_b,
            "project_subject_id": subj_id_b,
            "known_subject_iri": subject_iri,
        },
        auth=sigv4_auth,
    )
    assert resp.status_code == 200
    assert resp.json()["subject"]["iri"] == subject_iri

    # --- Verify both project mappings work ---
    resp_a = requests.post(
        f"{api_base_url}/subject",
        json={"project_subject_iri": project_subject_iri_a},
        auth=sigv4_auth,
    )
    assert resp_a.status_code == 200
    assert resp_a.json()["subject_id"] == subject_id

    resp_b = requests.post(
        f"{api_base_url}/subject",
        json={"project_subject_iri": project_subject_iri_b},
        auth=sigv4_auth,
    )
    assert resp_b.status_code == 200
    assert resp_b.json()["subject_id"] == subject_id

    # --- Remove Project A mapping (NOT the last mapping) ---
    resp = requests.post(
        f"{api_base_url}/subject/remove",
        json={"project_subject_iri": project_subject_iri_a},
        auth=sigv4_auth,
    )
    assert resp.status_code == 200
    assert "unlinked" in resp.json()["message"].lower()

    # --- Project A mapping should be gone ---
    resp = requests.post(
        f"{api_base_url}/subject",
        json={"project_subject_iri": project_subject_iri_a},
        auth=sigv4_auth,
    )
    assert resp.status_code == 404

    # --- Project B mapping should STILL work ---
    resp = requests.post(
        f"{api_base_url}/subject",
        json={"project_subject_iri": project_subject_iri_b},
        auth=sigv4_auth,
    )
    assert resp.status_code == 200
    assert resp.json()["subject_id"] == subject_id

    # --- Subject should still exist with terms (by_subject table not deleted) ---
    resp = requests.post(
        f"{api_base_url}/subject",
        json={"subject_id": subject_id},
        auth=sigv4_auth,
    )
    assert resp.status_code == 200
    assert len(resp.json()["terms"]) > 0, "Subject should still have terms after unlinking from Project A"

    # --- Remove Project B mapping (the LAST mapping) ---
    resp = requests.post(
        f"{api_base_url}/subject/remove",
        json={"project_subject_iri": project_subject_iri_b},
        auth=sigv4_auth,
    )
    assert resp.status_code == 200
    assert "removed" in resp.json()["message"].lower()

    # --- Now subject should be completely gone from Iceberg ---
    resp = requests.post(
        f"{api_base_url}/subject",
        json={"subject_id": subject_id},
        auth=sigv4_auth,
    )
    assert resp.status_code == 404, "Subject should be fully deleted after removing last mapping"
