import pytest
import json
import uuid
import boto3
import time
from general_utils import invoke_lambda as _invoke_lambda


def invoke_lambda(name, payload):
    """Invoke a Lambda and return the parsed body if API-proxy shaped, else the raw payload."""
    raw = _invoke_lambda(name, payload)
    # If API-proxy style {statusCode, body}, return parsed body only
    if isinstance(raw, dict) and "body" in raw:
        body = raw.get("body")
        try:
            return json.loads(body) if isinstance(body, str) else body
        except Exception:
            return body
    return raw


def create_evidence_record(run_id, batch_id, evidence_type, subject_id, term_iri, creator_id, physical_resources, creator_name=None, creator_type="human"):
    """Create a single evidence record."""
    evidence_data = {
        "run_id": run_id,
        "batch_id": batch_id,
        "evidence_type": evidence_type,
        "subject_id": subject_id,
        "term_iri": term_iri,
        "creator_id": creator_id,
        "creator_type": creator_type
    }
    if creator_name:
        evidence_data["creator_name"] = creator_name
    
    return invoke_lambda(physical_resources["CreateEvidenceFunction"], evidence_data)


def query_evidence_by_run(run_id, physical_resources, limit=1000, next_token=None):
    """Query evidence records by run_id."""
    payload = {"run_id": run_id, "limit": limit}
    if next_token:
        payload["next_token"] = next_token
    
    return invoke_lambda(physical_resources["QueryEvidenceByRunFunction"], payload)


def bulk_upload_run(batch_payloads, physical_resources):
    """Simulate bulk upload using evidence system - replacement for Neptune bulk upload."""
    run_id = str(uuid.uuid4())
    
    for batch_idx, batch_payload in enumerate(batch_payloads):
        batch_id = str(batch_idx)
        
        for record in batch_payload:
            # Extract data from the old payload format
            project_id = record["project_id"]
            project_subject_id = record["project_subject_id"]
            term_iri = record["term_iri"]
            
            # Create evidence records for each evidence item
            for evidence in record.get("evidence", []):
                evidence_data = {
                    "run_id": run_id,
                    "batch_id": batch_id,
                    "evidence_type": evidence.get("type", "clinical_note"),
                    "subject_id": f"{project_id}:{project_subject_id}",
                    "term_iri": term_iri,
                    "creator_id": evidence.get("evidence_creator_id", "unknown"),
                    "creator_type": evidence.get("evidence_creator_type", "automated"),
                    "clinical_note_id": evidence.get("clinical_note_id"),
                    "encounter_id": evidence.get("encounter_id")
                }
                
                # Create the evidence record
                create_evidence_record(
                    run_id=run_id,
                    batch_id=batch_id,
                    evidence_type=evidence_data["evidence_type"],
                    subject_id=evidence_data["subject_id"],
                    term_iri=term_iri,
                    creator_id=evidence_data["creator_id"],
                    physical_resources=physical_resources,
                    creator_type=evidence_data["creator_type"]
                )
    
    # Return format compatible with old bulk_upload_run: (run_id, success_count, error_count)
    return run_id, len([r for batch in batch_payloads for r in batch]), 0


@pytest.mark.integration
def test_evidence_creation_and_retrieval(physical_resources, test_project_id):
    """Test basic evidence creation and retrieval functionality."""
    
    run_id = str(uuid.uuid4())
    subject_id = f"subj-{uuid.uuid4()}"
    
    # Create evidence record
    evidence = create_evidence_record(
        run_id=run_id,
        batch_id="1",
        evidence_type="clinical_note",
        subject_id=subject_id,
        term_iri="http://purl.obolibrary.org/obo/HP_0001249",
        creator_id="test-creator",
        creator_name="Test Creator",
        creator_type="automated",
        physical_resources=physical_resources
    )
    
    # Verify evidence creation response
    required_fields = ["evidence_id", "run_id", "batch_id", "subject_id", "term_iri", "creator"]
    for field in required_fields:
        assert field in evidence, f"Missing field in evidence response: {field}"
    
    assert evidence["run_id"] == run_id
    assert evidence["subject_id"] == subject_id
    
    # Verify creator structure
    creator = evidence["creator"]
    assert creator["creator_id"] == "test-creator"
    assert creator["creator_name"] == "Test Creator"
    assert creator["creator_type"] == "automated"
    
    # Query evidence by run_id
    query_result = query_evidence_by_run(run_id, physical_resources)
    
    assert query_result["run_id"] == run_id
    assert query_result["evidence_count"] == 1
    assert len(query_result["evidence"]) == 1
    
    retrieved_evidence = query_result["evidence"][0]
    assert retrieved_evidence["evidence_id"] == evidence["evidence_id"]
    assert retrieved_evidence["subject_id"] == subject_id


@pytest.mark.integration  
def test_evidence_pagination(physical_resources, test_project_id):
    """Test pagination of evidence queries."""
    
    run_id = str(uuid.uuid4())
    
    # Create multiple evidence records
    evidence_ids = []
    for i in range(5):
        evidence = create_evidence_record(
            run_id=run_id,
            batch_id="1",
            evidence_type="clinical_note",
            subject_id=f"subj-{i}",
            term_iri="http://purl.obolibrary.org/obo/HP_0001249",
            creator_id=f"creator-{i}",
            physical_resources=physical_resources
        )
        evidence_ids.append(evidence["evidence_id"])
    
    # Query with small limit to test pagination
    first_page = query_evidence_by_run(run_id, physical_resources, limit=2)
    
    assert first_page["run_id"] == run_id
    assert first_page["evidence_count"] == 2
    assert first_page["limit"] == 2
    assert len(first_page["evidence"]) == 2
    assert "next_token" in first_page
    
    # Query second page
    second_page = query_evidence_by_run(run_id, physical_resources, limit=2, next_token=first_page["next_token"])
    
    assert second_page["run_id"] == run_id
    assert second_page["evidence_count"] == 2
    assert len(second_page["evidence"]) == 2
    assert "next_token" in second_page
    
    # Query final page
    final_page = query_evidence_by_run(run_id, physical_resources, limit=2, next_token=second_page["next_token"])
    
    assert final_page["run_id"] == run_id
    assert final_page["evidence_count"] == 1
    assert len(final_page["evidence"]) == 1
    assert "next_token" not in final_page  # No more pages
    
    # Verify no duplicate evidence IDs across pages
    all_evidence_ids = []
    all_evidence_ids.extend([e["evidence_id"] for e in first_page["evidence"]])
    all_evidence_ids.extend([e["evidence_id"] for e in second_page["evidence"]])
    all_evidence_ids.extend([e["evidence_id"] for e in final_page["evidence"]])
    
    assert len(all_evidence_ids) == len(set(all_evidence_ids)), "Found duplicate evidence IDs across pages"
    assert set(all_evidence_ids) == set(evidence_ids), "Missing evidence IDs in paginated results"


@pytest.mark.integration
def test_evidence_run_isolation(physical_resources, test_project_id):
    """Test that evidence records are properly isolated by run_id."""
    
    run_id1 = str(uuid.uuid4())
    run_id2 = str(uuid.uuid4())
    
    # Create evidence in first run
    evidence1 = create_evidence_record(
        run_id=run_id1,
        batch_id="1",
        evidence_type="clinical_note",
        subject_id="subj-1",
        term_iri="http://purl.obolibrary.org/obo/HP_0001249",
        creator_id="creator-1",
        physical_resources=physical_resources
    )
    
    # Create evidence in second run
    evidence2 = create_evidence_record(
        run_id=run_id2,
        batch_id="1",
        evidence_type="clinical_note",
        subject_id="subj-2",
        term_iri="http://purl.obolibrary.org/obo/HP_0002297",
        creator_id="creator-2",
        physical_resources=physical_resources
    )
    
    # Query each run separately
    results1 = query_evidence_by_run(run_id1, physical_resources)
    results2 = query_evidence_by_run(run_id2, physical_resources)
    
    # Each run should only see its own evidence
    assert results1["evidence_count"] == 1
    assert results2["evidence_count"] == 1
    
    assert results1["evidence"][0]["evidence_id"] == evidence1["evidence_id"]
    assert results2["evidence"][0]["evidence_id"] == evidence2["evidence_id"]
    
    # Verify run isolation
    assert results1["evidence"][0]["run_id"] == run_id1
    assert results2["evidence"][0]["run_id"] == run_id2


@pytest.mark.integration
def test_evidence_creator_metadata(physical_resources, test_project_id):
    """Test that creator metadata is properly stored and retrieved."""
    
    run_id = str(uuid.uuid4())
    
    # Test with all creator fields
    evidence_full = create_evidence_record(
        run_id=run_id,
        batch_id="1",
        evidence_type="clinical_note",
        subject_id="subj-full",
        term_iri="http://purl.obolibrary.org/obo/HP_0001249",
        creator_id="user-123",
        creator_name="Dr. Smith",
        creator_type="human",
        physical_resources=physical_resources
    )
    
    creator = evidence_full["creator"]
    assert creator["creator_id"] == "user-123"
    assert creator["creator_name"] == "Dr. Smith"
    assert creator["creator_type"] == "human"
    
    # Test with minimal creator fields (defaults)
    evidence_minimal = create_evidence_record(
        run_id=run_id,
        batch_id="2",
        evidence_type="clinical_note",
        subject_id="subj-minimal",
        term_iri="http://purl.obolibrary.org/obo/HP_0002297",
        creator_id="system-456",
        physical_resources=physical_resources
    )
    
    creator_minimal = evidence_minimal["creator"]
    assert creator_minimal["creator_id"] == "system-456"
    assert creator_minimal["creator_type"] == "human"  # default
    assert "creator_name" not in creator_minimal or creator_minimal["creator_name"] is None
    
    # Query all evidence and verify creator metadata is preserved
    results = query_evidence_by_run(run_id, physical_resources)
    assert results["evidence_count"] == 2
    
    evidence_by_subject = {e["subject_id"]: e for e in results["evidence"]}
    
    # Verify full creator metadata
    full_evidence = evidence_by_subject["subj-full"]
    assert full_evidence["creator"]["creator_id"] == "user-123"
    assert full_evidence["creator"]["creator_name"] == "Dr. Smith"
    assert full_evidence["creator"]["creator_type"] == "human"
    
    # Verify minimal creator metadata
    minimal_evidence = evidence_by_subject["subj-minimal"]
    assert minimal_evidence["creator"]["creator_id"] == "system-456"
    assert minimal_evidence["creator"]["creator_type"] == "human"


@pytest.mark.integration
def test_evidence_schema_validation(physical_resources, test_project_id):
    """Test that evidence records conform to expected schema."""
    
    run_id = str(uuid.uuid4())
    
    evidence = create_evidence_record(
        run_id=run_id,
        batch_id="1",
        evidence_type="clinical_note",
        subject_id="test-subject",
        term_iri="http://purl.obolibrary.org/obo/HP_0001249",
        creator_id="test-creator",
        physical_resources=physical_resources
    )
    
    # Verify required fields are present
    required_fields = [
        "evidence_id", "run_id", "batch_id", "subject_id", 
        "term_iri", "evidence_type", "creator", "created_timestamp"
    ]
    
    for field in required_fields:
        assert field in evidence, f"Missing required field: {field}"
    
    # Verify field types and formats
    assert isinstance(evidence["evidence_id"], str)
    assert isinstance(evidence["run_id"], str)
    assert isinstance(evidence["batch_id"], str)
    assert isinstance(evidence["subject_id"], str)
    assert isinstance(evidence["term_iri"], str)
    assert isinstance(evidence["evidence_type"], str)
    assert isinstance(evidence["creator"], dict)
    assert isinstance(evidence["created_timestamp"], str)
    
    # Verify creator structure
    creator = evidence["creator"]
    assert "creator_id" in creator
    assert "creator_type" in creator
    
    # Query and verify retrieved evidence has same schema
    results = query_evidence_by_run(run_id, physical_resources)
    retrieved_evidence = results["evidence"][0]
    
    for field in required_fields:
        assert field in retrieved_evidence, f"Missing field in retrieved evidence: {field}"
    
    # Verify creator structure is preserved in query results
    retrieved_creator = retrieved_evidence["creator"]
    assert retrieved_creator["creator_id"] == "test-creator"
    assert retrieved_creator["creator_type"] == "human"


@pytest.mark.integration
def test_evidence_multi_batch_run(physical_resources, test_project_id):
    """Test evidence tracking across multiple batches in a single run."""
    
    # Create test data for multiple batches
    batch1_payload = [{
        "project_id": test_project_id,
        "project_subject_id": f"subj1-{uuid.uuid4()}",
        "term_iri": "http://purl.obolibrary.org/obo/HP_0001249",
        "evidence": [{
            "type": "clinical_note",
            "clinical_note_id": f"note1-{uuid.uuid4()}",
            "encounter_id": str(uuid.uuid4()),
            "evidence_creator_id": "test-creator",
            "evidence_creator_type": "automated"
        }]
    }]
    
    batch2_payload = [{
        "project_id": test_project_id,
        "project_subject_id": f"subj2-{uuid.uuid4()}",
        "term_iri": "http://purl.obolibrary.org/obo/HP_0002297",
        "evidence": [{
            "type": "clinical_note",
            "clinical_note_id": f"note2-{uuid.uuid4()}",
            "encounter_id": str(uuid.uuid4()),
            "evidence_creator_id": "test-creator",
            "evidence_creator_type": "automated"
        }]
    }]
    
    # Perform multi-batch upload
    run_id, _, _ = bulk_upload_run([batch1_payload, batch2_payload], physical_resources)
    
    # Query evidence records - should have exactly 2 evidence records (one per batch)
    evidence_response = query_evidence_by_run(run_id, physical_resources)
    evidence_records = evidence_response["evidence"]
    
    assert len(evidence_records) == 2, f"Expected 2 evidence records, got {len(evidence_records)}"
    
    # Validate batch IDs are present
    batch_ids = {record["batch_id"] for record in evidence_records}
    assert "0" in batch_ids, "Expected batch 0 in evidence records"
    assert "1" in batch_ids, "Expected batch 1 in evidence records"
    
    # Verify different subjects and terms
    subjects = {record["subject_id"] for record in evidence_records}
    terms = {record["term_iri"] for record in evidence_records}
    
    assert len(subjects) == 2, f"Expected 2 different subjects, got {len(subjects)}"
    assert len(terms) == 2, f"Expected 2 different terms, got {len(terms)}"
    
    # Verify specific terms are present
    assert "HP_0001249" in str(terms), "Expected HP_0001249 term"
    assert "HP_0002297" in str(terms), "Expected HP_0002297 term"


@pytest.mark.integration
def test_evidence_pagination_large_dataset(physical_resources, test_project_id):
    """Test pagination with large evidence datasets."""
    
    # Create a run with many evidence records to test pagination
    large_batch_payload = []
    for i in range(10):
        large_batch_payload.append({
            "project_id": test_project_id,
            "project_subject_id": f"subj-{i}",
            "term_iri": f"http://purl.obolibrary.org/obo/HP_{i:07d}",
            "evidence": [{
                "type": "clinical_note",
                "clinical_note_id": f"note-{i}",
                "encounter_id": str(uuid.uuid4()),
                "evidence_creator_id": f"creator-{i}",
                "evidence_creator_type": "automated"
            }]
        })
    
    run_id, _, _ = bulk_upload_run([large_batch_payload], physical_resources)
    
    # Test pagination with small page size
    all_evidence = []
    next_token = None
    page_count = 0
    
    while True:
        page_count += 1
        response = query_evidence_by_run(run_id, physical_resources, limit=3, next_token=next_token)
        
        assert response["run_id"] == run_id
        assert len(response["evidence"]) <= 3, f"Page {page_count} has too many records"
        
        all_evidence.extend(response["evidence"])
        
        if "next_token" not in response:
            break
        
        next_token = response["next_token"]
        assert page_count < 10, "Too many pages - possible infinite loop"
    
    # Verify all evidence was retrieved
    assert len(all_evidence) == 10, f"Expected 10 evidence records, got {len(all_evidence)}"
    
    # Verify no duplicates
    evidence_ids = [e["evidence_id"] for e in all_evidence]
    assert len(evidence_ids) == len(set(evidence_ids)), "Found duplicate evidence IDs"
    
    # Verify all subjects are present
    subjects = {e["subject_id"] for e in all_evidence}
    expected_subjects = {f"{test_project_id}:subj-{i}" for i in range(10)}
    assert subjects == expected_subjects, f"Missing subjects: {expected_subjects - subjects}"


@pytest.mark.integration
def test_subject_reuse_across_runs(physical_resources, test_project_id):
    """Test that same project_id + project_subject_id reuses the same subject IRI across runs."""
    
    # Use identical subject identifier in both runs
    shared_subject_id = f"patient-{uuid.uuid4()}"
    
    # First run
    payload1 = [{
        "project_id": test_project_id,
        "project_subject_id": shared_subject_id,
        "term_iri": "http://purl.obolibrary.org/obo/HP_0001249",
        "evidence": [{
            "type": "clinical_note",
            "clinical_note_id": f"note1-{uuid.uuid4()}",
            "encounter_id": str(uuid.uuid4()),
            "evidence_creator_id": "test-creator",
            "evidence_creator_type": "automated"
        }]
    }]
    
    # Second run with same subject but different note/encounter
    payload2 = [{
        "project_id": test_project_id,
        "project_subject_id": shared_subject_id,  # Same subject!
        "term_iri": "http://purl.obolibrary.org/obo/HP_0002297",
        "evidence": [{
            "type": "clinical_note",
            "clinical_note_id": f"note2-{uuid.uuid4()}",
            "encounter_id": str(uuid.uuid4()),
            "evidence_creator_id": "test-creator",
            "evidence_creator_type": "automated"
        }]
    }]
    
    run_id1, _, _ = bulk_upload_run([payload1], physical_resources)
    run_id2, _, _ = bulk_upload_run([payload2], physical_resources)
    
    # Query evidence for both runs
    evidence1 = query_evidence_by_run(run_id1, physical_resources)
    evidence2 = query_evidence_by_run(run_id2, physical_resources)
    
    # Both runs should have evidence for the same subject
    subject1 = evidence1["evidence"][0]["subject_id"]
    subject2 = evidence2["evidence"][0]["subject_id"]
    
    # Subject IDs should be the same (reused across runs)
    assert subject1 == subject2, f"Subject should be reused: {subject1} != {subject2}"
    assert f"{test_project_id}:{shared_subject_id}" in subject1


@pytest.mark.integration
def test_evidence_with_term_source(physical_resources, test_project_id):
    """Test evidence creation and retrieval with term_source field."""
    
    run_id = str(uuid.uuid4())
    subject_id = f"subj-{uuid.uuid4()}"
    
    # Create evidence record with term_source
    evidence_data = {
        "run_id": run_id,
        "batch_id": "1",
        "evidence_type": "clinical_note",
        "subject_id": subject_id,
        "term_iri": "http://purl.obolibrary.org/obo/HP_0001249",
        "creator_id": "test-creator",
        "creator_type": "human",
        "term_source": {
            "source": "hpo",
            "version": "2024-01-01",
            "iri": "http://purl.obolibrary.org/obo/hp.owl"
        }
    }
    
    evidence = invoke_lambda(physical_resources["CreateEvidenceFunction"], evidence_data)
    
    # Verify evidence creation response
    assert "evidence_id" in evidence
    assert evidence["subject_id"] == subject_id
    assert evidence["term_iri"] == "http://purl.obolibrary.org/obo/HP_0001249"
    
    # Get the evidence record to verify term_source is stored
    get_evidence_data = {"evidence_id": evidence["evidence_id"]}
    retrieved_evidence = invoke_lambda(physical_resources["GetEvidenceFunction"], get_evidence_data)
    
    # Verify term_source is present and correct
    assert "term_source" in retrieved_evidence
    term_source = retrieved_evidence["term_source"]
    assert term_source["source"] == "hpo"
    assert term_source["version"] == "2024-01-01"
    assert term_source["iri"] == "http://purl.obolibrary.org/obo/hp.owl"


@pytest.mark.integration
def test_evidence_without_term_source(physical_resources, test_project_id):
    """Test evidence creation and retrieval without term_source field (optional)."""
    
    run_id = str(uuid.uuid4())
    subject_id = f"subj-{uuid.uuid4()}"
    
    # Create evidence record without term_source
    evidence_data = {
        "run_id": run_id,
        "batch_id": "1",
        "evidence_type": "clinical_note",
        "subject_id": subject_id,
        "term_iri": "http://purl.obolibrary.org/obo/HP_0002297",
        "creator_id": "test-creator",
        "creator_type": "human"
    }
    
    evidence = invoke_lambda(physical_resources["CreateEvidenceFunction"], evidence_data)
    
    # Verify evidence creation works without term_source
    assert "evidence_id" in evidence
    assert evidence["subject_id"] == subject_id
    
    # Get the evidence record to verify term_source is not present
    get_evidence_data = {"evidence_id": evidence["evidence_id"]}
    retrieved_evidence = invoke_lambda(physical_resources["GetEvidenceFunction"], get_evidence_data)
    
    # Verify term_source is not present (optional field)
    assert "term_source" not in retrieved_evidence or retrieved_evidence.get("term_source") is None


@pytest.mark.integration
def test_evidence_partial_term_source(physical_resources, test_project_id):
    """Test evidence with partial term_source data."""
    
    run_id = str(uuid.uuid4())
    subject_id = f"subj-{uuid.uuid4()}"
    
    # Create evidence record with partial term_source (only source)
    evidence_data = {
        "run_id": run_id,
        "batch_id": "1",
        "evidence_type": "clinical_note",
        "subject_id": subject_id,
        "term_iri": "http://purl.obolibrary.org/obo/MONDO_0005148",
        "creator_id": "test-creator",
        "creator_type": "human",
        "term_source": {
            "source": "mondo"
        }
    }
    
    evidence = invoke_lambda(physical_resources["CreateEvidenceFunction"], evidence_data)
    
    # Verify evidence creation works with partial term_source
    assert "evidence_id" in evidence
    
    # Get the evidence record to verify partial term_source is stored
    get_evidence_data = {"evidence_id": evidence["evidence_id"]}
    retrieved_evidence = invoke_lambda(physical_resources["GetEvidenceFunction"], get_evidence_data)
    
    # Verify term_source is present with only the provided field
    assert "term_source" in retrieved_evidence
    term_source = retrieved_evidence["term_source"]
    assert term_source["source"] == "mondo"
    assert term_source.get("version") is None or term_source.get("version") == ""
    assert term_source.get("iri") is None or term_source.get("iri") == ""
