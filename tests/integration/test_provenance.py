import pytest
import json
import uuid
import boto3
import requests
import time


def invoke_lambda(name, payload):
    """Invoke a Lambda and return the parsed body if API-proxy shaped, else the raw payload."""
    lambda_client = boto3.client("lambda")
    resp = lambda_client.invoke(FunctionName=name, Payload=json.dumps(payload).encode())
    raw = json.loads(resp["Payload"].read())
    # If API-proxy style {statusCode, body}, return parsed body only (as before)
    if isinstance(raw, dict) and "body" in raw:
        body = raw.get("body")
        try:
            return json.loads(body) if isinstance(body, str) else body
        except Exception:
            return body
    return raw


def wait_for_loader_success(load_id, physical_resources, timeout_seconds=600):
    """Wait for a single Neptune bulk loader job to finish."""
    print(f"Waiting for load success for job {load_id}")
    start_time = time.time()
    while time.time() - start_time < timeout_seconds:
        status_resp = invoke_lambda(physical_resources["GetLoadStatusFunction"], {"load_job_id": load_id})
        print(status_resp)
        overall = (status_resp or {}).get("payload", {}).get("overallStatus", {}) or {}
        status = (overall.get("status") or "").lower()
        print(f"Loader status: {status}")
        if status == "load_completed":
            return
        elif status in ["load_failed", "load_cancelled"]:
            raise Exception(f"Load failed with status: {status}")
        time.sleep(2)
    raise Exception(f"Timeout waiting for load {load_id}")


def wait_for_all_loads_success(load_ids, physical_resources):
    """Wait for all loads to complete."""
    for load_id in load_ids:
        wait_for_loader_success(load_id, physical_resources)


def prepare_upload(run_id, batch_id, physical_resources):
    """Prepare upload and return upload_url, s3_key."""
    prepare_fn = physical_resources["PrepareBulkUploadFunction"]
    prep_response = invoke_lambda(prepare_fn, {"body": json.dumps({"run_id": run_id, "batch_id": batch_id})})
    print(f"prepare_upload response: {prep_response}")
    return prep_response["upload_url"], prep_response["s3_key"]


def put_json_to_presigned(upload_url, payload):
    """Upload JSON payload to presigned URL."""
    resp = requests.put(upload_url, json=payload)
    resp.raise_for_status()


def perform_shard(run_id, batch_id, physical_resources, agent_iri=None):
    """Perform shard processing."""
    perform_fn = physical_resources["PerformBulkUploadFunction"]
    body = {"run_id": run_id, "batch_id": batch_id}
    if agent_iri:
        body["agent_iri"] = agent_iri
    resp = invoke_lambda(perform_fn, {"body": json.dumps(body)})
    print(f"perform_shard: {resp}")
    return resp


def finalize_run(run_id, batch_count, physical_resources):
    """Finalize run and return domain_load_id, prov_load_id."""
    finalize_fn = physical_resources["FinalizeBulkUploadFunction"]
    resp = invoke_lambda(finalize_fn, {"body": json.dumps({"run_id": run_id, "expected_batches": batch_count})})
    print(f"finalize_run: {resp}")
    return resp["domain_load_id"], resp["prov_load_id"]


def bulk_upload_run(shards, physical_resources, agent_iri=None):
    """
    High-level helper:
      - Creates a new run_id
      - For each shard payload, prepares, uploads, and performs
      - Finalizes the run
      - Waits for both loaders
      - Returns (run_id, domain_load_id, prov_load_id)
    """
    run_id = str(uuid.uuid4())
    for i, payload in enumerate(shards, start=1):
        upload_url, s3_key = prepare_upload(run_id, i, physical_resources)
        put_json_to_presigned(upload_url, payload)
        perform_shard(run_id, i, physical_resources, agent_iri=agent_iri)
    domain_lid, prov_lid = finalize_run(run_id, len(shards), physical_resources)
    wait_for_all_loads_success([domain_lid, prov_lid], physical_resources)
    return run_id, domain_lid, prov_lid


@pytest.mark.integration
def test_provenance_activity_query(physical_resources, test_project_id):
    """Test querying provenance activities for a bulk upload run."""
    
    # Create test data
    test_payload = [{
        "project_id": test_project_id,
        "project_subject_id": f"subj-{uuid.uuid4()}",
        "term_iri": "http://purl.obolibrary.org/obo/HP_0001249",
        "evidence": [{
            "type": "clinical_note",
            "clinical_note_id": f"note-{uuid.uuid4()}",
            "encounter_id": str(uuid.uuid4()),
            "evidence_creator_id": "test-creator",
            "evidence_creator_type": "automated",
            "evidence_creator_name": "Test Creator"
        }]
    }]
    
    # Perform bulk upload
    run_id, domain_load_id, prov_load_id = bulk_upload_run([test_payload], physical_resources)
    
    # Query provenance activities
    query_fn = physical_resources["QueryProvenanceFunction"]
    response = invoke_lambda(query_fn, {
        "query_type": "activity",
        "run_id": run_id
    })
    
    assert response["query_type"] == "activity"
    activities = response["results"]
    
    # Should have exactly one activity for one batch
    assert len(activities) == 1
    
    activity = activities[0]
    
    # Validate activity IRI structure
    assert "activity" in activity
    expected_activity_iri = f"http://ods.nationwidechildrens.org/phebee/activity/run/{run_id}/batch/00001"
    assert activity["activity"] == expected_activity_iri
    
    # Must have start time
    assert "startTime" in activity
    assert activity["startTime"]  # Should have a timestamp value
    
    # Must have input (S3 JSON file)
    assert "input" in activity
    assert f"phebee/runs/{run_id}/input/batch-00001.json" in activity["input"]
    
    # Must have output (S3 TTL file)
    assert "output" in activity
    assert f"phebee/runs/{run_id}/data/batch-00001.ttl.gz" in activity["output"]


@pytest.mark.integration  
def test_provenance_entity_query(physical_resources, test_project_id):
    """Test querying entities created in a bulk upload run."""
    
    # Create test data with specific identifiers we can validate
    subject_id = f"subj-{uuid.uuid4()}"
    note_id = f"note-{uuid.uuid4()}"
    encounter_id = str(uuid.uuid4())
    
    test_payload = [{
        "project_id": test_project_id,
        "project_subject_id": subject_id,
        "term_iri": "http://purl.obolibrary.org/obo/HP_0001249",
        "evidence": [{
            "type": "clinical_note",
            "clinical_note_id": note_id,
            "encounter_id": encounter_id,
            "evidence_creator_id": "test-creator",
            "evidence_creator_type": "automated"
        }]
    }]
    
    # Perform bulk upload
    run_id, _, _ = bulk_upload_run([test_payload], physical_resources)
    print(f"Test run ID: {run_id}")
    
    # Query provenance entities
    query_fn = physical_resources["QueryProvenanceFunction"]
    response = invoke_lambda(query_fn, {
        "query_type": "entity",
        "run_id": run_id
    })
    
    entities = response["results"]
    
    # Should have multiple entities (subject, encounter, clinical note, term link, text annotation)
    assert len(entities) >= 5
    
    # Extract entity IRIs for validation
    entity_iris = {e["entity"] for e in entities}
    
    # Validate entity types based on actual URI structure
    # All entities contain /subjects/{uuid} but we check for specific entity types
    subject_entities = [iri for iri in entity_iris if "/subjects/" in iri and "/encounter/" not in iri and "/note/" not in iri and "/annotation/" not in iri and "/term-link/" not in iri]
    encounter_entities = [iri for iri in entity_iris if "/encounter/" in iri and "/note/" not in iri]
    note_entities = [iri for iri in entity_iris if "/note/" in iri and "/annotation/" not in iri and "/term-link/" not in iri]
    annotation_entities = [iri for iri in entity_iris if "/annotation/" in iri]
    term_link_entities = [iri for iri in entity_iris if "/term-link/" in iri]
    
    # Should have at least one of each entity type including the subject itself
    assert len(subject_entities) >= 1, f"No subject entities found in: {entity_iris}"
    assert len(encounter_entities) >= 1, f"No encounter entities found in: {entity_iris}"
    assert len(note_entities) >= 1, f"No note entities found in: {entity_iris}"
    assert len(annotation_entities) >= 1, f"No annotation entities found in: {entity_iris}"
    assert len(term_link_entities) >= 1, f"No term-link entities found in: {entity_iris}"
    
    # All entities should contain the subjects path (showing they're part of the subject hierarchy)
    subjects_entities = [iri for iri in entity_iris if "/subjects/" in iri]
    assert len(subjects_entities) == len(entities), f"All entities should contain /subjects/ path: {entity_iris}"


@pytest.mark.integration
def test_provenance_lineage_query(physical_resources, test_project_id):
    """Test tracing lineage of a specific entity."""
    
    # Create test data
    test_payload = [{
        "project_id": test_project_id,
        "project_subject_id": f"subj-{uuid.uuid4()}",
        "term_iri": "http://purl.obolibrary.org/obo/HP_0001249",
        "evidence": [{
            "type": "clinical_note",
            "clinical_note_id": f"note-{uuid.uuid4()}",
            "encounter_id": str(uuid.uuid4()),
            "evidence_creator_id": "test-creator",
            "evidence_creator_type": "automated"
        }]
    }]
    
    # Perform bulk upload
    run_id, _, _ = bulk_upload_run([test_payload], physical_resources)
    
    # Get entities first
    query_fn = physical_resources["QueryProvenanceFunction"]
    entities_response = invoke_lambda(query_fn, {
        "query_type": "entity",
        "run_id": run_id
    })
    
    entities = entities_response["results"]
    assert len(entities) > 0
    
    # Pick the first entity to trace lineage
    entity_iri = entities[0]["entity"]
    
    # Query lineage for this entity
    lineage_response = invoke_lambda(query_fn, {
        "query_type": "lineage",
        "entity_iri": entity_iri
    })
    
    lineage = lineage_response["results"]
    
    # Should have lineage information
    assert len(lineage) > 0
    
    # Lineage should contain activity that generated this entity
    lineage_item = lineage[0]
    assert "activity" in lineage_item
    assert f"/run/{run_id}/batch/00001" in lineage_item["activity"]





@pytest.mark.integration
def test_provenance_multi_run_isolation(physical_resources, test_project_id):
    """Test that multiple runs have isolated provenance graphs."""
    
    # Create test data for two runs with different phenotypes
    test_payload1 = [{
        "project_id": test_project_id,
        "project_subject_id": f"subj1-{uuid.uuid4()}",
        "term_iri": "http://purl.obolibrary.org/obo/HP_0001249",  # Intellectual disability
        "evidence": [{
            "type": "clinical_note",
            "clinical_note_id": f"note1-{uuid.uuid4()}",
            "encounter_id": str(uuid.uuid4()),
            "evidence_creator_id": "test-creator",
            "evidence_creator_type": "automated"
        }]
    }]
    
    test_payload2 = [{
        "project_id": test_project_id,
        "project_subject_id": f"subj2-{uuid.uuid4()}",
        "term_iri": "http://purl.obolibrary.org/obo/HP_0002297",  # Low weight
        "evidence": [{
            "type": "clinical_note",
            "clinical_note_id": f"note2-{uuid.uuid4()}",
            "encounter_id": str(uuid.uuid4()),
            "evidence_creator_id": "test-creator",
            "evidence_creator_type": "automated"
        }]
    }]
    
    # Perform two separate bulk uploads
    run_id1, _, _ = bulk_upload_run([test_payload1], physical_resources)
    run_id2, _, _ = bulk_upload_run([test_payload2], physical_resources)
    
    # Query entities for each run
    query_fn = physical_resources["QueryProvenanceFunction"]
    
    entities1_response = invoke_lambda(query_fn, {
        "query_type": "entity",
        "run_id": run_id1
    })
    
    entities2_response = invoke_lambda(query_fn, {
        "query_type": "entity", 
        "run_id": run_id2
    })
    
    entities1 = entities1_response["results"]
    entities2 = entities2_response["results"]
    
    # Both runs should have entities
    assert len(entities1) > 0
    assert len(entities2) > 0
    
    # Entity sets should be completely disjoint (no overlap)
    entities1_iris = {e["entity"] for e in entities1}
    entities2_iris = {e["entity"] for e in entities2}
    
    overlap = entities1_iris.intersection(entities2_iris)
    assert len(overlap) == 0, f"Found overlapping entities between runs: {overlap}"
    
    # Both runs should have the same types of entities but different instances
    # Each run should have encounter, note, annotation, and term-link entities
    for entities_iris, run_name in [(entities1_iris, "run1"), (entities2_iris, "run2")]:
        encounter_entities = [iri for iri in entities_iris if "/encounter/" in iri and "/note/" not in iri]
        note_entities = [iri for iri in entities_iris if "/note/" in iri and "/annotation/" not in iri and "/term-link/" not in iri]
        annotation_entities = [iri for iri in entities_iris if "/annotation/" in iri]
        term_link_entities = [iri for iri in entities_iris if "/term-link/" in iri]
        
        assert len(encounter_entities) >= 1, f"No encounter entities in {run_name}: {entities_iris}"
        assert len(note_entities) >= 1, f"No note entities in {run_name}: {entities_iris}"
        assert len(annotation_entities) >= 1, f"No annotation entities in {run_name}: {entities_iris}"
        assert len(term_link_entities) >= 1, f"No term-link entities in {run_name}: {entities_iris}"


@pytest.mark.integration
def test_provenance_custom_sparql(physical_resources, test_project_id):
    """Test custom SPARQL query capability."""
    
    # Create test data
    test_payload = [{
        "project_id": test_project_id,
        "project_subject_id": f"subj-{uuid.uuid4()}",
        "term_iri": "http://purl.obolibrary.org/obo/HP_0001249",
        "evidence": [{
            "type": "clinical_note",
            "clinical_note_id": f"note-{uuid.uuid4()}",
            "encounter_id": str(uuid.uuid4()),
            "evidence_creator_id": "test-creator",
            "evidence_creator_type": "automated"
        }]
    }]
    
    # Perform bulk upload
    run_id, _, _ = bulk_upload_run([test_payload], physical_resources)
    
    # Custom SPARQL to count activities in the run
    custom_sparql = f"""
    PREFIX prov: <http://www.w3.org/ns/prov#>
    
    SELECT (COUNT(?activity) AS ?count) WHERE {{
        GRAPH <http://ods.nationwidechildrens.org/phebee/provenance/run/{run_id}> {{
            ?activity a prov:Activity .
        }}
    }}
    """
    
    # Execute custom query
    query_fn = physical_resources["QueryProvenanceFunction"]
    response = invoke_lambda(query_fn, {
        "custom_sparql": custom_sparql
    })
    
    results = response["results"]
    
    # Should return exactly one result with count
    assert len(results) == 1
    
    count_result = results[0]
    assert "count" in count_result
    
    # Should have exactly 1 activity (one batch)
    activity_count = int(count_result["count"])
    assert activity_count == 1


@pytest.mark.integration
def test_provenance_multi_batch_run(physical_resources, test_project_id):
    """Test provenance tracking across multiple batches in a single run."""
    
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
    
    # Query activities - should have exactly 2 activities
    query_fn = physical_resources["QueryProvenanceFunction"]
    activity_response = invoke_lambda(query_fn, {
        "query_type": "activity",
        "run_id": run_id
    })
    
    activities = activity_response["results"]
    assert len(activities) == 2, f"Expected 2 activities, got {len(activities)}"
    
    # Validate activity IRIs for both batches
    activity_iris = {a["activity"] for a in activities}
    expected_batch1_iri = f"http://ods.nationwidechildrens.org/phebee/activity/run/{run_id}/batch/00001"
    expected_batch2_iri = f"http://ods.nationwidechildrens.org/phebee/activity/run/{run_id}/batch/00002"
    
    assert expected_batch1_iri in activity_iris
    assert expected_batch2_iri in activity_iris
    
    # Query entities - should have entities from both batches
    entity_response = invoke_lambda(query_fn, {
        "query_type": "entity",
        "run_id": run_id
    })
    
    entities = entity_response["results"]
    # Should have 8 entities total (4 per batch: encounter, note, annotation, term-link)
    assert len(entities) >= 8, f"Expected at least 8 entities, got {len(entities)}"


@pytest.mark.integration
def test_provenance_entity_activity_relationships(physical_resources, test_project_id):
    """Test that entities are properly linked to activities via prov:wasGeneratedBy."""
    
    test_payload = [{
        "project_id": test_project_id,
        "project_subject_id": f"subj-{uuid.uuid4()}",
        "term_iri": "http://purl.obolibrary.org/obo/HP_0001249",
        "evidence": [{
            "type": "clinical_note",
            "clinical_note_id": f"note-{uuid.uuid4()}",
            "encounter_id": str(uuid.uuid4()),
            "evidence_creator_id": "test-creator",
            "evidence_creator_type": "automated"
        }]
    }]
    
    run_id, _, _ = bulk_upload_run([test_payload], physical_resources)
    
    # Custom SPARQL to validate entity-activity relationships
    query_fn = physical_resources["QueryProvenanceFunction"]
    relationship_sparql = f"""
    PREFIX prov: <http://www.w3.org/ns/prov#>
    
    SELECT ?entity ?activity WHERE {{
        GRAPH <http://ods.nationwidechildrens.org/phebee/provenance/run/{run_id}> {{
            ?entity prov:wasGeneratedBy ?activity .
            ?activity a prov:Activity .
        }}
    }}
    """
    
    response = invoke_lambda(query_fn, {"custom_sparql": relationship_sparql})
    relationships = response["results"]
    
    # Should have relationships for all entities
    assert len(relationships) >= 4, f"Expected at least 4 entity-activity relationships, got {len(relationships)}"
    
    # All relationships should point to the same activity
    activities = {r["activity"] for r in relationships}
    assert len(activities) == 1, f"Expected 1 activity, found {len(activities)}: {activities}"
    
    expected_activity = f"http://ods.nationwidechildrens.org/phebee/activity/run/{run_id}/batch/00001"
    assert expected_activity in activities


@pytest.mark.integration
def test_provenance_entity_count_validation(physical_resources, test_project_id):
    """Test that entity counts match expected patterns per input record."""
    
    # Single record should produce exactly 4 entities
    single_payload = [{
        "project_id": test_project_id,
        "project_subject_id": f"subj-{uuid.uuid4()}",
        "term_iri": "http://purl.obolibrary.org/obo/HP_0001249",
        "evidence": [{
            "type": "clinical_note",
            "clinical_note_id": f"note-{uuid.uuid4()}",
            "encounter_id": str(uuid.uuid4()),
            "evidence_creator_id": "test-creator",
            "evidence_creator_type": "automated"
        }]
    }]
    
    run_id, _, _ = bulk_upload_run([single_payload], physical_resources)
    
    query_fn = physical_resources["QueryProvenanceFunction"]
    response = invoke_lambda(query_fn, {
        "query_type": "entity",
        "run_id": run_id
    })
    
    entities = response["results"]
    # Exactly 5 entities: subject, encounter, note, annotation, term-link
    assert len(entities) == 5, f"Expected exactly 5 entities for single record, got {len(entities)}"
    
    # Validate entity type distribution
    entity_iris = {e["entity"] for e in entities}
    encounter_count = len([iri for iri in entity_iris if "/encounter/" in iri and "/note/" not in iri])
    note_count = len([iri for iri in entity_iris if "/note/" in iri and "/annotation/" not in iri and "/term-link/" not in iri])
    annotation_count = len([iri for iri in entity_iris if "/annotation/" in iri])
    term_link_count = len([iri for iri in entity_iris if "/term-link/" in iri])
    
    assert encounter_count == 1, f"Expected 1 encounter, got {encounter_count}"
    assert note_count == 1, f"Expected 1 note, got {note_count}"
    assert annotation_count == 1, f"Expected 1 annotation, got {annotation_count}"
    assert term_link_count == 1, f"Expected 1 term-link, got {term_link_count}"


@pytest.mark.integration
def test_provenance_graph_isolation(physical_resources, test_project_id):
    """Test that provenance graphs are properly isolated by run_id."""
    
    # Create two separate runs
    payload1 = [{
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
    
    payload2 = [{
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
    
    run_id1, _, _ = bulk_upload_run([payload1], physical_resources)
    run_id2, _, _ = bulk_upload_run([payload2], physical_resources)
    
    # Custom SPARQL to verify graph isolation
    query_fn = physical_resources["QueryProvenanceFunction"]
    
    # Query entities from run1's graph should not see run2's entities
    isolation_sparql = f"""
    PREFIX prov: <http://www.w3.org/ns/prov#>
    
    SELECT ?entity WHERE {{
        GRAPH <http://ods.nationwidechildrens.org/phebee/provenance/run/{run_id1}> {{
            ?entity prov:wasGeneratedBy ?activity .
        }}
    }}
    """
    
    response1 = invoke_lambda(query_fn, {"custom_sparql": isolation_sparql})
    run1_entities = {e["entity"] for e in response1["results"]}
    
    # Query entities from run2's graph
    isolation_sparql2 = f"""
    PREFIX prov: <http://www.w3.org/ns/prov#>
    
    SELECT ?entity WHERE {{
        GRAPH <http://ods.nationwidechildrens.org/phebee/provenance/run/{run_id2}> {{
            ?entity prov:wasGeneratedBy ?activity .
        }}
    }}
    """
    
    response2 = invoke_lambda(query_fn, {"custom_sparql": isolation_sparql2})
    run2_entities = {e["entity"] for e in response2["results"]}
    
    # Verify complete isolation
    assert len(run1_entities) > 0, "Run 1 should have entities"
    assert len(run2_entities) > 0, "Run 2 should have entities"
    assert run1_entities.isdisjoint(run2_entities), f"Graphs not isolated: {run1_entities & run2_entities}"


@pytest.mark.integration
def test_provenance_same_logical_entities_different_runs(physical_resources, test_project_id):
    """Test that same logical entities in multiple runs get separate provenance tracking."""
    
    # Use identical input data for both runs (same logical entities)
    shared_subject_id = f"shared-subj-{uuid.uuid4()}"
    shared_encounter_id = str(uuid.uuid4())
    shared_note_id = f"shared-note-{uuid.uuid4()}"
    
    identical_payload = [{
        "project_id": test_project_id,
        "project_subject_id": shared_subject_id,
        "term_iri": "http://purl.obolibrary.org/obo/HP_0001249",
        "evidence": [{
            "type": "clinical_note",
            "clinical_note_id": shared_note_id,
            "encounter_id": shared_encounter_id,
            "evidence_creator_id": "test-creator",
            "evidence_creator_type": "automated"
        }]
    }]
    
    # Run the same payload twice in separate runs
    run_id1, _, _ = bulk_upload_run([identical_payload], physical_resources)
    run_id2, _, _ = bulk_upload_run([identical_payload], physical_resources)
    
    query_fn = physical_resources["QueryProvenanceFunction"]
    
    # Get entities from both runs
    entities1_response = invoke_lambda(query_fn, {
        "query_type": "entity",
        "run_id": run_id1
    })
    
    entities2_response = invoke_lambda(query_fn, {
        "query_type": "entity",
        "run_id": run_id2
    })
    
    entities1_iris = {e["entity"] for e in entities1_response["results"]}
    entities2_iris = {e["entity"] for e in entities2_response["results"]}
    
    # Both runs should have the same number and types of entities
    assert len(entities1_iris) == 5, f"Run 1 should have exactly 5 entities, got {len(entities1_iris)}"
    assert len(entities2_iris) == 5, f"Run 2 should have exactly 5 entities, got {len(entities2_iris)}"
    
    # Key insight: When the same logical entities are processed in different runs,
    # PheBee reuses the same entity URIs (same subject+project, same encounter ID, etc.)
    # but tracks them separately in provenance for each run
    assert entities1_iris == entities2_iris, f"Same logical entities should reuse the same URIs across runs"
    
    # Both runs should have the same entity types
    for entities_iris, run_name in [(entities1_iris, "run1"), (entities2_iris, "run2")]:
        encounter_count = len([iri for iri in entities_iris if "/encounter/" in iri and "/note/" not in iri])
        note_count = len([iri for iri in entities_iris if "/note/" in iri and "/annotation/" not in iri and "/term-link/" not in iri])
        annotation_count = len([iri for iri in entities_iris if "/annotation/" in iri])
        term_link_count = len([iri for iri in entities_iris if "/term-link/" in iri])
        
        assert encounter_count == 1, f"{run_name}: Expected 1 encounter, got {encounter_count}"
        assert note_count == 1, f"{run_name}: Expected 1 note, got {note_count}"
        assert annotation_count == 1, f"{run_name}: Expected 1 annotation, got {annotation_count}"
        assert term_link_count == 1, f"{run_name}: Expected 1 term-link, got {term_link_count}"
    
    # Verify each run has its own provenance graph with separate tracking
    for run_id, run_name in [(run_id1, "run1"), (run_id2, "run2")]:
        activity_response = invoke_lambda(query_fn, {
            "query_type": "activity",
            "run_id": run_id
        })
        
        activities = activity_response["results"]
        assert len(activities) == 1, f"{run_name} should have exactly 1 activity"
        
        expected_activity_iri = f"http://ods.nationwidechildrens.org/phebee/activity/run/{run_id}/batch/00001"
        assert activities[0]["activity"] == expected_activity_iri


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
    
    query_fn = physical_resources["QueryProvenanceFunction"]
    
    # Get entities from both runs
    entities1_response = invoke_lambda(query_fn, {
        "query_type": "entity",
        "run_id": run_id1
    })
    
    entities2_response = invoke_lambda(query_fn, {
        "query_type": "entity",
        "run_id": run_id2
    })
    
    entities1_iris = {e["entity"] for e in entities1_response["results"]}
    entities2_iris = {e["entity"] for e in entities2_response["results"]}
    
    # Extract subject UUIDs from the entity IRIs
    def extract_subject_uuid(iri):
        # Extract UUID from: http://ods.nationwidechildrens.org/phebee/subjects/{uuid}/...
        import re
        match = re.search(r'/subjects/([a-f0-9-]{36})/', iri)
        return match.group(1) if match else None
    
    subject_uuids1 = {extract_subject_uuid(iri) for iri in entities1_iris if extract_subject_uuid(iri)}
    subject_uuids2 = {extract_subject_uuid(iri) for iri in entities2_iris if extract_subject_uuid(iri)}
    
    # Remove None values
    subject_uuids1 = {uuid for uuid in subject_uuids1 if uuid}
    subject_uuids2 = {uuid for uuid in subject_uuids2 if uuid}
    
    print(f"Run 1 subject UUIDs: {subject_uuids1}")
    print(f"Run 2 subject UUIDs: {subject_uuids2}")
    print(f"Run 1 entities: {entities1_iris}")
    print(f"Run 2 entities: {entities2_iris}")
    
    # Both runs should use the SAME subject UUID since they have the same project_id + project_subject_id
    assert len(subject_uuids1) == 1, f"Run 1 should have exactly 1 subject UUID, got {subject_uuids1}"
    assert len(subject_uuids2) == 1, f"Run 2 should have exactly 1 subject UUID, got {subject_uuids2}"
    assert subject_uuids1 == subject_uuids2, f"Both runs should use the same subject UUID: {subject_uuids1} vs {subject_uuids2}"


@pytest.mark.integration
def test_encounter_note_reuse_across_runs(physical_resources, test_project_id):
    """Test that same encounter and note IDs are reused across runs."""
    
    # Use identical identifiers in both runs
    shared_subject_id = f"patient-{uuid.uuid4()}"
    shared_encounter_id = str(uuid.uuid4())
    shared_note_id = f"shared-note-{uuid.uuid4()}"
    
    # First run with HP_0001249
    payload1 = [{
        "project_id": test_project_id,
        "project_subject_id": shared_subject_id,
        "term_iri": "http://purl.obolibrary.org/obo/HP_0001249",
        "evidence": [{
            "type": "clinical_note",
            "clinical_note_id": shared_note_id,
            "encounter_id": shared_encounter_id,
            "evidence_creator_id": "test-creator",
            "evidence_creator_type": "automated"
        }]
    }]
    
    # Second run with HP_0002297 but SAME subject/encounter/note
    payload2 = [{
        "project_id": test_project_id,
        "project_subject_id": shared_subject_id,
        "term_iri": "http://purl.obolibrary.org/obo/HP_0002297",
        "evidence": [{
            "type": "clinical_note",
            "clinical_note_id": shared_note_id,  # Same note!
            "encounter_id": shared_encounter_id,  # Same encounter!
            "evidence_creator_id": "test-creator",
            "evidence_creator_type": "automated"
        }]
    }]
    
    run_id1, _, _ = bulk_upload_run([payload1], physical_resources)
    run_id2, _, _ = bulk_upload_run([payload2], physical_resources)
    
    query_fn = physical_resources["QueryProvenanceFunction"]
    
    # Get entities from both runs
    entities1_response = invoke_lambda(query_fn, {
        "query_type": "entity",
        "run_id": run_id1
    })
    
    entities2_response = invoke_lambda(query_fn, {
        "query_type": "entity",
        "run_id": run_id2
    })
    
    entities1_iris = {e["entity"] for e in entities1_response["results"]}
    entities2_iris = {e["entity"] for e in entities2_response["results"]}
    
    print(f"Run 1 entities: {entities1_iris}")
    print(f"Run 2 entities: {entities2_iris}")
    
    # Find encounter and note entities
    encounter_entities1 = [iri for iri in entities1_iris if "/encounter/" in iri and "/note/" not in iri]
    encounter_entities2 = [iri for iri in entities2_iris if "/encounter/" in iri and "/note/" not in iri]
    
    note_entities1 = [iri for iri in entities1_iris if "/note/" in iri and "/annotation/" not in iri and "/term-link/" not in iri]
    note_entities2 = [iri for iri in entities2_iris if "/note/" in iri and "/annotation/" not in iri and "/term-link/" not in iri]
    
    # Should have exactly one encounter and one note in each run
    assert len(encounter_entities1) == 1, f"Run 1 should have 1 encounter, got {encounter_entities1}"
    assert len(encounter_entities2) == 1, f"Run 2 should have 1 encounter, got {encounter_entities2}"
    assert len(note_entities1) == 1, f"Run 1 should have 1 note, got {note_entities1}"
    assert len(note_entities2) == 1, f"Run 2 should have 1 note, got {note_entities2}"
    
    # The SAME encounter and note should be used in both runs
    assert encounter_entities1[0] == encounter_entities2[0], f"Encounter should be reused: {encounter_entities1[0]} vs {encounter_entities2[0]}"
    assert note_entities1[0] == note_entities2[0], f"Note should be reused: {note_entities1[0]} vs {note_entities2[0]}"
    
    # Verify the entities actually exist in the domain graph (not just provenance)
    encounter_uri = encounter_entities1[0]
    note_uri = note_entities1[0]
    
    verification_sparql = f"""
    PREFIX phebee: <http://ods.nationwidechildrens.org/phebee#>
    
    SELECT ?encounter ?note WHERE {{
        <{encounter_uri}> a phebee:Encounter .
        <{note_uri}> a phebee:ClinicalNote .
        BIND(<{encounter_uri}> AS ?encounter)
        BIND(<{note_uri}> AS ?note)
    }}
    """
    
    verification_response = invoke_lambda(query_fn, {"custom_sparql": verification_sparql})
    verification_results = verification_response["results"]
    
    assert len(verification_results) == 1, f"Entities should exist in domain graph: {verification_results}"
    assert verification_results[0]["encounter"] == encounter_uri
    assert verification_results[0]["note"] == note_uri
    
    # However, annotations and term-links should be different (different phenotypes)
    annotation_entities1 = [iri for iri in entities1_iris if "/annotation/" in iri]
    annotation_entities2 = [iri for iri in entities2_iris if "/annotation/" in iri]
    term_link_entities1 = [iri for iri in entities1_iris if "/term-link/" in iri]
    term_link_entities2 = [iri for iri in entities2_iris if "/term-link/" in iri]
    
    # Annotations and term-links should be different due to different phenotypes
    assert annotation_entities1 != annotation_entities2, "Annotations should be different for different phenotypes"
    assert term_link_entities1 != term_link_entities2, "Term-links should be different for different phenotypes"
    
    print(f"✓ Same encounter reused: {encounter_entities1[0]}")
    print(f"✓ Same note reused: {note_entities1[0]}")
    print(f"✓ Entities verified in domain graph")
    print(f"✓ Different annotations: {annotation_entities1[0]} vs {annotation_entities2[0]}")
    print(f"✓ Different term-links: {term_link_entities1[0]} vs {term_link_entities2[0]}")


@pytest.mark.integration
def test_lineage_cross_graph_search(physical_resources, test_project_id):
    """Test that lineage queries can find all runs that processed the same subject."""
    
    # Use the SAME patient in both runs
    shared_subject_id = f"patient-{uuid.uuid4()}"
    
    # Create two separate runs for the same patient with different phenotypes
    payload1 = [{
        "project_id": test_project_id,
        "project_subject_id": shared_subject_id,  # Same patient!
        "term_iri": "http://purl.obolibrary.org/obo/HP_0001249",
        "evidence": [{
            "type": "clinical_note",
            "clinical_note_id": f"note1-{uuid.uuid4()}",
            "encounter_id": str(uuid.uuid4()),
            "evidence_creator_id": "test-creator",
            "evidence_creator_type": "automated"
        }]
    }]
    
    payload2 = [{
        "project_id": test_project_id,
        "project_subject_id": shared_subject_id,  # Same patient!
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
    
    query_fn = physical_resources["QueryProvenanceFunction"]
    
    # Get entities from both runs
    entities1_response = invoke_lambda(query_fn, {
        "query_type": "entity",
        "run_id": run_id1
    })
    
    entities2_response = invoke_lambda(query_fn, {
        "query_type": "entity",
        "run_id": run_id2
    })
    
    entities1_iris = {e["entity"] for e in entities1_response["results"]}
    entities2_iris = {e["entity"] for e in entities2_response["results"]}
    
    print(f"Run 1 entities: {entities1_iris}")
    print(f"Run 2 entities: {entities2_iris}")
    
    # Extract the subject URI (should be the same in both runs since same patient)
    def extract_subject_uri(iri):
        import re
        # Match the subject part: http://ods.nationwidechildrens.org/phebee/subjects/{uuid}
        match = re.search(r'(http://[^/]+/phebee/subjects/[a-f0-9-]{36})', iri)
        return match.group(1) if match else None
    
    subject_uris1 = {extract_subject_uri(iri) for iri in entities1_iris if extract_subject_uri(iri)}
    subject_uris2 = {extract_subject_uri(iri) for iri in entities2_iris if extract_subject_uri(iri)}
    
    # Remove None values
    subject_uris1 = {uri for uri in subject_uris1 if uri}
    subject_uris2 = {uri for uri in subject_uris2 if uri}
    
    print(f"Extracted subject URIs from run 1: {subject_uris1}")
    print(f"Extracted subject URIs from run 2: {subject_uris2}")
    
    # Should be the same subject URI in both runs
    assert len(subject_uris1) == 1, f"Run 1 should have exactly 1 subject, got {subject_uris1}"
    assert len(subject_uris2) == 1, f"Run 2 should have exactly 1 subject, got {subject_uris2}"
    assert subject_uris1 == subject_uris2, f"Both runs should use same subject: {subject_uris1} vs {subject_uris2}"
    
    subject_uri = list(subject_uris1)[0]
    print(f"Testing lineage for shared subject: {subject_uri}")
    
    # Test custom SPARQL to find ALL activities that processed this subject
    subject_lineage_sparql = f"""
    PREFIX prov: <http://www.w3.org/ns/prov#>
    
    SELECT ?entity ?activity ?run_graph WHERE {{
        GRAPH ?run_graph {{
            ?entity prov:wasGeneratedBy ?activity .
        }}
        FILTER(CONTAINS(STR(?run_graph), "provenance/run/"))
        FILTER(CONTAINS(STR(?entity), "{subject_uri}"))
    }}
    ORDER BY ?activity
    """
    
    subject_lineage_response = invoke_lambda(query_fn, {"custom_sparql": subject_lineage_sparql})
    subject_lineage_results = subject_lineage_response["results"]
    
    # Should find entities from BOTH runs since they both processed the same subject
    assert len(subject_lineage_results) >= 8, f"Should find entities from both runs: {len(subject_lineage_results)}"
    
    # Extract the activities and graphs
    activities = {r["activity"] for r in subject_lineage_results}
    graphs = {r["run_graph"] for r in subject_lineage_results}
    
    # Should have activities from both runs
    assert len(activities) == 2, f"Should have activities from 2 runs: {activities}"
    assert len(graphs) == 2, f"Should span 2 provenance graphs: {graphs}"
    
    # Verify the activities belong to our specific runs
    activity_run_ids = {activity.split("/run/")[1].split("/")[0] for activity in activities}
    assert run_id1 in activity_run_ids, f"Should include run 1: {activity_run_ids}"
    assert run_id2 in activity_run_ids, f"Should include run 2: {activity_run_ids}"
    
    # Test built-in lineage query on a specific entity from the subject
    annotation1 = [iri for iri in entities1_iris if "/annotation/" in iri][0]
    lineage1_response = invoke_lambda(query_fn, {
        "query_type": "lineage",
        "entity_iri": annotation1
    })
    
    lineage1 = lineage1_response["results"]
    assert len(lineage1) > 0, f"Should find lineage for annotation: {annotation1}"
    assert run_id1 in lineage1[0]["activity"], f"Should trace to run 1: {lineage1[0]['activity']['value']}"
    
    print(f"✓ Same subject URI used in both runs: {subject_uri}")
    print(f"✓ Found {len(subject_lineage_results)} entities across both runs for this subject")
    print(f"✓ Cross-graph search found activities from both runs: {activity_run_ids}")
    print(f"✓ Built-in lineage query works for individual entities")
