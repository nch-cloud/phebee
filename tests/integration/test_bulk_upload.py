@pytest.fixture
def test_duplicate_encounters_in_batch_payload(test_project_id):
    """Fixture for testing encounter duplicate detection within the same batch"""
    subject_id = f"subj-{uuid.uuid4()}"
    term_iri = "http://purl.obolibrary.org/obo/HP_0004322"
    encounter_id = str(uuid.uuid4())  # Same encounter ID used for multiple notes
    
    return [
        # Single entry with multiple notes using the same encounter
        {
            "project_id": test_project_id,
            "project_subject_id": subject_id,
            "term_iri": term_iri,
            "evidence": [
                {
                    "type": "clinical_note",
                    "encounter_id": encounter_id,  # Same encounter ID
                    "clinical_note_id": "note-batch-1",
                    "note_timestamp": "2025-06-06T12:00:00Z",
                    "evidence_creator_id": "robot-creator",
                    "evidence_creator_type": "automated",
                    "evidence_creator_name": "Robot Creator",
                    "evidence_creator_version": "1.2"
                },
                {
                    "type": "clinical_note",
                    "encounter_id": encounter_id,  # Same encounter ID
                    "clinical_note_id": "note-batch-2",
                    "note_timestamp": "2025-06-06T14:00:00Z",
                    "evidence_creator_id": "robot-creator",
                    "evidence_creator_type": "automated",
                    "evidence_creator_name": "Robot Creator",
                    "evidence_creator_version": "1.2"
                },
                {
                    "type": "clinical_note",
                    "encounter_id": encounter_id,  # Same encounter ID
                    "clinical_note_id": "note-batch-3",
                    "note_timestamp": "2025-06-06T16:00:00Z",
                    "evidence_creator_id": "robot-creator",
                    "evidence_creator_type": "automated",
                    "evidence_creator_name": "Robot Creator",
                    "evidence_creator_version": "1.2"
                }
            ]
        }
    ]

def test_bulk_upload_encounter_in_batch_duplicate_handling(test_duplicate_encounters_in_batch_payload, test_project_id, physical_resources):
    """Test that duplicate encounters within the same batch are correctly detected and handled"""
    # Upload the payload with multiple notes using the same encounter
    load_id = perform_bulk_upload(test_duplicate_encounters_in_batch_payload, physical_resources)
    
    # Get the subject
    payload = test_duplicate_encounters_in_batch_payload[0]
    project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{payload['project_id']}/{payload['project_subject_id']}"
    get_subject_fn = physical_resources["GetSubjectFunction"]
    subject_result = invoke_lambda(get_subject_fn, {"project_subject_iri": project_subject_iri})
    subject_iri = subject_result["subject_iri"]
    
    # Verify one term link was created
    assert len(subject_result["terms"]) == 1
    
    # Get the encounter
    encounter_id = payload["evidence"][0]["encounter_id"]
    encounter_iri = f"{subject_iri}/encounter/{encounter_id}"
    
    # Get the clinical notes
    get_note_fn = physical_resources["GetClinicalNoteFunction"]
    
    # Check all three notes
    for i in range(1, 4):
        note_iri = f"{encounter_iri}/note/note-batch-{i}"
        note_result = invoke_lambda(get_note_fn, {"clinical_note_iri": note_iri})
        assert note_result["clinical_note_id"] == f"note-batch-{i}"
    
    # Now let's verify the encounter doesn't have duplicate created timestamps
    # We need to query the encounter directly to check its properties
    get_encounter_fn = physical_resources["GetEncounterFunction"]
    encounter_result = invoke_lambda(get_encounter_fn, {
        "subject_iri": subject_iri,
        "encounter_id": encounter_id
    })
    
    # Check that there's only one created timestamp
    # The created property might be a list or a single value depending on how it's returned
    created_timestamps = encounter_result.get("created", [])
    if not isinstance(created_timestamps, list):
        created_timestamps = [created_timestamps]
    
    # There should be exactly one created timestamp
    assert len(created_timestamps) == 1, f"Expected 1 created timestamp, found {len(created_timestamps)}: {created_timestamps}"

def test_bulk_upload_encounter_duplicate_handling(test_duplicate_encounters_payload, test_project_id, physical_resources):
    """Test that duplicate encounters are correctly detected and handled"""
    # First upload to create the encounters
    load_id = perform_bulk_upload(test_duplicate_encounters_payload, physical_resources)
    
    # Get the subject
    payload = test_duplicate_encounters_payload[0]
    project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{payload['project_id']}/{payload['project_subject_id']}"
    get_subject_fn = physical_resources["GetSubjectFunction"]
    subject_result = invoke_lambda(get_subject_fn, {"project_subject_iri": project_subject_iri})
    subject_iri = subject_result["subject_iri"]
    
    # Verify two term links were created (one for each term)
    assert len(subject_result["terms"]) == 2
    
    # Get the encounter
    encounter_id = payload["evidence"][0]["encounter_id"]
    encounter_iri = f"{subject_iri}/encounter/{encounter_id}"
    
    # Get the clinical notes
    get_note_fn = physical_resources["GetClinicalNoteFunction"]
    
    # Check first note
    note1_iri = f"{encounter_iri}/note/note-enc-1"
    note1_result = invoke_lambda(get_note_fn, {"clinical_note_iri": note1_iri})
    assert note1_result["clinical_note_id"] == "note-enc-1"
    
    # Check second note
    note2_iri = f"{encounter_iri}/note/note-enc-2"
    note2_result = invoke_lambda(get_note_fn, {"clinical_note_iri": note2_iri})
    assert note2_result["clinical_note_id"] == "note-enc-2"
    
    # Now upload the same payload again - this should reuse the existing encounter
    second_load_id = perform_bulk_upload(test_duplicate_encounters_payload, physical_resources)
    
    # The encounter should still exist and have the same properties
    # We can verify this by checking that both notes are still accessible
    note1_result_again = invoke_lambda(get_note_fn, {"clinical_note_iri": note1_iri})
    assert note1_result_again["clinical_note_id"] == "note-enc-1"
    
    note2_result_again = invoke_lambda(get_note_fn, {"clinical_note_iri": note2_iri})
    assert note2_result_again["clinical_note_id"] == "note-enc-2"
    
    # Check that there's only one created timestamp
    get_encounter_fn = physical_resources["GetEncounterFunction"]
    encounter_result = invoke_lambda(get_encounter_fn, {
        "subject_iri": subject_iri,
        "encounter_id": encounter_id
    })
    
    created_timestamps = encounter_result.get("created", [])
    if not isinstance(created_timestamps, list):
        created_timestamps = [created_timestamps]
    
    # There should be exactly one created timestamp
    assert len(created_timestamps) == 1, f"Expected 1 created timestamp, found {len(created_timestamps)}: {created_timestamps}"
@pytest.fixture
def test_duplicate_notes_in_batch_payload(test_project_id):
    """Fixture for testing clinical note duplicate detection within the same batch"""
    subject_id = f"subj-{uuid.uuid4()}"
    term_iri_1 = "http://purl.obolibrary.org/obo/HP_0004322"
    term_iri_2 = "http://purl.obolibrary.org/obo/HP_0002297"
    encounter_id = str(uuid.uuid4())
    clinical_note_id = f"note-{uuid.uuid4()}"  # Same clinical note ID used for multiple term links
    
    return [
        # First entry with clinical note
        {
            "project_id": test_project_id,
            "project_subject_id": subject_id,
            "term_iri": term_iri_1,
            "evidence": [
                {
                    "type": "clinical_note",
                    "encounter_id": encounter_id,
                    "clinical_note_id": clinical_note_id,  # Same clinical note ID
                    "note_timestamp": "2025-06-06T12:00:00Z",
                    "evidence_creator_id": "robot-creator",
                    "evidence_creator_type": "automated",
                    "evidence_creator_name": "Robot Creator",
                    "evidence_creator_version": "1.2",
                    "author_specialty": "Neurology"
                }
            ]
        },
        # Second entry with same clinical note
        {
            "project_id": test_project_id,
            "project_subject_id": subject_id,
            "term_iri": term_iri_2,
            "evidence": [
                {
                    "type": "clinical_note",
                    "encounter_id": encounter_id,
                    "clinical_note_id": clinical_note_id,  # Same clinical note ID
                    "note_timestamp": "2025-06-06T12:00:00Z",
                    "evidence_creator_id": "robot-creator",
                    "evidence_creator_type": "automated",
                    "evidence_creator_name": "Robot Creator",
                    "evidence_creator_version": "1.2",
                    "author_specialty": "Neurology"
                }
            ]
        }
    ]
def test_bulk_upload_clinical_note_duplicate_handling(test_duplicate_notes_in_batch_payload, test_project_id, physical_resources):
    """Test that duplicate clinical notes within the same batch are correctly detected and handled"""
    # Upload the payload with multiple term links using the same clinical note
    load_id = perform_bulk_upload(test_duplicate_notes_in_batch_payload, physical_resources)
    
    # Get the subject
    payload = test_duplicate_notes_in_batch_payload[0]
    project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{payload['project_id']}/{payload['project_subject_id']}"
    get_subject_fn = physical_resources["GetSubjectFunction"]
    subject_result = invoke_lambda(get_subject_fn, {"project_subject_iri": project_subject_iri})
    subject_iri = subject_result["subject_iri"]
    
    # Verify two term links were created (one for each term)
    assert len(subject_result["terms"]) == 2
    
    # Get the encounter and clinical note
    encounter_id = payload["evidence"][0]["encounter_id"]
    clinical_note_id = payload["evidence"][0]["clinical_note_id"]
    encounter_iri = f"{subject_iri}/encounter/{encounter_id}"
    note_iri = f"{encounter_iri}/note/{clinical_note_id}"
    
    # Get the clinical note details
    get_note_fn = physical_resources["GetClinicalNoteFunction"]
    note_result = invoke_lambda(get_note_fn, {"clinical_note_iri": note_iri})
    
    # Verify the clinical note exists and has the correct properties
    assert note_result["clinical_note_id"] == clinical_note_id
    assert note_result["author_specialty"] == "Neurology"
    
    # Check that the note has links to both term links
    term_links = note_result.get("has_term_link", [])
    assert len(term_links) == 2, f"Expected 2 term links, found {len(term_links)}"
    
    # Now check that there's only one created timestamp
    created_timestamps = note_result.get("created", [])
    if not isinstance(created_timestamps, list):
        created_timestamps = [created_timestamps]
    
    # There should be exactly one created timestamp
    assert len(created_timestamps) == 1, f"Expected 1 created timestamp, found {len(created_timestamps)}: {created_timestamps}"
def test_term_link_source_node(test_payload, test_project_id, physical_resources):
    """Test that term links use the appropriate source node (clinical note) as their base IRI"""
    load_id = perform_bulk_upload(test_payload, physical_resources)
    
    # Get the subject
    payload = test_payload[0]
    project_subject_iri = f"http://ods.nationwidechildrens.org/phebee/projects/{payload['project_id']}/{payload['project_subject_id']}"
    get_subject_fn = physical_resources["GetSubjectFunction"]
    subject_result = invoke_lambda(get_subject_fn, {"project_subject_iri": project_subject_iri})
    subject_iri = subject_result["subject_iri"]
    
    # Get the clinical note
    encounter_id = payload["evidence"][0]["encounter_id"]
    clinical_note_id = payload["evidence"][0]["clinical_note_id"]
    encounter_iri = f"{subject_iri}/encounter/{encounter_id}"
    note_iri = f"{encounter_iri}/note/{clinical_note_id}"
    
    get_note_fn = physical_resources["GetClinicalNoteFunction"]
    note_result = invoke_lambda(get_note_fn, {"clinical_note_iri": note_iri})
    
    # Verify the note has hasTermLink property
    assert "has_term_link" in note_result, "Clinical note should have hasTermLink property"
    
    # Get the term link
    term_link_iri = note_result["has_term_link"][0]
    
    # Verify the term link IRI is based on the clinical note IRI
    assert note_iri in term_link_iri, f"Term link IRI {term_link_iri} should be based on clinical note IRI {note_iri}"
    
    # Get the term link details
    get_termlink_fn = physical_resources["GetTermLinkFunction"]
    termlink_result = invoke_lambda(get_termlink_fn, {"termlink_iri": term_link_iri})
    
    # Verify the term link has the correct source node
    assert termlink_result["source_node"] == note_iri, f"Term link source node should be {note_iri}, got {termlink_result['source_node']}"
