"""
Enhanced Scale/performance evaluation harness for PheBee with realistic clinical data patterns.

This module generates realistic clinical datasets with proper phenotype clustering, qualifier
distributions, temporal patterns, and comprehensive query testing.

How to run (example):
  PHEBEE_EVAL_SCALE=1 \
  PHEBEE_EVAL_SCALE_SUBJECTS=5000 \
  PHEBEE_EVAL_SCALE_MIN_TERMS=5 \
  PHEBEE_EVAL_SCALE_MAX_TERMS=50 \
  PHEBEE_EVAL_SCALE_MIN_EVIDENCE=1 \
  PHEBEE_EVAL_SCALE_MAX_EVIDENCE=8 \
  PHEBEE_EVAL_BATCH_SIZE=10000 \
  PHEBEE_EVAL_INGEST_TIMEOUT_S=7200 \
  PHEBEE_EVAL_LATENCY_N=500 \
  PHEBEE_EVAL_CONCURRENCY=25 \
  pytest -m perf tests/performance/test_evaluation_perf_scale_v2.py

Optional:
  PHEBEE_EVAL_METRICS_S3_URI=s3://<bucket>/<prefix>   # upload metrics JSON to S3
  PHEBEE_EVAL_METRICS_PATH=/tmp/phebee_r11_metrics.json # write metrics JSON locally
  PHEBEE_EVAL_STRICT_LATENCY=1                         # enforce p95<=5s gates
"""

from __future__ import annotations

import json
import os
import random
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import boto3
import pytest
import requests

# Set random seed for reproducible datasets
random.seed(42)

# ---------------------------------------------------------------------
# Module-level gate: don't run perf scale tests unless explicitly enabled
# ---------------------------------------------------------------------
if os.environ.get("PHEBEE_EVAL_SCALE") != "1":
    pytest.skip(
        "Scale perf tests disabled (set PHEBEE_EVAL_SCALE=1).",
        allow_module_level=True,
    )

pytestmark = [pytest.mark.integration, pytest.mark.perf]

# ---------------------------------------------------------------------
# Clinical data patterns and distributions
# ---------------------------------------------------------------------

# Common phenotypes (70% of assignments) - based on real clinical frequency
COMMON_PHENOTYPES = [
    "HP_0001627",  # Abnormal heart morphology
    "HP_0001250",  # Seizures
    "HP_0001943",  # Hyperglycemia
    "HP_0000819",  # Diabetes mellitus
    "HP_0001635",  # Heart failure
    "HP_0002069",  # Bilateral tonic-clonic seizures
    "HP_0001644",  # Dilated cardiomyopathy
    "HP_0001298",  # Encephalopathy
    "HP_0001513",  # Obesity
    "HP_0002155",  # Hypertriglyceridemia
    "HP_0001678",  # Atrioventricular block
    "HP_0002376",  # Developmental regression
    "HP_0000822",  # Hypertension
    "HP_0001508",  # Failure to thrive
    "HP_0002664",  # Neoplasm
    "HP_0000708",  # Behavioral abnormality
    "HP_0001252",  # Muscular hypotonia
    "HP_0000365",  # Hearing impairment
    "HP_0000478",  # Abnormality of the eye
    "HP_0000707",  # Abnormality of the nervous system
    "HP_0001626",  # Abnormality of the cardiovascular system
    "HP_0000924",  # Abnormality of the skeletal system
    "HP_0000118",  # Phenotypic abnormality
    "HP_0001871",  # Abnormality of blood and blood-forming tissues
    "HP_0000951",  # Abnormality of the skin
    "HP_0000119",  # Abnormality of the genitourinary system
    "HP_0025031",  # Abnormality of the digestive system
    "HP_0002715",  # Abnormality of the immune system
    "HP_0000152",  # Abnormality of head or neck
    "HP_0040064",  # Abnormality of limbs
    "HP_0000598",  # Abnormality of the ear
    "HP_0002086",  # Abnormality of the respiratory system
    "HP_0000769",  # Abnormality of the breast
    "HP_0001197",  # Abnormality of prenatal development or birth
    "HP_0000271",  # Abnormality of the face
    "HP_0001507",  # Growth abnormality
    "HP_0000234",  # Abnormality of the head
    "HP_0001574",  # Abnormality of the integument
    "HP_0000929",  # Abnormality of the skull
    "HP_0000818",  # Abnormality of the endocrine system
    "HP_0002011",  # Morphological abnormality of the central nervous system
    "HP_0000707",  # Abnormality of the nervous system
    "HP_0001939",  # Abnormality of metabolism/homeostasis
    "HP_0002060",  # Abnormality of the cerebrum
    "HP_0000707",  # Abnormality of the nervous system
]

# Rare phenotypes (30% of assignments) - actual specific rare clinical findings
RARE_PHENOTYPES = [
    "HP_0000316",  # Hypertelorism
    "HP_0000486",  # Strabismus
    "HP_0000508",  # Ptosis
    "HP_0000639",  # Nystagmus
    "HP_0000767",  # Pectus excavatum
    "HP_0001156",  # Brachydactyly
    "HP_0001249",  # Intellectual disability
    "HP_0001263",  # Global developmental delay
    "HP_0001371",  # Flexion contracture
    "HP_0001629",  # Ventricular septal defect
    "HP_0001631",  # Atrial septal defect
    "HP_0001636",  # Tetralogy of Fallot
    "HP_0001655",  # Patent foramen ovale
    "HP_0001999",  # Abnormal facial shape
    "HP_0002007",  # Frontal bossing
    "HP_0002079",  # Hypoplasia of the corpus callosum
    "HP_0002119",  # Ventriculomegaly
    "HP_0002564",  # Malformation of the heart and great vessels
    "HP_0002650",  # Scoliosis
    "HP_0002751",  # Kyphoscoliosis
    "HP_0003196",  # Short nose
    "HP_0003272",  # Abnormality of the hip bone
    "HP_0003577",  # Congenital onset
    "HP_0004322",  # Short stature
    "HP_0004374",  # Hemiplegia/hemiparesis
    "HP_0005280",  # Depressed nasal bridge
    "HP_0005347",  # Webbed neck
    "HP_0006101",  # Finger syndactyly
    "HP_0007018",  # Attention deficit hyperactivity disorder
    "HP_0008551",  # Microtia
    "HP_0009381",  # Short finger
    "HP_0009623",  # Proximal placement of thumb
    "HP_0010055",  # Broad hallux
    "HP_0010554",  # Cutaneous finger syndactyly
    "HP_0011304",  # Broad thumb
    "HP_0012368",  # Flat face
    "HP_0012471",  # Thick vermilion border
    "HP_0025031",  # Abnormality of the digestive system
    "HP_0030084",  # Clinodactyly
    "HP_0040064",  # Abnormality of limbs
]

# Disease clusters - realistic phenotype co-occurrence
DISEASE_CLUSTERS = {
    "cardiomyopathy": ["HP_0001627", "HP_0001635", "HP_0001644", "HP_0001678", "HP_0000822"],
    "epilepsy": ["HP_0001250", "HP_0002069", "HP_0001298", "HP_0002376", "HP_0001508"],
    "metabolic": ["HP_0001943", "HP_0000819", "HP_0001513", "HP_0002155", "HP_0001508"],
    "oncology": ["HP_0002664", "HP_0001508", "HP_0000822", "HP_0001943"],
    "complex_rare": RARE_PHENOTYPES[:5]  # Rare diseases often have multiple rare phenotypes
}

# Provider specialties and their associated phenotypes
SPECIALTY_PHENOTYPES = {
    "cardiology": ["HP_0001627", "HP_0001635", "HP_0001644", "HP_0001678", "HP_0000822"],
    "neurology": ["HP_0001250", "HP_0002069", "HP_0001298", "HP_0002376"],
    "endocrinology": ["HP_0001943", "HP_0000819", "HP_0001513", "HP_0002155"],
    "oncology": ["HP_0002664"],
    "pediatrics": ["HP_0001508", "HP_0002376"],
    "internal_medicine": COMMON_PHENOTYPES[:8]  # General medicine sees common conditions
}

# Evidence importance weights (affects documentation frequency)
EVIDENCE_IMPORTANCE = {
    "chief_complaint": (5, 12),    # Heavily documented
    "active_problem": (2, 6),      # Moderate documentation
    "past_history": (1, 3),        # Light documentation
    "incidental": (1, 2)           # Minimal documentation
}

# Estimated label lengths for common phenotype patterns (fallback if no label available)
LABEL_LENGTH_ESTIMATES = {
    "default": (15, 25),      # Most clinical terms
    "short": (8, 15),         # Simple terms like "seizures"
    "long": (25, 45),         # Complex descriptive terms
}

# Note types and their temporal patterns
NOTE_TYPES = [
    ("admission", "emergency_dept", 0),
    ("progress_1", "internal_medicine", 1),
    ("progress_2", "specialty", 2),
    ("progress_3", "specialty", 3),
    ("discharge", "internal_medicine", 5)
]

@dataclass(frozen=True)
class TermSource:
    source: str
    version: str
    iri: str

    def as_dict(self) -> Dict[str, str]:
        return {"source": self.source, "version": self.version, "iri": self.iri}

@pytest.fixture
def term_source_hpo() -> TermSource:
    """Default term source used for synthetic data."""
    return TermSource(source="hpo", version="2024-01-01", iri="http://purl.obolibrary.org/obo/hp.owl")

@pytest.fixture
def scale_run_enabled() -> bool:
    return True

@pytest.fixture
def evaluation_run_id() -> str:
    return f"perf-run-{uuid.uuid4().hex[:10]}"

# ---------------------------------------------------------------------
# Realistic data generation functions
# ---------------------------------------------------------------------

def generate_realistic_qualifiers() -> Dict[str, float]:
    """Generate realistic qualifier distributions based on clinical data."""
    return {
        "negated": 1.0 if random.random() < 0.15 else 0.0,      # 15% negated findings
        "family": 1.0 if random.random() < 0.08 else 0.0,       # 8% family history
        "hypothetical": 1.0 if random.random() < 0.05 else 0.0  # 5% hypothetical/rule-out
    }

def select_weighted_phenotype() -> str:
    """Select phenotype with realistic frequency distribution."""
    # 70% common phenotypes, 30% rare phenotypes
    if random.random() < 0.7:
        return random.choice(COMMON_PHENOTYPES)
    else:
        return random.choice(RARE_PHENOTYPES)

def generate_subject_phenotypes(min_terms: int, max_terms: int) -> List[str]:
    """Generate realistic phenotype clusters for a subject."""
    # Determine subject complexity using power law distribution
    rand = random.random()
    if rand < 0.7:  # 70% simple cases
        n_terms = random.randint(min_terms, min(max_terms, 15))
    elif rand < 0.95:  # 25% moderate complexity
        n_terms = random.randint(16, min(max_terms, 40))
    else:  # 5% complex cases
        n_terms = random.randint(41, max_terms)
    
    phenotypes = []
    
    # 60% of subjects have clustered phenotypes (single disease)
    if random.random() < 0.6:
        cluster_name = random.choice(list(DISEASE_CLUSTERS.keys()))
        cluster_phenotypes = DISEASE_CLUSTERS[cluster_name]
        
        # Sample from cluster first, then fill with related phenotypes
        cluster_sample = random.sample(cluster_phenotypes, min(n_terms, len(cluster_phenotypes)))
        phenotypes.extend(cluster_sample)
        
        # Fill remaining with weighted selection
        while len(phenotypes) < n_terms:
            candidate = select_weighted_phenotype()
            if candidate not in phenotypes:
                phenotypes.append(candidate)
    else:
        # 40% mixed/complex cases - diverse phenotypes
        while len(phenotypes) < n_terms:
            candidate = select_weighted_phenotype()
            if candidate not in phenotypes:
                phenotypes.append(candidate)
    
    return phenotypes[:n_terms]

def get_appropriate_specialty(term_iri: str) -> str:
    """Return appropriate medical specialty for a given phenotype."""
    for specialty, terms in SPECIALTY_PHENOTYPES.items():
        if term_iri in terms:
            return specialty
    return "internal_medicine"  # Default

def get_evidence_importance(term_iri: str, is_primary: bool = False) -> str:
    """Determine clinical importance of a phenotype for evidence generation."""
    if is_primary or term_iri in COMMON_PHENOTYPES[:5]:  # Top 5 most common
        return "chief_complaint"
    elif term_iri in COMMON_PHENOTYPES:
        return "active_problem"
    elif term_iri in RARE_PHENOTYPES:
        return "incidental"  # Rare findings often incidental
    else:
        return "past_history"

def generate_evidence_count(importance: str) -> int:
    """Generate realistic evidence count based on clinical importance."""
    min_ev, max_ev = EVIDENCE_IMPORTANCE[importance]
    return random.randint(min_ev, max_ev)

def generate_realistic_spans(term_iri: str, base_start: int = 50) -> Tuple[int, int]:
    """Generate realistic text spans based on estimated phenotype label length."""
    # For performance testing, we'll estimate label lengths rather than doing actual lookups
    # In real usage, you could query Neptune for actual rdfs:label values
    
    # Estimate label length based on term patterns
    if any(x in term_iri for x in ["HP_0001250", "HP_0000819", "HP_0001513"]):  # Simple terms
        min_len, max_len = LABEL_LENGTH_ESTIMATES["short"]
    elif any(x in term_iri for x in ["HP_0001627", "HP_0002664", "HP_0001298"]):  # Complex terms  
        min_len, max_len = LABEL_LENGTH_ESTIMATES["long"]
    else:
        min_len, max_len = LABEL_LENGTH_ESTIMATES["default"]
    
    # Generate realistic span length with some variation
    span_length = random.randint(min_len, max_len)
    
    # Add some randomness to start position to spread spans across note
    actual_start = base_start + random.randint(0, 20)
    return actual_start, actual_start + span_length

def generate_encounter_timeline(subject_id: str, n_encounters: int = None) -> List[Dict[str, Any]]:
    """Generate realistic clinical encounter timeline."""
    if n_encounters is None:
        # Most subjects have 2-5 encounters, complex cases have more
        n_encounters = random.choices([2, 3, 4, 5, 6, 7, 8], 
                                    weights=[0.3, 0.25, 0.2, 0.15, 0.05, 0.03, 0.02])[0]
    
    base_date = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 365))
    encounters = []
    
    for i in range(min(n_encounters, len(NOTE_TYPES))):
        note_type, dept_type, day_offset = NOTE_TYPES[i]
        encounter_date = base_date + timedelta(days=day_offset)
        
        encounters.append({
            "encounter_id": f"enc-{subject_id}-{i:02d}",
            "date": encounter_date.strftime("%Y-%m-%d"),
            "note_type": note_type,
            "department": dept_type
        })
    
    return encounters

def generate_synthetic_records(
    *,
    project_id: str,
    n_subjects: int,
    min_terms_per_subject: int,
    max_terms_per_subject: int,
    min_evidence_per_term: int,
    max_evidence_per_term: int,
    term_source: Optional[TermSource] = None,
) -> List[Dict[str, Any]]:
    """
    Generate realistic clinical records matching the JSONL input expected by BulkImport.
    """
    records: List[Dict[str, Any]] = []
    row_num = 0

    for s in range(n_subjects):
        project_subject_id = f"subj-{s:06d}"
        
        # Generate realistic phenotype profile for this subject
        subject_phenotypes = generate_subject_phenotypes(min_terms_per_subject, max_terms_per_subject)
        
        # Generate encounter timeline
        encounters = generate_encounter_timeline(project_subject_id)
        
        # Process each phenotype for this subject
        for t_idx, term_iri in enumerate(subject_phenotypes):
            # Determine if this is a primary phenotype (first few are more important)
            is_primary = t_idx < 3
            importance = get_evidence_importance(term_iri, is_primary)
            
            # Generate appropriate number of evidence records
            base_evidence_count = random.randint(min_evidence_per_term, max_evidence_per_term)
            importance_multiplier = generate_evidence_count(importance) / 3  # Scale to base range
            evidence_count = max(1, int(base_evidence_count * importance_multiplier))
            
            # Get appropriate specialty
            specialty = get_appropriate_specialty(term_iri)
            
            for e in range(evidence_count):
                row_num += 1
                
                # Select encounter (prefer later encounters for ongoing conditions)
                encounter = encounters[min(e, len(encounters) - 1)]
                note_id = f"note-{project_subject_id}-{t_idx:02d}-{e:02d}"
                
                # Generate realistic text spans
                base_span = 50 + (e * 30)  # Spread spans across note
                span_start, span_end = generate_realistic_spans(term_iri, base_span)
                
                # Use specialty-appropriate department if available
                provider_type = "physician"
                author_specialty = specialty if encounter["department"] == "specialty" else "internal_medicine"
                
                evidence_obj: Dict[str, Any] = {
                    "type": "clinical_note",
                    "clinical_note_id": note_id,
                    "encounter_id": encounter["encounter_id"],
                    "evidence_creator_id": f"ods/phebee-{specialty}:v1",
                    "evidence_creator_type": "automated",
                    "evidence_creator_name": f"PheBee {specialty.title()} NLP",
                    "note_timestamp": encounter["date"],
                    "note_type": encounter["note_type"],
                    "provider_type": provider_type,
                    "author_specialty": author_specialty,
                    "span_start": span_start,
                    "span_end": span_end,
                    "contexts": generate_realistic_qualifiers(),
                }

                rec: Dict[str, Any] = {
                    "project_id": project_id,
                    "project_subject_id": project_subject_id,
                    "term_iri": term_iri,
                    "evidence": [evidence_obj],
                    "row_num": row_num,
                    "batch_id": 0,
                }

                # Include term_source metadata
                if term_source is not None:
                    rec["term_source"] = term_source.as_dict()

                records.append(rec)

    return records

@pytest.fixture
def evaluation_dataset_scale(
    scale_run_enabled: bool,
    test_project_id: str,
    term_source_hpo: TermSource,
) -> Tuple[str, List[Dict[str, Any]]]:
    """
    Returns (project_id, records) for a realistic scale run.
    """
    n_subjects = int(os.environ.get("PHEBEE_EVAL_SCALE_SUBJECTS", "5000"))
    min_terms = int(os.environ.get("PHEBEE_EVAL_SCALE_MIN_TERMS", "5"))
    max_terms = int(os.environ.get("PHEBEE_EVAL_SCALE_MAX_TERMS", "50"))
    min_evidence = int(os.environ.get("PHEBEE_EVAL_SCALE_MIN_EVIDENCE", "1"))
    max_evidence = int(os.environ.get("PHEBEE_EVAL_SCALE_MAX_EVIDENCE", "8"))

    records = generate_synthetic_records(
        project_id=test_project_id,
        n_subjects=n_subjects,
        min_terms_per_subject=min_terms,
        max_terms_per_subject=max_terms,
        min_evidence_per_term=min_evidence,
        max_evidence_per_term=max_evidence,
        term_source=term_source_hpo,
    )
    return test_project_id, records

# ---------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------

def upload_jsonl_shards(
    *,
    s3_bucket: str,
    s3_prefix: str,
    records: List[Dict[str, Any]],
    shard_size: int,
) -> str:
    """Upload records as multiple JSONL files under s3://bucket/<s3_prefix>/jsonl/."""
    s3 = boto3.client("s3")
    jsonl_prefix = f"{s3_prefix.rstrip('/')}/jsonl"
    total = len(records)

    shard_size = max(1, shard_size)
    shard_count = (total + shard_size - 1) // shard_size

    for i in range(shard_count):
        chunk = records[i * shard_size : (i + 1) * shard_size]
        body = "\n".join(json.dumps(r) for r in chunk).encode("utf-8")
        key = f"{jsonl_prefix}/shard-{i:05d}.json"
        s3.put_object(
            Bucket=s3_bucket,
            Key=key,
            Body=body,
            ContentType="application/x-ndjson",
        )

    # Verify upload
    resp = s3.list_objects_v2(Bucket=s3_bucket, Prefix=jsonl_prefix + "/")
    keys = [o.get("Key") for o in resp.get("Contents", [])]
    if not any(k and k.endswith(".json") for k in keys):
        raise RuntimeError(f"No .json shards uploaded under s3://{s3_bucket}/{jsonl_prefix}/")

    return f"s3://{s3_bucket}/{jsonl_prefix}"

def run_bulk_import_sfn(
    *,
    physical_resources: Dict[str, Any],
    run_id: str,
    input_path_s3: str,
    timeout_s: int,
) -> Dict[str, Any]:
    """Run BulkImport Step Function and wait for completion."""
    sfn_arn = physical_resources.get("BulkImportStateMachine")
    if not sfn_arn:
        pytest.skip("BulkImportStateMachine not found in physical resources")

    sfn = boto3.client("stepfunctions")
    exec_name = f"perf-exec-{uuid.uuid4().hex[:10]}"

    start = sfn.start_execution(
        stateMachineArn=sfn_arn,
        name=exec_name,
        input=json.dumps({"run_id": run_id, "input_path": input_path_s3}),
    )
    exec_arn = start["executionArn"]

    start_time = time.time()
    while True:
        desc = sfn.describe_execution(executionArn=exec_arn)
        status = desc["status"]

        if status == "SUCCEEDED":
            return {"executionArn": exec_arn, "describe_execution": desc}
        if status in ("FAILED", "TIMED_OUT", "ABORTED"):
            hist = sfn.get_execution_history(executionArn=exec_arn, reverseOrder=True, maxResults=25)
            return {"executionArn": exec_arn, "describe_execution": desc, "history": hist}

        if time.time() - start_time > timeout_s:
            pytest.fail(f"Bulk import SFN did not complete within {timeout_s} seconds (executionArn={exec_arn})")

        time.sleep(30)

def api_post(api_base_url: str, path: str, payload: Dict[str, Any], sigv4_auth) -> requests.Response:
    """Make authenticated API POST request."""
    url = f"{api_base_url}{path}"
    return requests.post(url, json=payload, auth=sigv4_auth, timeout=60)

def pctl(values: List[float], percentile: float) -> float:
    """Return percentile of list of floats (seconds)."""
    if not values:
        return float("nan")
    vals = sorted(values)
    k = (len(vals) - 1) * (percentile / 100.0)
    f = int(k)
    c = min(f + 1, len(vals) - 1)
    if f == c:
        return vals[f]
    return vals[f] + (vals[c] - vals[f]) * (k - f)

def timed_call(fn) -> float:
    """Time a function call and return duration in seconds."""
    t0 = time.perf_counter()
    fn()
    return time.perf_counter() - t0

def run_concurrent(fn, n: int, concurrency: int) -> List[float]:
    """Run function concurrently and return timing results."""
    timings: List[float] = []
    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futs = [ex.submit(timed_call, fn) for _ in range(n)]
        for fut in as_completed(futs):
            timings.append(fut.result())
    return timings

def maybe_write_metrics(metrics: Dict[str, Any]) -> None:
    """Write metrics to console, local file, and/or S3."""
    # Always print for logs
    print("[EVAL_R11_JSON]", json.dumps(metrics, indent=2, sort_keys=True))

    # Optional local file write
    out_path = os.environ.get("PHEBEE_EVAL_METRICS_PATH")
    if out_path:
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(metrics, f, indent=2, sort_keys=True)
        print(f"[EVAL_METRICS_WRITTEN] {out_path}")

    # Optional S3 upload
    out_s3 = os.environ.get("PHEBEE_EVAL_METRICS_S3_URI")
    if out_s3 and out_s3.startswith("s3://"):
        m = re.match(r"s3://([^/]+)/?(.*)", out_s3)
        if m:
            bucket, prefix = m.group(1), m.group(2)
            key = f"{prefix.rstrip('/')}/r11_metrics_{metrics.get('run_id','unknown')}.json"
            boto3.client("s3").put_object(
                Bucket=bucket,
                Key=key,
                Body=json.dumps(metrics, indent=2, sort_keys=True).encode("utf-8"),
                ContentType="application/json",
            )
            print(f"[EVAL_METRICS_UPLOADED] s3://{bucket}/{key}")

# ---------------------------------------------------------------------
# Comprehensive API testing functions
# ---------------------------------------------------------------------

def create_api_test_functions(api_base_url: str, sigv4_auth, project_id: str, 
                            project_subject_iris: List[str]) -> Dict[str, callable]:
    """Create comprehensive API test functions covering all realistic query patterns."""
    
    # Rotation index for subject queries
    idx = {"i": 0}
    
    def call_basic_subjects_query():
        """Basic project subjects query - most common pattern."""
        r = api_post(api_base_url, "/subjects/query", {
            "project_id": project_id, 
            "limit": 10
        }, sigv4_auth)
        assert r.status_code == 200

    def call_individual_subject():
        """Individual subject lookup - common for detailed views."""
        i = idx["i"]
        idx["i"] = (i + 1) % len(project_subject_iris)
        r = api_post(api_base_url, "/subject", {
            "project_subject_iri": project_subject_iris[i]
        }, sigv4_auth)
        assert r.status_code == 200

    def call_hierarchy_query():
        """Hierarchy expansion query - research pattern."""
        r = api_post(api_base_url, "/subjects/query", {
            "project_id": project_id,
            "term_iri": "HP_0000118",        # Phenotypic abnormality (root)
            "include_child_terms": True,     # Test hierarchy expansion
            "limit": 20
        }, sigv4_auth)
        assert r.status_code == 200

    def call_qualified_filtering():
        """Qualifier filtering - exclude negated/family/hypothetical."""
        term = random.choice(COMMON_PHENOTYPES)
        r = api_post(api_base_url, "/subjects/query", {
            "project_id": project_id,
            "term_iri": term,
            "include_qualified": False,      # Exclude qualified findings
            "limit": 15
        }, sigv4_auth)
        assert r.status_code == 200

    def call_specific_phenotype():
        """Specific phenotype query - targeted research."""
        term = random.choice(COMMON_PHENOTYPES + RARE_PHENOTYPES)
        r = api_post(api_base_url, "/subjects/query", {
            "project_id": project_id,
            "term_iri": term,
            "include_child_terms": True,
            "limit": 25
        }, sigv4_auth)
        assert r.status_code == 200

    def call_cardiac_cohort():
        """Cardiac phenotype cohort - specialty research pattern."""
        cardiac_terms = ["HP_0001627", "HP_0001635", "HP_0001644"]
        term = random.choice(cardiac_terms)
        r = api_post(api_base_url, "/subjects/query", {
            "project_id": project_id,
            "term_iri": term,
            "include_child_terms": True,
            "include_qualified": False,
            "limit": 30
        }, sigv4_auth)
        assert r.status_code == 200

    def call_neuro_cohort():
        """Neurological phenotype cohort - specialty research pattern."""
        neuro_terms = ["HP_0001250", "HP_0002069", "HP_0001298"]
        term = random.choice(neuro_terms)
        r = api_post(api_base_url, "/subjects/query", {
            "project_id": project_id,
            "term_iri": term,
            "include_child_terms": True,
            "limit": 25
        }, sigv4_auth)
        assert r.status_code == 200

    def call_paginated_large_cohort():
        """Large cohort with pagination - stress test."""
        r = api_post(api_base_url, "/subjects/query", {
            "project_id": project_id,
            "term_iri": "HP_0000118",  # Root - matches most subjects
            "include_child_terms": True,
            "limit": 50               # Force pagination
        }, sigv4_auth)
        assert r.status_code == 200
        
        # Follow pagination if available
        body = r.json().get("body", {})
        if isinstance(body, dict) and body.get("pagination", {}).get("next_cursor"):
            r2 = api_post(api_base_url, "/subjects/query", {
                "project_id": project_id,
                "cursor": body["pagination"]["next_cursor"],
                "limit": 50
            }, sigv4_auth)
            assert r2.status_code == 200

    def call_subject_term_info():
        """Subject term info - detailed phenotype view."""
        i = idx["i"]
        idx["i"] = (i + 1) % len(project_subject_iris)
        term = random.choice(COMMON_PHENOTYPES)
        
        # Extract subject_id from project_subject_iri
        subject_iri = project_subject_iris[i]
        subject_id = subject_iri.split("/")[-1]  # Get UUID from IRI
        
        r = api_post(api_base_url, "/subject/term-info", {
            "subject_id": subject_id,  # Use subject_id instead of project_subject_iri
            "term_iri": term
        }, sigv4_auth)
        # Note: May return 404 if subject doesn't have this term - that's OK for perf testing
        assert r.status_code in [200, 404]

    def call_version_specific_query():
        """Version-specific ontology query."""
        r = api_post(api_base_url, "/subjects/query", {
            "project_id": project_id,
            "term_source": "hpo",
            "term_source_version": "2024-01-01",
            "limit": 20
        }, sigv4_auth)
        assert r.status_code == 200

    return {
        "basic_subjects_query": call_basic_subjects_query,
        "individual_subject": call_individual_subject,
        "hierarchy_query": call_hierarchy_query,
        "qualified_filtering": call_qualified_filtering,
        "specific_phenotype": call_specific_phenotype,
        "cardiac_cohort": call_cardiac_cohort,
        "neuro_cohort": call_neuro_cohort,
        "paginated_large_cohort": call_paginated_large_cohort,
        "subject_term_info": call_subject_term_info,
        "version_specific_query": call_version_specific_query,
    }

# ---------------------------------------------------------------------
# Main enhanced performance test
# ---------------------------------------------------------------------

@pytest.mark.perf
def test_r11_enhanced_api_latency_at_scale(
    api_base_url,
    sigv4_auth,
    physical_resources,
    evaluation_run_id: str,
    evaluation_dataset_scale: Tuple[str, List[Dict[str, Any]]],
):
    """
    Enhanced performance test with realistic clinical data patterns and comprehensive API testing.
    """
    project_id, records = evaluation_dataset_scale

    # Resolve physical resources
    s3_bucket = physical_resources.get("PheBeeBucket")
    if not s3_bucket:
        pytest.skip("PheBeeBucket not found in physical resources")

    shard_size = int(os.environ.get("PHEBEE_EVAL_BATCH_SIZE", "10000"))
    ingest_timeout = int(os.environ.get("PHEBEE_EVAL_INGEST_TIMEOUT_S", "7200"))

    # Calculate dataset statistics
    total_subjects = len(set(r["project_subject_id"] for r in records))
    total_terms = len(set(r["term_iri"] for r in records))
    total_evidence = sum(len(r["evidence"]) for r in records)
    
    # Calculate average unique terms per subject
    subject_term_counts = {}
    for record in records:
        subject_id = record["project_subject_id"]
        term_iri = record["term_iri"]
        if subject_id not in subject_term_counts:
            subject_term_counts[subject_id] = set()
        subject_term_counts[subject_id].add(term_iri)
    
    avg_terms_per_subject = sum(len(terms) for terms in subject_term_counts.values()) / len(subject_term_counts)
    
    # Analyze qualifier distribution
    qualifier_stats = {"negated": 0, "family": 0, "hypothetical": 0, "total": 0}
    for record in records:
        for evidence in record["evidence"]:
            contexts = evidence.get("contexts", {})
            qualifier_stats["total"] += 1
            if contexts.get("negated", 0.0) > 0:
                qualifier_stats["negated"] += 1
            if contexts.get("family", 0.0) > 0:
                qualifier_stats["family"] += 1
            if contexts.get("hypothetical", 0.0) > 0:
                qualifier_stats["hypothetical"] += 1

    print(f"[DATASET_STATS] {len(records)} records, {total_subjects} subjects, {total_terms} unique terms, {total_evidence} evidence")
    print(f"[QUALIFIER_STATS] {qualifier_stats}")

    # Upload data
    input_prefix = f"perf-data/{evaluation_run_id}"
    input_path_s3 = upload_jsonl_shards(
        s3_bucket=s3_bucket,
        s3_prefix=input_prefix,
        records=records,
        shard_size=shard_size,
    )

    # Bulk import
    print(f"[BULK_IMPORT_START] {input_path_s3}")
    t0 = time.time()
    ingest_result = run_bulk_import_sfn(
        physical_resources=physical_resources,
        run_id=evaluation_run_id,
        input_path_s3=input_path_s3,
        timeout_s=ingest_timeout,
    )
    ingest_s = time.time() - t0

    status = ingest_result["describe_execution"]["status"]
    if status != "SUCCEEDED":
        hist = ingest_result.get("history")
        if hist:
            print("[SFN_HISTORY_TAIL]", json.dumps(hist, indent=2, default=str)[:8000])
        pytest.fail(f"Bulk import did not succeed (status={status})")

    print(f"[BULK_IMPORT_COMPLETE] {ingest_s:.1f}s, {len(records)/ingest_s:.1f} records/sec")

    # Fetch subjects for API testing
    resp = api_post(api_base_url, "/subjects/query", {"project_id": project_id, "limit": 100}, sigv4_auth)
    assert resp.status_code == 200, resp.text
    body = resp.json().get("body") or []
    project_subject_iris = [x.get("project_subject_iri") for x in body if x.get("project_subject_iri")]
    assert project_subject_iris, "No subjects returned after ingest; cannot run latency workload."

    print(f"[API_TEST_PREP] {len(project_subject_iris)} subjects available for testing")

    # Create comprehensive API test functions
    api_functions = create_api_test_functions(api_base_url, sigv4_auth, project_id, project_subject_iris)

    # Warm-up to reduce cold-start skew
    print("[WARMUP_START]")
    for name, func in list(api_functions.items())[:3]:  # Warm up first 3 functions
        try:
            func()
        except Exception as e:
            print(f"[WARMUP_WARNING] {name} failed: {e}")

    # Test parameters
    n = int(os.environ.get("PHEBEE_EVAL_LATENCY_N", "500"))
    conc = int(os.environ.get("PHEBEE_EVAL_CONCURRENCY", "25"))

    print(f"[LATENCY_TEST_START] {n} requests per endpoint, {conc} concurrent")

    # Run comprehensive latency tests
    latency_results = []
    for name, func in api_functions.items():
        print(f"[TESTING] {name}")
        try:
            timings = run_concurrent(func, n=n, concurrency=conc)
            result = {
                "endpoint": name,
                "n": len(timings),
                "p50_ms": round(pctl(timings, 50) * 1000, 2),
                "p95_ms": round(pctl(timings, 95) * 1000, 2),
                "p99_ms": round(pctl(timings, 99) * 1000, 2),
                "max_ms": round(max(timings) * 1000, 2) if timings else None,
                "min_ms": round(min(timings) * 1000, 2) if timings else None,
                "avg_ms": round(sum(timings) / len(timings) * 1000, 2) if timings else None,
            }
            latency_results.append(result)
            print(f"[RESULT] {name}: p50={result['p50_ms']}ms, p95={result['p95_ms']}ms, p99={result['p99_ms']}ms")
        except Exception as e:
            print(f"[ERROR] {name} failed: {e}")
            latency_results.append({
                "endpoint": name,
                "error": str(e),
                "n": 0
            })

    # Compile comprehensive metrics
    metrics: Dict[str, Any] = {
        "run_id": evaluation_run_id,
        "project_id": project_id,
        "dataset": {
            "n_records": len(records),
            "n_subjects": total_subjects,
            "n_unique_terms": total_terms,
            "n_evidence": total_evidence,
            "avg_terms_per_subject": round(avg_terms_per_subject, 2),
            "avg_evidence_per_record": round(total_evidence / len(records), 2),
            "qualifier_distribution": {
                "negated_pct": round(qualifier_stats["negated"] / qualifier_stats["total"] * 100, 1),
                "family_pct": round(qualifier_stats["family"] / qualifier_stats["total"] * 100, 1),
                "hypothetical_pct": round(qualifier_stats["hypothetical"] / qualifier_stats["total"] * 100, 1),
            },
            "phenotype_distribution": {
                "common_phenotypes": len([r for r in records if r["term_iri"] in COMMON_PHENOTYPES]),
                "rare_phenotypes": len([r for r in records if r["term_iri"] in RARE_PHENOTYPES]),
            }
        },
        "ingestion": {
            "seconds": round(ingest_s, 2),
            "records_per_sec": round((len(records) / ingest_s), 2) if ingest_s > 0 else None,
            "sfn_execution_arn": ingest_result.get("executionArn"),
        },
        "load_testing": {
            "concurrency": conc, 
            "requests_per_endpoint": n,
            "total_api_calls": sum(r.get("n", 0) for r in latency_results)
        },
        "latency": latency_results,
        "performance_summary": {
            "fastest_p95_ms": min((r.get("p95_ms", float('inf')) for r in latency_results if "p95_ms" in r), default=None),
            "slowest_p95_ms": max((r.get("p95_ms", 0) for r in latency_results if "p95_ms" in r), default=None),
            "avg_p95_ms": round(sum(r.get("p95_ms", 0) for r in latency_results if "p95_ms" in r) / 
                              len([r for r in latency_results if "p95_ms" in r]), 2) if latency_results else None,
        }
    }

    maybe_write_metrics(metrics)

    # Optional strict performance gates
    if os.environ.get("PHEBEE_EVAL_STRICT_LATENCY") == "1":
        for item in latency_results:
            if "p95_ms" in item:
                assert item["p95_ms"] <= 5000, f"{item['endpoint']} p95_ms={item['p95_ms']} > 5000ms"

    print(f"[TEST_COMPLETE] Enhanced performance test completed successfully")
