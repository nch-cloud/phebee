# PheBee Performance Testing

This directory contains tools and tests for evaluating PheBee's performance at scale, including bulk data ingestion throughput and API query latency under realistic clinical data patterns.

## Overview

Performance testing infrastructure consists of:

1. **Data Generation**: Tools to create reproducible synthetic datasets with realistic clinical patterns
2. **Import Performance Tests**: Measure bulk data ingestion throughput
3. **API Latency Tests**: Measure query performance under load with various access patterns
4. **Benchmark Dataset Generation**: Create shareable datasets for manuscript reproducibility

## Table of Contents

- [Methodology](#methodology)
- [Quick Start](#quick-start)
- [Data Generation](#data-generation)
- [Running Performance Tests](#running-performance-tests)
- [Environment Variables](#environment-variables)
- [Benchmark Datasets](#benchmark-datasets)

---

## Methodology

### Realistic Clinical Data Patterns

Our synthetic data generation incorporates clinically realistic patterns based on published literature:

#### 1. Disease Clustering (60% of subjects)

Subjects are assigned phenotypes that co-occur in real clinical practice, rather than random sampling. This reflects how patients typically present with related symptoms from a common underlying condition.

**Disease Clusters:**
- **Cardiomyopathy**: Heart failure, dilated cardiomyopathy, arrhythmias
- **Epilepsy**: Seizures, encephalopathy, developmental issues
- **Metabolic**: Diabetes, obesity, hypertriglyceridemia
- **Oncology**: Neoplasm with associated complications
- **Rare Dysmorphic**: Multiple congenital anomalies

*References:*
- Cardiomyopathy: Hershberger et al., *Circulation* 2018
- Epilepsy: Scheffer et al., *Nat Rev Neurol* 2017
- Metabolic: Ferreira et al., *Nat Rev Endocrinol* 2019

#### 2. Term Frequency Weighting

When a prevalence CSV is provided (`PHEBEE_EVAL_PREVALENCE_CSV_PATH`), terms are sampled according to real-world clinical frequencies:
- **Common phenotypes** (70% of assignments): Frequently documented findings
- **Rare phenotypes** (30% of assignments): Less common but clinically important findings

This ensures synthetic data matches real clinical data distributions.

#### 3. Qualifier Distributions

Realistic context qualifiers based on clinical documentation patterns:
- **Negated** (15%): Explicitly ruled-out findings
- **Family History** (8%): Conditions observed in relatives
- **Hypothetical** (5%): Suspected or rule-out diagnoses
- **Unqualified** (72%): Present/observed findings

#### 4. Evidence Importance Weighting

Documentation frequency varies by clinical importance:
- **Chief complaints** (5-12 evidence items): Primary presenting symptoms
- **Active problems** (2-6 evidence items): Current issues under management
- **Past history** (1-3 evidence items): Historical findings
- **Incidental** (1-2 evidence items): Noted but not primary concern

#### 5. Specialty Attribution

Phenotypes are documented by appropriate medical specialties:
- Cardiac phenotypes → Cardiology
- Neurological phenotypes → Neurology
- Metabolic phenotypes → Endocrinology
- Rare dysmorphic features → Genetics

---

## Quick Start

**Step 1: Download HPO ontology**
```bash
cd tests/integration/performance
curl -L http://purl.obolibrary.org/obo/hp.obo -o data/hp.obo
```

**Step 2: Generate HPO terms index**
```bash
python generate_hpo_terms_json.py --obo data/hp.obo --out data/hpo_terms.json
```

**Step 3: Configure environment**
```bash
# Required
export PHEBEE_EVAL_SCALE=1
export PHEBEE_EVAL_TERMS_JSON_PATH="tests/integration/performance/data/hpo_terms.json"

# Manuscript parameters (recommended for Table 3 & 4)
export PHEBEE_EVAL_SCALE_SUBJECTS=10000      # Dataset size (default: 10000)
export PHEBEE_EVAL_CONCURRENCY=25            # Concurrent workers (default: 25)
```

**Step 4: Run tests from project root**
```bash
# IMPORTANT: Run from project root so .phebee-test-stack file is found
cd /path/to/phebee  # Navigate to project root

pytest -v tests/integration/performance/test_import_performance.py \
          tests/integration/performance/test_evaluation_perf_scale.py
```

The test output will show: `[Performance Tests] Creating shared project: test_project_abc12345`

**Step 5: Run additional evaluations (optional)**

To test different load parameters without reimporting data, reuse the project ID from Step 4:

```bash
# Capture the project ID from Step 4 output
export PHEBEE_EVAL_PROJECT_ID=test_project_abc12345  # Use actual ID from output

# Test with different concurrency (run from project root)
export PHEBEE_EVAL_CONCURRENCY=50
pytest -v tests/integration/performance/test_evaluation_perf_scale.py
```

**Optional:** Generate static benchmark dataset for manuscript reproducibility:
```bash
python tests/integration/performance/generate_benchmark_dataset.py  # Creates data/benchmark/{n_subjects}-subjects-seed{seed}/
```

Each dataset is isolated in its own subdirectory based on parameters (subjects, seed). Non-default configurations (e.g., `--no-disease-clustering`) add a suffix.

**Using Pre-Generated Datasets:** To use a previously generated benchmark dataset instead of generating fresh data, set `PHEBEE_EVAL_BENCHMARK_DIR`:
```bash
export PHEBEE_EVAL_BENCHMARK_DIR="tests/integration/performance/data/benchmark/10000-subjects-seed42"

# Run from project root
cd /path/to/phebee
pytest -v tests/integration/performance/test_import_performance.py \
          tests/integration/performance/test_evaluation_perf_scale.py
```

---

## Data Generation

### Step 1: Generate HPO Terms Index

The HPO terms JSON provides a compact, searchable index of HPO terms with precomputed pools for sampling.

```bash
python generate_hpo_terms_json.py \
  --obo /path/to/hp.obo \
  --out data/hpo_terms_v2026-01-08.json
```

**Input:** HPO OBO file from https://hpo.jax.org/downloads/ontology
**Output:** JSON with term records, hierarchy, and sampling pools

The output includes:
- All term metadata (ID, IRI, name, synonyms, parents)
- Precomputed pools: `all`, `internal`, `leaf`
- HPO version and metadata

### Step 2: Generate Benchmark Dataset

Create a reproducible synthetic dataset for performance testing and manuscript figures.

#### Basic Usage

```bash
export PHEBEE_EVAL_TERMS_JSON_PATH="/path/to/hpo_terms.json"
python generate_benchmark_dataset.py
```

This creates a dataset in `data/benchmark/` with:
- NDJSON batch files (`batches/batch-NNNNN.json`)
- Metadata file (`metadata.json`)
- Human-readable documentation (`README.md`)

#### With Realistic Clinical Patterns (Recommended)

```bash
export PHEBEE_EVAL_TERMS_JSON_PATH="/path/to/hpo_terms.json"
export PHEBEE_EVAL_PREVALENCE_CSV_PATH="/path/to/term_frequencies.csv"
export PHEBEE_EVAL_SEED=42  # For reproducibility

python generate_benchmark_dataset.py --use-disease-clustering
```

**Disease clustering** is **enabled by default** for manuscript benchmarks. To disable:

```bash
python generate_benchmark_dataset.py --no-disease-clustering
```

#### Customization Options

```bash
# Scale parameters
export PHEBEE_EVAL_SCALE_SUBJECTS=50000       # Number of subjects (default: 10,000)
export PHEBEE_EVAL_SCALE_MIN_TERMS=5          # Min HPO terms per subject
export PHEBEE_EVAL_SCALE_MAX_TERMS=50         # Max HPO terms per subject
export PHEBEE_EVAL_SCALE_MIN_EVIDENCE=1       # Min evidence items per term
export PHEBEE_EVAL_SCALE_MAX_EVIDENCE=25      # Max evidence items per term

# Output configuration
export PHEBEE_EVAL_BATCH_SIZE=10000           # Records per batch file

python generate_benchmark_dataset.py \
  --output-dir /custom/output/path \
  --project-id my-benchmark-v1
```

### Prevalence CSV Format

Optional CSV for scientifically accurate term frequency weighting:

```csv
term_iri,frequency
http://purl.obolibrary.org/obo/HP_0001945,15234
http://purl.obolibrary.org/obo/HP_0011134,12891
http://purl.obolibrary.org/obo/HP_0001626,10503
```

**Column name flexibility:** The loader accepts various column names:
- Term column: `term_iri`, `term`, `iri`, `hpo_term`, `phenotype`
- Frequency column: `frequency`, `count`, `prevalence`, `freq`, `n`, `occurrences`

---

## Running Performance Tests

### Import Performance Test

Measures bulk data ingestion throughput (Manuscript Table 3).

```bash
export PHEBEE_EVAL_SCALE=1
export PHEBEE_EVAL_TERMS_JSON_PATH="tests/integration/performance/data/hpo_terms_v2026-01-08.json"
export PHEBEE_EVAL_PREVALENCE_CSV_PATH="/path/to/term_frequencies.csv"  # optional

# Configure scale
export PHEBEE_EVAL_SCALE_SUBJECTS=10000
export PHEBEE_EVAL_SCALE_MIN_TERMS=5
export PHEBEE_EVAL_SCALE_MAX_TERMS=50

# Enable disease clustering for realistic clinical patterns (recommended)
export PHEBEE_EVAL_USE_DISEASE_CLUSTERING=1

# Run test from project root
cd /path/to/phebee
pytest -v -s tests/integration/performance/test_import_performance.py
```

**Outputs:**
- Console: Import statistics and throughput metrics
- `/tmp/phebee-eval-artifacts/{run_id}/table3_ingestion.csv`
- `/tmp/phebee-eval-artifacts/{run_id}/import_run.json`

**Key Metrics:**
- Total ingest time (seconds)
- Records per second throughput
- Evidence items per second throughput

### API Latency Test (Manuscript Table 4)

Comprehensive API latency testing with 10 realistic query patterns under concurrent load.

**Important:** This test requires a project with imported data. Either:
1. Run both tests together: `pytest -v test_import_performance.py test_evaluation_perf_scale.py`
2. Run separately with project ID: Set `PHEBEE_EVAL_PROJECT_ID=<project_id_from_import_test>`

```bash
export PHEBEE_EVAL_SCALE=1
export PHEBEE_EVAL_TERMS_JSON_PATH="tests/integration/performance/data/hpo_terms_v2026-01-08.json"
export PHEBEE_EVAL_USE_DISEASE_CLUSTERING=1  # Use realistic clinical patterns

# Configure load testing
export PHEBEE_EVAL_LATENCY_N=500              # Requests per endpoint (default: 500)
export PHEBEE_EVAL_CONCURRENCY=25             # Concurrent workers (default: 25)

# Optional: Enable strict performance gates
export PHEBEE_EVAL_STRICT_LATENCY=1           # Fail if p95 > 5 seconds

# Run from project root
cd /path/to/phebee

# Option A: Run standalone (requires PHEBEE_EVAL_PROJECT_ID from previous import test)
export PHEBEE_EVAL_PROJECT_ID=test_project_abc12345
pytest -v -s tests/integration/performance/test_evaluation_perf_scale.py

# Option B: Run with import test in same session (recommended)
pytest -v -s tests/integration/performance/test_import_performance.py \
          tests/integration/performance/test_evaluation_perf_scale.py
```

#### Query Patterns Tested

The test executes 10 comprehensive query patterns representing realistic clinical and research use cases:

| # | Pattern | Description | Use Case |
|---|---------|-------------|----------|
| 1 | **basic_subjects_query** | Simple project query with limit | Most common access pattern |
| 2 | **individual_subject** | Single subject detail lookup | Patient detail views |
| 3 | **hierarchy_query** | Descendant term expansion | Research: "all heart conditions" |
| 4 | **qualified_filtering** | Exclude negated/family/hypothetical | Clinical: confirmed findings only |
| 5 | **specific_phenotype** | Direct term query with hierarchy | Research: specific condition cohorts |
| 6 | **cardiac_cohort** | Cardiology specialty cohort | Specialty research queries |
| 7 | **neuro_cohort** | Neurology specialty cohort | Specialty research queries |
| 8 | **paginated_large_cohort** | Large result set with pagination | Broad cohort queries with cursor |
| 9 | **subject_term_info** | Detailed subject-term evidence | Curator: evidence review |
| 10 | **version_specific_query** | Ontology version filtering | Research: specific HPO version |

#### Performance Metrics Collected

**Per-Endpoint Metrics:**
- **p50** (median): Typical response time
- **p95**: 95th percentile (SLA target)
- **p99**: 99th percentile (worst case)
- **max**: Maximum observed latency
- **min**: Minimum observed latency
- **avg**: Mean response time
- **n**: Number of requests tested

**Aggregate Metrics:**
- **Fastest p95**: Best-performing endpoint
- **Slowest p95**: Worst-performing endpoint
- **Average p95**: Overall system performance

**Dataset Metrics:**
- Records, subjects, terms, evidence counts
- Terms per subject distribution (min/max/mean/median)
- Evidence per record distribution (min/max/mean/median)
- Qualifier distribution (negated/family/hypothetical percentages)
- Disease cluster distribution (if clustering enabled)

**Import Performance:**
- Total ingest time (seconds)
- Records per second throughput
- Evidence per second throughput

#### Outputs

**Console:**
- Real-time progress for each endpoint
- Summary statistics table
- Performance warnings if thresholds exceeded

**Optional Files:**
- Local JSON: `PHEBEE_EVAL_METRICS_PATH=/tmp/phebee_api_metrics.json`
- S3 Upload: `PHEBEE_EVAL_METRICS_S3_URI=s3://bucket/prefix/`

**JSON Structure:**
```json
{
  "run_id": "api-perf-abc123",
  "project_id": "test_project_xyz",
  "dataset": {
    "n_records": 150000,
    "n_subjects": 10000,
    "n_unique_terms": 350,
    "n_evidence": 450000,
    "avg_terms_per_subject": 15.0,
    "avg_evidence_per_record": 3.0,
    "disease_clustering_enabled": true,
    "cluster_distribution": {...}
  },
  "ingestion": {
    "seconds": 180.5,
    "records_per_sec": 831.0,
    "sfn_execution_arn": "arn:aws:..."
  },
  "load_testing": {
    "concurrency": 25,
    "requests_per_endpoint": 500,
    "total_api_calls": 5000
  },
  "latency": [
    {
      "endpoint": "basic_subjects_query",
      "n": 500,
      "p50_ms": 1250.5,
      "p95_ms": 2100.2,
      "p99_ms": 2800.1,
      "max_ms": 3200.0,
      "min_ms": 800.5,
      "avg_ms": 1350.8
    },
    ...
  ],
  "performance_summary": {
    "fastest_p95_ms": 1200.0,
    "slowest_p95_ms": 3500.0,
    "avg_p95_ms": 2200.5
  }
}
```

#### Performance Gates

Enable strict performance validation with `PHEBEE_EVAL_STRICT_LATENCY=1`. This enforces:
- **p95 ≤ 5000ms** for all endpoints
- Test fails if any endpoint exceeds threshold
- Use for continuous integration performance regression testing

---

## Environment Variables

### Required

| Variable | Description | Example |
|----------|-------------|---------|
| `PHEBEE_EVAL_SCALE` | Enable performance tests (set to `1`) | `1` |
| `PHEBEE_EVAL_TERMS_JSON_PATH` | Path to HPO terms JSON | `./data/hpo_terms.json` |

### Optional - Test Execution

| Variable | Default | Description |
|----------|---------|-------------|
| `PHEBEE_EVAL_PROJECT_ID` | None | Use existing project instead of creating new one. Required when running tests separately. Set to project_id from import test output. |

### Optional - Load Testing (API Latency Test)

| Variable | Default | Description |
|----------|---------|-------------|
| `PHEBEE_EVAL_LATENCY_N` | 500 | Number of requests per API endpoint pattern |
| `PHEBEE_EVAL_CONCURRENCY` | 25 | Number of concurrent workers for load testing |
| `PHEBEE_EVAL_STRICT_LATENCY` | 0 | Enforce p95 ≤ 5000ms performance gates (1=enabled, 0=disabled) |
| `PHEBEE_EVAL_INGEST_TIMEOUT_S` | 7,200 | Timeout in seconds for bulk import Step Function (2 hours) |
| `PHEBEE_EVAL_METRICS_PATH` | None | Local file path to write performance metrics JSON |
| `PHEBEE_EVAL_METRICS_S3_URI` | None | S3 URI to upload performance metrics JSON (e.g., `s3://bucket/prefix/`) |

### Optional - Data Generation

| Variable | Default | Description |
|----------|---------|-------------|
| `PHEBEE_EVAL_BENCHMARK_DIR` | None | Path to pre-generated benchmark dataset directory (if set, loads from disk instead of generating) |
| `PHEBEE_EVAL_PREVALENCE_CSV_PATH` | None | Term frequency CSV for realistic distributions |
| `PHEBEE_EVAL_SEED` | 42 | Random seed for reproducibility |
| `PHEBEE_EVAL_SCALE_SUBJECTS` | 10,000 | Number of subjects |
| `PHEBEE_EVAL_SCALE_MIN_TERMS` | 5 | Min HPO terms per subject |
| `PHEBEE_EVAL_SCALE_MAX_TERMS` | 50 | Max HPO terms per subject |
| `PHEBEE_EVAL_SCALE_MIN_EVIDENCE` | 1 | Min evidence items per term |
| `PHEBEE_EVAL_SCALE_MAX_EVIDENCE` | 25 | Max evidence items per term |
| `PHEBEE_EVAL_BATCH_SIZE` | 10,000 | Records per S3 batch file |
| `PHEBEE_EVAL_USE_DISEASE_CLUSTERING` | 1 | Enable disease clustering (1=enabled, 0=disabled) |

### Optional - Clinical Realism (Advanced)

| Variable | Default | Description |
|----------|---------|-------------|
| `PHEBEE_EVAL_QUAL_NEGATED_PCT` | 0.15 | Negated findings percentage |
| `PHEBEE_EVAL_QUAL_FAMILY_PCT` | 0.08 | Family history percentage |
| `PHEBEE_EVAL_QUAL_HYPOTHETICAL_PCT` | 0.05 | Hypothetical findings percentage |
| `PHEBEE_EVAL_COMMON_TERM_PCT` | 0.70 | Common vs rare term ratio |
| `PHEBEE_EVAL_ANCHOR_TERM_PCT` | 0.60 | Include anchor term probability |
| `PHEBEE_EVAL_NOTE_DATE_START` | 2023-01-01 | Clinical note date range start |
| `PHEBEE_EVAL_NOTE_DATE_END` | 2024-12-31 | Clinical note date range end |

### Optional - Test Execution

| Variable | Default | Description |
|----------|---------|-------------|
| `PHEBEE_EVAL_INGEST_TIMEOUT_S` | 7200 | Max seconds to wait for import (2 hours) |
| `PHEBEE_EVAL_LATENCY_N` | 500 | Requests per API endpoint |
| `PHEBEE_EVAL_CONCURRENCY` | 25 | Concurrent request workers |
| `PHEBEE_EVAL_STRICT_LATENCY` | 0 | Fail test if p95 > 5s |
| `PHEBEE_EVAL_WRITE_ARTIFACTS` | 1 | Write CSV/JSON artifacts |
| `PHEBEE_EVAL_METRICS_PATH` | None | Local path for metrics JSON |
| `PHEBEE_EVAL_METRICS_S3_URI` | None | S3 URI for metrics upload |
| `PHEBEE_EVAL_PROJECT_ID` | None | Use existing project (skip creation) |

---

## Benchmark Datasets

### Generating Shareable Datasets for Manuscripts

For manuscript reproducibility, generate a benchmark dataset with a fixed seed:

```bash
export PHEBEE_EVAL_TERMS_JSON_PATH="./data/hpo_terms_v2026-01-08.json"
export PHEBEE_EVAL_PREVALENCE_CSV_PATH="/path/to/term_frequencies.csv"
export PHEBEE_EVAL_SEED=42
export PHEBEE_EVAL_SCALE_SUBJECTS=50000

python generate_benchmark_dataset.py \
  --project-id phebee-manuscript-2026 \
  --use-disease-clustering
```

**Output Structure:**
```
data/benchmark/
├── metadata.json           # Generation parameters and statistics
├── README.md              # Human-readable documentation
└── batches/
    ├── batch-00000.json   # NDJSON batch files
    ├── batch-00001.json
    └── ...
```

### Sharing via Zenodo

1. Generate dataset with fixed seed (as above)
2. Create a new Zenodo deposit at https://zenodo.org/
3. Upload all files from `data/benchmark/`
4. Add keywords: `PheBee`, `phenotype`, `performance`, `benchmark`, `HPO`
5. Publish and obtain DOI
6. Cite DOI in manuscript methods section

### Using Pre-Generated Benchmarks

To use a previously generated benchmark dataset instead of generating fresh data:

```bash
# Download from Zenodo and extract to project directory
cd /path/to/phebee
wget https://zenodo.org/record/YOUR_RECORD_ID/files/benchmark_dataset.tar.gz
tar -xzf benchmark_dataset.tar.gz -C tests/integration/performance/data/benchmark/

# Point tests to the pre-generated dataset
export PHEBEE_EVAL_SCALE=1
export PHEBEE_EVAL_TERMS_JSON_PATH="tests/integration/performance/data/hpo_terms_v2026-01-08.json"
export PHEBEE_EVAL_BENCHMARK_DIR="tests/integration/performance/data/benchmark/10000-subjects-seed42"

# Run tests using the static benchmark dataset (from project root)
pytest -v tests/integration/performance/test_import_performance.py \
          tests/integration/performance/test_evaluation_perf_scale.py
```

**Note:** When `PHEBEE_EVAL_BENCHMARK_DIR` is set, the tests will load pre-generated data from that directory instead of generating fresh synthetic data. This ensures exact reproducibility across test runs and is recommended for manuscript performance evaluations.

### Important: Running Tests from Project Root

**All performance tests must be run from the project root directory**, not from `tests/integration/performance/`. This is because:

1. The test infrastructure looks for `.phebee-test-stack` file (containing the deployed stack name) in the current working directory
2. This file exists at the project root and specifies which CloudFormation stack to use
3. Running from the wrong directory will cause the tests to attempt a new stack deployment (and likely fail during SAM build)

If you see errors about SAM CLI failing during test setup, ensure you're running pytest from the project root.

---

## Files in This Directory

| File | Purpose |
|------|---------|
| `conftest.py` | Shared fixtures and data generation utilities with disease clustering |
| `generate_hpo_terms_json.py` | Convert HPO OBO to searchable JSON index |
| `generate_benchmark_dataset.py` | Create reproducible benchmark datasets |
| `test_import_performance.py` | Bulk import throughput test (Manuscript Table 3) |
| `test_evaluation_perf_scale.py` | Comprehensive API latency test (Manuscript Table 4) |
| `data/hpo_terms_v*.json` | HPO term indexes (can be checked in or gitignored) |
| `data/benchmark/` | Generated benchmark datasets (gitignored) |

### Consolidated Test Structure

**Two focused test files** covering both manuscript tables:

1. **test_import_performance.py**: Measures bulk data ingestion throughput
   - Generates synthetic data with optional disease clustering
   - Uploads to S3 and triggers bulk import Step Function
   - Reports: records/sec, evidence/sec, total time
   - Output: Table 3 (ingestion performance)

2. **test_evaluation_perf_scale.py**: Comprehensive API performance testing
   - Uses same project as import test (via session-scoped `test_project_id` fixture or `PHEBEE_EVAL_PROJECT_ID`)
   - Tests 10 realistic query patterns concurrently
   - Reports: p50/p95/p99 latencies per endpoint
   - Output: Table 4 (API latency)

**Running Together:** Use `pytest -v test_import_performance.py test_evaluation_perf_scale.py` to run both in the same session, ensuring they share the same project. Running separately requires setting `PHEBEE_EVAL_PROJECT_ID` to the project created by the import test.

---

## Troubleshooting

### "Could not load prevalence data" warning

**Cause:** The prevalence CSV path is set but the file is missing or malformed.

**Solution:** Either fix the CSV path/format, or unset `PHEBEE_EVAL_PREVALENCE_CSV_PATH` to use fallback heuristics.

### Import test times out

**Cause:** Dataset is too large for the configured timeout.

**Solution:** Increase timeout or reduce dataset size:
```bash
export PHEBEE_EVAL_INGEST_TIMEOUT_S=14400  # 4 hours
# OR
export PHEBEE_EVAL_SCALE_SUBJECTS=5000  # Smaller dataset
```

### "No subjects returned after ingest"

**Cause:** Import succeeded but data hasn't propagated yet.

**Solution:** The test includes a 60-second wait for indexing. If still failing, check CloudWatch logs for the import Step Function.

### API latency test fails with 404s

**Cause:** You must run `test_import_performance.py` first to populate data.

**Solution:**
```bash
# Ensure you're in the project root
cd /path/to/phebee

# Run import test first
pytest -v tests/integration/performance/test_import_performance.py

# Then run latency test (uses same project)
pytest -v tests/integration/performance/test_evaluation_perf_scale.py
```

---

## Citation

If you use these performance testing tools or generated benchmarks in published research, please cite:

```
@software{phebee_performance_tools,
  title = {PheBee Performance Testing Framework},
  author = {[Your Name/Team]},
  year = {2026},
  url = {https://github.com/[your-repo]/phebee}
}
```

If using a specific benchmark dataset:
```
@dataset{phebee_benchmark_v1,
  title = {PheBee Performance Benchmark Dataset v1.0},
  author = {[Your Name/Team]},
  year = {2026},
  publisher = {Zenodo},
  doi = {[DOI from Zenodo]}
}
```

---

## Additional Resources

- **HPO Website**: https://hpo.jax.org/
- **HPO Downloads**: https://hpo.jax.org/downloads/ontology
- **Zenodo**: https://zenodo.org/ (for dataset sharing)
- **PheBee Documentation**: [Link to main docs]

---

## Contributing

When adding new clinical patterns or disease clusters, please:

1. Include literature citations justifying the co-occurrence patterns
2. Update the `DISEASE_CLUSTERS` and `SPECIALTY_PHENOTYPES` dictionaries in `conftest.py`
3. Document changes in this README
4. Verify reproducibility with a fixed seed

---

**Last Updated:** 2026-02-19
