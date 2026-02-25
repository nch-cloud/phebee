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
- [Environment Variables](#environment-variables)
- [Benchmark Datasets](#benchmark-datasets)
- [Troubleshooting](#troubleshooting)

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

**Step 4a: Run import performance test**
```bash
# IMPORTANT: Run from project root so .phebee-test-stack file is found
cd /path/to/phebee  # Navigate to project root

# Use -s flag to see all output including artifact locations
pytest -v -s tests/integration/performance/test_import_performance.py
```

This will print detailed output including:
- Dataset statistics (subjects, records, terms, evidence)
- Import progress and throughput metrics
- **Artifact locations**: `/tmp/phebee-eval-artifacts/{run_id}/`
- **Project ID**: Needed for query performance tests

The test creates these artifacts:
- `table3_ingestion.csv` - Import performance metrics
- `import_run.json` - Full run details including project_id
- `subject_id_mapping.json` - Subject UUID mappings

**Step 4b: Run query performance test**
```bash
# Find the project_id from the import test output or JSON file
PROJECT_ID=$(cat /tmp/phebee-eval-artifacts/import-perf-*/import_run.json | jq -r '.project_id')

# Run query performance test against the imported data
export PHEBEE_EVAL_PROJECT_ID=$PROJECT_ID
pytest -v -s tests/integration/performance/test_evaluation_perf_scale.py
```

This will print:
- Per-endpoint latency metrics (p50, p95, p99, max)
- Query pattern descriptions
- Performance summary

**Alternative: Run both tests in one session**
```bash
# Both tests will share the same project automatically
pytest -v -s tests/integration/performance/test_import_performance.py \
          tests/integration/performance/test_evaluation_perf_scale.py
```

### Query Patterns Tested

The API latency test executes 8 comprehensive query patterns representing realistic clinical and research use cases:

| # | Pattern | Description | Use Case |
|---|---------|-------------|----------|
| 1 | **basic_subjects_query** | Simple project query with limit | Most common access pattern |
| 2 | **individual_subject** | Single subject detail lookup | Patient detail views |
| 3 | **hierarchy_expansion** | Descendant term expansion with ontology traversal | Research: "all cardiovascular conditions" |
| 4 | **qualified_filtering** | Exclude negated/family/hypothetical | Clinical: confirmed findings only |
| 5 | **specific_phenotype** | Direct term query without hierarchy | Research: exact term matching |
| 6 | **paginated_large_cohort** | Large result set with pagination | Broad cohort queries with cursor |
| 7 | **subject_term_info** | Detailed subject-term evidence | Curator: evidence review |
| 8 | **version_specific_query** | Ontology version filtering | Research: specific HPO version |

**Step 5: Run additional evaluations (optional)**

To test different load parameters without reimporting data:

```bash
# Use the project_id from Step 4b
export PHEBEE_EVAL_PROJECT_ID=test_project_abc12345  # Use actual ID from import test

# Test with different concurrency (run from project root)
export PHEBEE_EVAL_CONCURRENCY=50
pytest -v -s tests/integration/performance/test_evaluation_perf_scale.py
```

**Optional:** Generate static benchmark dataset for manuscript reproducibility:
```bash
python tests/integration/performance/generate_benchmark_dataset.py  # Creates data/benchmark/{n_subjects}-subjects-seed{seed}/
```

Each dataset is isolated in its own subdirectory based on parameters (subjects, seed). Non-default configurations (e.g., `--no-disease-clustering`) add a suffix.

**Using Pre-Generated Datasets:** To use a previously generated benchmark dataset instead of generating fresh data, set `PHEBEE_EVAL_BENCHMARK_DIR`:
```bash
export PHEBEE_EVAL_BENCHMARK_DIR="tests/integration/performance/data/benchmark/10000-subjects-seed42"

# Run from project root (use -s to see output)
cd /path/to/phebee
pytest -v -s tests/integration/performance/test_import_performance.py \
          tests/integration/performance/test_evaluation_perf_scale.py
```

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
| `PHEBEE_EVAL_PROJECT_ID` | None | Use existing project instead of creating new one. Required when running tests separately. |
| `PHEBEE_EVAL_INGEST_TIMEOUT_S` | 21,600 | Timeout in seconds for bulk import Step Function (6 hours) |
| `PHEBEE_EVAL_LATENCY_N` | 500 | Number of requests per API endpoint pattern |
| `PHEBEE_EVAL_CONCURRENCY` | 25 | Number of concurrent workers for load testing |
| `PHEBEE_EVAL_STRICT_LATENCY` | 0 | Enforce p95 ≤ 5000ms performance gates (1=enabled, 0=disabled) |
| `PHEBEE_EVAL_WRITE_ARTIFACTS` | 1 | Write CSV/JSON artifacts to /tmp/phebee-eval-artifacts/ |
| `PHEBEE_EVAL_METRICS_PATH` | None | Local file path to write performance metrics JSON |
| `PHEBEE_EVAL_METRICS_S3_URI` | None | S3 URI to upload performance metrics JSON (e.g., `s3://bucket/prefix/`) |

### Optional - Data Generation

| Variable | Default | Description |
|----------|---------|-------------|
| `PHEBEE_EVAL_BENCHMARK_DIR` | None | Path to pre-generated benchmark dataset directory (if set, loads from disk instead of generating) |
| `PHEBEE_EVAL_PREVALENCE_CSV_PATH` | None | Term frequency CSV for realistic distributions |
| `PHEBEE_EVAL_SEED` | 42 | Random seed for reproducibility |
| `PHEBEE_EVAL_SCALE_SUBJECTS` | 10,000 | Number of subjects |
| `PHEBEE_EVAL_SCALE_MIN_TERMS` | 150 | Min HPO terms per subject (calibrated to production p75) |
| `PHEBEE_EVAL_SCALE_MAX_TERMS` | 500 | Max HPO terms per subject (calibrated to production p90) |
| `PHEBEE_EVAL_SCALE_MIN_EVIDENCE` | 1 | Min evidence items per term (matches production minimum) |
| `PHEBEE_EVAL_SCALE_MAX_EVIDENCE` | 50 | Max evidence items per term (calibrated to production p95) |
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

---

## Benchmark Datasets

### Generating Shareable Datasets for Manuscripts

For manuscript reproducibility, generate a benchmark dataset with a fixed seed:

```bash
export PHEBEE_EVAL_SCALE_SUBJECTS=1000
export PHEBEE_EVAL_TERMS_JSON_PATH="tests/integration/performance/data/hpo_terms_v2026-01-08.json"
export PHEBEE_EVAL_PREVALENCE_CSV_PATH="tests/integration/performance/data/term_frequencies.csv"
export PHEBEE_EVAL_SEED=42

python tests/integration/performance/generate_benchmark_dataset.py
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
export PHEBEE_EVAL_BENCHMARK_DIR="tests/data/benchmark/10000-subjects-seed42"

# Run tests using the static benchmark dataset (from project root, use -s to see output)
pytest -v -s tests/integration/performance/test_import_performance.py \
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

---

## Troubleshooting

### "Could not load prevalence data" warning

**Cause:** The prevalence CSV path is set but the file is missing or malformed.

**Solution:** Either fix the CSV path/format, or unset `PHEBEE_EVAL_PREVALENCE_CSV_PATH` to use fallback heuristics.

### Import test times out

**Cause:** Dataset is too large for the configured timeout (default: 6 hours).

**Solution:** Increase timeout or reduce dataset size:
```bash
export PHEBEE_EVAL_INGEST_TIMEOUT_S=28800  # 8 hours for very large imports
# OR
export PHEBEE_EVAL_SCALE_SUBJECTS=5000  # Smaller dataset
```

### API latency test fails with 404s

**Cause:** You must run `test_import_performance.py` first to populate data.

**Solution:**
```bash
# Ensure you're in the project root
cd /path/to/phebee

# Run import test first (use -s to see output)
pytest -v -s tests/integration/performance/test_import_performance.py

# Get the project_id from artifacts
export PHEBEE_EVAL_PROJECT_ID=$(cat /tmp/phebee-eval-artifacts/import-perf-*/import_run.json | jq -r '.project_id')

# Then run latency test (use -s to see output)
pytest -v -s tests/integration/performance/test_evaluation_perf_scale.py
```

### Can't find test output or artifacts

**Cause:** Pytest captured the output because `-s` flag was not used.

**Solution:** Always use the `-s` flag to see detailed output:
```bash
pytest -v -s tests/integration/performance/test_import_performance.py
```

The artifacts are still written to `/tmp/phebee-eval-artifacts/` even without `-s`, you just won't see the console output showing where they are.

---

**Last Updated:** 2026-02-25
