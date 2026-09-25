# PheBee Performance Testing

This directory contains tools and tests for evaluating PheBee's performance at scale, including bulk data ingestion throughput and API query latency under realistic clinical data patterns.

> **IMPORTANT**: All performance tests MUST be run from the project root directory (`/path/to/phebee`), not from `tests/integration/performance/`. The test infrastructure requires the `.phebee-test-stack` configuration file located in the project root.

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

Our synthetic data generation incorporates clinically plausible patterns:

#### 1. Disease Clustering (60% of subjects)

Subjects are assigned phenotypes that co-occur in real clinical practice, rather than random sampling. This reflects how patients typically present with related symptoms from a common underlying condition.

**Disease Clusters:**
- **Cardiomyopathy**: Heart failure, dilated cardiomyopathy, arrhythmias
- **Epilepsy**: Seizures, encephalopathy, developmental issues
- **Metabolic**: Diabetes, obesity, hypertriglyceridemia
- **Oncology**: Neoplasm with associated complications
- **Rare Dysmorphic**: Multiple congenital anomalies

A clustered subject receives 3–5 terms from one cluster. Independently, every subject (clustered or not) includes one of two anchor terms, HP:0001627 (Abnormal heart morphology) or HP:0000707 (Abnormality of the nervous system), with probability 0.60 (`PHEBEE_EVAL_ANCHOR_TERM_PCT`). The remaining terms are drawn as described below. A subject never carries the same term twice.

#### 2. Term Frequency Weighting

Remaining terms are drawn from two pools: 70% of draws from the common pool, 30% from the rare pool (`PHEBEE_EVAL_COMMON_TERM_PCT`).

- **With a prevalence CSV** (`PHEBEE_EVAL_PREVALENCE_CSV_PATH`): the common pool is the 2,000 terms with the highest prevalence, and the rare pool is the 5,000 terms with the lowest non-zero prevalence. Terms absent from the CSV, or between the two pools, are never drawn (apart from cluster and anchor terms), so a dataset uses at most 7,000 distinct pool terms.
- **Without a CSV**: the common pool is the first 2,000 internal (non-leaf) terms and the rare pool the first 5,000 leaf terms of the terms index.

Pool membership is set by prevalence rank; within a pool, terms are drawn uniformly.

#### 3. Qualifier Distributions

Realistic context qualifiers based on clinical documentation patterns:
- **Negated** (15%): Explicitly ruled-out findings
- **Family History** (8%): Conditions observed in relatives
- **Hypothetical** (5%): Suspected or rule-out diagnoses
- **Unqualified** (72%): Present/observed findings

Each TermLink carries at most one qualifier, applied to all of its evidence items.

#### 4. Evidence Importance Weighting

Each TermLink draws a base evidence count uniformly from `[PHEBEE_EVAL_SCALE_MIN_EVIDENCE, PHEBEE_EVAL_SCALE_MAX_EVIDENCE]` (default 1–50). The count is then scaled by an importance multiplier: an integer drawn from the importance tier's range, divided by the mean of the configured bounds (25.5 by default). The result is floored, with a minimum of 1.
- **Chief complaints** (tier range 5–12): the first three terms of each subject
- **Active problems** (tier range 2–6): all other terms

The generator also defines past-history (1–3) and incidental (1–2) tiers, but it does not currently assign them. With the defaults, the realized maximum is 23 evidence items per TermLink (⌊50 × 12 / 25.5⌋), below the configured maximum of 50.

#### 5. Specialty Attribution

With disease clustering enabled, the evidence creator encodes a specialty chosen from the term, e.g. `evidence_creator_id: ods/phebee-cardiology:v1`:
- Cardiac phenotypes → Cardiology
- Neurological phenotypes → Neurology
- Metabolic phenotypes → Endocrinology
- Neoplasm → Oncology
- Rare dysmorphic features → Genetics
- All other terms → Internal Medicine

The evidence item's `author_specialty` field is `pediatrics` for every item.

---

## Quick Start

**Step 1: Choose the HPO terms index**

To reproduce the manuscript datasets, use the committed index for HPO v2026-01-08, `tests/integration/performance/data/hpo_terms_v2026-01-08.json`, and skip to Step 3.

To build an index from the current HPO release instead (this produces different datasets):
```bash
cd tests/integration/performance
curl -L http://purl.obolibrary.org/obo/hp.obo -o data/hp.obo
```

**Step 2: Generate HPO terms index** (only if you downloaded `hp.obo` above)
```bash
python generate_hpo_terms_json.py --obo data/hp.obo --out data/hpo_terms.json
```

**Step 3: Configure environment** (run from the project root)
```bash
# Required
export PHEBEE_EVAL_SCALE=1
export PHEBEE_EVAL_TERMS_JSON_PATH="tests/integration/performance/data/hpo_terms_v2026-01-08.json"

# Set explicitly: without it, data generated inside the test run is seeded from the clock
export PHEBEE_EVAL_SEED=42

# Required to reproduce the manuscript datasets (defines the common/rare term pools)
export PHEBEE_EVAL_PREVALENCE_CSV_PATH="tests/integration/performance/data/term_frequencies.csv"

# Manuscript parameters (Table 3, Figure 2, Supplementary Table S1)
export PHEBEE_EVAL_SCALE_SUBJECTS=10000      # Dataset size (default: 10000)
export PHEBEE_EVAL_CONCURRENCY=25            # Concurrent workers (default: 25)
```

Alternatively, point `PHEBEE_EVAL_BENCHMARK_DIR` at a downloaded benchmark dataset (see [Benchmark Datasets](#benchmark-datasets)); the terms index is still required.

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

The API latency test executes 7 comprehensive query patterns representing realistic clinical and research use cases:

| # | Pattern | Description | Use Case |
|---|---------|-------------|----------|
| 1 | **basic_subjects_query** | Unfiltered project query, `limit` 10 | Most common access pattern |
| 2 | **individual_subject** | Single subject detail lookup, rotating over the first (up to 100) subjects returned for the project | Patient detail views |
| 3 | **hierarchy_expansion** | HP:0001626 with `include_child_terms=true`, `limit` 20; descendants come from the Iceberg ontology hierarchy table, cached in DynamoDB | Research: "all cardiovascular conditions" |
| 4 | **qualified_filtering** | Random dataset term with `include_qualified=false`, `limit` 15 | Clinical: confirmed findings only |
| 5 | **specific_phenotype** | Random dataset term, `limit` 25; the server default `include_child_terms=true` also matches descendants | Research: single-phenotype cohort |
| 6 | **paginated_large_cohort** | Unfiltered project query, `limit` 50; only the first page is requested | Broad cohort queries |
| 7 | **subject_term_info** | Detailed subject-term evidence | Curator: evidence review |

Before timing, the test makes one warm-up call to each of the first three patterns. Random terms for patterns 4, 5 and 7 are drawn from the distinct terms in the dataset's records; when a `PHEBEE_EVAL_BENCHMARK_DIR` dataset has more than 500,000 records it is loaded lazily and the terms come from the first 10,000 records of its first batch file. This sampling is not seeded.

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
python tests/integration/performance/generate_benchmark_dataset.py  # Creates tests/data/benchmark/{n_subjects}-subjects-seed{seed}/ (or --output-dir)
```

Each dataset is isolated in its own subdirectory based on parameters (subjects, seed). Non-default configurations (e.g., `--no-disease-clustering`) add a suffix.

**Using Pre-Generated Datasets:** To use a previously generated benchmark dataset instead of generating fresh data, set `PHEBEE_EVAL_BENCHMARK_DIR`:
```bash
export PHEBEE_EVAL_BENCHMARK_DIR="tests/data/benchmark/10000-subjects-seed42"

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
| `PHEBEE_EVAL_TERMS_JSON_PATH` | Path to HPO terms JSON | `tests/integration/performance/data/hpo_terms_v2026-01-08.json` |

### Optional - Test Execution

| Variable | Default | Description |
|----------|---------|-------------|
| `PHEBEE_EVAL_PROJECT_ID` | None | Use existing project instead of creating new one. Required when running tests separately. |
| `PHEBEE_EVAL_INGEST_TIMEOUT_S` | 21,600 | Timeout in seconds for bulk import Step Function (6 hours) |
| `PHEBEE_EVAL_LATENCY_N` | 100 | Number of requests per API endpoint pattern (100 provides stable p50/p95 estimates) |
| `PHEBEE_EVAL_CONCURRENCY` | 25 | Number of concurrent workers for load testing |
| `PHEBEE_EVAL_STRICT_LATENCY` | 0 | Enforce p95 ≤ 5000ms performance gates (1=enabled, 0=disabled) |
| `PHEBEE_EVAL_WRITE_ARTIFACTS` | 1 | Write CSV/JSON artifacts to /tmp/phebee-eval-artifacts/ |
| `PHEBEE_EVAL_METRICS_PATH` | None | Local file path to write performance metrics JSON |
| `PHEBEE_EVAL_METRICS_S3_URI` | None | S3 URI to upload performance metrics JSON (e.g., `s3://bucket/prefix/`) |

### Optional - Data Generation

| Variable | Default | Description |
|----------|---------|-------------|
| `PHEBEE_EVAL_BENCHMARK_DIR` | None | Path to pre-generated benchmark dataset directory (if set, loads from disk instead of generating) |
| `PHEBEE_EVAL_PREVALENCE_CSV_PATH` | None | Term prevalence CSV (`term_iri,frequency`) that defines the common/rare term pools; required to reproduce the manuscript datasets |
| `PHEBEE_EVAL_SEED` | 42 in `generate_benchmark_dataset.py`; unset in pytest | Random seed. When unset, data generated inside a pytest run is seeded from the clock and is not reproducible, so set it explicitly |
| `PHEBEE_EVAL_HPO_VERSION` | `unknown` | Version written to each record's `term_source` when the terms index metadata has no `release`/`version` key (the committed index has neither) |
| `PHEBEE_EVAL_HPO_VERSION_IRI` | `http://purl.obolibrary.org/obo/hp/releases/{version}/hp.owl` | IRI written to each record's `term_source` |
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

### Optional - Athena Metrics (`test_athena_metrics.py`)

| Variable | Default | Description |
|----------|---------|-------------|
| `PHEBEE_EVAL_ATHENA_RUN_ID` | newest `import_run.json` | Import run to measure |
| `PHEBEE_EVAL_ATHENA_METRICS_DIR` | `/tmp/phebee-eval-artifacts/<run_id>` | Artifact output directory |
| `PHEBEE_EVAL_ATHENA_EXPECTED_EVIDENCE` | from `PHEBEE_EVAL_BENCHMARK_DIR` metadata | Override the expected evidence count |
| `PHEBEE_EVAL_ATHENA_EXPECTED_RECORDS` | from `PHEBEE_EVAL_BENCHMARK_DIR` metadata | Override the expected TermLink count |

See the `test_athena_metrics.py` module docstring for the measurement procedure.

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

**Output Structure** (default `--output-dir`; the generator holds the whole dataset in memory, about 4.7 GB peak at 10,000 subjects):
```
tests/data/benchmark/1000-subjects-seed42/
├── metadata.json           # Generation parameters and statistics
├── README.md              # Human-readable documentation
└── batches/
    ├── batch-00000.json   # NDJSON batch files
    ├── batch-00001.json
    └── ...
```

### Verifying a Regenerated Dataset

Generation is deterministic given the seed, terms index and prevalence CSV. `metadata.json` and `README.md` contain generation timestamps, so compare the batch files. With the committed index and CSV and seed 42:

| Subjects | TermLinks (records) | Evidence items | MD5 of concatenated batches |
|---|---|---|---|
| 1,000 | 329,240 | 1,214,327 | `83559f78a791d711924bff3fb61358b6` |
| 5,000 | 1,624,945 | 5,994,039 | `ad7883bcc506d2f8de7fce844044092e` |
| 10,000 | 3,251,666 | 11,993,639 | `b3bd441d72b04c4c3d767d24b80f538d` |
| 50,000 | 16,232,032 | 59,878,126 | |
| 100,000 | 32,516,163 | 119,945,659 | |

```bash
cd tests/data/benchmark/1000-subjects-seed42/batches && cat $(ls batch-*.json | sort) | md5sum
```

The checksums are those of the batch files in the Zenodo deposit below. Omitting `PHEBEE_EVAL_PREVALENCE_CSV_PATH`, or building the index from a different HPO release, produces a different dataset.

### Using Pre-Generated Benchmarks

The benchmark datasets used in the manuscript are deposited at [doi:10.5281/zenodo.19698733](https://doi.org/10.5281/zenodo.19698733), one archive per scale (`phebee-benchmark-<n>-subjects.tar.gz` for n = 1000, 5000, 10000, 50000, 100000), each extracting to `<n>-subjects-seed42/`:

```bash
# Download from Zenodo and extract (from the project root)
curl -L -o phebee-benchmark-10000-subjects.tar.gz \
  "https://zenodo.org/records/19698733/files/phebee-benchmark-10000-subjects.tar.gz?download=1"
mkdir -p tests/data/benchmark
tar -xzf phebee-benchmark-10000-subjects.tar.gz -C tests/data/benchmark/

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
| `test_evaluation_perf_scale.py` | Comprehensive API latency test (Manuscript Figure 2, Supplementary Table S1 client-side values) |
| `test_athena_metrics.py` | Athena time, bytes scanned and cost over the evidence table (Supplementary Table S2) |
| `data/hpo_terms_v2026-01-08.json` | HPO v2026-01-08 term index used for the manuscript datasets |
| `data/term_frequencies.csv` | Term prevalence (`term_iri,frequency`) used to define the common/rare term pools |
| `tests/data/benchmark/` (repo root) | Generated or downloaded benchmark datasets (gitignored) |

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

**Last Updated:** 2026-09-25
