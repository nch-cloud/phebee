# PheBee Performance Testing Suite

This directory contains performance evaluation tools for the PheBee phenotype-to-cohort query system, designed to generate manuscript-quality performance metrics (Tables 3 & 4).

## Overview

The performance test suite evaluates PheBee across three dimensions:
1. **Import Performance (Table 3)**: Bulk data ingestion throughput and scalability
2. **API Latency (Table 4)**: Query response times under load (p50/p95/p99 latencies)
3. **Cache Performance**: DynamoDB cache vs Neptune query performance at scale

## File Structure

```
tests/integration/performance/
├── README.md                                  # This file
├── conftest.py                                # Shared fixtures and data generation utilities
├── test_import_performance.py                 # Table 3: Bulk ingestion performance
├── test_api_latency.py                        # Table 4: API query latency
├── test_cache_performance_scale.py            # Cache vs Neptune performance at scale
├── generate_benchmark_dataset.py              # Script to create static benchmark dataset
├── create_import_performance_datasets.sh      # Script to generate all dataset scales
├── data/
│   ├── hpo_terms_v2026-01-08.json            # HPO term universe
│   └── term_frequencies.csv                   # Clinical term prevalence data
└── test_evaluation_perf_scale_terms_json.py  # (Original combined test - deprecated)
```

## Quick Start

### Prerequisites

1. **Deployed PheBee stack** with required infrastructure:
   - API Gateway endpoint
   - S3 bucket for bulk imports
   - Step Functions for bulk import workflow
   - DynamoDB, Neptune, Glue/Iceberg data warehouse

2. **Python dependencies** (from project root):
   ```bash
   pip install -r requirements.txt
   ```

3. **AWS credentials** configured with access to your PheBee stack

### Required Environment Variables

```bash
# Required for all tests
export PHEBEE_EVAL_SCALE=1                                    # Enable performance tests
export PHEBEE_EVAL_TERMS_JSON_PATH=tests/integration/performance/data/hpo_terms_v2026-01-08.json

# Optional but recommended for realistic distributions
export PHEBEE_EVAL_PREVALENCE_CSV_PATH=tests/integration/performance/data/term_frequencies.csv

# Optional: Control dataset size (defaults shown)
export PHEBEE_EVAL_SCALE_SUBJECTS=10000
export PHEBEE_EVAL_SCALE_MIN_TERMS=5
export PHEBEE_EVAL_SCALE_MAX_TERMS=50
export PHEBEE_EVAL_SCALE_MIN_EVIDENCE=1
export PHEBEE_EVAL_SCALE_MAX_EVIDENCE=25

# Optional: Reproducibility
export PHEBEE_EVAL_SEED=42

# Optional: Latency test configuration
export PHEBEE_EVAL_LATENCY_N=500       # Requests per endpoint
export PHEBEE_EVAL_CONCURRENCY=25      # Concurrent requests
```

## Running Tests

### Run Both Tests Sequentially (Tables 3 & 4)

For complete manuscript data, run both tests in sequence:

```bash
cd /path/to/phebee

# Table 3: Import performance
pytest tests/integration/performance/test_import_performance.py -v

# Table 4: API latency (uses data from import test)
pytest tests/integration/performance/test_api_latency.py -v
```

### Run All Performance Tests Together

```bash
cd /path/to/phebee
pytest tests/integration/performance/ -v -m perf
```

**Note**: This runs both tests in sequence, but `test_api_latency.py` requires data to be imported first.

### Run Import Performance Test Only (Table 3)

```bash
pytest tests/integration/performance/test_import_performance.py -v
```

### Run API Latency Test Only (Table 4)

**Prerequisite**: Project must have data already imported.

```bash
pytest tests/integration/performance/test_api_latency.py -v
```

### Iterate on Latency Test with Existing Project

To iterate on the latency test without re-running the import each time, use an existing project ID:

```bash
# First, note the project ID from a previous import run
# (printed as "[Performance Tests] Creating shared project: test_project_XXXXXXXX")

# Then run latency test against that project
export PHEBEE_EVAL_PROJECT_ID=test_project_c3608683  # Use your actual project ID
pytest tests/integration/performance/test_api_latency.py -v
```

This is especially useful when tweaking latency test parameters (concurrency, number of requests, etc.) without waiting for the import to complete each time.

### Run with Different Scale

```bash
# Test with 50K subjects
export PHEBEE_EVAL_SCALE_SUBJECTS=50000
pytest tests/integration/performance/test_import_performance.py -v

# Then run latency test on the imported data
pytest tests/integration/performance/test_api_latency.py -v
```

### Run Cache Performance Tests

Tests cache vs Neptune performance at scale (1000+ subjects):

```bash
# Run with default settings (1000 subjects)
pytest tests/integration/performance/test_cache_performance_scale.py -v

# Run with custom subject count
export PHEBEE_PERF_SCALE_N=5000
pytest tests/integration/performance/test_cache_performance_scale.py -v

# Skip performance tests (useful for CI/CD)
export PHEBEE_PERF_SKIP=1
pytest tests/integration/performance/ -v
```

**What it measures:**
- Full project query latency (cache vs Neptune)
- Term-filtered query latency with hierarchy expansion
- Pagination performance at scale
- p50/p95/p99 latencies and speedup factors

**Expected results:**
- Cache should be 5-10x faster than Neptune at 1000+ subjects
- Consistent sub-second latencies even with large datasets
- Efficient pagination with stable per-page latency

**Note:** This test creates its own test project with generated data and cleans up afterwards. Runtime scales with `PHEBEE_PERF_SCALE_N` (default 1000 subjects takes ~5-10 minutes).

## Generating Static Benchmark Dataset

### Reproducible Generation

All datasets are generated **deterministically** using a fixed random seed (default: 42). This means anyone with the same input files and parameters can regenerate the exact same datasets.

### Generate Single Dataset

For manuscript submission or local testing:

```bash
# Set environment variables
export PHEBEE_EVAL_TERMS_JSON_PATH=tests/integration/performance/data/hpo_terms_v2026-01-08.json
export PHEBEE_EVAL_PREVALENCE_CSV_PATH=tests/integration/performance/data/term_frequencies.csv
export PHEBEE_EVAL_SEED=42  # For reproducibility

# Generate dataset
python tests/integration/performance/generate_benchmark_dataset.py \
    --output-dir ./phebee_benchmark_v1 \
    --project-id phebee-benchmark-v1 \
    --batch-size 10000
```

This creates:
- `metadata.json` - Generation parameters and statistics
- `README.md` - Human-readable description
- `batches/*.json` - NDJSON data files ready for bulk import

### Generate Multiple Scales

To generate all four benchmark scales (1K, 10K, 50K, 100K subjects) at once:

```bash
# Using default paths (if data files are in tests/integration/performance/data/)
./tests/integration/performance/create_import_performance_datasets.sh

# Or specify custom output directory
./tests/integration/performance/create_import_performance_datasets.sh ./my_benchmarks

# Or set custom input paths
export PHEBEE_EVAL_TERMS_JSON_PATH=/path/to/hpo_terms.json
export PHEBEE_EVAL_PREVALENCE_CSV_PATH=/path/to/term_frequencies.csv
./tests/integration/performance/create_import_performance_datasets.sh
```

### Data Sharing Strategy

Generated datasets can be **very large** (1K scale: ~213MB, 100K scale: ~20GB). For manuscript data sharing:

**Recommended Approach**: Share generation scripts instead of raw data
- Upload to repository (GitHub/Zenodo):
  - Generation scripts: `conftest.py`, `generate_benchmark_dataset.py`, `create_import_performance_datasets.sh`
  - Input data: `data/hpo_terms_v2026-01-08.json`, `data/term_frequencies.csv`
  - Documentation: `README.md`
- Reviewers can regenerate datasets deterministically using the fixed seed
- Provides verifiable reproducibility
- Much faster downloads (~50MB vs ~32GB for all scales)

**Alternative Approach**: Share raw datasets
- Upload generated datasets directly to Zenodo
- Faster for users who just want to run tests
- Requires more storage and bandwidth
- Less transparent (opaque data files vs. generation code)

## Configuration Options

### Dataset Generation

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `PHEBEE_EVAL_SCALE` | (none) | Must be `1` to enable tests |
| `PHEBEE_EVAL_TERMS_JSON_PATH` | (required) | Path to HPO terms JSON |
| `PHEBEE_EVAL_PREVALENCE_CSV_PATH` | (optional) | Path to term frequency CSV |
| `PHEBEE_EVAL_SEED` | timestamp | Random seed for reproducibility |
| `PHEBEE_EVAL_SCALE_SUBJECTS` | 10000 | Number of subjects to generate |
| `PHEBEE_EVAL_SCALE_MIN_TERMS` | 5 | Min HPO terms per subject |
| `PHEBEE_EVAL_SCALE_MAX_TERMS` | 50 | Max HPO terms per subject |
| `PHEBEE_EVAL_SCALE_MIN_EVIDENCE` | 1 | Min evidence items per term link |
| `PHEBEE_EVAL_SCALE_MAX_EVIDENCE` | 25 | Max evidence items per term link |
| `PHEBEE_EVAL_BATCH_SIZE` | 10000 | Records per S3 batch file |

### Import Performance

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `PHEBEE_EVAL_INGEST_TIMEOUT_S` | 7200 | Max seconds to wait for import (2 hours) |

### API Latency Testing

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `PHEBEE_EVAL_PROJECT_ID` | (none) | Use existing project instead of creating new one |
| `PHEBEE_EVAL_LATENCY_N` | 500 | Requests per endpoint |
| `PHEBEE_EVAL_CONCURRENCY` | 25 | Concurrent request threads |
| `PHEBEE_EVAL_HTTP_TIMEOUT_S` | 30 | HTTP request timeout |

### Qualifier Distribution

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `PHEBEE_EVAL_QUAL_NEGATED_PCT` | 0.15 | % of term links that are negated |
| `PHEBEE_EVAL_QUAL_FAMILY_PCT` | 0.08 | % of term links that are family history |
| `PHEBEE_EVAL_QUAL_HYPOTHETICAL_PCT` | 0.05 | % of term links that are hypothetical |

### Clinical Note Timestamps

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `PHEBEE_EVAL_NOTE_DATE_START` | 2023-01-01 | Start date for synthetic notes |
| `PHEBEE_EVAL_NOTE_DATE_END` | 2024-12-31 | End date for synthetic notes |

## Output Artifacts

### Import Performance (Table 3)

CSV file with columns:
- `run_id` - Unique run identifier
- `project_id` - PheBee project ID
- `n_subjects` - Number of subjects
- `n_records` - Number of term links (records)
- `n_evidence` - Total evidence items
- `n_unique_terms` - Unique HPO terms used
- `terms_per_subject_mean/median/min/max` - Distribution statistics
- `evidence_per_record_mean/median/min/max` - Distribution statistics
- `ingest_seconds` - Total import time
- `records_per_sec` - Throughput (records/second)
- `evidence_per_sec` - Throughput (evidence/second)

### API Latency (Table 4)

CSV file with columns:
- `endpoint` - API endpoint name
- `n` - Number of requests
- `min_ms` - Minimum latency
- `max_ms` - Maximum latency
- `avg_ms` - Average latency
- `p50_ms` - 50th percentile (median)
- `p95_ms` - 95th percentile
- `p99_ms` - 99th percentile

### Full Results JSON

Both tests also output a complete JSON file with:
- Dataset generation statistics
- Import performance metrics
- API latency results
- Configuration parameters
- Query workload details

By default, artifacts are written to `/tmp/phebee-eval-artifacts/{run_id}/`:
- `table3_ingestion.csv`
- `table4_latency.csv`
- `eval_run.json`

Set `PHEBEE_EVAL_WRITE_ARTIFACTS=0` to disable artifact generation.

## Data Generation Strategy

### Term Selection

Each subject receives 5-50 HPO terms:
- **60% chance** to include one anchor term (cardiovascular or neuro)
- **70% of remaining terms** sampled from high-frequency terms (top 2000 by clinical prevalence)
- **30% of remaining terms** sampled from low-frequency terms (bottom 5000 by clinical prevalence)

This distribution reflects realistic clinical data patterns when `PHEBEE_EVAL_PREVALENCE_CSV_PATH` is provided.

### Qualifier Distribution

Each term link receives at most one qualifier:
- **15%** Negated (e.g., "no fever")
- **8%** Family history
- **5%** Hypothetical
- **72%** Unqualified (positive assertion)

### Evidence Generation

Each term link receives 1-25 evidence items representing clinical note annotations:
- Automated NLP creator (with one manual curator example for validation)
- Unique note IDs per evidence item
- Shared encounter ID per subject
- Random timestamps within specified date range
- Realistic text span positions

## Expected Performance

Typical results (10K subjects, ~250K term links, ~3M evidence items):
- **Import**: 300-600 records/sec (varies by instance size)
- **API p95 latency**: 100-500ms for most queries
- **Large cohort queries**: May take 1-2s at p95 for hierarchy expansion

## Troubleshooting

### Tests Skip with "PHEBEE_EVAL_SCALE not set"

Set `export PHEBEE_EVAL_SCALE=1` to enable performance tests.

### "PHEBEE_EVAL_TERMS_JSON_PATH not found"

Ensure the environment variable points to a valid HPO terms JSON file:
```bash
export PHEBEE_EVAL_TERMS_JSON_PATH=tests/integration/performance/data/hpo_terms_v2026-01-08.json
```

### Import Timeout

Increase timeout for larger datasets:
```bash
export PHEBEE_EVAL_INGEST_TIMEOUT_S=14400  # 4 hours
```

### API Latency Test Fails After Import

Some tests require data to exist before running API latency tests. Run import test first, or run against an existing project with sufficient data.

## For Manuscript Authors

### Recommended Test Configuration

For reproducible manuscript results:

```bash
# Fixed seed for reproducibility
export PHEBEE_EVAL_SEED=42

# Moderate scale for reasonable runtime
export PHEBEE_EVAL_SCALE_SUBJECTS=10000

# Use real prevalence data
export PHEBEE_EVAL_PREVALENCE_CSV_PATH=tests/integration/performance/data/term_frequencies.csv

# Sufficient samples for stable latency measurements
export PHEBEE_EVAL_LATENCY_N=500
export PHEBEE_EVAL_CONCURRENCY=25
```

### Methods Section Template

> We evaluated PheBee performance using synthetic phenotype data reflecting real-world clinical frequency distributions. We generated 10,000 subjects with 5-50 HPO terms each (mean: X), where 70% of terms were sampled from high-frequency terms (top 2000 by clinical prevalence) and 30% from low-frequency terms (bottom 5000), based on actual clinical data from [your institution]. Each term link included 1-25 evidence items (mean: Y) representing clinical note annotations. We measured bulk import throughput and API query latency (p50/p95/p99) across representative cohort query workloads with 500 requests per endpoint at 25 concurrent connections.

## Citation

If you use this performance testing framework, please cite:

[Insert manuscript citation when published]

## Contact

[Insert contact information]
