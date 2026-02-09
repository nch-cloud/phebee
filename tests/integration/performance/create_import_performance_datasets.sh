#!/bin/bash
#
# Generate PheBee performance benchmark datasets for manuscript Table 3.
#
# This script creates multiple datasets at different scales (1K, 10K, 50K, 100K subjects)
# with consistent parameters to demonstrate scalability. All datasets use the same:
# - Random seed (42) for reproducibility
# - Term distribution (5-50 terms per subject)
# - Evidence density (1-25 evidence items per term link)
# - Clinical term frequency distribution
#
# Generated datasets can be uploaded to Zenodo for manuscript data sharing.
#
# Usage:
#   ./create_import_performance_datasets.sh [output_base_dir]
#
# Example:
#   ./create_import_performance_datasets.sh ./my_import_benchmarks
#
# Requirements:
#   - PHEBEE_EVAL_TERMS_JSON_PATH must be set (or use default)
#   - PHEBEE_EVAL_PREVALENCE_CSV_PATH must be set (or use default)
#

set -e  # Exit on error
set -u  # Exit on undefined variable

# Default output directory
OUTPUT_BASE_DIR="${1:-./phebee_import_benchmarks}"

# Get the directory containing this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Set default paths relative to script directory if not already set
export PHEBEE_EVAL_TERMS_JSON_PATH="${PHEBEE_EVAL_TERMS_JSON_PATH:-${SCRIPT_DIR}/data/hpo_terms_v2026-01-08.json}"
export PHEBEE_EVAL_PREVALENCE_CSV_PATH="${PHEBEE_EVAL_PREVALENCE_CSV_PATH:-${SCRIPT_DIR}/data/term_frequencies.csv}"

# Fixed parameters for reproducibility and consistency
export PHEBEE_EVAL_SEED=42
export PHEBEE_EVAL_SCALE_MIN_TERMS=5
export PHEBEE_EVAL_SCALE_MAX_TERMS=50
export PHEBEE_EVAL_SCALE_MIN_EVIDENCE=1
export PHEBEE_EVAL_SCALE_MAX_EVIDENCE=25

# Batch size for NDJSON files
BATCH_SIZE=10000

# Validate required files exist
if [ ! -f "$PHEBEE_EVAL_TERMS_JSON_PATH" ]; then
    echo "ERROR: HPO terms JSON not found: $PHEBEE_EVAL_TERMS_JSON_PATH"
    echo "Set PHEBEE_EVAL_TERMS_JSON_PATH or place file at default location"
    exit 1
fi

if [ ! -f "$PHEBEE_EVAL_PREVALENCE_CSV_PATH" ]; then
    echo "WARNING: Prevalence CSV not found: $PHEBEE_EVAL_PREVALENCE_CSV_PATH"
    echo "Will use fallback term classification (less scientifically accurate)"
    echo "Set PHEBEE_EVAL_PREVALENCE_CSV_PATH to use real prevalence data"
    echo ""
fi

# Print configuration
echo "=============================================="
echo "PheBee Performance Benchmark Dataset Generator"
echo "=============================================="
echo ""
echo "Configuration:"
echo "  Output directory: $OUTPUT_BASE_DIR"
echo "  Terms JSON: $PHEBEE_EVAL_TERMS_JSON_PATH"
echo "  Prevalence CSV: $PHEBEE_EVAL_PREVALENCE_CSV_PATH"
echo "  Random seed: $PHEBEE_EVAL_SEED"
echo "  Terms per subject: ${PHEBEE_EVAL_SCALE_MIN_TERMS}-${PHEBEE_EVAL_SCALE_MAX_TERMS}"
echo "  Evidence per term link: ${PHEBEE_EVAL_SCALE_MIN_EVIDENCE}-${PHEBEE_EVAL_SCALE_MAX_EVIDENCE}"
echo "  Batch size: $BATCH_SIZE"
echo ""

# Create output base directory
mkdir -p "$OUTPUT_BASE_DIR"

# Dataset scales to generate
# Format: "subjects:label:description"
SCALES=(
    "1000:1k:Small dataset for quick testing"
    "10000:10k:Medium dataset (default scale)"
    "50000:50k:Large dataset for scalability testing"
    "100000:100k:Extra-large dataset (optional)"
)

# Generate each dataset
for scale_spec in "${SCALES[@]}"; do
    IFS=':' read -r subjects label description <<< "$scale_spec"

    echo "=============================================="
    echo "Generating ${label} dataset (${subjects} subjects)"
    echo "$description"
    echo "=============================================="
    echo ""

    export PHEBEE_EVAL_SCALE_SUBJECTS=$subjects

    output_dir="${OUTPUT_BASE_DIR}/phebee_import_benchmark_${label}"
    project_id="phebee-import-benchmark-${label}"

    # Run the generator
    python "${SCRIPT_DIR}/generate_benchmark_dataset.py" \
        --output-dir "$output_dir" \
        --project-id "$project_id" \
        --batch-size $BATCH_SIZE

    echo ""
    echo "✓ ${label} dataset complete: $output_dir"
    echo ""
done

# Print summary
echo "=============================================="
echo "All datasets generated successfully!"
echo "=============================================="
echo ""
echo "Output location: $OUTPUT_BASE_DIR"
echo ""
echo "Generated datasets:"
for scale_spec in "${SCALES[@]}"; do
    IFS=':' read -r subjects label description <<< "$scale_spec"
    output_dir="${OUTPUT_BASE_DIR}/phebee_import_benchmark_${label}"

    # Calculate approximate size (rough estimate)
    if [ -d "$output_dir" ]; then
        size=$(du -sh "$output_dir" 2>/dev/null | cut -f1 || echo "unknown")
        echo "  - ${label} (${subjects} subjects): $size"
    fi
done

echo ""
echo "Next steps:"
echo "  Run import performance tests at each scale:"
echo "    for subjects in 1000 10000 50000 100000; do"
echo "      export PHEBEE_EVAL_SCALE_SUBJECTS=\$subjects"
echo "      pytest tests/integration/performance/test_import_performance.py -v"
echo "    done"
echo ""
