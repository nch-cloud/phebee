#!/usr/bin/env python3
"""
Quick test to verify benchmark dataset loading and subject_id mapping.
Run this to verify the fix works without running the full performance test.

Usage:
    PHEBEE_EVAL_BENCHMARK_DIR="tests/integration/performance/data/benchmark/1000-subjects-seed42" \
    python tests/integration/performance/test_benchmark_loading.py
"""

import os
import sys
from pathlib import Path

# Add parent directory to path to import conftest
sys.path.insert(0, str(Path(__file__).parent))

from conftest import _load_benchmark_dataset

def main():
    benchmark_dir = os.environ.get("PHEBEE_EVAL_BENCHMARK_DIR")
    if not benchmark_dir:
        print("ERROR: PHEBEE_EVAL_BENCHMARK_DIR environment variable is required")
        print("Example: PHEBEE_EVAL_BENCHMARK_DIR='tests/integration/performance/data/benchmark/1000-subjects-seed42'")
        sys.exit(1)

    benchmark_path = Path(benchmark_dir)
    test_project_id = "test-project-quick-verify"

    print(f"Loading benchmark from: {benchmark_path}")
    print(f"Test project ID: {test_project_id}")
    print()

    # Load the benchmark dataset
    dataset = _load_benchmark_dataset(benchmark_path, test_project_id)

    print(f"\n{'='*80}")
    print("Results")
    print(f"{'='*80}")
    print(f"Records loaded: {len(dataset.records):,}")
    print(f"Subject ID mappings created: {len(dataset.subject_id_map):,}")
    print()

    if dataset.subject_id_map:
        print("✓ SUCCESS: Subject ID mapping is working!")
        print()
        print("Sample mappings (first 5):")
        for i, (old_id, new_id) in enumerate(list(dataset.subject_id_map.items())[:5]):
            print(f"  {old_id} → {new_id}")
        print()

        # Verify records have the new subject_id
        sample_record = dataset.records[0]
        print("Sample record fields:")
        print(f"  project_id: {sample_record.get('project_id')}")
        print(f"  subject_id: {sample_record.get('subject_id')}")
        print(f"  project_subject_id: {sample_record.get('project_subject_id', 'N/A')}")
        print()

        if sample_record.get('subject_id') in dataset.subject_id_map.values():
            print("✓ Record has new UUID subject_id from mapping")
        else:
            print("✗ WARNING: Record subject_id doesn't match any mapping value")
    else:
        print("✗ FAILED: No subject ID mappings were created!")
        print("This means the fallback logic (project_subject_id) is not working.")
        print()
        print("Sample record fields:")
        sample_record = dataset.records[0]
        print(f"  project_id: {sample_record.get('project_id')}")
        print(f"  subject_id: {sample_record.get('subject_id', 'MISSING')}")
        print(f"  project_subject_id: {sample_record.get('project_subject_id', 'MISSING')}")

    print()

if __name__ == "__main__":
    main()
