#!/usr/bin/env python3
"""
Generate static benchmark dataset for PheBee performance evaluation.

This script creates a reproducible synthetic dataset for manuscript performance testing.
The generated dataset can be uploaded to Zenodo or similar repositories for data sharing.

Usage:
    python generate_benchmark_dataset.py --output-dir ./benchmark_data

Environment variables (or use defaults):
    - PHEBEE_EVAL_TERMS_JSON_PATH (required): Path to HPO terms JSON
    - PHEBEE_EVAL_PREVALENCE_CSV_PATH (optional): Path to term frequency CSV
    - PHEBEE_EVAL_SEED (optional): Random seed for reproducibility (default: 42)
    - PHEBEE_EVAL_SCALE_SUBJECTS (default: 10000)
    - PHEBEE_EVAL_SCALE_MIN_TERMS (default: 5)
    - PHEBEE_EVAL_SCALE_MAX_TERMS (default: 50)
    - PHEBEE_EVAL_SCALE_MIN_EVIDENCE (default: 1)
    - PHEBEE_EVAL_SCALE_MAX_EVIDENCE (default: 25)

Output structure:
    {output_dir}/
        metadata.json           # Dataset generation parameters and statistics
        batches/
            batch-00000.json    # NDJSON batch files
            batch-00001.json
            ...
        README.md               # Human-readable description
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

# Add parent directory to path to import conftest utilities
sys.path.insert(0, str(Path(__file__).parent))

from conftest import (
    GeneratedDataset,
    TermUniverse,
    _env_int,
    _env_str,
    generate_scale_dataset,
    load_terms_json,
)


def _split_batches(records: List[Dict[str, Any]], batch_size: int) -> List[List[Dict[str, Any]]]:
    """Split records into batches."""
    return [records[i : i + batch_size] for i in range(0, len(records), batch_size)]


def _write_ndjson(path: Path, records: List[Dict[str, Any]]) -> None:
    """Write records as newline-delimited JSON."""
    with path.open("w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")


def _write_metadata(
    path: Path,
    dataset: GeneratedDataset,
    generation_params: Dict[str, Any],
    universe: TermUniverse,
) -> None:
    """Write dataset metadata."""
    metadata = {
        "dataset_version": "1.0",
        "generation_timestamp": datetime.utcnow().isoformat() + "Z",
        "generator": "PheBee performance benchmark dataset generator",
        "generation_parameters": generation_params,
        "dataset_statistics": dataset.stats,
        "term_universe": {
            "total_terms": len(universe.all_terms),
            "common_terms_count": len(universe.common_terms),
            "rare_terms_count": len(universe.rare_terms),
            "prevalence_source": universe.metadata.get("prevalence_source"),
        },
        "anchor_terms": dataset.anchor_terms,
        "parent_term_for_queries": dataset.parent_term,
        "format": "NDJSON (newline-delimited JSON)",
        "schema": {
            "project_id": "string",
            "project_subject_id": "string",
            "term_iri": "string (HPO IRI)",
            "evidence": "array of evidence items",
            "qualifiers": "array of qualifier strings",
            "term_source": "object with source, version, iri",
            "row_num": "integer (global row number)",
            "batch_id": "integer (batch identifier)",
        },
    }

    with path.open("w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)
        f.write("\n")


def _write_readme(
    path: Path,
    dataset: GeneratedDataset,
    generation_params: Dict[str, Any],
    universe: TermUniverse,
) -> None:
    """Write human-readable README."""
    stats = dataset.stats

    # Extract HPO version from metadata
    hpo_version = "unknown"
    if universe.metadata:
        # Try obo_header.data-version first
        obo_header = universe.metadata.get("obo_header", {})
        if isinstance(obo_header, dict):
            hpo_version = obo_header.get("data-version", hpo_version)
        # Fallback to other possible locations
        if hpo_version == "unknown":
            hpo_version = universe.metadata.get("hpo_data_version",
                         universe.metadata.get("data_version", "unknown"))

    readme = f"""# PheBee Performance Benchmark Dataset

## Overview

This dataset was generated for performance evaluation of the PheBee phenotype-to-cohort query system.
It contains synthetic clinical phenotype data with realistic term frequency distributions derived from
actual clinical data.

## Dataset Statistics

- **Subjects**: {stats['n_subjects']:,}
- **Term Links (records)**: {stats['n_records']:,}
- **Evidence Items**: {stats['n_evidence']:,}
- **Unique HPO Terms**: {stats['n_unique_terms']:,}
- **Terms per Subject**: {stats['terms_per_subject']['min']}-{stats['terms_per_subject']['max']} (mean: {stats['terms_per_subject']['mean']:.1f})
- **Evidence per Record**: {stats['evidence_per_record']['min']}-{stats['evidence_per_record']['max']} (mean: {stats['evidence_per_record']['mean']:.1f})

## Generation Parameters

- **Random Seed**: {generation_params['seed']} (reproducible)
- **Project ID**: {generation_params['project_id']}
- **Subjects**: {generation_params['n_subjects']:,}
- **Terms per Subject**: {generation_params['min_terms']}-{generation_params['max_terms']}
- **Evidence per Term Link**: {generation_params['min_evidence']}-{generation_params['max_evidence']}
- **Batch Size**: {generation_params['batch_size']:,} records per file
- **HPO Version**: {hpo_version}

## Qualifier Distribution

- **Unqualified**: {stats['qualifier_distribution_records']['unqualified']:,} ({stats['qualifier_distribution_records']['unqualified']/stats['n_records']*100:.1f}%)
- **Negated**: {stats['qualifier_distribution_records']['negated']:,} ({stats['qualifier_distribution_records']['negated']/stats['n_records']*100:.1f}%)
- **Family History**: {stats['qualifier_distribution_records']['family']:,} ({stats['qualifier_distribution_records']['family']/stats['n_records']*100:.1f}%)
- **Hypothetical**: {stats['qualifier_distribution_records']['hypothetical']:,} ({stats['qualifier_distribution_records']['hypothetical']/stats['n_records']*100:.1f}%)

## Term Source

- **Ontology**: {stats['term_source']['source']}
- **Version**: {stats['term_source']['version']}
- **IRI**: {stats['term_source']['iri']}

## File Format

Data is stored as newline-delimited JSON (NDJSON) in numbered batch files:
- `batches/batch-00000.json`
- `batches/batch-00001.json`
- etc.

Each line in a batch file is a complete JSON object representing one subject-term link with associated evidence.

## Schema

Each record contains:
- `project_id`: PheBee project identifier
- `project_subject_id`: Subject identifier within the project
- `term_iri`: HPO term IRI (e.g., "http://purl.obolibrary.org/obo/HP_0001945")
- `evidence`: Array of evidence items (clinical notes with provenance)
- `qualifiers`: Array of qualifier strings (e.g., ["negated"], ["family_history"])
- `term_source`: Ontology source metadata
- `row_num`: Global row number across all batches
- `batch_id`: Batch file identifier

## Generation Timestamp

{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC
"""

    with path.open("w", encoding="utf-8") as f:
        f.write(readme)


def main():
    parser = argparse.ArgumentParser(
        description="Generate PheBee performance benchmark dataset",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./benchmark_data"),
        help="Output directory for generated dataset (default: ./benchmark_data)",
    )
    parser.add_argument(
        "--project-id",
        type=str,
        default="phebee-benchmark-v1",
        help="Project ID for the dataset (default: phebee-benchmark-v1)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10000,
        help="Records per batch file (default: 10000)",
    )

    args = parser.parse_args()

    # Check for required environment variables
    terms_path = os.environ.get("PHEBEE_EVAL_TERMS_JSON_PATH")
    if not terms_path:
        print("ERROR: PHEBEE_EVAL_TERMS_JSON_PATH environment variable is required", file=sys.stderr)
        print("Set it to the path of your HPO terms JSON file", file=sys.stderr)
        sys.exit(1)

    # Load configuration
    prevalence_csv_path = os.environ.get("PHEBEE_EVAL_PREVALENCE_CSV_PATH")
    seed = _env_int("PHEBEE_EVAL_SEED", 42)  # Default to 42 for reproducibility
    n_subjects = _env_int("PHEBEE_EVAL_SCALE_SUBJECTS", 10_000)
    min_terms = _env_int("PHEBEE_EVAL_SCALE_MIN_TERMS", 5)
    max_terms = _env_int("PHEBEE_EVAL_SCALE_MAX_TERMS", 50)
    min_evidence = _env_int("PHEBEE_EVAL_SCALE_MIN_EVIDENCE", 1)
    max_evidence = _env_int("PHEBEE_EVAL_SCALE_MAX_EVIDENCE", 25)

    generation_params = {
        "seed": seed,
        "project_id": args.project_id,
        "n_subjects": n_subjects,
        "min_terms": min_terms,
        "max_terms": max_terms,
        "min_evidence": min_evidence,
        "max_evidence": max_evidence,
        "batch_size": args.batch_size,
        "terms_json_path": terms_path,
        "prevalence_csv_path": prevalence_csv_path,
    }

    print("=" * 80)
    print("PheBee Performance Benchmark Dataset Generator")
    print("=" * 80)
    print(f"\nConfiguration:")
    print(f"  Output directory: {args.output_dir}")
    print(f"  Project ID: {args.project_id}")
    print(f"  Random seed: {seed}")
    print(f"  Subjects: {n_subjects:,}")
    print(f"  Terms per subject: {min_terms}-{max_terms}")
    print(f"  Evidence per term link: {min_evidence}-{max_evidence}")
    print(f"  Batch size: {args.batch_size:,} records")
    print(f"  Terms JSON: {terms_path}")
    if prevalence_csv_path:
        print(f"  Prevalence CSV: {prevalence_csv_path}")
    print()

    # Load term universe
    print("Loading term universe...")
    universe = load_terms_json(terms_path, prevalence_csv_path=prevalence_csv_path)
    print(f"  Loaded {len(universe.all_terms):,} total terms")
    print(f"  Common terms pool: {len(universe.common_terms):,}")
    print(f"  Rare terms pool: {len(universe.rare_terms):,}")
    print()

    # Generate dataset
    print("Generating synthetic dataset...")
    dataset = generate_scale_dataset(
        project_id=args.project_id,
        universe=universe,
        n_subjects=n_subjects,
        min_terms=min_terms,
        max_terms=max_terms,
        min_evidence=min_evidence,
        max_evidence=max_evidence,
        rng_seed=seed,
    )

    stats = dataset.stats
    print(f"  Generated {stats['n_records']:,} records")
    print(f"  Total evidence items: {stats['n_evidence']:,}")
    print(f"  Unique terms used: {stats['n_unique_terms']:,}")
    print()

    # Create output directory structure
    args.output_dir.mkdir(parents=True, exist_ok=True)
    batches_dir = args.output_dir / "batches"
    batches_dir.mkdir(exist_ok=True)

    # Split into batches and write
    print(f"Writing batches (size={args.batch_size:,})...")
    batches = _split_batches(dataset.records, args.batch_size)
    for i, batch in enumerate(batches):
        # Update batch_id in each record
        for record in batch:
            record["batch_id"] = i

        batch_file = batches_dir / f"batch-{i:05d}.json"
        _write_ndjson(batch_file, batch)
        print(f"  Wrote {len(batch):,} records to {batch_file.name}")

    print()

    # Write metadata
    print("Writing metadata...")
    metadata_path = args.output_dir / "metadata.json"
    _write_metadata(metadata_path, dataset, generation_params, universe)
    print(f"  Wrote {metadata_path}")

    # Write README
    print("Writing README...")
    readme_path = args.output_dir / "README.md"
    _write_readme(readme_path, dataset, generation_params, universe)
    print(f"  Wrote {readme_path}")

    print()
    print("=" * 80)
    print("Dataset generation complete!")
    print("=" * 80)
    print(f"\nOutput location: {args.output_dir.absolute()}")
    print(f"Total size: {len(batches)} batch files")
    print(f"\nTo upload to Zenodo:")
    print(f"  1. Create a new Zenodo deposit")
    print(f"  2. Upload all files from {args.output_dir}")
    print(f"  3. Add keywords: PheBee, phenotype, performance, benchmark, HPO")
    print(f"  4. Publish and obtain DOI")
    print()


if __name__ == "__main__":
    main()
