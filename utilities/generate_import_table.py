#!/usr/bin/env python3
"""
Generate import performance table (Table 3) from PheBee import test results.

Usage:
    python generate_import_table.py 1000/import_run.json 5000/import_run.json 10000/import_run.json
    python generate_import_table.py -o table3.csv -f csv */import_run.json
    python generate_import_table.py -o table3.md -f markdown */import_run.json
"""

import argparse
import json
from pathlib import Path
from typing import List, Dict, Any


def load_import_result(file_path: str) -> Dict[str, Any]:
    """Load import performance results from JSON file."""
    with open(file_path, 'r') as f:
        return json.load(f)


def extract_table_row(result: Dict[str, Any]) -> Dict[str, Any]:
    """Extract key metrics from import result for table row."""
    dataset = result.get('dataset', {})
    perf = result.get('import_performance', {})

    return {
        'n_subjects': dataset.get('n_subjects', 0),
        'n_records': dataset.get('n_records', 0),
        'n_evidence': dataset.get('n_evidence', 0),
        'n_unique_terms': dataset.get('n_unique_terms', 0),
        'terms_per_subject_mean': dataset.get('terms_per_subject', {}).get('mean', 0),
        'evidence_per_record_mean': dataset.get('evidence_per_record', {}).get('mean', 0),
        'ingest_seconds': perf.get('ingest_seconds', 0),
        'records_per_sec': perf.get('records_per_sec', 0),
        'evidence_per_sec': perf.get('evidence_per_sec', 0),
        'batch_size': perf.get('batch_size', 0),
        'n_batches': perf.get('n_batches', 0),
    }


def format_number(value: float, decimals: int = 1) -> str:
    """Format number with commas and specified decimal places."""
    if isinstance(value, int) or value == int(value):
        return f"{int(value):,}"
    return f"{value:,.{decimals}f}"


def generate_csv_table(rows: List[Dict[str, Any]], output_file: str):
    """Generate CSV table."""
    import csv

    fieldnames = [
        'n_subjects',
        'n_records',
        'n_evidence',
        'n_unique_terms',
        'terms_per_subject_mean',
        'evidence_per_record_mean',
        'ingest_seconds',
        'records_per_sec',
        'evidence_per_sec',
        'batch_size',
        'n_batches',
    ]

    with open(output_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, '') for k in fieldnames})

    print(f"CSV table saved to: {output_file}")


def generate_markdown_table(rows: List[Dict[str, Any]], output_file: str):
    """Generate Markdown table."""
    lines = []

    # Header
    lines.append("| Subjects | Records | Evidence | Terms | Terms/Subj | Evid/Rec | Time (s) | Rec/s | Evid/s |")
    lines.append("|----------|---------|----------|-------|------------|----------|----------|-------|--------|")

    # Data rows
    for row in rows:
        lines.append(
            f"| {format_number(row['n_subjects'])} "
            f"| {format_number(row['n_records'])} "
            f"| {format_number(row['n_evidence'])} "
            f"| {format_number(row['n_unique_terms'])} "
            f"| {format_number(row['terms_per_subject_mean'], 1)} "
            f"| {format_number(row['evidence_per_record_mean'], 1)} "
            f"| {format_number(row['ingest_seconds'], 1)} "
            f"| {format_number(row['records_per_sec'], 1)} "
            f"| {format_number(row['evidence_per_sec'], 1)} |"
        )

    output = '\n'.join(lines) + '\n'

    with open(output_file, 'w') as f:
        f.write(output)

    print(f"Markdown table saved to: {output_file}")
    print("\nTable preview:")
    print(output)


def generate_text_table(rows: List[Dict[str, Any]]):
    """Generate plain text table for console output."""
    print("\n" + "="*100)
    print("PheBee Import Performance (Table 3)")
    print("="*100)

    # Header
    print(f"{'Subjects':>10} {'Records':>12} {'Evidence':>12} {'Terms':>8} "
          f"{'T/Subj':>8} {'E/Rec':>8} {'Time(s)':>10} {'Rec/s':>10} {'Evid/s':>10}")
    print("-" * 100)

    # Data rows
    for row in rows:
        print(f"{format_number(row['n_subjects']):>10} "
              f"{format_number(row['n_records']):>12} "
              f"{format_number(row['n_evidence']):>12} "
              f"{format_number(row['n_unique_terms']):>8} "
              f"{format_number(row['terms_per_subject_mean'], 1):>8} "
              f"{format_number(row['evidence_per_record_mean'], 1):>8} "
              f"{format_number(row['ingest_seconds'], 1):>10} "
              f"{format_number(row['records_per_sec'], 1):>10} "
              f"{format_number(row['evidence_per_sec'], 1):>10}")

    print("="*100)
    print("\nColumn Definitions:")
    print("  Subjects: Number of unique subjects")
    print("  Records: Number of phenotype records (subject-term links)")
    print("  Evidence: Total evidence items across all records")
    print("  Terms: Number of unique HPO terms")
    print("  T/Subj: Average terms per subject")
    print("  E/Rec: Average evidence items per record")
    print("  Time(s): Total ingestion time in seconds")
    print("  Rec/s: Records processed per second")
    print("  Evid/s: Evidence items processed per second")
    print()


def main():
    parser = argparse.ArgumentParser(
        description='Generate import performance table from PheBee import test results',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Display table in console
  python generate_import_table.py 1000/import_run.json 5000/import_run.json

  # Generate CSV file
  python generate_import_table.py -o table3.csv -f csv */import_run.json

  # Generate Markdown file
  python generate_import_table.py -o table3.md -f markdown */import_run.json
        """
    )
    parser.add_argument('results', nargs='+', help='JSON result files from import performance tests')
    parser.add_argument('-o', '--output', help='Output file path')
    parser.add_argument('-f', '--format', choices=['csv', 'markdown', 'text'], default='text',
                       help='Output format (default: text for console)')

    args = parser.parse_args()

    # Load all results
    results_list = []
    for result_file in args.results:
        try:
            result = load_import_result(result_file)
            row = extract_table_row(result)
            results_list.append(row)
            n_subjects = row['n_subjects']
            n_records = row['n_records']
            ingest_s = row['ingest_seconds']
            print(f"Loaded: {result_file} (N={n_subjects:,} subjects, {n_records:,} records, {ingest_s:.1f}s)")
        except Exception as e:
            print(f"Warning: Failed to load {result_file}: {e}")

    if not results_list:
        print("Error: No valid result files found")
        return 1

    # Sort by number of subjects
    results_list = sorted(results_list, key=lambda x: x['n_subjects'])

    # Generate output
    if args.format == 'csv':
        if not args.output:
            args.output = 'table3_import_performance.csv'
        generate_csv_table(results_list, args.output)
    elif args.format == 'markdown':
        if not args.output:
            args.output = 'table3_import_performance.md'
        generate_markdown_table(results_list, args.output)
    else:  # text
        generate_text_table(results_list)

    return 0


if __name__ == '__main__':
    exit(main())
