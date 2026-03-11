#!/usr/bin/env python3
"""
Generate comprehensive performance figure matrix from PheBee API performance test results.

Creates a full set of supplementary figures showing:
- Concurrency scaling at each dataset size (1K, 5K, 10K, 50K)
- Dataset scaling at each concurrency level (c=1, c=10, c=25)

Outputs manuscript-compatible formats: PNG (300 DPI), PDF, EPS, and SVG (text-to-paths).

Usage:
    python generate_performance_matrix.py results_dir/ -o output_dir/
    python generate_performance_matrix.py 1000/c*/api_run.json 5000/c*/api_run.json ... -o figs/
"""

import argparse
import json
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
from typing import List, Dict, Any
from collections import defaultdict


def load_results(file_path: str) -> Dict[str, Any]:
    """Load performance results from JSON file."""
    with open(file_path, 'r') as f:
        return json.load(f)


def aggregate_replicates(results_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Aggregate multiple replicates by taking the median value for each endpoint.

    Groups results by (dataset_size, concurrency, endpoint) and computes median
    of all metrics (p50, p95, p99, etc.) across replicates.

    Returns: List of aggregated results (one per unique combination)
    """
    from collections import defaultdict

    # Group by (dataset_size, concurrency, endpoint)
    groups = defaultdict(lambda: defaultdict(list))

    for result in results_list:
        dataset_size = result['dataset']['n_subjects']
        concurrency = result['load_testing']['concurrency']

        for endpoint_data in result.get('latency', []):
            endpoint = endpoint_data['endpoint']
            key = (dataset_size, concurrency, endpoint)

            # Collect all metric values for this combination
            groups[key]['p50'].append(endpoint_data.get('p50_ms', 0))
            groups[key]['p95'].append(endpoint_data.get('p95_ms', 0))
            groups[key]['p99'].append(endpoint_data.get('p99_ms', 0))
            groups[key]['mean'].append(endpoint_data.get('mean_ms', 0))
            groups[key]['min'].append(endpoint_data.get('min_ms', 0))
            groups[key]['max'].append(endpoint_data.get('max_ms', 0))

    # Compute median for each group and reconstruct result structure
    aggregated = defaultdict(lambda: {
        'dataset': {},
        'load_testing': {},
        'latency': []
    })

    for (dataset_size, concurrency, endpoint), metrics in groups.items():
        agg_key = (dataset_size, concurrency)

        # Set dataset and load_testing info (same for all replicates)
        if not aggregated[agg_key]['dataset']:
            aggregated[agg_key]['dataset'] = {'n_subjects': dataset_size}
            aggregated[agg_key]['load_testing'] = {'concurrency': concurrency}

        # Add median endpoint data
        aggregated[agg_key]['latency'].append({
            'endpoint': endpoint,
            'p50_ms': float(np.median(metrics['p50'])),
            'p95_ms': float(np.median(metrics['p95'])),
            'p99_ms': float(np.median(metrics['p99'])),
            'mean_ms': float(np.median(metrics['mean'])),
            'min_ms': float(np.median(metrics['min'])),
            'max_ms': float(np.median(metrics['max'])),
        })

    # Convert back to list
    return list(aggregated.values())


def plot_concurrency_scaling(ax, results_list: List[Dict[str, Any]],
                             dataset_size: int, metric: str = 'p95', max_y: float = None):
    """
    Plot concurrency scaling for a specific dataset size.
    Shows stacked bars with P50 (darker) and P95 (lighter).

    Args:
        max_y: Optional maximum y-axis value for consistent scaling across plots
    """
    # Filter to specific dataset size
    size_results = [r for r in results_list if r['dataset']['n_subjects'] == dataset_size]

    if not size_results or len(size_results) < 2:
        ax.text(0.5, 0.5, f'Insufficient data for N={dataset_size//1000}K',
                ha='center', va='center', transform=ax.transAxes, fontsize=11)
        return

    # Sort by concurrency
    size_results = sorted(size_results, key=lambda x: x['load_testing']['concurrency'])
    concurrency_levels = [r['load_testing']['concurrency'] for r in size_results]

    # Get all unique endpoints (filter out deprecated version_specific_query)
    endpoints = [e['endpoint'] for e in size_results[0]['latency']
                 if e['endpoint'] != 'version_specific_query']

    # Okabe-Ito colorblind-safe palette (scientifically designed for accessibility)
    colors = ['#E69F00', '#56B4E9', '#009E73', '#F0E442', '#0072B2', '#D55E00', '#CC79A7', '#000000']
    # Hatching patterns for bar charts (helps with grayscale printing and colorblindness)
    hatches = ['', '///', '\\\\\\', 'xxx', '+++', '...', '|||', '---']

    n_endpoints = len(endpoints)
    n_concurrency = len(concurrency_levels)
    bar_width = 0.8 / n_concurrency
    cluster_positions = np.arange(n_endpoints)

    # Build data mapping
    data = {}
    for result in size_results:
        conc = result['load_testing']['concurrency']
        for endpoint_data in result['latency']:
            endpoint = endpoint_data['endpoint']
            if endpoint in endpoints:
                data[(endpoint, conc)] = (
                    endpoint_data['p50_ms'] / 1000,
                    endpoint_data['p95_ms'] / 1000
                )

    # Plot each concurrency level
    for conc_idx, conc in enumerate(concurrency_levels):
        p50_values = []
        p95_values = []

        for endpoint in endpoints:
            if (endpoint, conc) in data:
                p50, p95 = data[(endpoint, conc)]
                p50_values.append(p50)
                p95_values.append(p95)
            else:
                p50_values.append(0)
                p95_values.append(0)

        offset = (conc_idx - n_concurrency/2 + 0.5) * bar_width
        x_positions = cluster_positions + offset

        color = colors[conc_idx % len(colors)]
        hatch = hatches[conc_idx % len(hatches)]
        label = f'c={conc}'

        # Stacked bars: P50 (darker) + P95-P50 (lighter)
        p95_minus_p50 = [p95 - p50 for p50, p95 in zip(p50_values, p95_values)]

        ax.bar(x_positions, p50_values, bar_width, label=label, color=color,
               hatch=hatch, edgecolor='black', linewidth=0.5, alpha=0.85)
        ax.bar(x_positions, p95_minus_p50, bar_width, bottom=p50_values,
               color=color, hatch=hatch, edgecolor='black', linewidth=0.5, alpha=0.3)

    ax.set_xlabel('Workflow', fontsize=11, fontweight='bold')
    ax.set_ylabel('Latency (seconds)', fontsize=11, fontweight='bold')
    ax.set_title(f'Concurrency Scaling (N={dataset_size//1000}K subjects)',
                 fontsize=12, fontweight='bold')
    ax.set_xticks(cluster_positions)
    ax.set_xticklabels([e.replace('_', ' ').title() for e in endpoints], rotation=45, ha='right')

    # Add proxy artists to legend to explain P50/P95 stacking
    from matplotlib.patches import Patch
    handles, labels = ax.get_legend_handles_labels()
    # Add separator and P50/P95 explanation
    handles.extend([
        Patch(facecolor='gray', alpha=0.85, edgecolor='black', linewidth=0.5),
        Patch(facecolor='gray', alpha=0.3, edgecolor='black', linewidth=0.5)
    ])
    labels.extend(['P50 (darker)', 'P95 (lighter)'])

    ax.legend(handles, labels, fontsize=10, loc='upper left', framealpha=0.9, ncol=1)
    ax.grid(axis='y', alpha=0.3, linestyle='--')
    ax.set_axisbelow(True)

    # Set consistent y-axis limit if provided
    if max_y is not None:
        ax.set_ylim(0, max_y * 1.1)


def plot_dataset_scaling(ax, results_list: List[Dict[str, Any]],
                        concurrency: int, metric: str = 'p95', max_y: float = None):
    """
    Plot dataset size scaling for a specific concurrency level.

    Args:
        max_y: Optional maximum y-axis value for consistent scaling across plots
    """
    # Filter to specific concurrency level
    conc_results = [r for r in results_list
                    if r['load_testing']['concurrency'] == concurrency]

    if not conc_results or len(conc_results) < 2:
        ax.text(0.5, 0.5, f'Insufficient data for c={concurrency}',
                ha='center', va='center', transform=ax.transAxes, fontsize=11)
        return

    # Sort by dataset size
    conc_results = sorted(conc_results, key=lambda x: x['dataset']['n_subjects'])
    sizes = [r['dataset']['n_subjects'] / 1000 for r in conc_results]

    # Get all unique endpoints (filter out deprecated version_specific_query)
    endpoints = [e['endpoint'] for e in conc_results[0]['latency']
                 if e['endpoint'] != 'version_specific_query']

    # Okabe-Ito colorblind-safe palette
    colors = ['#E69F00', '#56B4E9', '#009E73', '#F0E442', '#0072B2', '#D55E00', '#CC79A7', '#000000']
    markers = ['o', 's', '^', 'D', 'v', '<', '>']

    # Plot each endpoint
    for idx, endpoint in enumerate(endpoints):
        values = []
        for result in conc_results:
            endpoint_data = next((e for e in result['latency'] if e['endpoint'] == endpoint), None)
            if endpoint_data:
                values.append(endpoint_data[f'{metric}_ms'] / 1000)
            else:
                values.append(None)

        label = endpoint.replace('_', ' ').title()

        ax.plot(sizes, values, marker=markers[idx % len(markers)],
                label=label, color=colors[idx % len(colors)],
                linewidth=2, markersize=7, alpha=0.85)

    ax.set_xlabel('Dataset Size (subjects)', fontsize=11, fontweight='bold')
    ax.set_ylabel(f'Latency (seconds, {metric.upper()})', fontsize=11, fontweight='bold')
    ax.set_title(f'Dataset Scaling (c={concurrency}, {metric.upper()})', fontsize=12, fontweight='bold')
    ax.legend(fontsize=10, loc='upper left', framealpha=0.9, ncol=1)
    ax.grid(alpha=0.3, linestyle='--')
    ax.set_axisbelow(True)

    # Set x-axis to log scale if spanning multiple orders of magnitude
    if max(sizes) / min(sizes) > 10:
        ax.set_xscale('log')
        ax.set_xticks(sizes)
        ax.set_xticklabels([f'{int(s)}K' for s in sizes])

    # Set consistent y-axis limit if provided
    if max_y is not None:
        ax.set_ylim(0, max_y * 1.1)


def calculate_global_max_y(results_list: List[Dict[str, Any]], metric: str = 'p95') -> float:
    """
    Calculate the global maximum y-value across all plots for consistent scaling.
    Considers both p95 values (for concurrency scaling) and the selected metric (for dataset scaling).
    """
    max_y = 0.0

    for result in results_list:
        for endpoint_data in result.get('latency', []):
            if endpoint_data['endpoint'] == 'version_specific_query':
                continue  # Skip deprecated endpoint

            # Check p95 for concurrency scaling plots
            p95_val = endpoint_data.get('p95_ms', 0) / 1000
            max_y = max(max_y, p95_val)

            # Check the selected metric for dataset scaling plots
            metric_val = endpoint_data.get(f'{metric}_ms', 0) / 1000
            max_y = max(max_y, metric_val)

    return max_y


def save_figure_all_formats(output_file: Path):
    """
    Save current matplotlib figure in all manuscript-compatible formats.

    Saves as: PNG (300 DPI), PDF, EPS, and SVG (with text-to-paths).
    """
    plt.rcParams['svg.fonttype'] = 'path'  # Convert text to paths for SVG

    # PNG for preview (high resolution)
    plt.savefig(output_file, dpi=300, bbox_inches='tight')

    # PDF with embedded fonts
    pdf_path = output_file.with_suffix('.pdf')
    plt.savefig(pdf_path, format='pdf', bbox_inches='tight')

    # EPS with embedded fonts
    eps_path = output_file.with_suffix('.eps')
    plt.savefig(eps_path, format='eps', bbox_inches='tight')

    # SVG with text converted to paths
    svg_path = output_file.with_suffix('.svg')
    plt.savefig(svg_path, format='svg', bbox_inches='tight')


def generate_tiled_figure(results_list: List[Dict[str, Any]], output_file: Path,
                         metric: str = 'p95', caption: str = None):
    """
    Generate a single comprehensive tiled figure with all performance plots.

    Layout: 3 rows with adaptive columns
    - Row 1: All concurrency scaling plots (1K, 5K, 10K, 50K, 100K...)
    - Row 2: All dataset scaling plots (c=1, c=10, c=25) - P50
    - Row 3: All dataset scaling plots (c=1, c=10, c=25) - P95

    All plots use the same y-axis scale for consistent visual comparison.

    Args:
        results_list: List of aggregated result dictionaries
        output_file: Path for output files
        metric: Metric to use (default: p95)
        caption: Optional figure caption text to display below the plots

    Note: Expects results_list to already be aggregated (median across replicates).
    """
    dataset_sizes = sorted(set(r['dataset']['n_subjects'] for r in results_list))
    concurrency_levels = sorted(set(r['load_testing']['concurrency'] for r in results_list))

    n_conc_plots = len(dataset_sizes)
    n_dataset_plots = len(concurrency_levels)
    n_cols = max(n_conc_plots, n_dataset_plots)

    # Calculate global maximum y-value for consistent scaling across all plots
    # Check both p50 and p95 since we're plotting both
    max_y = max(calculate_global_max_y(results_list, 'p95'),
                calculate_global_max_y(results_list, 'p50'))

    # Create 3-row grid with enough columns
    # Larger plots with legends inside, less horizontal spacing needed
    fig = plt.figure(figsize=(10 * n_cols, 26))
    gs = fig.add_gridspec(3, n_cols, hspace=0.60, wspace=0.25)

    plot_idx = 0

    # Row 1: All concurrency scaling plots
    for i, size in enumerate(dataset_sizes):
        ax = fig.add_subplot(gs[0, i])
        plot_concurrency_scaling(ax, results_list, size, metric, max_y)
        # Add panel label
        ax.text(-0.1, 1.1, chr(65 + plot_idx), transform=ax.transAxes,
                fontsize=16, fontweight='bold', va='top')
        plot_idx += 1

    # Row 2: All dataset scaling plots (P50)
    for i, conc in enumerate(concurrency_levels):
        ax = fig.add_subplot(gs[1, i])
        plot_dataset_scaling(ax, results_list, conc, 'p50', max_y)
        ax.text(-0.1, 1.1, chr(65 + plot_idx), transform=ax.transAxes,
                fontsize=16, fontweight='bold', va='top')
        plot_idx += 1

    # Row 3: All dataset scaling plots (P95)
    for i, conc in enumerate(concurrency_levels):
        ax = fig.add_subplot(gs[2, i])
        plot_dataset_scaling(ax, results_list, conc, 'p95', max_y)
        ax.text(-0.1, 1.1, chr(65 + plot_idx), transform=ax.transAxes,
                fontsize=16, fontweight='bold', va='top')
        plot_idx += 1

    # Add optional caption at the bottom of the figure
    if caption:
        # Increased font size for better readability (14pt vs previous 11pt)
        # Note: Matplotlib doesn't support inline bold/italic mixing easily.
        # For manuscript submission, format "Figure X." prefix as bold in your document.
        fig.text(0.5, 0.01, caption, ha='center', va='bottom',
                fontsize=14, transform=fig.transFigure)

    # Save figure in multiple formats for manuscript submission
    save_figure_all_formats(output_file)

    print(f"\nTiled figure saved:")
    print(f"  PNG: {output_file}")
    print(f"  PDF: {output_file.with_suffix('.pdf')}")
    print(f"  EPS: {output_file.with_suffix('.eps')}")
    print(f"  SVG: {output_file.with_suffix('.svg')}")
    print(f"  Layout: 3 rows × {n_cols} columns ({n_conc_plots} concurrency + {n_dataset_plots} P50 + {n_dataset_plots} P95)")
    plt.close()


def generate_matrix(results_list: List[Dict[str, Any]], output_dir: Path,
                   metric: str = 'p95', tiled: bool = False, caption: str = None):
    """
    Generate full matrix of performance figures.

    Creates:
    - One figure per dataset size showing concurrency scaling
    - One figure per concurrency level showing dataset scaling
    OR
    - One comprehensive tiled figure with all plots (if tiled=True)

    Args:
        results_list: List of result dictionaries
        output_dir: Output directory for figures
        metric: Metric to plot (default: p95)
        tiled: If True, generate single comprehensive tiled figure
        caption: Optional caption for tiled figure

    If multiple replicates exist for the same (dataset_size, concurrency, endpoint),
    the median value across replicates is used.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # Aggregate replicates by taking median values
    print(f"Loaded {len(results_list)} result files")
    results_list = aggregate_replicates(results_list)
    print(f"Aggregated to {len(results_list)} unique conditions (median across replicates)")

    # Get all unique dataset sizes and concurrency levels
    dataset_sizes = sorted(set(r['dataset']['n_subjects'] for r in results_list))
    concurrency_levels = sorted(set(r['load_testing']['concurrency'] for r in results_list))

    print(f"\nGenerating performance matrix:")
    print(f"  Dataset sizes: {[f'{s//1000}K' for s in dataset_sizes]}")
    print(f"  Concurrency levels: {concurrency_levels}")
    print(f"  Output directory: {output_dir}")
    print()

    if tiled:
        # Generate single comprehensive tiled figure
        output_file = output_dir / 'performance_matrix_comprehensive.png'
        generate_tiled_figure(results_list, output_file, metric, caption)
        return

    # Generate concurrency scaling figures (one per dataset size)
    print("Generating concurrency scaling figures:")
    for size in dataset_sizes:
        fig, ax = plt.subplots(1, 1, figsize=(10, 6))
        plot_concurrency_scaling(ax, results_list, size, metric)

        output_file = output_dir / f'concurrency_scaling_{size//1000}k.png'
        plt.tight_layout()
        save_figure_all_formats(output_file)
        plt.close()
        print(f"  Created: {output_file} (+ PDF, EPS, SVG)")

    # Generate dataset scaling figures (one per concurrency level)
    print("\nGenerating dataset scaling figures:")
    for conc in concurrency_levels:
        fig, ax = plt.subplots(1, 1, figsize=(10, 6))
        plot_dataset_scaling(ax, results_list, conc, metric)

        output_file = output_dir / f'dataset_scaling_c{conc}.png'
        plt.tight_layout()
        save_figure_all_formats(output_file)
        plt.close()
        print(f"  Created: {output_file} (+ PDF, EPS, SVG)")

    # Create index/summary file
    index_file = output_dir / 'README.md'
    with index_file.open('w') as f:
        f.write("# PheBee Performance Test Results - Supplementary Figures\n\n")
        f.write("## Concurrency Scaling\n\n")
        f.write("Shows how each endpoint scales with concurrent requests at different dataset sizes.\n\n")
        for size in dataset_sizes:
            f.write(f"- [{size//1000}K subjects](concurrency_scaling_{size//1000}k.png)\n")

        f.write("\n## Dataset Size Scaling\n\n")
        f.write("Shows how each endpoint scales with dataset size at different concurrency levels.\n\n")
        for conc in concurrency_levels:
            f.write(f"- [Concurrency {conc}](dataset_scaling_c{conc}.png)\n")

    print(f"\nIndex created: {index_file}")
    print(f"\nGenerated {len(dataset_sizes) + len(concurrency_levels)} supplementary figures")


def main():
    parser = argparse.ArgumentParser(
        description='Generate comprehensive performance figure matrix',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate all figures from results directory (separate files)
  python generate_performance_matrix.py 1000/c*/api_run.json 5000/c*/api_run.json \\
      10000/c*/api_run.json 50000/c*/api_run.json -o supplementary_figs/

  # Generate single comprehensive tiled figure
  python generate_performance_matrix.py results/**/*.json -o figs/ --tiled

  # Generate tiled figure with caption
  python generate_performance_matrix.py results/**/*.json -o figs/ --tiled \\
      --caption "Comprehensive performance evaluation across dataset sizes and concurrency levels"

  # Specify metric
  python generate_performance_matrix.py results/**/*.json -o figs/ -m p50
        """
    )
    parser.add_argument('results', nargs='+', help='JSON result files from performance tests')
    parser.add_argument('-o', '--output', required=True, help='Output directory for figures')
    parser.add_argument('-m', '--metric', default='p95', choices=['p50', 'p95', 'p99'],
                       help='Metric to use (default: p95)')
    parser.add_argument('--tiled', action='store_true',
                       help='Generate single comprehensive tiled figure instead of separate files')
    parser.add_argument('--caption', type=str, default=None,
                       help='Optional caption text for tiled figure (displayed below plots)')

    args = parser.parse_args()

    # Load all results
    results_list = []
    for result_file in args.results:
        try:
            result = load_results(result_file)
            results_list.append(result)
            n_subjects = result['dataset']['n_subjects']
            concurrency = result['load_testing']['concurrency']
            n_endpoints = len([e for e in result['latency'] if e['endpoint'] != 'version_specific_query'])
            print(f"Loaded: {result_file} (N={n_subjects:,}, c={concurrency}, {n_endpoints} endpoints)")
        except Exception as e:
            print(f"Warning: Failed to load {result_file}: {e}")

    if not results_list:
        print("Error: No valid result files found")
        return 1

    # Generate matrix
    output_dir = Path(args.output)
    generate_matrix(results_list, output_dir, args.metric, args.tiled, args.caption)

    return 0


if __name__ == '__main__':
    exit(main())
