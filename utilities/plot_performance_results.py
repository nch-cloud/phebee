#!/usr/bin/env python3
"""
Generate two-panel performance figure from PheBee API performance test results.

Panel A: Concurrency scaling (how endpoints scale with concurrent requests)
Panel B: Dataset size scaling (how endpoints scale with data volume)

Usage:
    python plot_performance_results.py results*.json -o figure.png

Results JSON files should be output from test_evaluation_perf_scale.py.
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
                             dataset_size: int = None, metric: str = 'p95',
                             show_p95_bars: bool = False):
    """
    Panel A: Clustered stacked bar chart showing how endpoints scale with concurrency.

    Args:
        ax: Matplotlib axis
        results_list: List of result dictionaries
        dataset_size: Target dataset size (if None, uses largest available)
        metric: Which metric to plot ('p50', 'p95', or 'p99')
        show_p95_bars: If True and metric='p50', show stacked bars with P50+P95
    """
    # Filter to specific dataset size
    if dataset_size is None:
        dataset_size = max(r['dataset']['n_subjects'] for r in results_list)

    size_results = [r for r in results_list if r['dataset']['n_subjects'] == dataset_size]

    if not size_results or len(size_results) < 2:
        ax.text(0.5, 0.5, 'Insufficient concurrency data\n(need c=1, c=10, c=25)',
                ha='center', va='center', transform=ax.transAxes, fontsize=11)
        return

    # Sort by concurrency
    size_results = sorted(size_results, key=lambda x: x['load_testing']['concurrency'])

    # Extract concurrency levels
    concurrency_levels = [r['load_testing']['concurrency'] for r in size_results]

    # Get all unique endpoints (filter out deprecated version_specific_query)
    endpoints = [e['endpoint'] for e in size_results[0]['latency']
                 if e['endpoint'] != 'version_specific_query']

    # Color palette
    colors = ['#e74c3c', '#3498db', '#2ecc71', '#f39c12', '#9b59b6', '#1abc9c', '#34495e']

    # If showing stacked P50+P95 bars
    if show_p95_bars and metric == 'p50':
        n_endpoints = len(endpoints)
        n_concurrency = len(concurrency_levels)
        bar_width = 0.8 / n_concurrency  # Width of each bar
        cluster_positions = np.arange(n_endpoints)  # Positions for each endpoint

        # Build a dict mapping (endpoint, concurrency) -> (p50, p95)
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

        # Plot each concurrency level as a set of bars across all endpoints
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

            # Calculate positions for this concurrency level's bars
            offset = (conc_idx - n_concurrency/2 + 0.5) * bar_width
            x_positions = cluster_positions + offset

            # Use a color that represents concurrency level
            color = colors[conc_idx % len(colors)]
            label = f'c={conc}'

            # Stack: bottom = P50 (solid), top = P95-P50 (lighter)
            p95_minus_p50 = [p95 - p50 for p50, p95 in zip(p50_values, p95_values)]

            # Bottom portion (P50) - solid color
            ax.bar(x_positions, p50_values, bar_width, label=label, color=color, alpha=0.85)

            # Top portion (P95-P50) - lighter color
            ax.bar(x_positions, p95_minus_p50, bar_width, bottom=p50_values,
                   color=color, alpha=0.3)

        ax.set_xlabel('Endpoint', fontsize=11, fontweight='bold')
        ax.set_ylabel('Latency (seconds)', fontsize=11, fontweight='bold')
        ax.set_title(f'Concurrency Scaling (N={dataset_size//1000}K subjects)\nDarker = P50, Lighter = P95',
                     fontsize=12, fontweight='bold')
        ax.set_xticks(cluster_positions)
        ax.set_xticklabels([e.replace('_', ' ').title() for e in endpoints], rotation=45, ha='right')
        ax.legend(fontsize=8, loc='upper left', bbox_to_anchor=(1.02, 1), ncol=1)
        ax.grid(axis='y', alpha=0.3, linestyle='--')
        ax.set_axisbelow(True)

    else:
        # Original line plot for single metric
        markers = ['o', 's', '^', 'D', 'v', '<', '>']

        for idx, endpoint in enumerate(endpoints):
            metric_values = []
            for result in size_results:
                endpoint_data = next((e for e in result['latency'] if e['endpoint'] == endpoint), None)
                if endpoint_data:
                    metric_values.append(endpoint_data[f'{metric}_ms'] / 1000)
                else:
                    metric_values.append(None)

            # Clean endpoint name for legend
            label = endpoint.replace('_', ' ').title()
            color = colors[idx % len(colors)]
            marker = markers[idx % len(markers)]

            ax.plot(concurrency_levels, metric_values, marker=marker, label=label, color=color,
                    linewidth=2, markersize=7, alpha=0.85)

        ax.set_xlabel('Concurrent Requests', fontsize=11, fontweight='bold')
        ax.set_ylabel(f'Latency (seconds, {metric.upper()})', fontsize=11, fontweight='bold')
        ax.set_title(f'Concurrency Scaling (N={dataset_size//1000}K subjects)',
                     fontsize=12, fontweight='bold')
        ax.legend(fontsize=8, loc='upper left', bbox_to_anchor=(1.02, 1), ncol=1)
        ax.grid(alpha=0.3, linestyle='--')
        ax.set_axisbelow(True)
        ax.set_xticks(concurrency_levels)
        ax.set_xticklabels([f'c={c}' for c in concurrency_levels])


def plot_dataset_scaling(ax, results_list: List[Dict[str, Any]],
                        concurrency: int = 1, metric: str = 'p95'):
    """
    Panel B: Line plot showing how all endpoints scale with dataset size.

    Args:
        ax: Matplotlib axis
        results_list: List of result dictionaries
        concurrency: Target concurrency level (default: 1)
        metric: Which metric to plot ('p50', 'p95', or 'p99')
    """
    # Filter to specific concurrency level
    conc_results = [r for r in results_list
                    if r['load_testing']['concurrency'] == concurrency]

    if not conc_results or len(conc_results) < 2:
        ax.text(0.5, 0.5, f'Insufficient data for dataset scaling\n(need multiple sizes at c={concurrency})',
                ha='center', va='center', transform=ax.transAxes, fontsize=11)
        return

    # Sort by dataset size
    conc_results = sorted(conc_results, key=lambda x: x['dataset']['n_subjects'])

    # Extract dataset sizes
    sizes = [r['dataset']['n_subjects'] / 1000 for r in conc_results]  # Convert to thousands

    # Get all unique endpoints (filter out deprecated version_specific_query)
    endpoints = [e['endpoint'] for e in conc_results[0]['latency']
                 if e['endpoint'] != 'version_specific_query']

    # Color palette
    colors = ['#e74c3c', '#3498db', '#2ecc71', '#f39c12', '#9b59b6', '#1abc9c', '#34495e']
    markers = ['o', 's', '^', 'D', 'v', '<', '>']

    # Plot each endpoint
    for idx, endpoint in enumerate(endpoints):
        values = []
        for result in conc_results:
            endpoint_data = next((e for e in result['latency'] if e['endpoint'] == endpoint), None)
            if endpoint_data:
                values.append(endpoint_data[f'{metric}_ms'] / 1000)  # Convert to seconds
            else:
                values.append(None)

        # Clean endpoint name for legend
        label = endpoint.replace('_', ' ').title()

        ax.plot(sizes, values, marker=markers[idx % len(markers)],
                label=label, color=colors[idx % len(colors)],
                linewidth=2, markersize=7, alpha=0.85)

    ax.set_xlabel('Dataset Size (thousands of subjects)', fontsize=11, fontweight='bold')
    ax.set_ylabel(f'Latency (seconds, {metric.upper()})', fontsize=11, fontweight='bold')
    ax.set_title(f'Dataset Scaling (c={concurrency}, {metric.upper()})', fontsize=12, fontweight='bold')
    ax.legend(fontsize=8, loc='upper left', bbox_to_anchor=(1.02, 1), ncol=1)
    ax.grid(alpha=0.3, linestyle='--')
    ax.set_axisbelow(True)

    # Set x-axis to log scale if spanning multiple orders of magnitude
    if max(sizes) / min(sizes) > 10:
        ax.set_xscale('log')
        ax.set_xticks(sizes)
        ax.set_xticklabels([f'{int(s)}K' for s in sizes])


def create_performance_figure(results_list: List[Dict[str, Any]],
                              output_file: str = 'performance_figure.png',
                              metric: str = 'p95',
                              panel_a_size: int = None,
                              panel_b_conc: int = 1,
                              show_p95_bars: bool = False):
    """
    Create two-panel figure:
    - Panel A: Concurrency scaling (c=1, c=10, c=25) for a specific dataset size
    - Panel B: Dataset size scaling (1K, 5K, 10K, ...) at a specific concurrency

    Args:
        results_list: List of result dictionaries
        output_file: Output file path
        metric: Metric to plot ('p50', 'p95', or 'p99')
        panel_a_size: Dataset size for Panel A (if None, uses largest)
        panel_b_conc: Concurrency level for Panel B (default: 1)
        show_p95_bars: If True and metric='p50', show clustered stacked bars

    If multiple replicates exist for the same (dataset_size, concurrency, endpoint),
    the median value across replicates is used.
    """
    # Aggregate replicates by taking median values
    print(f"\nAggregating {len(results_list)} result files...")
    results_list = aggregate_replicates(results_list)
    print(f"Aggregated to {len(results_list)} unique conditions (median across replicates)\n")

    # Create figure with two panels
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

    # Panel A: Concurrency scaling
    plot_concurrency_scaling(ax1, results_list, panel_a_size, metric, show_p95_bars)

    # Panel B: Dataset size scaling
    plot_dataset_scaling(ax2, results_list, panel_b_conc, metric)

    # Add panel labels
    ax1.text(-0.15, 1.05, 'A', transform=ax1.transAxes, fontsize=16, fontweight='bold')
    ax2.text(-0.15, 1.05, 'B', transform=ax2.transAxes, fontsize=16, fontweight='bold')

    plt.tight_layout()

    # Save figure
    output_path = Path(output_file)
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"Figure saved to: {output_path}")

    # Also save as PDF for publication
    pdf_path = output_path.with_suffix('.pdf')
    plt.savefig(pdf_path, bbox_inches='tight')
    print(f"PDF version saved to: {pdf_path}")

    plt.close()


def main():
    parser = argparse.ArgumentParser(
        description='Generate two-panel performance figure from PheBee API test results',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Panel A: Concurrency scaling (c=1, c=10, c=25)
Panel B: Dataset size scaling (1K, 5K, 10K, ...)

Examples:
  # Basic usage with all results
  python plot_performance_results.py results_*.json

  # Specify output file and metric
  python plot_performance_results.py -o figure1.png -m p95 results_*.json

  # Show p50 with p95 as stacked bars (shows both typical and tail latency)
  python plot_performance_results.py -m p50 --show-p95-bars results_*.json

  # Use 5K dataset for Panel A concurrency comparison
  python plot_performance_results.py --panel-a-size 5000 results_*.json

  # Show p99 metric
  python plot_performance_results.py -m p99 results_*.json
        """
    )
    parser.add_argument('results', nargs='+', help='JSON result files from performance tests')
    parser.add_argument('-o', '--output', default='performance_figure.png',
                       help='Output file path (default: performance_figure.png)')
    parser.add_argument('-m', '--metric', default='p95', choices=['p50', 'p95', 'p99'],
                       help='Metric to use for both panels (default: p95)')
    parser.add_argument('--panel-a-size', type=int, default=None,
                       help='Dataset size for Panel A concurrency plot (default: largest)')
    parser.add_argument('--panel-b-conc', type=int, default=1,
                       help='Concurrency level for Panel B dataset scaling (default: 1)')
    parser.add_argument('--show-p95-bars', action='store_true',
                       help='Show clustered stacked bars with P50 (solid) and P95 (hatched) (only with -m p50)')

    args = parser.parse_args()

    # Load all results
    results_list = []
    for result_file in args.results:
        try:
            result = load_results(result_file)
            results_list.append(result)
            n_subjects = result['dataset']['n_subjects']
            concurrency = result['load_testing']['concurrency']
            n_endpoints = len(result['latency'])
            print(f"Loaded: {result_file} (N={n_subjects:,}, c={concurrency}, {n_endpoints} endpoints)")
        except Exception as e:
            print(f"Warning: Failed to load {result_file}: {e}")

    if not results_list:
        print("Error: No valid result files found")
        return 1

    # Create figure
    create_performance_figure(results_list, args.output, args.metric,
                             args.panel_a_size, args.panel_b_conc, args.show_p95_bars)

    return 0


if __name__ == '__main__':
    exit(main())
