#!/usr/bin/env python3
"""
Generate comprehensive performance figure matrix from PheBee API performance test results.

Creates a full set of supplementary figures showing:
- Concurrency scaling at each dataset size (1K, 5K, 10K, 50K)
- Dataset scaling at each concurrency level (c=1, c=10, c=25)

Each panel is written at single-column print size and composed into the
supplementary figure artboard in Illustrator, so it must not be resized there.
Output follows manuscript/revision/scientific-figures-guide.md: vector PDF with
text that stays editable, plus PNG and SVG and the plotted values as CSV. See
figure_style.py for the rules.

Usage:
    python generate_performance_matrix.py results_dir/ -o output_dir/
    python generate_performance_matrix.py 1000/c*/api_run.json 5000/c*/api_run.json ... -o figs/
"""

import argparse
import json
from pathlib import Path
from typing import List, Dict, Any
from collections import defaultdict

import numpy as np

from figure_style import (HATCHES, OKABE_ITO, SINGLE_COLUMN_MM, apply_style,
                          figure_size, lighten, panel_label, save_figure)
import matplotlib.pyplot as plt  # noqa: E402  (figure_style selects the Agg backend)

from workflow_names import display_name, workload_number

apply_style()

# Final print sizes. The bar panels carry seven rotated workflow names along the
# x-axis and need the extra height for them.
BAR_PANEL_SIZE_MM = (SINGLE_COLUMN_MM, 75.0)
LINE_PANEL_SIZE_MM = (SINGLE_COLUMN_MM, 65.0)


def load_results(file_path: str) -> Dict[str, Any]:
    """Load performance results from JSON file."""
    with open(file_path, 'r') as f:
        return json.load(f)


def metric_value(endpoint_data: Dict[str, Any], key: str) -> float:
    """Read one latency metric, failing loudly if the artifact lacks it.

    Missing keys used to default to 0. That is how the avg_ms/mean_ms mismatch
    went unnoticed: the harness writes avg_ms, this script asked for mean_ms,
    and every mean silently rendered as 0 instead of raising.
    """
    if key not in endpoint_data:
        raise KeyError(
            f"{endpoint_data.get('endpoint', '<unknown endpoint>')}: artifact has no "
            f"{key!r} (present: {sorted(endpoint_data)})"
        )
    return float(endpoint_data[key])


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
            groups[key]['p50'].append(metric_value(endpoint_data, 'p50_ms'))
            groups[key]['p95'].append(metric_value(endpoint_data, 'p95_ms'))
            groups[key]['p99'].append(metric_value(endpoint_data, 'p99_ms'))
            groups[key]['avg'].append(metric_value(endpoint_data, 'avg_ms'))
            groups[key]['min'].append(metric_value(endpoint_data, 'min_ms'))
            groups[key]['max'].append(metric_value(endpoint_data, 'max_ms'))

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
            'avg_ms': float(np.median(metrics['avg'])),
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

    Returns: the plotted values as rows for the figure's CSV sidecar.
    """
    # Filter to specific dataset size
    size_results = [r for r in results_list if r['dataset']['n_subjects'] == dataset_size]

    if not size_results or len(size_results) < 2:
        ax.text(0.5, 0.5, f'Insufficient data for N={dataset_size//1000}K',
                ha='center', va='center', transform=ax.transAxes)
        return []

    # Sort by concurrency
    size_results = sorted(size_results, key=lambda x: x['load_testing']['concurrency'])
    concurrency_levels = [r['load_testing']['concurrency'] for r in size_results]

    # Get all unique endpoints (filter out deprecated version_specific_query)
    endpoints = [e['endpoint'] for e in size_results[0]['latency']
                 if e['endpoint'] != 'version_specific_query']

    colors, hatches = OKABE_ITO, HATCHES
    plotted = []

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

        for endpoint, p50, p95 in zip(endpoints, p50_values, p95_values):
            plotted.append({'workload': workload_number(endpoint), 'workflow': endpoint,
                            'subjects': dataset_size, 'concurrency': conc,
                            'metric': 'p50', 'latency_s': f'{p50:.3f}'})
            plotted.append({'workload': workload_number(endpoint), 'workflow': endpoint,
                            'subjects': dataset_size, 'concurrency': conc,
                            'metric': 'p95', 'latency_s': f'{p95:.3f}'})

        offset = (conc_idx - n_concurrency/2 + 0.5) * bar_width
        x_positions = cluster_positions + offset

        color = colors[conc_idx % len(colors)]
        hatch = hatches[conc_idx % len(hatches)]
        label = f'c={conc}'

        # Stacked bars: P50 (darker) + P95-P50 (lighter)
        p95_minus_p50 = [p95 - p50 for p50, p95 in zip(p50_values, p95_values)]

        ax.bar(x_positions, p50_values, bar_width, label=label,
               color=lighten(color, 0.85), hatch=hatch, edgecolor='black', linewidth=0.5)
        ax.bar(x_positions, p95_minus_p50, bar_width, bottom=p50_values,
               color=lighten(color, 0.3), hatch=hatch, edgecolor='black', linewidth=0.5)

    ax.set_xlabel('Workflow')
    ax.set_ylabel('Latency (seconds)')
    ax.set_title(f'Concurrency scaling (N = {dataset_size:,} subjects)')
    ax.set_xticks(cluster_positions)
    ax.set_xticklabels([display_name(e) for e in endpoints], rotation=45, ha='right')

    # Add proxy artists to legend to explain P50/P95 stacking
    from matplotlib.patches import Patch
    handles, labels = ax.get_legend_handles_labels()
    # Add separator and P50/P95 explanation
    handles.extend([
        Patch(facecolor=lighten('gray', 0.85), edgecolor='black', linewidth=0.5),
        Patch(facecolor=lighten('gray', 0.3), edgecolor='black', linewidth=0.5)
    ])
    labels.extend(['P50 (darker)', 'P95 (lighter)'])

    # `loc='best'` rather than a fixed corner: the style file turns the legend
    # frame off, so an overlapping legend is now unreadable rather than merely
    # ugly, and which corner is free depends on the dataset size being plotted.
    ax.legend(handles, labels, loc='best', ncol=1)

    # Set consistent y-axis limit if provided
    if max_y is not None:
        ax.set_ylim(0, max_y * 1.1)

    return plotted


def plot_dataset_scaling(ax, results_list: List[Dict[str, Any]],
                        concurrency: int, metric: str = 'p95', max_y: float = None):
    """
    Plot dataset size scaling for a specific concurrency level.

    Args:
        max_y: Optional maximum y-axis value for consistent scaling across plots

    Returns: the plotted values as rows for the figure's CSV sidecar.
    """
    # Filter to specific concurrency level
    conc_results = [r for r in results_list
                    if r['load_testing']['concurrency'] == concurrency]

    if not conc_results or len(conc_results) < 2:
        ax.text(0.5, 0.5, f'Insufficient data for c={concurrency}',
                ha='center', va='center', transform=ax.transAxes)
        return []

    # Sort by dataset size
    conc_results = sorted(conc_results, key=lambda x: x['dataset']['n_subjects'])
    sizes = [r['dataset']['n_subjects'] / 1000 for r in conc_results]

    # Get all unique endpoints (filter out deprecated version_specific_query)
    endpoints = [e['endpoint'] for e in conc_results[0]['latency']
                 if e['endpoint'] != 'version_specific_query']

    colors = OKABE_ITO
    markers = ['o', 's', '^', 'D', 'v', '<', '>']
    plotted = []

    # Plot each endpoint
    for idx, endpoint in enumerate(endpoints):
        values = []
        for result in conc_results:
            endpoint_data = next((e for e in result['latency'] if e['endpoint'] == endpoint), None)
            if endpoint_data:
                values.append(endpoint_data[f'{metric}_ms'] / 1000)
            else:
                values.append(None)

        for result, value in zip(conc_results, values):
            if value is not None:
                plotted.append({'workload': workload_number(endpoint), 'workflow': endpoint,
                                'subjects': result['dataset']['n_subjects'],
                                'concurrency': concurrency, 'metric': metric,
                                'latency_s': f'{value:.3f}'})

        label = display_name(endpoint)

        ax.plot(sizes, values, marker=markers[idx % len(markers)],
                label=label, color=colors[idx % len(colors)])

    ax.set_xlabel('Dataset size (subjects)')
    ax.set_ylabel(f'Latency (seconds, {metric.upper()})')
    ax.set_title(f'Dataset scaling (c = {concurrency}, {metric.upper()})')
    ax.legend(loc='best', ncol=1)

    # Set x-axis to log scale if spanning multiple orders of magnitude
    if max(sizes) / min(sizes) > 10:
        ax.set_xscale('log')
        ax.set_xticks(sizes)
        ax.set_xticklabels([f'{int(s)}K' for s in sizes])

    # Set consistent y-axis limit if provided
    if max_y is not None:
        ax.set_ylim(0, max_y * 1.1)

    return plotted


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
            max_y = max(max_y, metric_value(endpoint_data, 'p95_ms') / 1000)

            # Check the selected metric for dataset scaling plots
            max_y = max(max_y, metric_value(endpoint_data, f'{metric}_ms') / 1000)

    return max_y


def generate_tiled_figure(results_list: List[Dict[str, Any]], output_file: Path,
                         metric: str = 'p95', caption: str = None,
                         formats=('pdf', 'png', 'svg')):
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
        formats: Output formats; add 'eps' only if a journal insists on it

    Note: Expects results_list to already be aggregated (median across replicates).

    This is a working overview, not a submission figure: it is taller than the
    170 mm the figures guide allows, and a caption set inside the figure file is
    exactly what the guide's naming rule tells you not to ship. The per-panel
    files are what gets composed into Supplementary Figures S1 and S2.
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

    # One single-column panel per grid cell, so the panels are the same physical
    # size here as in the per-panel files. Constrained layout owns the spacing;
    # gridspec hspace/wspace would be ignored.
    fig = plt.figure(figsize=figure_size(SINGLE_COLUMN_MM * n_cols,
                                        BAR_PANEL_SIZE_MM[1] + 2 * LINE_PANEL_SIZE_MM[1],
                                        allow_tall=True))
    gs = fig.add_gridspec(3, n_cols)

    plot_idx = 0
    plotted = []

    # Row 1: All concurrency scaling plots
    for i, size in enumerate(dataset_sizes):
        ax = fig.add_subplot(gs[0, i])
        rows = plot_concurrency_scaling(ax, results_list, size, metric, max_y)
        # Add panel label
        panel_label(ax, chr(65 + plot_idx))
        plotted += [{'panel': chr(65 + plot_idx), **row} for row in rows]
        plot_idx += 1

    # Row 2: All dataset scaling plots (P50)
    for i, conc in enumerate(concurrency_levels):
        ax = fig.add_subplot(gs[1, i])
        rows = plot_dataset_scaling(ax, results_list, conc, 'p50', max_y)
        panel_label(ax, chr(65 + plot_idx))
        plotted += [{'panel': chr(65 + plot_idx), **row} for row in rows]
        plot_idx += 1

    # Row 3: All dataset scaling plots (P95)
    for i, conc in enumerate(concurrency_levels):
        ax = fig.add_subplot(gs[2, i])
        rows = plot_dataset_scaling(ax, results_list, conc, 'p95', max_y)
        panel_label(ax, chr(65 + plot_idx))
        plotted += [{'panel': chr(65 + plot_idx), **row} for row in rows]
        plot_idx += 1

    # Add optional caption at the bottom of the figure
    if caption:
        # The figures guide asks for no caption inside the figure file: captions
        # belong in the document, where they can carry bold and italic and be
        # copyedited with the rest of the prose. Kept for the working overview.
        print('  NOTE: --caption sets the caption inside the figure file, which the '
              'figures guide advises against for anything submitted.')
        fig.supxlabel(caption)

    # Save figure in multiple formats for manuscript submission
    print('\nTiled figure saved:')
    save_figure(fig, output_file, formats=formats, data=plotted)
    print(f"  Layout: 3 rows × {n_cols} columns ({n_conc_plots} concurrency + {n_dataset_plots} P50 + {n_dataset_plots} P95)")
    plt.close(fig)


def generate_matrix(results_list: List[Dict[str, Any]], output_dir: Path,
                   metric: str = 'p95', tiled: bool = False, caption: str = None,
                   formats=('pdf', 'png', 'svg')):
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
        generate_tiled_figure(results_list, output_file, metric, caption, formats)
        return

    # Generate concurrency scaling figures (one per dataset size)
    print("Generating concurrency scaling figures:")
    for size in dataset_sizes:
        fig, ax = plt.subplots(1, 1, figsize=figure_size(*BAR_PANEL_SIZE_MM))
        rows = plot_concurrency_scaling(ax, results_list, size, metric)

        output_file = output_dir / f'concurrency_scaling_{size//1000}k.png'
        save_figure(fig, output_file, formats=formats,
                    data=[{'panel': output_file.stem, **row} for row in rows])
        plt.close(fig)

    # Generate dataset scaling figures (one per concurrency level)
    print("\nGenerating dataset scaling figures:")
    for conc in concurrency_levels:
        fig, ax = plt.subplots(1, 1, figsize=figure_size(*LINE_PANEL_SIZE_MM))
        rows = plot_dataset_scaling(ax, results_list, conc, metric)

        output_file = output_dir / f'dataset_scaling_c{conc}.png'
        save_figure(fig, output_file, formats=formats,
                    data=[{'panel': output_file.stem, **row} for row in rows])
        plt.close(fig)

    # Create index/summary file
    index_file = output_dir / 'README.md'
    with index_file.open('w') as f:
        f.write("# PheBee Performance Test Results - Supplementary Figures\n\n")
        f.write("Each panel is a vector PDF at single-column print size "
                f"({SINGLE_COLUMN_MM:.0f} mm wide) with editable text, listed below; "
                "the PNG and SVG beside it are for preview, and the `_data.csv` holds "
                "the plotted values. Place the PDFs in Illustrator, do not resize them.\n\n")
        f.write("## Concurrency Scaling\n\n")
        f.write("Shows how each endpoint scales with concurrent requests at different dataset sizes.\n\n")
        for size in dataset_sizes:
            f.write(f"- [{size//1000}K subjects](concurrency_scaling_{size//1000}k.pdf)\n")

        f.write("\n## Dataset Size Scaling\n\n")
        f.write("Shows how each endpoint scales with dataset size at different concurrency levels.\n\n")
        for conc in concurrency_levels:
            f.write(f"- [Concurrency {conc}](dataset_scaling_c{conc}.pdf)\n")

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
    parser.add_argument('-m', '--metric', default='p95', choices=['p50', 'p95', 'p99', 'avg'],
                       help='Metric to use (default: p95)')
    parser.add_argument('--tiled', action='store_true',
                       help='Generate single comprehensive tiled figure instead of separate files')
    parser.add_argument('--caption', type=str, default=None,
                       help='Optional caption text for tiled figure (displayed below plots)')
    parser.add_argument('--eps', action='store_true',
                       help='Also write EPS (only if a journal insists; PDF is the deliverable)')

    args = parser.parse_args()
    formats = ('pdf', 'png', 'svg') + (('eps',) if args.eps else ())

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
    generate_matrix(results_list, output_dir, args.metric, args.tiled, args.caption, formats)

    return 0


if __name__ == '__main__':
    exit(main())
