#!/usr/bin/env python3
"""Publication figure conventions for the PheBee performance figures.

Implements `manuscript/revision/scientific-figures-guide.md` so the two
plotting scripts do not each carry their own copy of the rules. The guide's
requirements that live here rather than in `publication.mplstyle`:

  * the Agg backend, selected at import time so it is in force before any
    script imports pyplot (guide 4.2: never rely on a GUI backend for export);
  * the `pdf.fonttype == 42` assertion the guide asks every script to carry;
  * sizes in millimetres at final print size, never resized afterwards;
  * `savefig` without `bbox_inches`, which would recompute the page size, and
    deterministic PDF/SVG metadata so re-runs are byte-identical;
  * the plotted values written beside the figure as CSV;
  * verification with `pdffonts` / `pdfimages` / `pdfinfo` after every save.

Verification goes through poppler subprocesses rather than the guide's
`check_figure_pdf.py`, because pypdf is not installed on this machine and the
poppler binaries are. The checks are the same three the guide's section 7
runs by hand.

`lighten()` exists because the guide forbids two things the previous figures
relied on: transparency in EPS (the PS backend drops it) and opacity groups
that arrive in Illustrator as a nested layer to fight with. Blending toward
white gives a solid fill that is indistinguishable on a white page.
"""

import csv
import shutil
import subprocess
import sys
from pathlib import Path

import matplotlib

matplotlib.use('Agg')  # before any pyplot import anywhere in the process

import matplotlib.colors  # noqa: E402  (must follow the backend selection)
import matplotlib.pyplot as plt  # noqa: E402

HERE = Path(__file__).resolve().parent
STYLE_FILE = HERE / 'publication.mplstyle'

MM = 1 / 25.4  # matplotlib figure sizes are in inches

# Nature's 2026 research-figure guide, as tabulated in the figures guide.
SINGLE_COLUMN_MM = 89.0
ONE_AND_HALF_COLUMN_MM = 120.0
DOUBLE_COLUMN_MM = 183.0
MAX_HEIGHT_MM = 170.0

# Okabe-Ito (Wong, Nature Methods 8:441, 2011). Colourblind-safe to 8 classes.
# Entities keep the same colour in every figure of the paper, so this list is
# indexed in a fixed order and never reshuffled or sorted.
OKABE_ITO = ['#E69F00', '#56B4E9', '#009E73', '#F0E442',
             '#0072B2', '#D55E00', '#CC79A7', '#000000']

# Hatch patterns paired with the palette above, so series identity survives
# greyscale printing and does not depend on colour alone (guide 2.3).
HATCHES = ['', '///', '\\\\\\', 'xxx', '+++', '...', '|||', '---']

PANEL_LABEL_PT = 8  # guide 2.2: panel labels 8 pt bold

# Long-form schema for the CSV written beside each figure (guide section 8).
PLOT_DATA_COLUMNS = ['panel', 'workload', 'workflow', 'subjects',
                     'concurrency', 'metric', 'latency_s']

_PT_PER_MM = 72 / 25.4

_style_applied = False


def apply_style():
    """Load publication.mplstyle and assert the settings that matter held.

    Idempotent, so scripts can call it at import time without worrying about
    being imported twice. The asserts are the guide's: a figure saved with
    `pdf.fonttype` back at its default 3 looks correct and is unusable for
    editing, and nothing in the output warns you.
    """
    global _style_applied
    if not _style_applied:
        plt.style.use(STYLE_FILE)
        _style_applied = True

    assert matplotlib.rcParams['pdf.fonttype'] == 42, \
        'pdf.fonttype is not 42: PDF text would arrive in Illustrator as outlined paths'
    assert matplotlib.rcParams['ps.fonttype'] == 42, 'ps.fonttype is not 42'
    assert matplotlib.rcParams['svg.fonttype'] == 'none', \
        "svg.fonttype is not 'none': SVG text would be written as <path>"
    assert matplotlib.rcParams['figure.constrained_layout.use'], \
        'constrained layout is off; tight_layout/bbox_inches would change the page size'

    _warn_if_font_missing()


def _warn_if_font_missing():
    """Warn if Arial is not installed. matplotlib substitutes in silence."""
    from matplotlib import font_manager
    try:
        font_manager.findfont(font_manager.FontProperties(family=['Arial']),
                              fallback_to_default=False)
    except Exception:
        resolved = font_manager.findfont(font_manager.FontProperties(family=['Arial']))
        print(f'WARNING: Arial not found; matplotlib will substitute {Path(resolved).name}. '
              f'Text will still be editable but the recipient sees a different face. '
              f'Check the pdffonts output below before handing the figure off.',
              file=sys.stderr)


def figure_size(width_mm, height_mm, allow_tall=False):
    """Figure size in inches from millimetres at final print size.

    `allow_tall` is for working overview figures that are never placed in the
    manuscript; anything destined for the submission must fit the 170 mm limit.
    """
    if not allow_tall and height_mm > MAX_HEIGHT_MM:
        raise ValueError(f'{height_mm} mm exceeds the {MAX_HEIGHT_MM} mm maximum figure '
                         f'height; pass allow_tall=True only for working figures')
    return (width_mm * MM, height_mm * MM)


def lighten(color, weight):
    """Blend `color` toward white. weight 1.0 leaves it alone, 0.0 gives white.

    A stand-in for `alpha`, matching what `alpha=weight` would look like over a
    white page, without the two costs the guide flags: the EPS backend drops
    transparency, and Illustrator imports an alpha fill as an opacity group.
    """
    r, g, b = matplotlib.colors.to_rgb(color)
    return tuple(1 - weight * (1 - channel) for channel in (r, g, b))


def panel_label(ax, letter, x=-0.16, y=1.02):
    """Bold panel letter in axes coordinates, top-left (guide 2.2).

    Uppercase A, B, C here rather than the guide's Nature-style lowercase,
    because the manuscript captions read "(A)" and "(B)"; the guide leaves the
    case to the journal. Nudge `x` outward when the y-axis labels are wide.
    """
    ax.text(x, y, letter, transform=ax.transAxes, fontsize=PANEL_LABEL_PT,
            fontweight='bold', va='bottom', ha='left')


def write_plot_data(base_path, rows):
    """Write the plotted values beside the figure as `<stem>_data.csv`.

    Sorted, so re-runs are byte-identical and the file diffs cleanly.
    """
    path = Path(base_path).with_suffix('')
    path = path.with_name(path.name + '_data.csv')
    rows = sorted((tuple(str(row[column]) for column in PLOT_DATA_COLUMNS) for row in rows))
    with path.open('w', newline='') as handle:
        writer = csv.writer(handle)
        writer.writerow(PLOT_DATA_COLUMNS)
        writer.writerows(rows)
    return path


def save_figure(fig, base_path, formats=('pdf', 'png', 'svg'), data=None,
                verify=True, label=None):
    """Save one figure at exactly its `figsize`, then verify the PDF.

    No `bbox_inches`: the guide measured it changing the page size, and the
    size is the one thing that must not drift. Constrained layout, set in the
    style file, does the job `tight_layout()` used to.

    `data` is an iterable of dicts keyed by PLOT_DATA_COLUMNS -- the values
    actually plotted, written alongside so the figure can be checked or rebuilt
    elsewhere.
    """
    base = Path(base_path).with_suffix('')
    base.parent.mkdir(parents=True, exist_ok=True)
    prefix = f'  {label} ' if label else '  '
    written = []

    for fmt in formats:
        keywords = {}
        if fmt == 'pdf':
            keywords['metadata'] = {'CreationDate': None}
        elif fmt == 'svg':
            keywords['metadata'] = {'Date': None}
        elif fmt == 'eps':
            # EPS has no alpha channel and no transparent background. Nothing
            # here uses alpha any more (see lighten), so this is safe, but the
            # deliverable is the PDF -- the guide's advice is to export EPS
            # from Illustrator if a journal insists.
            keywords['transparent'] = False
        path = base.with_suffix(f'.{fmt}')
        fig.savefig(path, format=fmt, **keywords)
        written.append(path)
        print(f'{prefix}{fmt.upper()}: {path}')

    if data is not None:
        print(f'{prefix}CSV: {write_plot_data(base, data)}')

    if verify:
        for path in written:
            if path.suffix == '.pdf':
                print(verify_pdf(path, indent='  '))

    return written


def _poppler(tool, path):
    """Run one poppler tool, or return None if it is not installed."""
    binary = shutil.which(tool)
    if binary is None:
        return None
    return subprocess.run([binary, *(['-list'] if tool == 'pdfimages' else []), str(path)],
                          capture_output=True, text=True, check=True).stdout


def _parse_pdffonts(output):
    """Rows of (name, type, embedded) from pdffonts' fixed-width table.

    Parsed from the right, because the type column contains spaces
    ("CID TrueType") and the trailing object ID is two tokens.
    """
    fonts = []
    for line in output.splitlines():
        if not line.strip() or line.startswith('name') or set(line.strip()) <= {'-', ' '}:
            continue
        tokens = line.split()
        if len(tokens) < 8:
            continue
        name, embedded = tokens[0], tokens[-5]
        font_type = ' '.join(tokens[1:-6])
        fonts.append((name, font_type, embedded == 'yes'))
    return fonts


def verify_pdf(path, expect_mm=None, indent=''):
    """The guide's section 7 checks, as a printable block.

    Reports page size in millimetres, every font with its type and embedding
    state, and any raster image. Fails on a Type 3 font (outlined text in
    Illustrator), on a font that is not embedded, and on a page size that does
    not match `expect_mm` when one is given.
    """
    path = Path(path)
    lines = [f'{indent}verify {path.name}']
    problems = []

    info = _poppler('pdfinfo', path)
    if info is None:
        return f'{indent}verify {path.name}: poppler not installed; not verified'

    size_line = next((ln for ln in info.splitlines() if ln.startswith('Page size:')), '')
    width_pt = height_pt = None
    if size_line:
        parts = size_line.split()
        width_pt, height_pt = float(parts[2]), float(parts[4])
        lines.append(f'{indent}  page size  {width_pt:.1f} x {height_pt:.1f} pt'
                     f'  = {width_pt / _PT_PER_MM:.1f} x {height_pt / _PT_PER_MM:.1f} mm')
        if expect_mm is not None:
            for got, want, axis in ((width_pt, expect_mm[0], 'width'),
                                    (height_pt, expect_mm[1], 'height')):
                if abs(got / _PT_PER_MM - want) > 0.2:
                    problems.append(f'{axis} is {got / _PT_PER_MM:.1f} mm, expected {want} mm')

    fonts = _parse_pdffonts(_poppler('pdffonts', path) or '')
    if not fonts:
        problems.append('no fonts: the text has been outlined, or there is no text')
    for name, font_type, embedded in fonts:
        flag = ''
        if 'Type 3' in font_type:
            problems.append(f'{name} is {font_type}: Illustrator opens it as paths')
            flag = '  <-- PROBLEM'
        elif not embedded:
            problems.append(f'{name} is not embedded')
            flag = '  <-- PROBLEM'
        lines.append(f'{indent}  font       {name:<28} {font_type:<14} '
                     f'emb={"yes" if embedded else "no"}{flag}')

    images = [ln for ln in (_poppler('pdfimages', path) or '').splitlines()
              if ln.split()[:1] and ln.split()[0].isdigit()]
    lines.append(f'{indent}  images     '
                 + (f'{len(images)} raster image(s) -- intentional only if a dense layer '
                    f'was rasterized' if images else 'none (fully vector)'))
    if images:
        problems.extend(f'unexpected raster image: {ln.strip()}' for ln in images)

    lines.append(f'{indent}  {"PASS" if not problems else "FAIL"}')
    lines.extend(f'{indent}    - {problem}' for problem in problems)
    return '\n'.join(lines)


if __name__ == '__main__':
    apply_style()
    if len(sys.argv) > 1:
        for argument in sys.argv[1:]:
            print(verify_pdf(argument))
    else:
        print(f'style: {STYLE_FILE}')
        for key in ('pdf.fonttype', 'ps.fonttype', 'svg.fonttype', 'font.sans-serif',
                    'font.size', 'figure.constrained_layout.use', 'savefig.dpi'):
            print(f'  {key} = {matplotlib.rcParams[key]}')
        print('\nusage: python figure_style.py FIGURE.pdf [...]  to verify saved PDFs')
