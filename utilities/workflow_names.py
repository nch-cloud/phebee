#!/usr/bin/env python3
"""Reader-facing names for the seven interactive performance workflows.

The performance harness identifies workflows by function key
(`basic_subjects_query`, `paginated_large_cohort`, ...). Those keys are stable
and must not be renamed: they are the join key into the retained
`api_run.json` and `table4_latency.csv` artifacts, so renaming them would
orphan every stored measurement.

Figures, however, are read alongside the prose, and the prose names these
workflows differently. Deriving a label from the key with
`endpoint.replace('_', ' ').title()` produced seven labels, none of which
matched what the manuscript says -- most visibly `paginated_large_cohort`,
which rendered as "Paginated Large Cohort" even though the workflow never
paginates and the text calls it large-page cohort retrieval. This module is
the single mapping from key to published title, so the figures, the
Supplementary Methods workload definitions and Supplementary Table S1 agree.

Casing is sentence case, matching the prose and Table S1. Numbers match the
workload numbering in Supplementary Methods: Interactive Performance Workload
Definitions.
"""

# key -> (workload number in Supplementary Methods, published title)
WORKFLOWS = {
    'basic_subjects_query':   (1, 'Unfiltered cohort retrieval'),
    'individual_subject':     (2, 'Individual subject retrieval'),
    'hierarchy_expansion':    (3, 'Hierarchical term expansion'),
    'qualified_filtering':    (4, 'Qualifier-aware filtering'),
    'specific_phenotype':     (5, 'Single-phenotype cohort query'),
    'paginated_large_cohort': (6, 'Large-page cohort retrieval'),
    'subject_term_info':      (7, 'Subject term detail'),
}

# `version_specific_query` was removed from the harness by 596e9cb on
# 26 February 2026, three minutes into the benchmark campaign, and survives in
# only two of the 45 retained runs. The plotting scripts filter it out before
# labelling, so it is deliberately absent here: if it ever reaches this module
# that filter has been lost, and raising is the correct outcome.


def display_name(endpoint: str) -> str:
    """Published title for one harness workflow key.

    Raises rather than falling back to a derived label. A silent fallback is
    how the old `.title()` labels reached the submitted figures in the first
    place, and an unrecognized key here means either a new workflow that needs
    a published name or a lost `version_specific_query` filter -- both worth
    stopping for.
    """
    try:
        return WORKFLOWS[endpoint][1]
    except KeyError:
        raise KeyError(
            f'no published title for workflow {endpoint!r}; '
            f'known keys are {sorted(WORKFLOWS)}. Add it to WORKFLOWS in '
            f'{__file__} rather than deriving a label from the key.'
        ) from None


def workload_number(endpoint: str) -> int:
    """Supplementary Methods workload number for one harness workflow key."""
    if endpoint not in WORKFLOWS:
        display_name(endpoint)  # raises with the full message
    return WORKFLOWS[endpoint][0]
