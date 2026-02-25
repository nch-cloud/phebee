"""
Shared fixtures and utilities for PheBee performance testing.

Provides data generation capabilities for both import performance and API latency tests.
"""

from __future__ import annotations

import csv
import json
import os
import random
import statistics
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pytest


# -----------------------------
# Config helpers
# -----------------------------

def _env_int(name: str, default: int) -> int:
    v = os.environ.get(name)
    return default if v is None else int(v)


def _env_float(name: str, default: float) -> float:
    v = os.environ.get(name)
    return default if v is None else float(v)


def _env_str(name: str, default: str) -> str:
    v = os.environ.get(name)
    return default if v is None else str(v)


def _now_ms() -> int:
    return int(time.time() * 1000)


# -----------------------------
# Clinical Data Patterns
# -----------------------------
# Disease clusters represent realistic phenotype co-occurrence patterns observed in clinical
# practice. These are based on published clinical phenotype studies and ensure synthetic
# data reflects real-world disease presentations.
#
# References:
# - Cardiomyopathy: Hershberger et al., Genetic Evaluation of Cardiomyopathy, Circulation 2018
# - Epilepsy: Scheffer et al., ILAE classification of the epilepsies, Nat Rev Neurol 2017
# - Metabolic: Ferreira et al., Inborn errors of metabolism, Nat Rev Endocrinol 2019

DISEASE_CLUSTERS = {
    "cardiomyopathy": [
        "http://purl.obolibrary.org/obo/HP_0001627",  # Abnormal heart morphology
        "http://purl.obolibrary.org/obo/HP_0001635",  # Heart failure
        "http://purl.obolibrary.org/obo/HP_0001644",  # Dilated cardiomyopathy
        "http://purl.obolibrary.org/obo/HP_0001678",  # Atrioventricular block
        "http://purl.obolibrary.org/obo/HP_0000822",  # Hypertension
    ],
    "epilepsy": [
        "http://purl.obolibrary.org/obo/HP_0001250",  # Seizures
        "http://purl.obolibrary.org/obo/HP_0002069",  # Bilateral tonic-clonic seizures
        "http://purl.obolibrary.org/obo/HP_0001298",  # Encephalopathy
        "http://purl.obolibrary.org/obo/HP_0002376",  # Developmental regression
        "http://purl.obolibrary.org/obo/HP_0001508",  # Failure to thrive
    ],
    "metabolic": [
        "http://purl.obolibrary.org/obo/HP_0001943",  # Hyperglycemia
        "http://purl.obolibrary.org/obo/HP_0000819",  # Diabetes mellitus
        "http://purl.obolibrary.org/obo/HP_0001513",  # Obesity
        "http://purl.obolibrary.org/obo/HP_0002155",  # Hypertriglyceridemia
        "http://purl.obolibrary.org/obo/HP_0001508",  # Failure to thrive
    ],
    "oncology": [
        "http://purl.obolibrary.org/obo/HP_0002664",  # Neoplasm
        "http://purl.obolibrary.org/obo/HP_0001508",  # Failure to thrive
        "http://purl.obolibrary.org/obo/HP_0000822",  # Hypertension
        "http://purl.obolibrary.org/obo/HP_0001943",  # Hyperglycemia
    ],
    "rare_dysmorphic": [
        "http://purl.obolibrary.org/obo/HP_0000316",  # Hypertelorism
        "http://purl.obolibrary.org/obo/HP_0001999",  # Abnormal facial shape
        "http://purl.obolibrary.org/obo/HP_0002007",  # Frontal bossing
        "http://purl.obolibrary.org/obo/HP_0004322",  # Short stature
        "http://purl.obolibrary.org/obo/HP_0001249",  # Intellectual disability
    ],
}

# Specialty to phenotype mappings for realistic provider attribution
SPECIALTY_PHENOTYPES = {
    "cardiology": [
        "http://purl.obolibrary.org/obo/HP_0001627",
        "http://purl.obolibrary.org/obo/HP_0001635",
        "http://purl.obolibrary.org/obo/HP_0001644",
        "http://purl.obolibrary.org/obo/HP_0001678",
        "http://purl.obolibrary.org/obo/HP_0000822",
    ],
    "neurology": [
        "http://purl.obolibrary.org/obo/HP_0001250",
        "http://purl.obolibrary.org/obo/HP_0002069",
        "http://purl.obolibrary.org/obo/HP_0001298",
        "http://purl.obolibrary.org/obo/HP_0002376",
    ],
    "endocrinology": [
        "http://purl.obolibrary.org/obo/HP_0001943",
        "http://purl.obolibrary.org/obo/HP_0000819",
        "http://purl.obolibrary.org/obo/HP_0001513",
        "http://purl.obolibrary.org/obo/HP_0002155",
    ],
    "oncology": [
        "http://purl.obolibrary.org/obo/HP_0002664",
    ],
    "genetics": [
        "http://purl.obolibrary.org/obo/HP_0000316",
        "http://purl.obolibrary.org/obo/HP_0001999",
        "http://purl.obolibrary.org/obo/HP_0002007",
        "http://purl.obolibrary.org/obo/HP_0004322",
        "http://purl.obolibrary.org/obo/HP_0001249",
    ],
}

# Evidence importance weights (affects documentation frequency)
# More important findings are documented more frequently in clinical notes
EVIDENCE_IMPORTANCE = {
    "chief_complaint": (5, 12),   # Heavily documented (primary complaint)
    "active_problem": (2, 6),     # Moderate documentation (active issue)
    "past_history": (1, 3),       # Light documentation (historical)
    "incidental": (1, 2),         # Minimal documentation (incidental finding)
}


# -----------------------------
# Term universe (from JSON)
# -----------------------------

@dataclass(frozen=True)
class TermUniverse:
    """Represents the HPO term universe loaded from JSON."""
    all_terms: List[str]
    internal_terms: List[str]
    leaf_terms: List[str]
    common_terms: List[str]
    rare_terms: List[str]
    metadata: Dict[str, Any]

    def pick_common(self, rng: random.Random) -> str:
        return rng.choice(self.common_terms or self.all_terms)

    def pick_rare(self, rng: random.Random) -> str:
        return rng.choice(self.rare_terms or self.leaf_terms or self.all_terms)

    def pick_any(self, rng: random.Random) -> str:
        return rng.choice(self.all_terms)


def _load_prevalence_csv(path: str) -> Dict[str, float]:
    """
    Load term prevalence data from CSV.

    Expected CSV format:
        term_iri,frequency
        http://purl.obolibrary.org/obo/HP_0001945,15234
        http://purl.obolibrary.org/obo/HP_0011134,12891
        ...

    The loader flexibly handles common column name variations:
    - Term column: 'term_iri', 'term', 'iri', 'hpo_term', 'phenotype'
    - Frequency column: 'frequency', 'count', 'prevalence', 'freq', 'n', 'occurrences'

    Returns: Dict mapping term_iri -> frequency_value
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Prevalence CSV not found: {path}")

    prevalence_map: Dict[str, float] = {}

    with p.open('r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError(f"CSV file is empty or has no header: {path}")

        # Detect column names (flexible matching)
        term_col = None
        frequency_col = None

        for col in reader.fieldnames:
            col_lower = col.lower()
            if col_lower in ('term_iri', 'term', 'iri', 'hpo_term', 'phenotype'):
                term_col = col
            if col_lower in ('frequency', 'count', 'prevalence', 'freq', 'n', 'occurrences'):
                frequency_col = col

        if not term_col or not frequency_col:
            raise ValueError(
                f"Could not identify term and frequency columns in CSV. "
                f"Available columns: {reader.fieldnames}. "
                f"Expected: 'term_iri' and 'frequency' (or similar variations)."
            )

        for row in reader:
            term_iri = row.get(term_col, "").strip()
            frequency_str = row.get(frequency_col, "").strip()

            if not term_iri or not frequency_str:
                continue

            try:
                frequency = float(frequency_str)
                prevalence_map[term_iri] = frequency
            except (ValueError, TypeError):
                # Skip rows with invalid frequency values
                continue

    if not prevalence_map:
        raise ValueError(f"No valid term frequency data loaded from CSV: {path}")

    return prevalence_map


def _derive_common_and_rare_from_prevalence(
    all_terms: List[str],
    prevalence_map: Dict[str, float],
    n_common: int = 2000,
    n_rare: int = 5000,
) -> Tuple[List[str], List[str]]:
    """
    Derive common and rare term pools based on actual prevalence data.

    Common terms = top N by prevalence
    Rare terms = bottom N by prevalence (excluding zero-prevalence terms)

    Returns: (common_terms, rare_terms)
    """
    # Filter to terms that exist in both all_terms and prevalence_map
    terms_with_prevalence = [
        (term, prevalence_map[term])
        for term in all_terms
        if term in prevalence_map
    ]

    if not terms_with_prevalence:
        # No overlap - return empty lists (caller will use fallback)
        return [], []

    # Sort by prevalence (descending)
    sorted_by_prevalence = sorted(terms_with_prevalence, key=lambda x: x[1], reverse=True)

    # Common: top N high-prevalence terms
    common_terms = [term for term, _ in sorted_by_prevalence[:n_common]]

    # Rare: bottom N low-prevalence terms (but exclude zeros - those might be errors)
    nonzero_sorted = [(t, p) for t, p in sorted_by_prevalence if p > 0]
    rare_terms = [term for term, _ in nonzero_sorted[-n_rare:]] if len(nonzero_sorted) > n_rare else [term for term, _ in nonzero_sorted]

    return common_terms, rare_terms


def load_terms_json(path: str, prevalence_csv_path: Optional[str] = None) -> TermUniverse:
    """
    Load and parse the HPO terms JSON file with flexible schema support.

    Args:
        path: Path to terms JSON file
        prevalence_csv_path: Optional path to CSV with term frequency data (e.g., "term_iri,frequency").
                            Uses actual clinical frequency to define common/rare term pools.
                            Falls back to hierarchy-based heuristic if not provided.

    Expected prevalence CSV format:
        term_iri,frequency
        http://purl.obolibrary.org/obo/HP_0001945,15234
        http://purl.obolibrary.org/obo/HP_0011134,12891

    The frequency can be raw counts, proportions, or any numeric measure of prevalence.
    Common terms = top 2000 by frequency; Rare terms = bottom 5000 by frequency.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"PHEBEE_EVAL_TERMS_JSON_PATH not found: {path}")

    data = json.loads(p.read_text())

    # Flexible parsing: support a few reasonable shapes.
    metadata = data.get("metadata", {}) if isinstance(data, dict) else {}

    # Helper to convert compact ID to full IRI
    def _to_iri(term_id: str) -> str:
        """Convert HP:0000001 to http://purl.obolibrary.org/obo/HP_0000001"""
        if term_id.startswith("http://") or term_id.startswith("https://"):
            return term_id
        # Convert HP:0000001 to HP_0000001
        iri_suffix = term_id.replace(":", "_")
        iri_prefix = metadata.get("iri_prefix", "http://purl.obolibrary.org/obo/")
        return f"{iri_prefix}{iri_suffix}"

    if isinstance(data, list):
        # Simple list format
        all_terms = [_to_iri(str(x)) for x in data]
        internal_terms: List[str] = []
        leaf_terms: List[str] = []
        common_terms: List[str] = []
        rare_terms: List[str] = []
    else:
        # Check for pools structure (generated HPO JSON format)
        pools = data.get("pools", {})
        if pools:
            all_terms = [_to_iri(str(x)) for x in pools.get("all", [])]
            internal_terms = [_to_iri(str(x)) for x in pools.get("internal", [])]
            leaf_terms = [_to_iri(str(x)) for x in pools.get("leaf", [])]
            common_terms = [_to_iri(str(x)) for x in pools.get("common", [])]
            rare_terms = [_to_iri(str(x)) for x in pools.get("rare", [])]
        else:
            # Fallback: check for direct keys
            def _get_list(*keys: str) -> List[str]:
                for k in keys:
                    v = data.get(k)
                    if isinstance(v, list) and v:
                        return [_to_iri(str(x)) for x in v]
                return []

            all_terms = _get_list("all_terms", "all", "terms", "allHpoTerms")
            internal_terms = _get_list("internal_terms", "internal")
            leaf_terms = _get_list("leaf_terms", "leaf", "leaves")
            common_terms = _get_list("common_terms", "common")
            rare_terms = _get_list("rare_terms", "rare")

            # If the file only contains one list, treat it as all_terms.
            if not all_terms:
                # Try to detect a dict-of-lists and pick the longest list as "all_terms".
                candidate_lists = [v for v in data.values() if isinstance(v, list)]
                if candidate_lists:
                    all_terms = [_to_iri(str(x)) for x in max(candidate_lists, key=len)]

    if not all_terms:
        raise ValueError(f"Could not find any term lists in terms JSON: {path}")

    # Derive fallbacks if pools are missing (keeps the generator robust).
    # Internal ~= non-leaf; leaf ~= provided or last-resort: all_terms.
    internal_terms = internal_terms or all_terms
    leaf_terms = leaf_terms or all_terms

    # Derive "common" and "rare" pools
    # Priority 1: Use prevalence CSV if provided (scientifically accurate)
    if prevalence_csv_path and not common_terms and not rare_terms:
        try:
            prevalence_map = _load_prevalence_csv(prevalence_csv_path)
            common_terms, rare_terms = _derive_common_and_rare_from_prevalence(
                all_terms, prevalence_map, n_common=2000, n_rare=5000
            )
            if common_terms and rare_terms:
                metadata["prevalence_source"] = prevalence_csv_path
        except Exception as e:
            # Log warning but continue with fallback
            import warnings
            warnings.warn(f"Could not load prevalence data from {prevalence_csv_path}: {e}. Using fallback.")

    # Priority 2: Use explicit lists from JSON if provided
    # (already loaded above in _get_list calls)

    # Priority 3: Fallback heuristic based on hierarchy position (less accurate)
    if not common_terms:
        common_terms = internal_terms[: min(2000, len(internal_terms))]
    if not rare_terms:
        rare_terms = leaf_terms[: min(5000, len(leaf_terms))]

    # Deduplicate while preserving order.
    def _dedupe(xs: List[str]) -> List[str]:
        seen = set()
        out = []
        for x in xs:
            if x not in seen:
                out.append(x)
                seen.add(x)
        return out

    return TermUniverse(
        all_terms=_dedupe(all_terms),
        internal_terms=_dedupe(internal_terms),
        leaf_terms=_dedupe(leaf_terms),
        common_terms=_dedupe(common_terms),
        rare_terms=_dedupe(rare_terms),
        metadata=metadata,
    )


# -----------------------------
# Synthetic data generation
# -----------------------------

@dataclass(frozen=True)
class QualifierContext:
    """Represents qualifier context for a term link (negated, family history, hypothetical)."""
    negated: bool = False
    family: bool = False
    hypothetical: bool = False

    def as_contexts(self) -> Dict[str, float]:
        return {
            "negated": 1.0 if self.negated else 0.0,
            "family": 1.0 if self.family else 0.0,
            "hypothetical": 1.0 if self.hypothetical else 0.0,
        }

    def as_qualifier_list(self) -> List[str]:
        # Only used for some endpoints; keep stable strings.
        quals: List[str] = []
        if self.negated:
            quals.append("negated")
        if self.family:
            quals.append("family_history")
        if self.hypothetical:
            quals.append("hypothetical")
        return quals


@dataclass(frozen=True)
class TermSource:
    """Represents ontology source metadata."""
    source: str
    version: str
    iri: str


def _mk_term_source_from_terms_json(universe: TermUniverse) -> TermSource:
    """
    Prefer metadata from the terms JSON; fall back to env defaults.
    """
    md = universe.metadata or {}
    source = md.get("ontology", md.get("source", "HPO"))
    version = md.get("release", md.get("version", _env_str("PHEBEE_EVAL_HPO_VERSION", "unknown")))
    iri = md.get(
        "release_iri",
        _env_str("PHEBEE_EVAL_HPO_VERSION_IRI", f"http://purl.obolibrary.org/obo/hp/releases/{version}/hp.owl"),
    )
    return TermSource(source=str(source), version=str(version), iri=str(iri))


def _parse_date(s: str) -> date:
    y, m, d = (int(x) for x in s.split("-"))
    return date(y, m, d)


def _rand_date_iso(rng: random.Random, start: date, end: date) -> str:
    if end < start:
        start, end = end, start
    days = (end - start).days
    offset = rng.randint(0, days if days > 0 else 0)
    return (start + timedelta(days=offset)).isoformat()


def _pick_qualifier_context(
    rng: random.Random,
    negated_pct: float,
    family_pct: float,
    hypothetical_pct: float,
) -> QualifierContext:
    """
    Choose *at most one* qualifier category for a TermLink (simplifies uniqueness + interpretation).
    Probability mass that doesn't land in a qualifier becomes "unqualified".
    """
    x = rng.random()
    if x < negated_pct:
        return QualifierContext(negated=True)
    x -= negated_pct
    if x < family_pct:
        return QualifierContext(family=True)
    x -= family_pct
    if x < hypothetical_pct:
        return QualifierContext(hypothetical=True)
    return QualifierContext()


def _draw_int_uniform(rng: random.Random, lo: int, hi: int) -> int:
    lo2 = min(lo, hi)
    hi2 = max(lo, hi)
    return rng.randint(lo2, hi2)


def _choose_terms_for_subject(
    rng: random.Random,
    universe: TermUniverse,
    k_terms: int,
    *,
    anchor_term_pct: float,
    common_term_pct: float,
    anchor_terms: Sequence[str],
) -> List[str]:
    """
    k_terms unique term IRIs:
    - With probability anchor_term_pct, include one anchor term (cardio/neuro).
    - Remaining terms sampled without replacement from mixture:
        - common_term_pct from common pool
        - remainder from rare pool
    """
    picked: List[str] = []
    seen = set()

    if k_terms <= 0:
        return picked

    if rng.random() < anchor_term_pct and anchor_terms:
        t = rng.choice(list(anchor_terms))
        picked.append(t)
        seen.add(t)

    # Sample remaining terms without replacement. If pools are small, we fall back to all_terms.
    while len(picked) < k_terms:
        use_common = rng.random() < common_term_pct
        cand = universe.pick_common(rng) if use_common else universe.pick_rare(rng)
        if cand in seen:
            continue
        picked.append(cand)
        seen.add(cand)

        # Safety: if we can't find enough unique terms, break.
        if len(seen) > len(universe.all_terms) - 5:
            break

    return picked


def _choose_terms_with_disease_clustering(
    rng: random.Random,
    universe: TermUniverse,
    k_terms: int,
    *,
    anchor_term_pct: float,
    common_term_pct: float,
    anchor_terms: Sequence[str],
    disease_cluster_pct: float = 0.60,
) -> Tuple[List[str], Optional[str]]:
    """
    Choose terms with realistic disease clustering patterns.

    Args:
        rng: Random number generator
        universe: TermUniverse with term pools
        k_terms: Number of terms to select
        anchor_term_pct: Probability of including an anchor term
        common_term_pct: For non-cluster terms, probability of selecting from common pool
        anchor_terms: List of anchor terms to choose from
        disease_cluster_pct: Probability that subject has clustered phenotypes (default: 0.60)

    Returns:
        Tuple of (selected_terms, cluster_name) where cluster_name is None for non-clustered subjects
    """
    picked: List[str] = []
    seen = set()
    cluster_name: Optional[str] = None

    if k_terms <= 0:
        return picked, cluster_name

    # Determine if this subject has disease clustering (60% of subjects)
    use_clustering = rng.random() < disease_cluster_pct

    if use_clustering:
        # Pick a disease cluster
        cluster_name = rng.choice(list(DISEASE_CLUSTERS.keys()))
        cluster_terms = DISEASE_CLUSTERS[cluster_name]

        # Sample 3-5 terms from the cluster (or fewer if k_terms is small)
        n_cluster_terms = min(k_terms, rng.randint(3, min(5, len(cluster_terms))))

        # Sample from cluster, weighted by prevalence if available
        cluster_sample = rng.sample(cluster_terms, min(n_cluster_terms, len(cluster_terms)))
        picked.extend(cluster_sample)
        seen.update(cluster_sample)

    # Include anchor term with probability (if not already in cluster)
    if rng.random() < anchor_term_pct and anchor_terms:
        t = rng.choice(list(anchor_terms))
        if t not in seen:
            picked.append(t)
            seen.add(t)

    # Fill remaining slots with frequency-weighted selection
    while len(picked) < k_terms:
        use_common = rng.random() < common_term_pct
        cand = universe.pick_common(rng) if use_common else universe.pick_rare(rng)
        if cand in seen:
            continue
        picked.append(cand)
        seen.add(cand)

        # Safety: if we can't find enough unique terms, break
        if len(seen) > len(universe.all_terms) - 5:
            break

    return picked, cluster_name


def _get_specialty_for_term(term_iri: str) -> str:
    """Return appropriate medical specialty for a given phenotype."""
    for specialty, terms in SPECIALTY_PHENOTYPES.items():
        if term_iri in terms:
            return specialty
    return "internal_medicine"  # Default


def _get_evidence_importance(term_iri: str, is_primary: bool, cluster_name: Optional[str]) -> str:
    """
    Determine clinical importance of a phenotype for evidence generation.

    Args:
        term_iri: HPO term IRI
        is_primary: Whether this is one of the first terms for the subject
        cluster_name: Disease cluster name if subject has clustering, else None

    Returns:
        Importance level: chief_complaint, active_problem, past_history, or incidental
    """
    # Primary phenotypes are always chief complaints
    if is_primary:
        return "chief_complaint"

    # If part of a disease cluster, likely an active problem
    if cluster_name and any(term_iri in DISEASE_CLUSTERS[cluster_name] for cluster_name in DISEASE_CLUSTERS if term_iri in DISEASE_CLUSTERS[cluster_name]):
        return "active_problem"

    # Default based on rarity (this is a heuristic)
    # In real implementation, you could check if term_iri is in common vs rare pools
    return "active_problem"


def _generate_evidence_count(rng: random.Random, importance: str) -> int:
    """Generate realistic evidence count based on clinical importance."""
    min_ev, max_ev = EVIDENCE_IMPORTANCE[importance]
    return rng.randint(min_ev, max_ev)


def _mk_evidence_item(
    *,
    creator_type: str,
    creator_id: str,
    creator_name: str,
    encounter_id: str,
    clinical_note_id: str,
    note_timestamp: str,
    qualifier_ctx: QualifierContext,
    term_source: TermSource,
    rng: random.Random,
) -> Dict[str, Any]:
    # Keep fields aligned with your existing scale harness + manuscript text.
    span_start = rng.randint(0, 200)
    span_end = span_start + rng.randint(5, 40)

    return {
        "type": "clinical_note",
        "clinical_note_id": clinical_note_id,
        "encounter_id": encounter_id,
        "evidence_creator_id": creator_id,
        "evidence_creator_type": creator_type,  # "automated" | "manual"
        "evidence_creator_name": creator_name,
        "note_timestamp": note_timestamp,
        # Optional but realistic-ish metadata
        "note_type": "progress_note",
        "provider_type": "physician",
        "author_specialty": "pediatrics",
        "span_start": span_start,
        "span_end": span_end,
        "contexts": qualifier_ctx.as_contexts(),
        # Term source metadata (record-level is canonical; duplicating here can be useful for audits)
        "term_source": {"source": term_source.source, "version": term_source.version, "iri": term_source.iri},
    }


def _mk_record(
    *,
    project_id: str,
    project_subject_id: str,
    term_iri: str,
    evidence_items: List[Dict[str, Any]],
    row_num: int,
    batch_id: int,
    qualifier_ctx: QualifierContext,
    term_source: TermSource,
) -> Dict[str, Any]:
    return {
        "project_id": project_id,
        "project_subject_id": project_subject_id,
        "term_iri": term_iri,
        "evidence": evidence_items,
        "row_num": row_num,
        "batch_id": batch_id,
        "qualifiers": qualifier_ctx.as_qualifier_list(),
        "term_source": {"source": term_source.source, "version": term_source.version, "iri": term_source.iri},
    }


@dataclass(frozen=True)
class GeneratedDataset:
    """Container for generated synthetic dataset and its statistics."""
    records: List[Dict[str, Any]] | None  # None when using lazy loading for large datasets
    stats: Dict[str, Any]
    anchor_terms: List[str]
    parent_term: str
    subject_id_map: Dict[str, str] = None  # Optional: maps old subject_ids to new UUIDs (for benchmark loads)
    benchmark_dir: Path | None = None  # Optional: path to benchmark dataset directory for streaming


def generate_scale_dataset(
    *,
    project_id: str,
    universe: TermUniverse,
    n_subjects: int,
    min_terms: int,
    max_terms: int,
    min_evidence: int,
    max_evidence: int,
    rng_seed: int | None = None,
    use_disease_clustering: bool = False,
) -> GeneratedDataset:
    """
    Generate synthetic PheBee evidence dataset.

    Args:
        project_id: PheBee project identifier
        universe: TermUniverse containing HPO terms to sample from
        n_subjects: Number of subjects to generate
        min_terms: Minimum HPO terms per subject
        max_terms: Maximum HPO terms per subject
        min_evidence: Minimum evidence items per term link
        max_evidence: Maximum evidence items per term link
        rng_seed: Random seed for reproducibility (defaults to current timestamp)
        use_disease_clustering: Enable realistic disease clustering patterns (default: False)

    Returns:
        GeneratedDataset with records (NDJSON-ready dicts) and statistics
    """
    if rng_seed is None:
        rng_seed = _now_ms()

    rng = random.Random(rng_seed)

    note_date_start = _parse_date(_env_str("PHEBEE_EVAL_NOTE_DATE_START", "2023-01-01"))
    note_date_end = _parse_date(_env_str("PHEBEE_EVAL_NOTE_DATE_END", "2024-12-31"))

    negated_pct = _env_float("PHEBEE_EVAL_QUAL_NEGATED_PCT", 0.15)
    family_pct = _env_float("PHEBEE_EVAL_QUAL_FAMILY_PCT", 0.08)
    hypothetical_pct = _env_float("PHEBEE_EVAL_QUAL_HYPOTHETICAL_PCT", 0.05)

    common_term_pct = _env_float("PHEBEE_EVAL_COMMON_TERM_PCT", 0.70)
    anchor_term_pct = _env_float("PHEBEE_EVAL_ANCHOR_TERM_PCT", 0.60)

    term_source = _mk_term_source_from_terms_json(universe)

    # Anchor terms: prefer real, widely used HPO terms that support interpretable cohort queries.
    anchor_terms = [
        "http://purl.obolibrary.org/obo/HP_0001627",  # Abnormality of the cardiovascular system
        "http://purl.obolibrary.org/obo/HP_0000707",  # Abnormality of the nervous system
    ]
    # Parent term used for descendant-expansion style queries.
    # Use mid-level term (not root) to avoid performance issues with massive hierarchy expansion.
    parent_term = "http://purl.obolibrary.org/obo/HP_0001507"  # Growth abnormality

    records: List[Dict[str, Any]] = []
    row_num = 0

    # Track distributions for reporting
    terms_per_subject: List[int] = []
    evidence_per_record: List[int] = []
    qualifier_counts = {"negated": 0, "family": 0, "hypothetical": 0, "unqualified": 0}
    cluster_counts: Dict[str, int] = {name: 0 for name in DISEASE_CLUSTERS.keys()}
    cluster_counts["no_cluster"] = 0

    for s in range(n_subjects):
        project_subject_id = f"perf-subj-{s:06d}"
        k_terms = _draw_int_uniform(rng, min_terms, max_terms)
        terms_per_subject.append(k_terms)

        # Choose terms with or without disease clustering
        subject_cluster_name: Optional[str] = None
        if use_disease_clustering:
            subject_terms, subject_cluster_name = _choose_terms_with_disease_clustering(
                rng,
                universe,
                k_terms,
                anchor_term_pct=anchor_term_pct,
                common_term_pct=common_term_pct,
                anchor_terms=anchor_terms,
                disease_cluster_pct=0.60,
            )
            if subject_cluster_name:
                cluster_counts[subject_cluster_name] += 1
            else:
                cluster_counts["no_cluster"] += 1
        else:
            subject_terms = _choose_terms_for_subject(
                rng,
                universe,
                k_terms,
                anchor_term_pct=anchor_term_pct,
                common_term_pct=common_term_pct,
                anchor_terms=anchor_terms,
            )
            cluster_counts["no_cluster"] += 1

        # Ensure deterministic-but-random encounter ID per subject (helps with "realistic" repeated context).
        encounter_id = f"enc-{s:06d}"

        for t_idx, term_iri in enumerate(subject_terms):
            # Determine evidence importance and count
            is_primary = t_idx < 3  # First 3 terms are primary
            if use_disease_clustering:
                importance = _get_evidence_importance(term_iri, is_primary, subject_cluster_name)
                # Scale evidence count based on importance
                base_evidence = _draw_int_uniform(rng, min_evidence, max_evidence)
                importance_multiplier = _generate_evidence_count(rng, importance) / max(1, (min_evidence + max_evidence) / 2)
                m_evidence = max(1, int(base_evidence * importance_multiplier))
            else:
                m_evidence = _draw_int_uniform(rng, min_evidence, max_evidence)

            evidence_per_record.append(m_evidence)

            qualifier_ctx = _pick_qualifier_context(rng, negated_pct, family_pct, hypothetical_pct)
            if qualifier_ctx.negated:
                qualifier_counts["negated"] += 1
            elif qualifier_ctx.family:
                qualifier_counts["family"] += 1
            elif qualifier_ctx.hypothetical:
                qualifier_counts["hypothetical"] += 1
            else:
                qualifier_counts["unqualified"] += 1

            # Get specialty for term if clustering is enabled
            if use_disease_clustering:
                specialty = _get_specialty_for_term(term_iri)
                creator_id = f"ods/phebee-{specialty}:v1"
                creator_name = f"PheBee {specialty.title()} NLP"
            else:
                specialty = "general"
                creator_id = "ods/phebee-eval:automated-v1"
                creator_name = "PheBee Eval NLP"

            evidence_items: List[Dict[str, Any]] = []
            for e in range(m_evidence):
                clinical_note_id = f"note-{s:06d}-{t_idx:03d}-{e:03d}"
                evidence_items.append(
                    _mk_evidence_item(
                        creator_type="automated",
                        creator_id=creator_id,
                        creator_name=creator_name,
                        encounter_id=encounter_id,
                        clinical_note_id=clinical_note_id,
                        note_timestamp=_rand_date_iso(rng, note_date_start, note_date_end),
                        qualifier_ctx=qualifier_ctx,
                        term_source=term_source,
                        rng=rng,
                    )
                )

            # Add one manual evidence item for a single well-known assertion (R5-ish coverage),
            # without changing the TermLink identity (same qualifier context).
            if s == 0 and t_idx == 0:
                evidence_items.append(
                    _mk_evidence_item(
                        creator_type="manual",
                        creator_id="ods/phebee-eval:curator-1",
                        creator_name="PheBee Eval Curator",
                        encounter_id=encounter_id,
                        clinical_note_id=f"note-{s:06d}-{t_idx:03d}-manual",
                        note_timestamp=_rand_date_iso(rng, note_date_start, note_date_end),
                        qualifier_ctx=qualifier_ctx,
                        term_source=term_source,
                        rng=rng,
                    )
                )
                evidence_per_record[-1] += 1  # reflect the manual evidence

            rec = _mk_record(
                project_id=project_id,
                project_subject_id=project_subject_id,
                term_iri=term_iri,
                evidence_items=evidence_items,
                row_num=row_num,
                batch_id=0,  # overwritten when batching for S3
                qualifier_ctx=qualifier_ctx,
                term_source=term_source,
            )
            # Helpful: carry parent term for hierarchy query selection
            if s == 0 and t_idx == 0:
                rec["_eval_parent_term_for_descendant_test"] = parent_term
            row_num += 1
            records.append(rec)

    # Derived totals
    n_records = len(records)
    n_evidence_total = sum(len(r["evidence"]) for r in records)

    def _summary(xs: List[int]) -> Dict[str, Any]:
        if not xs:
            return {"min": None, "max": None, "mean": None, "median": None}
        return {
            "min": int(min(xs)),
            "max": int(max(xs)),
            "mean": float(statistics.fmean(xs)),
            "median": float(statistics.median(xs)),
        }

    dataset_stats: Dict[str, Any] = {
        "n_subjects": n_subjects,
        "n_records": n_records,
        "n_evidence": n_evidence_total,
        "n_unique_terms": len({r["term_iri"] for r in records}),
        "terms_per_subject": _summary(terms_per_subject),
        "evidence_per_record": _summary(evidence_per_record),
        "qualifier_distribution_records": qualifier_counts,
        "term_source": {"source": term_source.source, "version": term_source.version, "iri": term_source.iri},
        "generator_seed": rng_seed,
        "anchor_terms": anchor_terms,
        "disease_clustering_enabled": use_disease_clustering,
    }

    # Add cluster statistics if clustering was enabled
    if use_disease_clustering:
        dataset_stats["cluster_distribution"] = cluster_counts

    return GeneratedDataset(
        records=records,
        stats=dataset_stats,
        anchor_terms=anchor_terms,
        parent_term=parent_term,
    )


# -----------------------------
# Pytest Fixtures
# -----------------------------

@pytest.fixture(scope="session")
def hpo_terms_universe() -> TermUniverse:
    """
    Load HPO terms JSON file once per test session.

    Environment variables:
    - PHEBEE_EVAL_TERMS_JSON_PATH (required): HPO terms list
    - PHEBEE_EVAL_PREVALENCE_CSV_PATH (optional): CSV with term_iri,frequency columns
      for scientifically accurate common/rare term classification based on actual data
    """
    terms_path = os.environ.get("PHEBEE_EVAL_TERMS_JSON_PATH")
    if not terms_path:
        pytest.skip("Set PHEBEE_EVAL_TERMS_JSON_PATH to run performance tests.")

    prevalence_csv_path = os.environ.get("PHEBEE_EVAL_PREVALENCE_CSV_PATH")
    return load_terms_json(terms_path, prevalence_csv_path=prevalence_csv_path)


@pytest.fixture(scope="session")
def test_project_id(cloudformation_stack):
    """
    Override parent conftest fixture with session scope for performance tests.

    This ensures both import and latency tests use the same project, allowing
    the latency test to query data imported by the import test.

    Environment Variables:
    - PHEBEE_EVAL_PROJECT_ID: If set, uses existing project instead of creating new one.
      Useful for iterating on latency tests without re-running imports.
    """
    # Check for existing project override
    existing_project = os.environ.get("PHEBEE_EVAL_PROJECT_ID")
    if existing_project:
        print(f"\n[Performance Tests] Using existing project: {existing_project}")
        yield existing_project
        return

    # Create new project
    import boto3
    import json
    import uuid

    def get_client(service_name: str):
        return boto3.client(service_name)

    lambda_client = get_client("lambda")
    project_id = f"test_project_{uuid.uuid4().hex[:8]}"
    payload = {"project_id": project_id, "project_label": "Performance Test Project"}

    print(f"\n[Performance Tests] Creating shared project: {project_id}")

    # Create the project by invoking the deployed Lambda
    create_response = lambda_client.invoke(
        FunctionName=f"{cloudformation_stack}-CreateProjectFunction",
        InvocationType="RequestResponse",
        Payload=json.dumps({"body": json.dumps(payload)}),
    )

    lambda_response = json.loads(create_response["Payload"].read().decode("utf-8"))
    assert create_response["StatusCode"] == 200, f"Invoke failed: {lambda_response}"

    # Handle both direct response and API Gateway format
    if "statusCode" in lambda_response:
        assert lambda_response["statusCode"] == 200, f"Lambda failed: {lambda_response}"
        body = json.loads(lambda_response["body"])
    else:
        # Direct Lambda response
        body = lambda_response

    assert body.get("project_created") is True
    print(f"[Performance Tests] Project created successfully: {body.get('project_id')}")

    yield body.get("project_id")


def _load_benchmark_dataset(benchmark_dir: Path, test_project_id: str, lazy: bool = False) -> GeneratedDataset:
    """
    Load a pre-generated benchmark dataset from disk.

    Args:
        benchmark_dir: Path to benchmark dataset directory (contains metadata.json and batches/)
        test_project_id: Project ID to use (overrides the one in metadata)
        lazy: If True, skip loading records into memory (for large datasets). Only loads metadata.

    Returns:
        GeneratedDataset with loaded records and statistics (records=None if lazy=True)
    """
    metadata_path = benchmark_dir / "metadata.json"
    batches_dir = benchmark_dir / "batches"

    if not metadata_path.exists():
        raise FileNotFoundError(f"Benchmark metadata not found: {metadata_path}")
    if not batches_dir.exists():
        raise FileNotFoundError(f"Benchmark batches directory not found: {batches_dir}")

    # Load metadata
    with metadata_path.open("r", encoding="utf-8") as f:
        metadata = json.load(f)

    batch_files = sorted(batches_dir.glob("batch-*.json"))
    if not batch_files:
        raise ValueError(f"No batch files found in {batches_dir}")

    print(f"Loading benchmark dataset from {benchmark_dir}")
    print(f"  Found {len(batch_files)} batch files")

    # Extract statistics from metadata
    stats = metadata.get("dataset_statistics", {})
    anchor_terms = metadata.get("anchor_terms", [])
    parent_term = metadata.get("parent_term_for_queries", "http://purl.obolibrary.org/obo/HP_0001507")

    # Build subject_id mapping by scanning files (needed for upload streaming)
    subject_id_map = {}

    if lazy:
        # Lazy mode: scan for subject IDs but don't load records
        print(f"  Lazy loading: scanning for subject IDs without loading records into memory")
        for batch_file in batch_files:
            with batch_file.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        record = json.loads(line)
                        old_subject_id = record.get("subject_id") or record.get("project_subject_id")
                        if old_subject_id and old_subject_id not in subject_id_map:
                            import uuid
                            subject_id_map[old_subject_id] = str(uuid.uuid4())

        print(f"  Generated {len(subject_id_map)} new subject IDs for this test run")
        print(f"  Records not loaded (lazy mode) - will stream from disk during upload")

        return GeneratedDataset(
            records=None,  # Lazy loading - records not in memory
            stats=stats,
            anchor_terms=anchor_terms,
            parent_term=parent_term,
            subject_id_map=subject_id_map,
            benchmark_dir=benchmark_dir,
        )
    else:
        # Eager mode: load all records into memory
        records: List[Dict[str, Any]] = []
        for batch_file in batch_files:
            with batch_file.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        record = json.loads(line)
                        # Override project_id to use the current test project
                        record["project_id"] = test_project_id

                        # Generate new subject_id for this test run to avoid duplicates
                        old_subject_id = record.get("subject_id") or record.get("project_subject_id")
                        if old_subject_id and old_subject_id not in subject_id_map:
                            import uuid
                            subject_id_map[old_subject_id] = str(uuid.uuid4())

                        if old_subject_id:
                            record["subject_id"] = subject_id_map[old_subject_id]

                        records.append(record)

        print(f"  Loaded {len(records)} records from benchmark dataset")
        print(f"  Generated {len(subject_id_map)} new subject IDs for this test run")

        return GeneratedDataset(
            records=records,
            stats=stats,
            anchor_terms=anchor_terms,
            parent_term=parent_term,
            subject_id_map=subject_id_map,
            benchmark_dir=None,
        )


@pytest.fixture
def synthetic_dataset(
    hpo_terms_universe: TermUniverse,
    test_project_id: str,
) -> GeneratedDataset:
    """
    Generate or load synthetic PheBee dataset for performance testing.

    Configuration via environment variables:
    - PHEBEE_EVAL_BENCHMARK_DIR: If set, loads pre-generated benchmark dataset from this directory
      (directory should contain metadata.json and batches/ subdirectory with batch-*.json files)

    If PHEBEE_EVAL_BENCHMARK_DIR is not set, generates fresh data using:
    - PHEBEE_EVAL_TERMS_JSON_PATH (required): HPO terms list
    - PHEBEE_EVAL_PREVALENCE_CSV_PATH (optional): CSV with "term_iri,frequency" for
      realistic common/rare term classification based on actual clinical data
    - PHEBEE_EVAL_SCALE_SUBJECTS (default: 10000)
    - PHEBEE_EVAL_SCALE_MIN_TERMS (default: 150): Calibrated to production p75 range
    - PHEBEE_EVAL_SCALE_MAX_TERMS (default: 500): Calibrated to production p90 range
    - PHEBEE_EVAL_SCALE_MIN_EVIDENCE (default: 1): Matches production minimum
    - PHEBEE_EVAL_SCALE_MAX_EVIDENCE (default: 50): Calibrated to production p95 range
    - PHEBEE_EVAL_SEED (optional): Random seed for reproducibility
    - PHEBEE_EVAL_USE_DISEASE_CLUSTERING (default: 1): Enable realistic disease clustering (1=enabled)
    """
    # Check for pre-generated benchmark dataset
    benchmark_dir_str = os.environ.get("PHEBEE_EVAL_BENCHMARK_DIR")
    if benchmark_dir_str:
        benchmark_dir = Path(benchmark_dir_str)
        # Check dataset size from metadata to decide on lazy loading
        metadata_path = benchmark_dir / "metadata.json"
        if metadata_path.exists():
            with metadata_path.open("r", encoding="utf-8") as f:
                metadata = json.load(f)
            stats = metadata.get("dataset_statistics", {})
            n_records = stats.get("n_records", 0)
            # Use lazy loading for large datasets (> 500K records) to avoid OOM
            lazy = n_records > 500_000
            if lazy:
                print(f"[MEMORY_OPTIMIZATION] Dataset has {n_records:,} records - using lazy loading to avoid OOM")
            return _load_benchmark_dataset(benchmark_dir, test_project_id, lazy=lazy)
        else:
            return _load_benchmark_dataset(benchmark_dir, test_project_id, lazy=False)

    # Generate fresh dataset
    #
    # Default values calibrated against production rare disease phenotyping data:
    # - Production: N=45,228 subjects, 90.4M evidence records
    # - Terms per subject: median=114, p75=183, p90=281, max=1,113
    # - Evidence per termlink: median=2, p75=6, p90=18, p95=37, max=7,153
    #
    # Defaults target p75-p95 range ("Complex Patient Load" profile) representing
    # deeply phenotyped rare disease cohorts with extensive longitudinal documentation.
    # This ensures performance testing reflects realistic production workloads for
    # comprehensive clinical phenotyping systems.
    n_subjects = _env_int("PHEBEE_EVAL_SCALE_SUBJECTS", 10_000)
    min_terms = _env_int("PHEBEE_EVAL_SCALE_MIN_TERMS", 150)
    max_terms = _env_int("PHEBEE_EVAL_SCALE_MAX_TERMS", 500)
    min_evidence = _env_int("PHEBEE_EVAL_SCALE_MIN_EVIDENCE", 1)
    max_evidence = _env_int("PHEBEE_EVAL_SCALE_MAX_EVIDENCE", 50)

    rng_seed = None
    if os.environ.get("PHEBEE_EVAL_SEED"):
        rng_seed = int(os.environ["PHEBEE_EVAL_SEED"])

    # Enable disease clustering via environment variable (enabled by default for realistic patterns)
    use_disease_clustering = os.environ.get("PHEBEE_EVAL_USE_DISEASE_CLUSTERING", "1") == "1"

    return generate_scale_dataset(
        project_id=test_project_id,
        universe=hpo_terms_universe,
        n_subjects=n_subjects,
        min_terms=min_terms,
        max_terms=max_terms,
        min_evidence=min_evidence,
        max_evidence=max_evidence,
        rng_seed=rng_seed,
        use_disease_clustering=use_disease_clustering,
    )
