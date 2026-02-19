#!/usr/bin/env python3
"""
Generate a compact JSON index of HPO terms from an OBO release file, including:
- Term records (id, iri, name, synonyms, parents, is_obsolete)
- Metadata (release / date / format hints)
- Precomputed pools of term IDs for synthetic dataset generation:
    * all: all non-obsolete terms
    * internal: non-obsolete terms with >= 1 child (within the non-obsolete set)
    * leaf: non-obsolete terms with 0 children (within the non-obsolete set)

Example:
  python generate_hpo_terms_json.py --obo hp.obo --out hpo_terms.json

Notes:
- This script does not attempt to resolve alternate IDs or replaced_by; it simply
  excludes obsolete terms from pools.
- Parent edges are taken from 'is_a:' relationships.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional


HPO_IRI_PREFIX = "http://purl.obolibrary.org/obo/"
HPO_ROOT_ID = "HP:0000001"


def _obo_date_to_iso(date_str: str) -> Optional[str]:
    # OBO commonly uses: "date: 06:02:2026 12:34"
    # Some files use: "date: 2026-02-06"
    s = date_str.strip()
    for fmt in ("%d:%m:%Y %H:%M", "%d:%m:%Y", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            return _dt.datetime.strptime(s, fmt).isoformat()
        except ValueError:
            continue
    return None


def parse_obo(obo_path: Path) -> Tuple[Dict[str, dict], dict]:
    """
    Minimal OBO parser for HPO-style files.
    Returns:
      terms_by_id: { "HP:0000001": {...}, ... }
      header_meta: { "data-version": ..., "date": ..., ... }
    """
    terms_by_id: Dict[str, dict] = {}
    header_meta: dict = {}

    cur: Optional[dict] = None
    in_term = False

    def flush():
        nonlocal cur
        if not cur:
            return
        tid = cur.get("id")
        if tid:
            # normalize missing lists
            cur.setdefault("parents", [])
            cur.setdefault("synonyms", [])
            cur.setdefault("is_obsolete", False)
            cur.setdefault("name", "")
            cur.setdefault("iri", f"{HPO_IRI_PREFIX}{tid.replace(':', '_')}")
            terms_by_id[tid] = cur
        cur = None

    with obo_path.open("r", encoding="utf-8", errors="replace") as f:
        for raw in f:
            line = raw.rstrip("\n")

            if not in_term:
                if line.strip() == "[Term]":
                    in_term = True
                    cur = {"parents": [], "synonyms": []}
                    continue

                # Header lines: key: value
                if ":" in line and not line.startswith("!"):
                    k, v = line.split(":", 1)
                    k = k.strip()
                    v = v.strip()
                    if k and v and k not in header_meta:
                        header_meta[k] = v
                continue

            # inside a [Term] stanza
            if line.strip() == "":
                flush()
                in_term = False
                continue

            if line.startswith("id:"):
                cur["id"] = line.split("id:", 1)[1].strip()
            elif line.startswith("name:"):
                cur["name"] = line.split("name:", 1)[1].strip()
            elif line.startswith("is_a:"):
                # is_a: HP:0000005 ! Mode of inheritance
                parent = line.split("is_a:", 1)[1].strip().split("!")[0].strip()
                if parent:
                    cur["parents"].append(parent)
            elif line.startswith("synonym:"):
                # synonym: "Abnormality of ..." EXACT []
                syn = line.split("synonym:", 1)[1].strip()
                cur["synonyms"].append(syn)
            elif line.startswith("is_obsolete:"):
                val = line.split("is_obsolete:", 1)[1].strip().lower()
                cur["is_obsolete"] = (val == "true")
            elif line.startswith("alt_id:"):
                # Keep for reference; not used for pools.
                cur.setdefault("alt_ids", []).append(line.split("alt_id:", 1)[1].strip())
            elif line.startswith("replaced_by:"):
                cur["replaced_by"] = line.split("replaced_by:", 1)[1].strip()

        # file may end while in stanza
        if in_term:
            flush()

    return terms_by_id, header_meta


def compute_pools(terms_by_id: Dict[str, dict]) -> dict:
    """Compute all/internal/leaf pools based on is_a relationships among non-obsolete terms."""
    non_obsolete: Set[str] = {tid for tid, t in terms_by_id.items() if not t.get("is_obsolete", False)}

    children: Dict[str, Set[str]] = {tid: set() for tid in non_obsolete}
    for tid in non_obsolete:
        for p in terms_by_id[tid].get("parents", []):
            if p in non_obsolete:
                children[p].add(tid)

    leaf = sorted([tid for tid in non_obsolete if len(children.get(tid, set())) == 0])
    internal = sorted([tid for tid in non_obsolete if len(children.get(tid, set())) > 0])

    # "all" includes everything non-obsolete; keep deterministic ordering.
    all_terms = sorted(non_obsolete)

    return {
        "all": all_terms,
        "internal": internal,
        "leaf": leaf,
        "counts": {
            "all": len(all_terms),
            "internal": len(internal),
            "leaf": len(leaf),
        },
    }


def build_output(terms_by_id: Dict[str, dict], header_meta: dict) -> dict:
    pools = compute_pools(terms_by_id)

    # Prefer these metadata keys if present in the header
    data_version = header_meta.get("data-version") or header_meta.get("version") or header_meta.get("ontology")
    raw_date = header_meta.get("date")
    date_iso = _obo_date_to_iso(raw_date) if raw_date else None

    out = {
        "metadata": {
            "source_file": None,
            "generated_at": _dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
            "obo_header": header_meta,
            "hpo_data_version": data_version,
            "hpo_date": date_iso or raw_date,
            "hpo_root_id": HPO_ROOT_ID,
            "iri_prefix": HPO_IRI_PREFIX,
        },
        "pools": {
            "all": pools["all"],
            "internal": pools["internal"],
            "leaf": pools["leaf"],
        },
        "pool_counts": pools["counts"],
        "terms": [
            terms_by_id[tid]
            for tid in sorted(terms_by_id.keys())
            if tid in terms_by_id
        ],
    }
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--obo", required=True, type=Path, help="Path to hp.obo (HPO OBO release file)")
    ap.add_argument("--out", required=True, type=Path, help="Path to output JSON file")
    args = ap.parse_args()

    terms_by_id, header_meta = parse_obo(args.obo)
    out = build_output(terms_by_id, header_meta)
    out["metadata"]["source_file"] = str(args.obo)

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(out, indent=2, sort_keys=False) + "\n", encoding="utf-8")

    print(f"Wrote {args.out} with {len(out['terms'])} term stanzas; pools: {out['pool_counts']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
