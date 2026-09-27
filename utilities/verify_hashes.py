#!/usr/bin/env python3
"""
Recompute evidence_id and termlink_id from stored evidence fields and compare.

Both ids are content-addressed: evidence_id is a hash of the assertion's
identifying fields, termlink_id a hash of the subject/term/qualifier triple.
Nothing in the pipeline re-derives them after ingest, so a change to the
hashing rules, to qualifier canonicalisation, or to how a loader spells a
value, produces rows whose stored id no longer matches their own content --
and every path that looks an assertion up by id silently stops finding it.
This reads exported evidence rows back and checks that the ids still follow
from the fields beside them.

Input is whatever an Athena query over the evidence table exports: either a
JSON array of row objects or JSONL, one row per line. The columns this needs
are subject_id, term_iri, evidence_id, termlink_id, and, where present,
clinical_note_id, encounter_id, qualifiers, creator and text_annotation.

Usage:
    utilities/verify_hashes.py rows.json
    utilities/verify_hashes.py rows.jsonl --verbose
    aws athena get-query-results ... | utilities/verify_hashes.py -

Exits non-zero if any row's stored id disagrees with the recomputed one.
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, TextIO

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "layers" / "phebee-utils"))

from phebee.constants import PHEBEE
from phebee.utils.hash import generate_evidence_hash, generate_termlink_hash
from phebee.utils.iceberg import parse_athena_struct_array, parse_qualifiers_field


def parse_struct(struct_str: Optional[str]) -> Dict[str, str]:
    """Parse a single Athena struct into a dict, tolerating an absent value.

    Athena renders struct columns as `{field=value, field=value}`. The struct
    parsers in phebee.utils.iceberg already handle that spelling, including a
    bare struct with no enclosing array, so this only has to pick the first
    element out and cope with null.
    """
    if not struct_str or struct_str == "null":
        return {}
    parsed = parse_athena_struct_array(struct_str)
    return parsed[0] if parsed else {}


def parse_span(text_annotation: Optional[str]) -> tuple:
    """Return (span_start, span_end) from a text_annotation struct.

    The struct is `<span_start:int, span_end:int, annotation_metadata:string>`.
    Athena's text rendering has no quoting, so a comma inside
    annotation_metadata shifts every field after it -- but the two spans come
    first, so reading only those is safe regardless of what the metadata holds.
    """
    fields = parse_struct(text_annotation)
    spans = []
    for key in ("span_start", "span_end"):
        value = fields.get(key)
        spans.append(int(value) if value not in (None, "", "null") else None)
    return tuple(spans)


def verify_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Recompute both ids for one row and report whether they match."""
    subject_id = record["subject_id"]
    term_iri = record["term_iri"]

    # Qualifier objects go straight into the hash functions, which accept them
    # directly. Round-tripping through "type:value" strings first, as an earlier
    # version of this script did, re-implements normalisation the layer already
    # owns and can disagree with it.
    qualifiers = parse_qualifiers_field(record.get("qualifiers"))
    span_start, span_end = parse_span(record.get("text_annotation"))

    expected_evidence_id = generate_evidence_hash(
        clinical_note_id=record.get("clinical_note_id"),
        encounter_id=record.get("encounter_id"),
        term_iri=term_iri,
        span_start=span_start,
        span_end=span_end,
        qualifiers=qualifiers,
        subject_id=subject_id,
        creator_id=parse_struct(record.get("creator")).get("creator_id"),
    )

    expected_termlink_id = generate_termlink_hash(
        source_node_iri=f"{PHEBEE}/subjects/{subject_id}",
        term_iri=term_iri,
        qualifiers=qualifiers,
    )

    return {
        "subject_id": subject_id,
        "clinical_note_id": record.get("clinical_note_id"),
        "term_iri": term_iri,
        "span": (span_start, span_end),
        "qualifiers": [f"{q.type}:{q.value}" for q in qualifiers],
        "evidence_match": expected_evidence_id == record["evidence_id"],
        "termlink_match": expected_termlink_id == record["termlink_id"],
        "expected_evidence_id": expected_evidence_id,
        "actual_evidence_id": record["evidence_id"],
        "expected_termlink_id": expected_termlink_id,
        "actual_termlink_id": record["termlink_id"],
    }


def read_records(stream: TextIO) -> Iterator[Dict[str, Any]]:
    """Yield rows from a JSON array or from JSONL.

    Sniffed rather than switched on the file extension, because an Athena
    export is as likely to be named .JSON while holding one object per line.
    JSONL is streamed so a large export does not have to fit in memory.
    """
    first = stream.read(1)
    while first and first.isspace():
        first = stream.read(1)
    if not first:
        return

    if first == "[":
        # Whole-array form: the opening bracket is already consumed.
        yield from json.loads(first + stream.read())
        return

    remainder_of_line = stream.readline()
    for line in [first + remainder_of_line, *stream]:
        line = line.strip().rstrip(",")
        if line:
            yield json.loads(line)


def report_mismatch(index: int, result: Dict[str, Any]) -> None:
    print(f"MISMATCH record {index}")
    if not result["evidence_match"]:
        print("  evidence_id:")
        print(f"    expected {result['expected_evidence_id']}")
        print(f"    stored   {result['actual_evidence_id']}")
    if not result["termlink_match"]:
        print("  termlink_id:")
        print(f"    expected {result['expected_termlink_id']}")
        print(f"    stored   {result['actual_termlink_id']}")
    print(f"  subject    {result['subject_id']}")
    print(f"  note       {result['clinical_note_id']}")
    print(f"  term       {result['term_iri']}")
    print(f"  span       {result['span'][0]}-{result['span'][1]}")
    print(f"  qualifiers {result['qualifiers']}")
    print()


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("input", help="exported evidence rows: JSON array, JSONL, or - for stdin")
    parser.add_argument("--limit", type=int, default=None,
                        help="stop after this many rows")
    parser.add_argument("--verbose", action="store_true",
                        help="print a line per row, not just mismatches")
    args = parser.parse_args()

    stream = sys.stdin if args.input == "-" else open(args.input, encoding="utf-8")
    checked = 0
    mismatches: List[int] = []
    try:
        for index, record in enumerate(read_records(stream), 1):
            if args.limit is not None and checked >= args.limit:
                break
            result = verify_record(record)
            checked += 1
            if result["evidence_match"] and result["termlink_match"]:
                if args.verbose:
                    print(f"ok record {index}: {result['subject_id']} {result['term_iri']}")
            else:
                mismatches.append(index)
                report_mismatch(index, result)
    finally:
        if stream is not sys.stdin:
            stream.close()

    if not checked:
        print("no records read", file=sys.stderr)
        return 1

    print(f"checked {checked} record(s): {checked - len(mismatches)} match, {len(mismatches)} mismatch")
    if mismatches:
        print(f"FAILED: stored ids disagree with their own content at record(s) "
              f"{', '.join(str(i) for i in mismatches[:20])}"
              f"{' ...' if len(mismatches) > 20 else ''}")
        return 1
    print("PASSED: every stored id follows from the fields beside it")
    return 0


if __name__ == "__main__":
    sys.exit(main())
