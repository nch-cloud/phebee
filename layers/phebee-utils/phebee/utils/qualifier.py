"""
Qualifier data type for PheBee evidence system.

Provides a structured representation of qualifiers (term modifiers) throughout
the PheBee pipeline, replacing the legacy colon-separated string format.
"""

from dataclasses import dataclass
from typing import List, Optional, Dict, Any


# Value spellings that mean "this qualifier does not apply", and those that mean it does.
# KEEP IN SYNC with normalize_qualifier_value in scripts/bulk_evidence_processor.py.
# The bulk importer runs standalone on EMR and cannot import this layer, so the two
# implementations have to agree by inspection; test_qualifier_canonicalization.py pins
# them against each other so the pair cannot drift silently.
_INACTIVE_VALUES = [False, "false", "0", 0, 0.0]
_ACTIVE_VALUES = [True, "true", "1", 1, 1.0]


def canonical_qualifier_value(value: Any) -> Optional[str]:
    """
    Reduce an untyped qualifier value to the canonical string used for hashing.

    Qualifier values arrive straight from JSON, so the same intent shows up as a
    bool, a number or a string. Hashing str(value) directly is what let the API and
    bulk paths disagree: str(True) is "True", which hashes differently from the
    "true" the bulk importer derives from the same input, so the identical assertion
    got two different evidence_ids depending on which path loaded it.

    Returns None when the qualifier is inactive and should be dropped rather than
    hashed or stored. Absent, null and false all mean the same thing.

    Boolean spellings match case-insensitively, so a client building a qualifier with
    f"negated:{flag}" from a Python bool - which yields "True" - lands on the same
    hash as a JSON true. Domain values keep their original case.

    Domain values pass through unchanged, so "mild" and "HP:0003593" survive.

    Examples:
        >>> canonical_qualifier_value(True)
        'true'
        >>> canonical_qualifier_value("True")
        'true'
        >>> canonical_qualifier_value(1.0)
        'true'
        >>> canonical_qualifier_value(None) is None
        True
        >>> canonical_qualifier_value("Mild")
        'Mild'
    """
    # Compare case-insensitively for strings, but return the original spelling for
    # domain values - "Mild" should stay "Mild", not become "mild".
    comparable = value.lower() if isinstance(value, str) else value

    if comparable is None or comparable in _INACTIVE_VALUES:
        return None
    if comparable in _ACTIVE_VALUES:
        return "true"
    return str(value)


@dataclass(frozen=True)
class Qualifier:
    """
    Represents a qualifier with type and value.

    Qualifiers modify the meaning of phenotypic terms. Examples include:
    - Temporal: onset, duration
    - Severity: mild, moderate, severe
    - Modality: negated, hypothetical, family history
    - Clinical context: laterality, body site

    Examples:
        >>> Qualifier(type="negated", value="true")
        >>> Qualifier(type="onset", value="HP:0003593")  # Infantile onset
        >>> Qualifier(type="http://purl.obolibrary.org/obo/HP_0012823", value="present")
    """
    type: str
    value: str

    def to_string(self) -> str:
        """
        Convert to legacy 'type:value' string format for hashing.

        Returns:
            String in format "type:value" (e.g., "onset:HP:0003593")
        """
        return f"{self.type}:{self.value}"

    @classmethod
    def from_string(cls, s: str) -> 'Qualifier':
        """
        Parse from 'type:value' string format.

        Handles:
        - Internal qualifiers: "negated:true"
        - External IRIs: "http://purl.obolibrary.org/obo/HP_0003593:present"
        - Bare names: "negated" → "negated:true"

        Args:
            s: String in "type:value" format or bare name

        Returns:
            Qualifier object

        Examples:
            >>> Qualifier.from_string("negated:true")
            Qualifier(type='negated', value='true')
            >>> Qualifier.from_string("negated")
            Qualifier(type='negated', value='true')
            >>> Qualifier.from_string("http://.../HP_0003593:present")
            Qualifier(type='http://.../HP_0003593', value='present')
        """
        if not s:
            raise ValueError("Cannot parse empty string as Qualifier")

        if ':' not in s:
            # Bare name implies true
            return cls(type=s, value="true")

        # Handle external IRIs with protocol colons
        if s.startswith('http://') or s.startswith('https://'):
            # Find last colon after last slash to avoid splitting on protocol
            last_slash = s.rfind('/')
            colon_after_slash = s.find(':', last_slash + 1)
            if colon_after_slash != -1:
                return cls(type=s[:colon_after_slash], value=s[colon_after_slash+1:])
            else:
                # No value specified after IRI
                return cls(type=s, value="true")
        else:
            # Internal qualifier - split on first colon
            type_part, value_part = s.split(':', 1)
            return cls(type=type_part, value=value_part)

    @classmethod
    def from_raw(cls, qualifier_type: str, value: Any) -> Optional['Qualifier']:
        """
        Create from an untyped input value, or None if the qualifier is inactive.

        Use this for values arriving from a request body or any other JSON source,
        where the value may be a bool, number, string or null. Both the type and the
        value are normalized, so callers do not need to carry their own falsey
        filter - which is how the previous copies of that filter drifted apart.

        Args:
            qualifier_type: Qualifier type, short name or full IRI
            value: Untyped value from JSON

        Returns:
            Qualifier with canonical type and value, or None if inactive

        Examples:
            >>> Qualifier.from_raw("negated", True)
            Qualifier(type='negated', value='true')
            >>> Qualifier.from_raw("negated", None) is None
            True
            >>> Qualifier.from_raw("negated", False) is None
            True
        """
        canonical = canonical_qualifier_value(value)
        if canonical is None:
            return None
        return cls(type=normalize_qualifier_type(qualifier_type), value=canonical)

    @classmethod
    def from_dict(cls, d: Dict[str, str]) -> 'Qualifier':
        """
        Create from storage dict format.

        Args:
            d: Dictionary with 'qualifier_type' and 'qualifier_value' keys
               (Athena/Iceberg struct format)

        Returns:
            Qualifier object

        Example:
            >>> Qualifier.from_dict({'qualifier_type': 'onset', 'qualifier_value': 'HP:0003593'})
            Qualifier(type='onset', value='HP:0003593')
        """
        return cls(
            type=d['qualifier_type'],
            value=d['qualifier_value']
        )

    def to_storage_dict(self) -> Dict[str, str]:
        """
        Convert to storage dict for database.

        Returns:
            Dictionary with 'qualifier_type' and 'qualifier_value' keys
            for Athena/Iceberg struct storage

        Example:
            >>> q = Qualifier(type="onset", value="HP:0003593")
            >>> q.to_storage_dict()
            {'qualifier_type': 'onset', 'qualifier_value': 'HP:0003593'}
        """
        return {
            'qualifier_type': self.type,
            'qualifier_value': self.value
        }

    def is_active(self) -> bool:
        """
        Check if qualifier is active (value not false/0).

        Qualifiers with false/0 values are considered inactive and should be
        filtered out before storage or hash calculation.

        Returns:
            True if qualifier is active, False otherwise

        Examples:
            >>> Qualifier(type="negated", value="true").is_active()
            True
            >>> Qualifier(type="negated", value="false").is_active()
            False
            >>> Qualifier(type="severity", value="0").is_active()
            False
        """
        return self.value.lower() not in ['false', '0']


def normalize_qualifiers(qualifiers: Optional[List[Qualifier]]) -> List[Qualifier]:
    """
    Normalize qualifiers for hash computation.

    Normalization:
    - Filters out inactive qualifiers (false/0 values)
    - Sorts for deterministic ordering (by string representation)
    - Returns list of active Qualifier objects

    This ensures consistent hash generation regardless of input order
    and excludes inactive qualifiers.

    Args:
        qualifiers: List of Qualifier objects or None

    Returns:
        Sorted list of active Qualifier objects

    Example:
        >>> qualifiers = [
        ...     Qualifier(type="severity", value="mild"),
        ...     Qualifier(type="negated", value="false"),
        ...     Qualifier(type="onset", value="HP:0003593")
        ... ]
        >>> normalized = normalize_qualifiers(qualifiers)
        >>> [q.to_string() for q in normalized]
        ['onset:HP:0003593', 'severity:mild']
    """
    if not qualifiers:
        return []

    active = [q for q in qualifiers if q.is_active()]
    return sorted(active, key=lambda q: q.to_string())


def qualifiers_to_string_list(qualifiers: List[Qualifier]) -> List[str]:
    """
    Convert list of Qualifier objects to legacy string format.

    Used for hash calculation, which requires string format for
    backward compatibility with existing hashes.

    Args:
        qualifiers: List of Qualifier objects

    Returns:
        List of strings in "type:value" format

    Example:
        >>> qualifiers = [
        ...     Qualifier(type="onset", value="HP:0003593"),
        ...     Qualifier(type="severity", value="mild")
        ... ]
        >>> qualifiers_to_string_list(qualifiers)
        ['onset:HP:0003593', 'severity:mild']
    """
    return [q.to_string() for q in qualifiers]


def normalize_qualifier_type(qualifier_type: str) -> str:
    """
    Normalize qualifier type to canonical form for storage.

    Rules:
    - Internal PheBee qualifiers: Extract short name from IRI if prefixed
      "http://ods.nationwidechildrens.org/phebee/qualifier/negated" → "negated"
    - External qualifiers: Keep full IRI
      "http://purl.obolibrary.org/obo/HP_0012823" → stays as-is
    - Already short form: Keep as-is
      "negated" → "negated"

    This maintains backwards compatibility with existing Iceberg data
    which stores internal qualifiers as short names.

    Args:
        qualifier_type: Qualifier type (short name or IRI)

    Returns:
        Normalized qualifier type

    Examples:
        >>> normalize_qualifier_type("negated")
        'negated'
        >>> normalize_qualifier_type("http://ods.nationwidechildrens.org/phebee/qualifier/negated")
        'negated'
        >>> normalize_qualifier_type("http://purl.obolibrary.org/obo/HP_0012823")
        'http://purl.obolibrary.org/obo/HP_0012823'
    """
    PHEBEE_QUALIFIER_PREFIX = "http://ods.nationwidechildrens.org/phebee/qualifier/"

    if qualifier_type.startswith(PHEBEE_QUALIFIER_PREFIX):
        # Extract short name from PheBee internal qualifier IRI
        return qualifier_type[len(PHEBEE_QUALIFIER_PREFIX):]
    else:
        # External IRI or already short form - keep as-is
        return qualifier_type
