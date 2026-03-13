"""
Monarch Knowledge Graph API integration utilities.

Provides functions to query the Monarch Knowledge Graph for term associations.
"""

import requests
import json
from typing import List
from math import ceil

# Logging setup following pattern from neptune.py
try:
    from aws_lambda_powertools import Logger
    logger = Logger()
except ImportError:
    # For testing environments without AWS Lambda Powertools
    class NoOpLogger:
        def info(self, *args, **kwargs):
            pass
        def error(self, *args, **kwargs):
            pass
        def warning(self, *args, **kwargs):
            pass
    logger = NoOpLogger()


def get_associated_terms(entity: str, limit: int = 500) -> List[str]:
    """
    Fetch phenotype and disease term associations from Monarch Knowledge Graph.

    Queries the Monarch KG /association endpoint to retrieve all HPO and MONDO terms
    associated with the given entity. Handles pagination automatically to fetch all
    available associations.

    Args:
        entity: Entity identifier in CURIE format (e.g., "MONDO:0007947")
        limit: Maximum number of associations to fetch per request (default: 500)

    Returns:
        List of term IDs in CURIE format (e.g., ["HP:0001249", "MONDO:0005148"]).
        Results are deduplicated and sorted.

    Raises:
        Exception: If Monarch API returns non-200 status code
        requests.exceptions.RequestException: If network error occurs
        json.JSONDecodeError: If response cannot be parsed as JSON

    Example:
        >>> terms = get_associated_terms("MONDO:0007947")
        >>> print(f"Found {len(terms)} associated terms")
        Found 42 associated terms
    """
    base_url = "https://api-v3.monarchinitiative.org/v3/api/association"

    # Build parameters for Monarch API
    # Filter to HPO and MONDO terms using object_namespace
    params = {
        "entity": entity,
        "object_namespace": ["HP", "MONDO"],
        "direct": "false",  # Include indirect associations
        "compact": "false",  # Full response format
        "format": "json",
        "limit": limit,
        "offset": 0
    }

    logger.info(f"Querying Monarch associations for entity {entity}")

    try:
        # Make initial request to get total count
        response = requests.get(base_url, params=params, timeout=30)

        if response.status_code != 200:
            error_msg = f"Monarch API request failed: status={response.status_code}, error={response.text}"
            logger.error(error_msg)
            raise Exception(error_msg)

        data = response.json()
        total = data.get("total", 0)

        logger.info(f"Monarch API returned {total} total associations for {entity}")

        if total == 0:
            logger.warning(f"No associations found for entity {entity}")
            return []

        # Collect all terms from first page
        all_terms = set()
        if "items" in data:
            for item in data["items"]:
                if "object" in item:
                    all_terms.add(item["object"])

        # Calculate number of additional pages needed
        num_pages = ceil(total / limit)

        # Fetch remaining pages if needed
        if num_pages > 1:
            logger.info(f"Fetching {num_pages - 1} additional pages (total: {num_pages} pages)")

            for page in range(1, num_pages):
                offset = page * limit
                params["offset"] = offset

                response = requests.get(base_url, params=params, timeout=30)

                if response.status_code != 200:
                    error_msg = f"Monarch API request failed on page {page + 1}: status={response.status_code}"
                    logger.error(error_msg)
                    raise Exception(error_msg)

                data = response.json()
                if "items" in data:
                    for item in data["items"]:
                        if "object" in item:
                            all_terms.add(item["object"])

        # Convert set to sorted list
        term_list = sorted(all_terms)

        logger.info(f"Extracted {len(term_list)} unique terms from Monarch associations")
        if term_list:
            # Log sample of terms for debugging
            sample_size = min(10, len(term_list))
            logger.info(f"Sample terms: {term_list[:sample_size]}")

        return term_list

    except requests.exceptions.Timeout:
        error_msg = f"Monarch API request timed out after 30 seconds for entity {entity}"
        logger.error(error_msg)
        raise Exception(error_msg)

    except requests.exceptions.RequestException as e:
        error_msg = f"Network error while querying Monarch API for entity {entity}: {str(e)}"
        logger.error(error_msg)
        raise

    except json.JSONDecodeError as e:
        error_msg = f"Failed to parse Monarch API response as JSON for entity {entity}: {str(e)}"
        logger.error(error_msg)
        raise
