"""Utility functions and configuration constants for the Qualtrics connector.

This module contains helper functions for key normalization, response processing,
and configuration constants used across the Qualtrics connector.
"""

import re


class QualtricsConfig:
    """Configuration constants for Qualtrics connector."""

    # Export polling configuration
    MAX_EXPORT_POLL_ATTEMPTS = 60  # Max polling attempts (2 min total at 2s/attempt)
    EXPORT_POLL_INTERVAL_FAST = 1  # seconds (when >50% complete)
    EXPORT_POLL_INTERVAL_SLOW = 2  # seconds (when <50% complete)

    # HTTP retry configuration (uses exponential backoff: 2^attempt)
    MAX_HTTP_RETRIES = 3  # Results in 1s, 2s, 4s waits = 7s total max

    # Rate limiting
    RATE_LIMIT_DEFAULT_WAIT = 60  # seconds (when Retry-After header missing)

    # Pagination
    DEFAULT_PAGE_SIZE = 100

    # Request timeout
    REQUEST_TIMEOUT = 30  # seconds per HTTP request

    # Auto-consolidation configuration (when surveyId not provided)
    DEFAULT_MAX_SURVEYS = 50  # Default limit for auto-consolidation
    CONSOLIDATION_DELAY_BETWEEN_SURVEYS = 0.5  # seconds (to respect rate limits)

    # Parallel processing configuration
    # Qualtrics allows 3000 requests/minute per brand (some endpoints have lower limits)
    # We use 5 workers as a conservative default to avoid overwhelming the API
    # Reference: https://api.qualtrics.com/a5e9a1a304902-limits
    MAX_PARALLEL_WORKERS = 5

    @staticmethod
    def get_poll_interval(progress_percent: float) -> float:
        """Get polling interval based on export progress."""
        return (
            QualtricsConfig.EXPORT_POLL_INTERVAL_FAST
            if progress_percent >= 50
            else QualtricsConfig.EXPORT_POLL_INTERVAL_SLOW
        )

    @staticmethod
    def get_retry_wait(attempt: int) -> float:
        """Calculate exponential backoff wait time for HTTP retries."""
        return 2 ** attempt


def to_snake_case(name: str) -> str:
    """
    Convert camelCase or PascalCase to snake_case.

    Args:
        name: Field name in camelCase or PascalCase

    Returns:
        Field name in snake_case
    """
    # Insert underscore before uppercase letters that follow lowercase letters
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    # Insert underscore before uppercase letters that follow numbers or lowercase letters
    s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)
    return s2.lower()


def normalize_keys(data: dict) -> dict:
    """
    Recursively transform all keys in a dict to snake_case.

    Args:
        data: Dictionary with API field names (camelCase or PascalCase)

    Returns:
        Dictionary with snake_case field names
    """
    if not isinstance(data, dict):
        return data

    normalized = {}
    for key, value in data.items():
        snake_key = to_snake_case(key)

        if isinstance(value, dict):
            normalized[snake_key] = normalize_keys(value)
        elif isinstance(value, list):
            normalized[snake_key] = [
                normalize_keys(item) if isinstance(item, dict) else item
                for item in value
            ]
        else:
            normalized[snake_key] = value

    return normalized
