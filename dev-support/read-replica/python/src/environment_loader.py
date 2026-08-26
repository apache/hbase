#!/usr/bin/env python3
import os


def get_env(key, default=None):
    """Retrieve environment variables, ensuring they are loaded from the GitHub Actions runner."""
    val = os.environ.get(key, default)
    if val is None:
        raise RuntimeError(f"Error: Environment variable {key} is not set.")
    return val
