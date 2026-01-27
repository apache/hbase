#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Fetch wave assignment files from GitHub Actions artifacts.

Usage: fetch-wave-files.py <wave-number> [branch]
  wave-number: 1, 2, or 3
  branch: git branch name (default: master)

Environment:
  GITHUB_TOKEN or GH_TOKEN: Required for GitHub API access

Output: path to the wave file on stdout
Exit: 0 on success, 1 on failure
"""

import io
import os
import sys
import tempfile
import zipfile

import requests

REPO = "apache/hbase"
WORKFLOW = "test-waves-aggregation.yml"
API_BASE = f"https://api.github.com/repos/{REPO}"


def get_token():
    """Get GitHub token from environment."""
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if not token:
        print("Error: GITHUB_TOKEN or GH_TOKEN environment variable required", file=sys.stderr)
        print("Create a token at https://github.com/settings/tokens (no scopes needed for public repos)", file=sys.stderr)
        sys.exit(1)
    return token


def get_wave_file_info(wave_num: int, branch: str) -> tuple:
    """Return (artifact_name, wave_file_name) for a wave number."""
    if wave_num == 1:
        return f"wave1-excludes-{branch}", "wave1-excludes.txt"
    else:
        return f"wave{wave_num}-includes-{branch}", f"wave{wave_num}-includes.txt"


def get_cache_dir(branch: str) -> str:
    """Get cache directory path."""
    tmpdir = os.environ.get("TMPDIR", "/tmp")
    return os.path.join(tmpdir, "hbase-wave-files", branch)


def fetch_wave_file(wave_num: int, branch: str) -> str:
    """Fetch wave file and return path to it."""
    artifact_name, wave_file = get_wave_file_info(wave_num, branch)
    cache_dir = get_cache_dir(branch)
    cache_path = os.path.join(cache_dir, wave_file)

    if os.path.exists(cache_path):
        return cache_path

    token = get_token()
    headers = {"Authorization": f"Bearer {token}"}

    response = requests.get(
        f"{API_BASE}/actions/workflows/{WORKFLOW}/runs",
        params={"branch": branch, "status": "success", "per_page": 1},
        headers=headers,
        timeout=30
    )
    response.raise_for_status()
    runs = response.json().get("workflow_runs", [])

    if not runs:
        print(f"Error: No successful test-waves-aggregation run found for branch {branch}", file=sys.stderr)
        sys.exit(1)

    run_id = runs[0]["id"]

    response = requests.get(
        f"{API_BASE}/actions/runs/{run_id}/artifacts",
        headers=headers,
        timeout=30
    )
    response.raise_for_status()
    artifacts = response.json().get("artifacts", [])

    artifact_url = None
    for artifact in artifacts:
        if artifact["name"] == artifact_name:
            artifact_url = artifact["archive_download_url"]
            break

    if not artifact_url:
        print(f"Error: Artifact '{artifact_name}' not found in run {run_id}", file=sys.stderr)
        sys.exit(1)

    response = requests.get(artifact_url, headers=headers, timeout=120)
    response.raise_for_status()

    os.makedirs(cache_dir, exist_ok=True)
    with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
        with zf.open(wave_file) as src, open(cache_path, "wb") as dst:
            dst.write(src.read())

    return cache_path


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <wave-number> [branch]", file=sys.stderr)
        sys.exit(1)

    try:
        wave_num = int(sys.argv[1])
    except ValueError:
        print(f"Error: wave-number must be an integer (got: {sys.argv[1]})", file=sys.stderr)
        sys.exit(1)

    if wave_num not in (1, 2, 3):
        print(f"Error: wave-number must be 1, 2, or 3 (got: {wave_num})", file=sys.stderr)
        sys.exit(1)

    branch = sys.argv[2] if len(sys.argv) > 2 else "master"

    try:
        path = fetch_wave_file(wave_num, branch)
        print(path)
    except requests.RequestException as e:
        print(f"Error: API request failed: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
