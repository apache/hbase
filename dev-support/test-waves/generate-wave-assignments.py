#!/usr/bin/env python3
##
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Aggregate test runtimes from Jenkins nightlies and generate wave assignments for HBase CI.

This script:
1. Downloads test_logs.zip from recent Jenkins nightly builds
2. Extracts and parses surefire XML files
3. Filters to Large tests only
4. Computes p90 runtime per test across builds
5. Bin-packs tests into 3 waves using first-fit-decreasing
6. Outputs wave assignment files

Usage:
    python generate-wave-assignments.py --output-dir ./output --num-builds 10
"""

import argparse
import io
import logging
import os
import re
import sys
import tempfile
import xml.etree.ElementTree as ET
import zipfile
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from urllib.parse import quote

import requests
import yaml

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stderr
)
logger = logging.getLogger(__name__)

NIGHTLIES_BASE_URL = "https://nightlies.apache.org/hbase/HBase%20Nightly"
DEFAULT_BRANCH = "master"
DEFAULT_JDK_HADOOP = "output-jdk17-hadoop3"
DEFAULT_NUM_BUILDS = 10
DEFAULT_NUM_WAVES = 3
MAX_WAVE_DURATION_MINUTES = 360  # 6 hour GHA job limit


@dataclass
class TestResult:
    """Runtime result for a single test class in a single build."""
    name: str
    time_seconds: float
    tests: int = 0
    failures: int = 0
    errors: int = 0
    skipped: int = 0


@dataclass
class TestStats:
    """Aggregated statistics for a test class across builds."""
    name: str
    runtimes: list = field(default_factory=list)
    sample_count: int = 0

    def add_runtime(self, time_seconds: float):
        self.runtimes.append(time_seconds)
        self.sample_count = len(self.runtimes)

    def p90(self) -> float:
        if not self.runtimes:
            return 0.0
        sorted_times = sorted(self.runtimes)
        idx = int(len(sorted_times) * 0.9)
        idx = min(idx, len(sorted_times) - 1)
        return sorted_times[idx]

    def mean(self) -> float:
        if not self.runtimes:
            return 0.0
        return sum(self.runtimes) / len(self.runtimes)

    def max(self) -> float:
        if not self.runtimes:
            return 0.0
        return max(self.runtimes)


def fetch_build_numbers(branch: str, limit: int) -> list:
    """Fetch available build numbers from nightlies index page."""
    url = f"{NIGHTLIES_BASE_URL}/{branch}/"
    logger.info(f"Fetching build list from {url}")

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Failed to fetch build list: {e}")
        return []

    build_numbers = []
    for match in re.finditer(r'href="(\d+)/"', response.text):
        build_numbers.append(int(match.group(1)))

    build_numbers.sort(reverse=True)
    result = build_numbers[:limit]
    logger.info(f"Found {len(build_numbers)} builds, using latest {len(result)}: {result}")
    return result


def download_test_logs(branch: str, build_num: int,
                       jdk_hadoop: str = DEFAULT_JDK_HADOOP) -> Optional[bytes]:
    """Download test_logs.zip for a specific build."""
    url = f"{NIGHTLIES_BASE_URL}/{branch}/{build_num}/{jdk_hadoop}/test_logs.zip"
    logger.info(f"Downloading {url}")

    try:
        response = requests.get(url, timeout=120)
        response.raise_for_status()
        return response.content
    except requests.RequestException as e:
        logger.warning(f"Failed to download build {build_num}: {e}")
        return None


def parse_surefire_xml(xml_content: str) -> list:
    """Parse a surefire XML report and return test results."""
    results = []
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError as e:
        logger.debug(f"Failed to parse XML: {e}")
        return results

    if root.tag == 'testsuite':
        name = root.get('name', '')
        time_str = root.get('time', '0')
        try:
            time_seconds = float(time_str)
        except ValueError:
            time_seconds = 0.0

        result = TestResult(
            name=name,
            time_seconds=time_seconds,
            tests=int(root.get('tests', 0)),
            failures=int(root.get('failures', 0)),
            errors=int(root.get('errors', 0)),
            skipped=int(root.get('skipped', 0))
        )
        results.append(result)
    elif root.tag == 'testsuites':
        for testsuite in root.findall('testsuite'):
            name = testsuite.get('name', '')
            time_str = testsuite.get('time', '0')
            try:
                time_seconds = float(time_str)
            except ValueError:
                time_seconds = 0.0

            result = TestResult(
                name=name,
                time_seconds=time_seconds,
                tests=int(testsuite.get('tests', 0)),
                failures=int(testsuite.get('failures', 0)),
                errors=int(testsuite.get('errors', 0)),
                skipped=int(testsuite.get('skipped', 0))
            )
            results.append(result)

    return results


def extract_and_parse_zip(zip_content: bytes) -> list:
    """Extract surefire XML files from zip and parse them."""
    all_results = []

    try:
        with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
            for name in zf.namelist():
                if name.endswith('.xml') and 'TEST-' in name:
                    try:
                        xml_content = zf.read(name).decode('utf-8')
                        results = parse_surefire_xml(xml_content)
                        all_results.extend(results)
                    except Exception as e:
                        logger.debug(f"Failed to process {name}: {e}")
    except zipfile.BadZipFile as e:
        logger.warning(f"Invalid zip file: {e}")

    return all_results


def is_large_test(test_name: str) -> bool:
    """
    Determine if a test is a Large test based on naming patterns.

    We can't determine the category from surefire XML alone, so we use heuristics:
    - Tests in the nightlies ran with runAllTests profile include Large tests
    - We'll include all tests with significant runtime (>15 seconds suggests not Small)
    - The caller should filter based on actual annotations if needed

    For now, return True for all tests - the filtering will happen based on
    whether the test appears in the Large test run results.
    """
    return True


def filter_large_tests(test_stats: dict, min_runtime_seconds: float = 15.0) -> dict:
    """
    Filter to likely Large tests based on runtime threshold.

    Small tests are <15 seconds, Medium <50 seconds by definition.
    We use p90 runtime to filter out tests that are consistently fast.
    """
    large_tests = {}
    for name, stats in test_stats.items():
        p90 = stats.p90()
        if p90 >= min_runtime_seconds:
            large_tests[name] = stats
    return large_tests


def first_fit_decreasing(items: list, num_bins: int, target_per_bin: float) -> list:
    """
    Bin-pack items using first-fit-decreasing algorithm.

    Args:
        items: List of (name, value) tuples sorted by value descending
        num_bins: Number of bins to create
        target_per_bin: Target capacity per bin

    Returns:
        List of lists, where each inner list contains item names
    """
    bins = [[] for _ in range(num_bins)]
    bin_totals = [0.0 for _ in range(num_bins)]

    sorted_items = sorted(items, key=lambda x: x[1], reverse=True)

    for name, value in sorted_items:
        best_bin = 0
        best_remaining = float('inf')

        for i in range(num_bins):
            remaining = target_per_bin - bin_totals[i]
            if remaining >= value and remaining < best_remaining:
                best_bin = i
                best_remaining = remaining
            elif remaining < best_remaining and bin_totals[i] < bin_totals[best_bin]:
                best_bin = i
                best_remaining = remaining

        min_total = min(bin_totals)
        min_idx = bin_totals.index(min_total)
        best_bin = min_idx

        bins[best_bin].append(name)
        bin_totals[best_bin] += value

    return bins, bin_totals


def generate_wave_assignments(test_stats: dict, num_waves: int) -> tuple:
    """
    Generate wave assignments using bin-packing.

    Target duration per wave is computed dynamically from total test runtime.
    Warns if any wave exceeds the GHA job time limit.

    Returns:
        Tuple of (wave_assignments, wave_durations) where wave_assignments is
        a list of lists of test names and wave_durations is estimated durations.
    """
    items = [(name, stats.p90()) for name, stats in test_stats.items()]
    total_time = sum(v for _, v in items)
    total_minutes = total_time / 60

    target_per_wave = (total_time / num_waves) * 1.1  # 10% buffer
    logger.info(f"Total estimated time: {total_minutes:.1f} minutes across {len(items)} tests")
    logger.info(f"Target per wave: {target_per_wave/60:.1f} minutes (with 10% buffer)")

    waves, wave_totals = first_fit_decreasing(items, num_waves, target_per_wave)

    for wave_tests in waves:
        wave_tests.sort()

    wave_durations = [t / 60 for t in wave_totals]

    for i, duration in enumerate(wave_durations):
        if duration > MAX_WAVE_DURATION_MINUTES:
            logger.warning(
                f"Wave {i+1} estimated at {duration:.0f} minutes, "
                f"exceeds {MAX_WAVE_DURATION_MINUTES} minute GHA limit! "
                f"Consider adding more waves."
            )

    return waves, wave_durations


def to_ant_pattern(fqcn: str) -> str:
    """Convert fully qualified class name to Ant-style pattern.

    Example: org.apache.hadoop.hbase.TestFoo -> **/TestFoo.java
    """
    class_name = fqcn.rsplit('.', 1)[-1]
    return f"**/{class_name}.java"


def write_wave_files(waves: list, output_dir: str):
    """Write wave assignment files in Ant-style pattern format."""
    os.makedirs(output_dir, exist_ok=True)

    wave2_file = os.path.join(output_dir, "wave2-includes.txt")
    wave3_file = os.path.join(output_dir, "wave3-includes.txt")
    excludes_file = os.path.join(output_dir, "wave1-excludes.txt")

    wave2_tests = waves[1] if len(waves) > 1 else []
    wave3_tests = waves[2] if len(waves) > 2 else []
    all_excludes = sorted(set(wave2_tests + wave3_tests))

    with open(wave2_file, 'w') as f:
        for test in wave2_tests:
            f.write(f"{to_ant_pattern(test)}\n")
    logger.info(f"Wrote {len(wave2_tests)} tests to {wave2_file}")

    with open(wave3_file, 'w') as f:
        for test in wave3_tests:
            f.write(f"{to_ant_pattern(test)}\n")
    logger.info(f"Wrote {len(wave3_tests)} tests to {wave3_file}")

    with open(excludes_file, 'w') as f:
        for test in all_excludes:
            f.write(f"{to_ant_pattern(test)}\n")
    logger.info(f"Wrote {len(all_excludes)} tests to {excludes_file}")


def write_yaml_output(test_stats: dict, waves: list, wave_durations: list,
                      output_dir: str, branch: str):
    """Write YAML format output files for debugging and CI consumption."""
    os.makedirs(output_dir, exist_ok=True)

    assignments_file = os.path.join(output_dir, "large-wave-assignments.yaml")
    stats_file = os.path.join(output_dir, "runtime-stats.yaml")

    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    wave2_tests = waves[1] if len(waves) > 1 else []
    wave3_tests = waves[2] if len(waves) > 2 else []

    assignments_data = {
        "version": 1,
        "branch": branch,
        "generated": timestamp,
        "wave2": wave2_tests,
        "wave3": wave3_tests,
        "metadata": {
            "total_large_tests": len(test_stats),
            **{f"wave{i+1}_estimated_duration_minutes": int(duration)
               for i, duration in enumerate(wave_durations)}
        }
    }

    with open(assignments_file, 'w') as f:
        f.write("# Generated by generate-wave-assignments.py\n")
        f.write("# Source: Jenkins nightly builds\n")
        yaml.dump(assignments_data, f, default_flow_style=False, sort_keys=False)

    logger.info(f"Wrote wave assignments to {assignments_file}")

    stats_data = {
        "version": 1,
        "generated": timestamp,
        "tests": {
            name: {
                "p90_seconds": round(stats.p90(), 1),
                "sample_count": stats.sample_count
            }
            for name, stats in sorted(test_stats.items())
        }
    }

    with open(stats_file, 'w') as f:
        f.write("# P90 runtimes from recent nightly builds\n")
        f.write("# Used by aggregation job for bin-packing\n")
        yaml.dump(stats_data, f, default_flow_style=False, sort_keys=False)

    logger.info(f"Wrote runtime stats to {stats_file}")


def print_summary(test_stats: dict, waves: list, wave_durations: list):
    """Print summary statistics to stdout."""
    print("\n" + "=" * 70)
    print("WAVE ASSIGNMENT SUMMARY")
    print("=" * 70)
    print(f"\nTotal Large tests: {len(test_stats)}")
    print(f"Total estimated runtime: {sum(wave_durations):.1f} minutes")
    print()

    for i, (wave_tests, duration) in enumerate(zip(waves, wave_durations)):
        print(f"Wave {i+1}: {len(wave_tests):4d} tests, {duration:6.1f} minutes estimated")

    print("\n" + "-" * 70)
    print("Top 20 slowest tests (by p90 runtime):")
    print("-" * 70)

    sorted_tests = sorted(test_stats.items(), key=lambda x: x[1].p90(), reverse=True)
    for name, stats in sorted_tests[:20]:
        print(f"  {stats.p90():7.1f}s  {name}")

    print("\n" + "=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description="Generate wave assignments for HBase Large tests",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--output-dir", "-o",
        default=".",
        help="Output directory for wave assignment files"
    )
    parser.add_argument(
        "--num-builds", "-n",
        type=int,
        default=DEFAULT_NUM_BUILDS,
        help="Number of recent builds to analyze"
    )
    parser.add_argument(
        "--branch", "-b",
        default=DEFAULT_BRANCH,
        help="Branch to analyze"
    )
    parser.add_argument(
        "--jdk-hadoop",
        default=DEFAULT_JDK_HADOOP,
        help="JDK/Hadoop output directory name"
    )
    parser.add_argument(
        "--min-runtime",
        type=float,
        default=15.0,
        help="Minimum p90 runtime (seconds) to be considered a Large test"
    )
    parser.add_argument(
        "--num-waves",
        type=int,
        default=DEFAULT_NUM_WAVES,
        help="Number of waves to create"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and analyze but don't write output files"
    )

    args = parser.parse_args()

    build_numbers = fetch_build_numbers(args.branch, args.num_builds)
    if not build_numbers:
        logger.error("No builds found")
        sys.exit(1)

    test_stats = defaultdict(TestStats)
    builds_processed = 0

    for build_num in build_numbers:
        zip_content = download_test_logs(args.branch, build_num, args.jdk_hadoop)
        if zip_content is None:
            continue

        results = extract_and_parse_zip(zip_content)
        if not results:
            logger.warning(f"No test results found in build {build_num}")
            continue

        for result in results:
            if result.name and result.time_seconds > 0:
                if result.name not in test_stats:
                    test_stats[result.name] = TestStats(name=result.name)
                test_stats[result.name].add_runtime(result.time_seconds)

        builds_processed += 1
        logger.info(f"Processed build {build_num}: {len(results)} test classes")

    if builds_processed == 0:
        logger.error("No builds could be processed")
        sys.exit(1)

    logger.info(f"Processed {builds_processed} builds, found {len(test_stats)} unique test classes")

    large_tests = filter_large_tests(dict(test_stats), args.min_runtime)
    logger.info(f"Filtered to {len(large_tests)} Large tests (p90 >= {args.min_runtime}s)")

    if not large_tests:
        logger.error("No Large tests found after filtering")
        sys.exit(1)

    waves, wave_durations = generate_wave_assignments(large_tests, args.num_waves)

    print_summary(large_tests, waves, wave_durations)

    if not args.dry_run:
        write_wave_files(waves, args.output_dir)
        write_yaml_output(large_tests, waves, wave_durations, args.output_dir, args.branch)
        logger.info(f"Output files written to {args.output_dir}")


if __name__ == "__main__":
    main()
