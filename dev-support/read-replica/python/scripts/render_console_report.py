#!/usr/bin/env python3
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
Generate a Yetus-style console report for the read-replica integration tests.

Reads infrastructure timing from a KEY=VALUE env file and per-test results from
JUnit XML, then produces an HTML fragment matching the format used by the
upstream HBase Nightly Build (Apache Yetus).
"""

from __future__ import annotations

import argparse
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path


@dataclass
class TestResult:
    name: str
    time_sec: float
    passed: bool
    rerun_count: int = 0


def parse_timing_env(path: Path) -> dict[str, int]:
    timing: dict[str, int] = {}
    if not path.exists():
        return timing
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        try:
            timing[key.strip()] = int(value.strip())
        except ValueError:
            pass
    return timing


def parse_junit_xml(path: Path) -> list[TestResult]:
    tree = ET.parse(path)
    root = tree.getroot()

    testcases: list[tuple[str, float, bool]] = []
    for tc in root.iter("testcase"):
        name = tc.get("name", "unknown")
        try:
            time_sec = float(tc.get("time", "0"))
        except ValueError:
            time_sec = 0.0
        has_failure = tc.find("failure") is not None or tc.find("error") is not None
        testcases.append((name, time_sec, not has_failure))

    seen_order: list[str] = []
    entries: dict[str, list[tuple[float, bool]]] = {}
    for name, time_sec, passed in testcases:
        if name not in entries:
            seen_order.append(name)
            entries[name] = []
        entries[name].append((time_sec, passed))

    results: list[TestResult] = []
    for name in seen_order:
        runs = entries[name]
        total_time = sum(t for t, _ in runs)
        final_passed = runs[-1][1]
        rerun_count = len(runs) - 1
        results.append(TestResult(name=name, time_sec=total_time,
                                  passed=final_passed, rerun_count=rerun_count))
    return results


def format_runtime(total_sec: int | float) -> str:
    total_sec = int(round(total_sec))
    minutes = total_sec // 60
    seconds = total_sec % 60
    return f"{minutes:>3d}m {seconds:>2d}s"


def _log_link(test_name: str, color: str, logs_url: str,
              output_dir: Path) -> str:
    test_log_dir = output_dir / test_name
    if not test_log_dir.is_dir():
        return ""
    if logs_url:
        href = f"{logs_url.rstrip('/')}/{test_name}/"
    else:
        href = f"{test_name}/"
    return f'<font color="{color}"><a href="{href}">/{test_name}/</a></font>'


def _row(vote: str, color: str, subsystem: str, runtime: str,
         log: str, comment: str) -> str:
    return (
        "<tr>\n"
        f'\t\t<td><font color="{color}">{vote}</font></td>\n'
        f'\t\t<td><font color="{color}"> {subsystem} </font></td>\n'
        f'\t\t<td><font color="{color}">{runtime}</font></td>\n'
        f"<td>{log}</td>\n"
        f'<td><font color="{color}"> {comment} </font></td>\n'
        "</tr>\n"
    )


def _infra_row(subsystem: str, seconds: int, comment: str) -> str:
    return _row("0", "blue", subsystem, format_runtime(seconds), "", comment)


def _detail_row(color: str, log: str, comment: str) -> str:
    return (
        "<tr>\n"
        "\t\t<td></td>\n"
        "\t\t<td></td>\n"
        "\t\t<td></td>\n"
        f"<td>{log}</td>\n"
        f'<td><font color="{color}"> {comment} </font></td>\n'
        "</tr>\n"
    )


def _pytest_rows(test_results: list[TestResult], timing: dict[str, int],
                 logs_url: str, output_dir: Path) -> list[str]:
    rows: list[str] = []

    passed_count = sum(1 for t in test_results if t.passed)
    failed_count = sum(1 for t in test_results if not t.passed)
    total_reruns = sum(t.rerun_count for t in test_results)

    # At least one test ran and there were no failures
    all_passed = bool(test_results) and failed_count == 0

    if all_passed:
        vote, color = "+1", "green"
    else:
        vote, color = "-1", "red"

    pytest_sec = timing.get("PYTEST_SEC", 0)
    if pytest_sec == 0:
        pytest_sec = int(round(sum(t.time_sec for t in test_results)))

    comment = f"{passed_count} passed, {failed_count} failed, {total_reruns} rerun(s)"
    rows.append(_row(vote, color, "pytest", format_runtime(pytest_sec), "", comment))

    for t in test_results:
        if not t.passed:
            log = _log_link(t.name, "red", logs_url, output_dir)
            rows.append(_detail_row("red", log, f"failed {t.name}"))

    if all_passed:
        for t in test_results:
            if t.rerun_count > 0:
                log = _log_link(t.name, "yellow", logs_url, output_dir)
                rows.append(_detail_row("yellow", log, f"rerun {t.name}"))

    return rows


def _total_row(total_sec: int) -> str:
    return (
        "<tr>\n"
        '\t\t<td><font color="black"></font></td>\n'
        '\t\t<td><font color="black"> </font></td>\n'
        f'\t\t<td><font color="black">{format_runtime(total_sec)}</font></td>\n'
        "<td></td>\n"
        '<td><font color="black"></font></td>\n'
        "</tr>\n"
    )


_INFRA_STAGES = [
    ("Docker test env", "DEV_SUPPORT_IMAGE_BUILD_SEC",
     "Build time for Docker test environment image"),
    ("rsync", "RSYNC_SEC",
     "Copy hbase repo for Docker build context with read-replica image"),
    ("mvn clean", "MVN_CLEAN_SEC",
     "Maven clean to remove previous build artifacts for read-replica image"),
    ("read-replica image", "DOCKER_BUILD_SEC",
     "Docker image used for containers running an HBase read-replica setup"),
]


def build_console_report(timing: dict[str, int],
                         test_results: list[TestResult],
                         logs_url: str = "",
                         output_dir: Path | None = None) -> str:
    all_passed = all(t.passed for t in test_results)

    if all_passed:
        overall_color, overall_vote = "green", "+1"
    else:
        overall_color, overall_vote = "red", "-1"

    # Table 1: Overall header
    header_table = (
        "<table><tbody>\n"
        f'<tr><th><font color="{overall_color}">'
        f"{overall_vote} overall</font></th></tr>\n"
        "</tbody></table>\n"
    )

    # Title
    title_table = (
        "<table><tbody>\n"
        "<tr><th>Read-Replica Nightly Test Console Report</th></tr>\n"
        "</tbody></table>\n"
    )

    # Table 2: Stage results
    rows: list[str] = []

    for subsystem, timing_key, comment in _INFRA_STAGES:
        seconds = timing.get(timing_key, 0)
        rows.append(_infra_row(subsystem, seconds, comment))

    effective_output_dir = output_dir if output_dir is not None else Path()
    rows.extend(_pytest_rows(test_results, timing, logs_url, effective_output_dir))

    total_sec = timing.get("TOTAL_SEC", 0)
    rows.append(_total_row(total_sec))

    stages_table = (
        "<table><tbody>\n"
        "<tr>\n"
        "<th>Vote</th>\n"
        "<th>Subsystem</th>\n"
        "<th>Runtime</th>\n"
        "<th>Log</th>\n"
        "<th>Comment</th>\n"
        "</tr>\n"
        + "".join(rows)
        + "</tbody></table>\n"
    )

    return (
        header_table
        + "<p></p>\n"
        + title_table
        + "<p></p>\n"
        + stages_table
        + "<p></p>\n"
        + "<p>This message was automatically generated.</p>\n"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate a Yetus-style console report for read-replica tests."
    )
    parser.add_argument(
        "--timing", required=True, type=Path,
        help="Path to timing env file (KEY=VALUE pairs)",
    )
    parser.add_argument(
        "--junit", required=True, type=Path,
        help="Path to JUnit XML results file",
    )
    parser.add_argument(
        "--output", required=True, type=Path,
        help="Path to write the HTML console report",
    )
    parser.add_argument(
        "--logs-url", type=str, default="",
        help="Base URL for per-test log directories. "
             "On Jenkins: ${BUILD_URL}artifact/${OUTPUT_DIR_RELATIVE}. "
             "When empty (default), test rows use relative links.",
    )
    args = parser.parse_args(argv)

    if not args.junit.exists():
        print(f"JUnit XML not found: {args.junit}", file=sys.stderr)
        return 1

    timing = parse_timing_env(args.timing)
    test_results = parse_junit_xml(args.junit)
    report_html = build_console_report(
        timing, test_results,
        logs_url=args.logs_url,
        output_dir=args.output.parent,
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(report_html, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
