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

"""Materialize pytest-html 4.x report rows as static HTML.

pytest-html v4 stores results in data-jsonblob and builds the results table with
JavaScript. Jenkins (and other hosts with strict Content-Security-Policy) block
inline scripts on archived HTML, so the summary counts appear but the table stays
empty. This script injects tbody/environment rows before publish/archive.
"""

from __future__ import annotations

import html
import json
import re
import sys
from pathlib import Path

CSP_STATIC_MARKER = "<!-- read-replica-csp-static-rows -->"


def _format_environment_value(value: object) -> str:
    if isinstance(value, dict):
        items = "".join(
            f"<li>{html.escape(str(k))}: {html.escape(str(v))}</li>"
            for k, v in value.items()
        )
        return f"<ul>{items}</ul>"
    return html.escape(str(value))


def _build_test_tbodies(tests: dict) -> str:
    chunks: list[str] = []
    for test_id, entries in tests.items():
        for entry in entries:
            row_cells = "".join(entry.get("resultsTableRow", []))
            result = str(entry.get("result", "Unknown")).lower()
            safe_test_id = html.escape(test_id, quote=True)
            log = entry.get("log") or ""
            extras_row = ""
            if log:
                log_html = html.escape(log).replace("\n", "<br/>\n")
                extras_row = (
                    '<tr class="extras-row"><td class="extra" colspan="4">'
                    '<div class="logwrapper"><div class="log">'
                    f"{log_html}</div></div></td></tr>"
                )
            chunks.append(
                f'<tbody class="results-table-row {result}" id="{safe_test_id}">'
                f'<tr class="collapsible">{row_cells}</tr>{extras_row}</tbody>'
            )
    return "\n".join(chunks)


def materialize(report_path: Path) -> None:
    text = report_path.read_text(encoding="utf-8")
    if CSP_STATIC_MARKER in text:
        return

    match = re.search(
        r'id="data-container"\s+data-jsonblob="([^"]*)"',
        text,
        flags=re.DOTALL,
    )
    if not match:
        raise SystemExit(f"No data-jsonblob found in {report_path}")

    data = json.loads(html.unescape(match.group(1)))
    tests = data.get("tests", {})
    environment = data.get("environment", {})

    tbodies = _build_test_tbodies(tests)
    env_rows = "".join(
        f"<tr><td>{html.escape(str(key))}</td><td>{_format_environment_value(val)}</td></tr>"
        for key, val in environment.items()
    )

    table_match = re.search(
        r"(<table id=\"results-table\">.*?<thead id=\"results-table-head\">.*?</thead>)",
        text,
        flags=re.DOTALL,
    )
    if not table_match:
        raise SystemExit(f"results-table header not found in {report_path}")

    injection = f"{CSP_STATIC_MARKER}\n{tbodies}"
    text = text.replace(table_match.group(1), table_match.group(1) + "\n" + injection, 1)

    text = text.replace(
        '<table id="environment"></table>',
        f"<table id=\"environment\">\n{env_rows}\n</table>",
        1,
    )

    report_path.write_text(text, encoding="utf-8")


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(f"Usage: {argv[0]} <pytest-html-report.html>", file=sys.stderr)
        return 2
    materialize(Path(argv[1]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
