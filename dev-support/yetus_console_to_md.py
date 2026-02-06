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
Convert Apache Yetus console output to Markdown format.
"""
import re
import sys
from pathlib import Path
from typing import List, Optional, Tuple


# Vote to emoji mapping
VOTE_EMOJI = {
    '+1': '✅',
    '-1': '❌',
    '0': '🆗',
    '+0': '🆗',
    '-0': '⚠️'
}


def convert_vote(vote: str) -> str:
    """Convert vote string to emoji."""
    return VOTE_EMOJI.get(vote, vote)


def is_runtime(text: str) -> bool:
    """Check if text is a runtime like '41m 24s'."""
    return bool(re.match(r'^\d+m\s+\d+s$', text))


def parse_table_row(line: str) -> List[str]:
    """
    Parse a table row and return list of cell values.
    Returns exactly 4 columns: [vote, subsystem, runtime, comment]
    """
    parts = line.split('|')
    # Remove first empty element (from leading |)
    parts = parts[1:] if len(parts) > 1 else []

    result = []
    for p in parts[:4]:  # Take first 4 columns
        result.append(p.strip())

    # Pad to 4 columns if needed
    while len(result) < 4:
        result.append('')

    return result


def process_first_table(lines: List[str], start_idx: int) -> Tuple[List[str], int]:
    """
    Process the first table (Vote, Subsystem, Runtime, Comment).

    Returns:
        Tuple of (markdown lines, next index to process)
    """
    content = []
    i = start_idx

    # Add table header
    content.append('\n')
    content.append('| Vote | Subsystem | Runtime | Comment |\n')
    content.append('|------|-----------|---------|---------|\n')

    # Skip the original separator line
    if i < len(lines) and '===' in lines[i]:
        i += 1

    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        # Check for second table start
        if '|| Subsystem || Report/Notes ||' in line:
            break

        # Skip section separator lines (like +-----------)
        if stripped.startswith('+--'):
            i += 1
            continue

        # Process table rows
        if stripped.startswith('|'):
            parts = parse_table_row(line)
            vote, subsystem, runtime, comment = parts[0], parts[1], parts[2], parts[3]

            # Case 1: Section header (vote and subsystem are empty, has comment)
            if not vote and not subsystem:
                if comment:
                    content.append(f'|  |  |  | {comment} |\n')
                    i += 1
                    continue
                # If there's only runtime, it's a total time row
                elif runtime and is_runtime(runtime):
                    content.append(f'|  |  | {runtime} |  |\n')
                    i += 1
                    continue
                else:
                    # Empty row, skip
                    i += 1
                    continue

            # Case 2: Data row with vote
            if vote in VOTE_EMOJI:
                vote_emoji = convert_vote(vote)
                comment_parts = [comment] if comment else []

                # Check for continuation lines
                i += 1
                while i < len(lines):
                    next_line = lines[i]
                    next_stripped = next_line.strip()

                    if not next_stripped.startswith('|'):
                        break

                    # Check for second table start
                    if '|| Subsystem || Report/Notes ||' in next_line:
                        break

                    next_parts = parse_table_row(next_line)
                    next_vote, next_subsystem, next_runtime, next_comment = next_parts[0], next_parts[1], next_parts[2], next_parts[3]

                    # Stop at new data row
                    if next_vote in VOTE_EMOJI:
                        break

                    # If vote and subsystem are empty, check if it's a continuation
                    if not next_vote and not next_subsystem:
                        # If there's a comment, it's a continuation
                        if next_comment:
                            comment_parts.append(next_comment)
                            i += 1
                        # If there's only runtime, it's a standalone total time row
                        elif next_runtime and is_runtime(next_runtime):
                            break
                        else:
                            i += 1
                    else:
                        break

                comment_text = ' '.join(comment_parts)
                content.append(f'| {vote_emoji} | {subsystem} | {runtime} | {comment_text} |\n')
                continue

            # Case 3: Other cases, skip
            i += 1
            continue

        i += 1

    return content, i


# TODO: Yetus should support this natively, but docker integration with job summaries doesn't seem
#       to work out of the box.
def extract_failed_tests_from_unit_files(output_dir: Path) -> List[Tuple[str, List[str]]]:
    """
    Extract failed test names from patch-unit-*.txt files.

    Parses Maven surefire output to find lines like:
    [ERROR] org.apache.hadoop.hbase.types.TestPBCell.testRoundTrip

    Returns:
        List of (module_name, [failed_test_names]) tuples
    """
    results = []

    for unit_file in output_dir.glob('patch-unit-*.txt'):
        module_name = unit_file.stem.replace('patch-unit-', '')
        failed_tests = set()

        with open(unit_file, 'r') as f:
            in_failures_section = False
            for line in f:
                stripped = line.strip()

                if stripped == '[ERROR] Failures:':
                    in_failures_section = True
                    continue

                if in_failures_section:
                    if stripped.startswith('[ERROR]') and not stripped.startswith('[ERROR]   Run'):
                        test_name = stripped.replace('[ERROR] ', '').strip()
                        if test_name and '.' in test_name:
                            failed_tests.add(test_name)
                    elif stripped.startswith('[INFO]') or not stripped:
                        in_failures_section = False

        if failed_tests:
            results.append((module_name, sorted(failed_tests)))

    return results


def format_failed_tests_section(failed_tests: List[Tuple[str, List[str]]]) -> List[str]:
    """
    Format failed tests into markdown.

    Args:
        failed_tests: List of (module_name, [test_names]) tuples

    Returns:
        List of markdown lines
    """
    if not failed_tests:
        return []

    content = []
    content.append('\n## ❌ Failed Tests\n\n')
    content.append('| Module | Failed Tests |\n')
    content.append('|--------|-------------|\n')

    for module_name, tests in failed_tests:
        tests_str = ', '.join(tests)
        content.append(f'| {module_name} | {tests_str} |\n')

    return content


def process_second_table(lines: List[str], start_idx: int) -> Tuple[List[str], int]:
    """
    Process the second table (Subsystem, Report/Notes).

    Returns:
        Tuple of (markdown lines, next index to process)
    """
    content = []
    i = start_idx

    # Add table header
    content.append('\n## Subsystem Reports\n\n')
    content.append('| Subsystem | Report/Notes |\n')
    content.append('|-----------|------------|\n')

    # Skip the original separator line
    if i < len(lines) and '===' in lines[i]:
        i += 1

    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        if not stripped.startswith('|'):
            break

        # Split by | and get non-empty parts (at least 2)
        parts = [p.strip() for p in stripped.split('|') if p.strip()]
        if len(parts) >= 2:
            content.append(f'| {parts[0]} | {parts[1]} |\n')

        i += 1

    return content, i


def convert_console_to_markdown(input_file: str, output_file: Optional[str] = None) -> str:
    """Convert console to Markdown format."""
    input_path = Path(input_file)
    output_dir = input_path.parent

    with open(input_file, 'r') as f:
        lines = f.readlines()

    content = []
    i = 0
    added_failed_tests = False

    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        # Handle overall line
        if stripped == '-1 overall':
            content.append(f'<h1><b>❌ {stripped}</b></h1>\n')
            i += 1
            continue

        if stripped == '+1 overall':
            content.append(f'<h1><b>✅ {stripped}</b></h1>\n')
            i += 1
            continue

        # Detect first table start
        if '| Vote |' in line and 'Subsystem' in line:
            table_content, i = process_first_table(lines, i + 1)
            content.extend(table_content)

            # Extract and add failed tests from patch-unit-*.txt files
            if not added_failed_tests:
                failed_tests = extract_failed_tests_from_unit_files(output_dir)
                content.extend(format_failed_tests_section(failed_tests))
                added_failed_tests = True
            continue

        # Detect second table start
        if '|| Subsystem || Report/Notes ||' in line:
            table_content, i = process_second_table(lines, i + 1)
            content.extend(table_content)
            continue

        i += 1

    result = ''.join(content)

    if output_file:
        with open(output_file, 'w') as f:
            f.write(result)
        print(f'Converted {input_file} to {output_file}', file=sys.stderr)
    else:
        print(result, end='')

    return result


def main():
    if len(sys.argv) < 2:
        print(f'Usage: {sys.argv[0]} <input_file> [output_file]', file=sys.stderr)
        print(f'  If output_file is not provided, output goes to stdout', file=sys.stderr)
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None

    if not Path(input_file).exists():
        print(f'Error: Input file "{input_file}" does not exist', file=sys.stderr)
        sys.exit(1)

    convert_console_to_markdown(input_file, output_file)


if __name__ == '__main__':
    main()
