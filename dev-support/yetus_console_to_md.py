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
import os
import re
import sys
from io import TextIOWrapper
from pathlib import Path
from typing import Dict, List, Optional, Tuple

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


def parse_table_row(line: str) -> Tuple[str, str, str, str]:
    """
    Parse a table row and return tuple of cell values.
    Returns exactly 4 columns: (vote, subsystem, runtime, comment)
    """
    parts = line.split('|')
    # Remove first empty element (from leading |)
    parts = parts[1:] if len(parts) > 1 else []

    # Take first 4 columns and strip whitespace
    result: List[str] = [p.strip() for p in parts[:4]]

    # Pad to 4 columns if needed
    while len(result) < 4:
        result.append('')

    return result[0], result[1], result[2], result[3]


def is_results_section_start(line: str) -> bool:
    """Check if line indicates the start of Results section."""
    return bool(re.search(r'^\[\w+] Results:', line.strip()))


def is_tests_run_summary(line: str) -> bool:
    """Check if line is the Tests run summary line."""
    return bool(re.search(r'^\[\w+] Tests run:', line.strip()))


def parse_results_section(
  f: TextIOWrapper,
  failures: List[str],
  flakes: List[str],
  errors: List[str]
) -> None:
    """
    Parse the Results section within a patch-unit file.
    """
    current_error_type = None
    while line := f.readline():
        stripped = line.strip()

        # Section end markers
        if is_tests_run_summary(line):
            return

        # Detect error type sections
        if re.search(r'^\[\w+] Failures:', stripped):
            current_error_type = failures
        elif re.search(r'^\[\w+] Flakes:', stripped):
            current_error_type = flakes
        elif re.search(r'^\[\w+] Errors:', stripped):
            current_error_type = errors
        else:
            # Parse test entries
            if current_error_type is not None:
                test_match = re.search(
                    r'^\[\w+]\s+((?:org\.)?\S+\.(?:\w+\.)*\w+\.\w+)',
                    stripped
                )
                if test_match:
                    test_name = test_match.group(1)
                    if 'test' in test_name.lower():
                        current_error_type.append(test_name)


def skip_to_results_section(f: TextIOWrapper) -> bool:
    """
    Skip the io stream to the Results section.
    After calling this method, the TextIOWrapper will locate at the next line of "Results: "

    Returns:
        True if we find a results section, False if we have reached the EOF
    """
    while line := f.readline():
        if is_results_section_start(line):
            return True
    return False


def scan_all_tests(dir: Path) -> Dict[str, str]:
    """
    Scan the archiver dir to find all the tests and their module

    Returns:
        Dict mapping test name to module name
    """
    module = None
    module_to_test_name = {}
    for dirpath, _, filenames in os.walk(dir):
        if len(filenames) > 0:
            # /archiver/<module>/target/surefire-reports
            module = dirpath.split(os.sep)[-3]
        for filename in filenames:
            match = re.match(r'(org\.apache\.[^-]+)\.txt', filename)
            if match:
                module_to_test_name[match.group(1)] = module
    return module_to_test_name


def parse_patch_unit_file(
  file_path: Path,
  failures: List[str],
  flakes: List[str],
  errors: List[str]
) -> None:
    """
    Parse a patch-unit-*.txt file and extract failed tests by module.
    """
    with open(file_path, 'r') as f:
        while skip_to_results_section(f):
            parse_results_section(f, failures, flakes, errors)


def get_module(test_name: str, test_name_to_module: Dict[str, str]) -> str:
    rindex_of_bracket = test_name.rfind('[')
    if rindex_of_bracket > 0:
        # parameterized test, remove the tailing parameters
        test_name = test_name[:rindex_of_bracket]

    module = test_name_to_module.get(test_name)
    if module:
        return module

    # usually the failed test name has the method name suffix, but the test_name_to_module only
    # contains class name, so let's try to remove the last part and try again
    rindex_of_dot = test_name.rfind('.')
    if rindex_of_dot > 0:
        test_name = test_name[:rindex_of_dot]

    module = test_name_to_module.get(test_name)
    if module:
        return module
    return 'default'


def increase(module_to_count: Dict[str, int], module: str) -> None:
    if module in module_to_count:
        module_to_count[module] += 1
    else:
        module_to_count[module] = 1


def add_to_details(test_name: str, module: str, error_type: str,
                   details: Dict[str, Dict[str, List[str]]]) -> None:
    if module not in details:
        error_type_to_tests = {}
        details[module] = error_type_to_tests
    else:
        error_type_to_tests = details[module]

    if error_type in error_type_to_tests:
        error_type_to_tests[error_type].append(test_name)
    else:
        error_type_to_tests[error_type] = [test_name]


def process_failed_tests(
  error_type: str,
  failed_tests: List[str],
  module_to_test_name: Dict[str, str],
  counts: Dict[str, Dict[str, int]],
  details: Dict[str, Dict[str, List[str]]]
) -> None:
    for test_name in failed_tests:
        module = get_module(test_name, module_to_test_name)
        increase(counts[error_type], module)
        add_to_details(test_name, module, error_type, details)


def aggregate_failed_tests(yetus_dir: Path) -> Tuple[
    Dict[str, Dict[str, int]], Dict[str, Dict[str, List[str]]]]:
    """
    Aggregate failed tests from all patch-unit-*.txt files.

    Returns:
        Tuple of:
        - counts: {error_type: {module: count}}
        - details: {module: {error_type: [test_names]}}
    """
    patch_files = list(yetus_dir.glob('patch-unit-*.txt'))

    if not patch_files:
        return {}, {}

    # Aggregate results from all files
    failures = []
    flakes = []
    errors = []

    for patch_file in patch_files:
        parse_patch_unit_file(patch_file, failures, flakes, errors)

    if not failures and not flakes and not errors:
        return {}, {}

    counts = {'Failures': {}, 'Flakes': {}, 'Errors': {}}
    details = {}
    module_to_test_name = scan_all_tests(yetus_dir / 'archiver')
    process_failed_tests('Failures', failures, module_to_test_name, counts, details)
    process_failed_tests('Flakes', flakes, module_to_test_name, counts, details)
    process_failed_tests('Errors', errors, module_to_test_name, counts, details)

    return dict(counts), dict(details)


def generate_failed_tests_table(
  counts: Dict[str, Dict[str, int]],
  details: Dict[str, Dict[str, List[str]]]
) -> List[str]:
    """Generate the Failed Tests HTML table."""
    total_failures = sum(sum(m.values()) for m in counts.values())
    if total_failures == 0:
        return []

    content = [
        '\n## Failed Tests\n\n',
        '<table>\n',
        '<thead><tr><th>Error Type</th><th>Count</th><th>Module</th><th>Tests</th></tr></thead>\n',
        '<tbody>\n'
    ]

    error_types = ['Failures', 'Flakes', 'Errors']

    for error_type in error_types:
        if error_type not in counts:
            continue

        modules = counts[error_type]
        total_count = sum(modules.values())
        num_modules = len(modules)

        first_row = True
        for module in sorted(modules.keys()):
            tests = details.get(module, {}).get(error_type, [])
            tests_str = '<br>'.join(sorted(set(tests))) if tests else ''

            if first_row:
                content.append(
                    f'<tr><td rowspan="{num_modules}">{error_type}</td>'
                    f'<td rowspan="{num_modules}">{total_count}</td>'
                    f'<td>{module}</td><td>{tests_str}</td></tr>\n'
                )
                first_row = False
            else:
                content.append(f'<tr><td>{module}</td><td>{tests_str}</td></tr>\n')

    content.extend(['</tbody>\n', '</table>\n'])

    return content


def collect_continuation_lines(
  lines: List[str],
  start_idx: int
) -> Tuple[List[str], int]:
    """
    Collect continuation lines for a table row.

    Args:
        lines: All lines from the file
        start_idx: Index to start checking from

    Returns:
        Tuple of (list of comment parts, next index to process)
    """
    comment_parts = []
    i = start_idx

    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        if not stripped.startswith('|'):
            break

        if '|| Subsystem || Report/Notes ||' in line:
            break

        vote, _, runtime, comment = parse_table_row(line)

        # Stop at new data row
        if vote in VOTE_EMOJI:
            break

        # Empty vote/subsystem means continuation or separator
        if not vote:
            if comment:
                comment_parts.append(comment)
                i += 1
            elif runtime and is_runtime(runtime):
                break
            else:
                i += 1
        else:
            break

    return comment_parts, i


def process_first_table(lines: List[str], start_idx: int) -> Tuple[List[str], int]:
    """
    Process the first table (Vote, Subsystem, Runtime, Comment).

    Returns:
        Tuple of (Markdown lines, next index to process)
    """
    content = [
        '\n',
        '| Vote | Subsystem | Runtime | Comment |\n',
        '|------|-----------|---------|---------|\n'
    ]

    i = start_idx

    # Skip the original separator line
    if i < len(lines) and '===' in lines[i]:
        i += 1

    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        if '|| Subsystem || Report/Notes ||' in line:
            break

        if stripped.startswith('+--'):
            i += 1
            continue

        if not stripped.startswith('|'):
            i += 1
            continue

        vote, subsystem, runtime, comment = parse_table_row(line)

        # Section header (vote and subsystem are empty)
        if not vote and not subsystem:
            if comment:
                content.append(f'|  |  |  | {comment} |\n')
            elif runtime and is_runtime(runtime):
                content.append(f'|  |  | {runtime} |  |\n')
            i += 1
            continue

        # Data row with vote
        if vote in VOTE_EMOJI:
            vote_emoji = convert_vote(vote)
            comment_parts = [comment] if comment else []

            continuation_parts, i = collect_continuation_lines(lines, i + 1)
            comment_parts.extend(continuation_parts)

            comment_text = ' '.join(comment_parts)
            content.append(f'| {vote_emoji} | {subsystem} | {runtime} | {comment_text} |\n')
            continue

        # Other cases, skip
        i += 1

    return content, i


def process_second_table(lines: List[str], start_idx: int) -> Tuple[List[str], int]:
    """
    Process the second table (Subsystem, Report/Notes).

    Returns:
        Tuple of (Markdown lines, next index to process)
    """
    content = [
        '\n## Subsystem Reports\n\n',
        '| Subsystem | Report/Notes |\n',
        '|-----------|------------|\n'
    ]

    i = start_idx

    # Skip the original separator line
    if i < len(lines) and '===' in lines[i]:
        i += 1

    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        if not stripped.startswith('|'):
            break

        # Split by | and get non-empty parts
        parts = [p.strip() for p in stripped.split('|') if p.strip()]
        if len(parts) >= 2:
            content.append(f'| {parts[0]} | {parts[1]} |\n')

        i += 1

    return content, i


def convert_console_to_markdown(input_dir: str, output_file: Optional[str] = None) -> str:
    """Convert Yetus console output to Markdown format."""
    input_path = Path(input_dir)

    if not input_path.is_dir():
        print(f'Error: Input path "{input_dir}" is not a directory', file=sys.stderr)
        sys.exit(1)

    console_file = input_path / 'console.txt'
    if not console_file.exists():
        print(f'Error: console.txt not found in "{input_dir}"', file=sys.stderr)
        sys.exit(1)

    with open(console_file, 'r') as f:
        lines = f.readlines()

    content = []
    i = 0

    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        if stripped == '-1 overall':
            content.append(f'<h1><b>❌ {stripped}</b></h1>\n')
            i += 1
        elif stripped == '+1 overall':
            content.append(f'<h1><b>✅ {stripped}</b></h1>\n')
            i += 1
        elif '| Vote |' in line and 'Subsystem' in line:
            table_content, i = process_first_table(lines, i + 1)
            content.extend(table_content)

            counts, details = aggregate_failed_tests(input_path)
            if counts:
                content.extend(generate_failed_tests_table(counts, details))
        elif '|| Subsystem || Report/Notes ||' in line:
            table_content, i = process_second_table(lines, i + 1)
            content.extend(table_content)
        else:
            i += 1

    result = ''.join(content)

    if output_file:
        with open(output_file, 'w') as f:
            f.write(result)
        print(f'Converted {input_dir} to {output_file}', file=sys.stderr)
    else:
        print(result, end='')

    return result


def main():
    if len(sys.argv) < 2:
        print(f'Usage: {sys.argv[0]} <input_directory> [output_file]', file=sys.stderr)
        print(
            f'  input_directory: Directory containing console.txt and optional patch-unit-*.txt files',
            file=sys.stderr)
        print(f'  If output_file is not provided, output goes to stdout', file=sys.stderr)
        sys.exit(1)

    input_dir = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None

    if not Path(input_dir).exists():
        print(f'Error: Input directory "{input_dir}" does not exist', file=sys.stderr)
        sys.exit(1)

    convert_console_to_markdown(input_dir, output_file)


if __name__ == '__main__':
    main()
