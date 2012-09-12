#!/usr/bin/env python

'''
An ad-hoc script to sort functions within the Hbase file. The output should
always be examined, Thrift stubs rebuilt, and the whole project built after
this script is used.
'''

import sys
import re


METHOD_RE = re.compile(r'  [a-zA-Z<>,0-9 ]+ ([a-zA-Z]+)\s*\(')


def appendSection(sections, section):
    i = 0
    while i < len(section) and not section[i]:
        i += 1
    j = len(section) - 1
    while j >= 0 and not section[j]:
        j -= 1
    if i > j:
        return
    section = section[i:j + 1]
    if section == ['}']:
        return
    sections.append(section)


def getFunctionName(section):
    for l in section:
        if l.startswith('    '):
            continue
        m = METHOD_RE.match(l)
        if m:
            return m.group(1)


if __name__ == '__main__':
    args = sys.argv[1:]
    if not args:
        print >> sys.stderr, 'Arguments: thrift_file_name'
        sys.exit(1)

    f_path = args[0]
    inside_service = False
    before_service = []
    sections = []
    section = []
    for l in open(f_path):
        l = l.rstrip()
        if not inside_service:
            print l
        if l.startswith('service Hbase {'):
            inside_service = True
            continue
        if inside_service:
            section.append(l)
            if 'throws' in l and l.endswith(')') or l == '  )':
                appendSection(sections, section)
                section = []

    appendSection(sections, section)
    section_by_name = {}
    for section in sections:
        name = getFunctionName(section)
        if not name:
            print >> sys.stderr, 'Could not determine method name:', \
                '\n'.join(section)
            sys.exit(1)
        section_by_name[name] = section

    sorted_sections = []
    for name, section in sorted(section_by_name.iteritems()):
        sorted_sections.append('\n'.join(section))
    print '\n\n'.join(sorted_sections)
    print '}'

