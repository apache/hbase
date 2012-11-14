#!/usr/bin/env python
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

'''
Merges Hadoop/HBase configuration files in the given order, so that options
specified in later configuration files override those specified in earlier
files.
'''

import os
import re
import sys
import textwrap

from optparse import OptionParser
from xml.dom.minidom import parse, getDOMImplementation


class MergeConfTool:
    '''
    Merges the given set of Hadoop/HBase configuration files, with later files
    overriding earlier ones.
    '''

    INDENT = ' ' * 2

    # Description text is inside configuration, property, and description tags.
    DESC_INDENT = INDENT * 3

    def main(self):
        '''The main entry point for the configuration merge tool.'''
        self.parse_options()
        self.merge()

    def parse_options(self):
        '''Parses command-line options.'''
        parser = OptionParser(usage='%prog <input_conf_files> -o <output_file>')
        parser.add_option('-o', '--output_file',
            help='Destination configuration file')
        opts, input_files = parser.parse_args()
        if not opts.output_file:
            self.fatal('--output_file is not specified')
        if not input_files:
            self.fatal('No input files specified')
        for f_path in input_files:
            if not os.path.isfile(f_path):
                self.fatal('Input file %s does not exist' % f_path)
        self.input_files = input_files
        self.output_file = opts.output_file

    def merge(self):
        '''Merges input configuration files into the output file.'''
        values = {}        # Conf key to values
        source_files = {}  # Conf key to the file name where the value came from
        descriptions = {}  # Conf key to description (optional)

        # Read input files in the given order and update configuration maps
        for f_path in self.input_files:
            self.current_file = f_path
            f_basename = os.path.basename(f_path)
            f_dom = parse(f_path)
            for property in f_dom.getElementsByTagName('property'):
                self.current_property = property
                name = self.element_text('name')
                value = self.element_text('value')
                values[name] = value
                source_files[name] = f_basename

                if property.getElementsByTagName('description'):
                    descriptions[name] = self.element_text('description')

        # Create the output configuration file
        dom_impl = getDOMImplementation()
        self.merged_conf = dom_impl.createDocument(None, 'configuration', None)
        for k in sorted(values.keys()):
            new_property = self.merged_conf.createElement('property')
            c = self.merged_conf.createComment('from ' + source_files[k])
            new_property.appendChild(c)
            self.append_text_child(new_property, 'name', k)
            self.append_text_child(new_property, 'value', values[k])

            description = descriptions.get(k, None)
            if description:
                description = ' '.join(description.strip().split())
                textwrap_kwargs = {}
                if sys.version_info >= (2, 6):
                    textwrap_kwargs = dict(break_on_hyphens=False)
                description = ('\n' + self.DESC_INDENT).join(
                    textwrap.wrap(description, 80 - len(self.DESC_INDENT),
                        break_long_words=False, **textwrap_kwargs))
                self.append_text_child(new_property, 'description', description)
            self.merged_conf.documentElement.appendChild(new_property)

        pretty_conf = self.merged_conf.toprettyxml(indent=self.INDENT)

        # Remove space before and after names and values. This way we don't have
        # to worry about leading and trailing whitespace creeping in.
        pretty_conf = re.sub(r'(?<=<name>)\s*', '', pretty_conf)
        pretty_conf = re.sub(r'(?<=<value>)\s*', '', pretty_conf)
        pretty_conf = re.sub(r'\s*(?=</name>)', '', pretty_conf)
        pretty_conf = re.sub(r'\s*(?=</value>)', '', pretty_conf)

        out_f = open(self.output_file, 'w')
        try:
            out_f.write(pretty_conf)
        finally:
            out_f.close()

    def element_text(self, tag_name):
        return self.whole_text(self.only_element(tag_name))

    def fatal(self, msg):
        print >> sys.stderr, msg
        sys.exit(1)

    def only_element(self, tag_name):
        l = self.current_property.getElementsByTagName(tag_name)
        if len(l) != 1:
            self.fatal('Invalid property in %s, only one '
                '"%s" element expected: %s' % (self.current_file, tag_name,
                    self.current_property.toxml()))
        return l[0]

    def whole_text(self, element):
        if len(element.childNodes) > 1:
            self.fatal('No more than one child expected in %s: %s' % (
                self.current_file, element.toxml()))
        if len(element.childNodes) == 1:
            return element.childNodes[0].wholeText.strip()
        return ''

    def append_text_child(self, property_element, tag_name, value):
        element = self.merged_conf.createElement(tag_name)
        element.appendChild(self.merged_conf.createTextNode(value))
        property_element.appendChild(element)


if __name__ == '__main__':
    MergeConfTool().main()

