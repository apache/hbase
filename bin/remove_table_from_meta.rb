#!/usr/bin/env hbase-jruby
#
# Copyright 2011 The Apache Software Foundation
#
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
#
# Script adds a table back to a running hbase.
# Currently only works on if table data is in place.
#
# To see usage for this script, run:
#
#  ${HBASE_HOME}/bin/hbase org.jruby.Main remove_table_from_meta.rb
#

include Java
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.regionserver.HRegion
import org.apache.hadoop.hbase.HRegionInfo
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.util.FSUtils
import org.apache.hadoop.hbase.util.Writables
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.commons.logging.LogFactory

# Name of this script
NAME = "remove_table_from_meta"

# Print usage for this script
def usage
  puts 'Usage: %s.rb TABLE_NAME' % NAME
  puts
  puts 'CAUTION: this script is extremely dangerous!'
  puts
  puts 'This script deletes all mentions of the given table name from the'
  puts 'META table. Make sure to disable the table before running this'
  puts 'script. Disabling the table, running this script, and removing the'
  puts 'table directory from HDFS effectively drops the table.'
  exit!
end

# Get configuration to use.
c = HBaseConfiguration.new()

# Set hadoop filesystem configuration using the hbase.rootdir.
# Otherwise, we'll always use localhost though the hbase.rootdir
# might be pointing at hdfs location.
c.set("fs.default.name", c.get(HConstants::HBASE_DIR))
fs = FileSystem.get(c)

# Get a logger and a metautils instance.
LOG = LogFactory.getLog(NAME)

# Check arguments
if ARGV.size != 1
  usage
end

# Get table name
tableName = ARGV[0]

HTableDescriptor.isLegalTableName(tableName.to_java_bytes)

puts 'Assuming the table has been disabled. If not, do not proceed.'
puts ('Delete all occurrences of the table ' + tableName +
      ' from .META.? [y/N]')
answer = STDIN.gets().strip()
if answer != 'y' && answer != 'Y'
  exit!
end

# Clean mentions of table from .META.
# Scan the .META. and remove all lines that begin with tablename
LOG.info("Deleting mention of " + tableName + " from .META.")
metaTable = HTable.new(c, HConstants::META_TABLE_NAME)
tableNameMetaPrefix = tableName + HConstants::META_ROW_DELIMITER.chr
scan = Scan.new((tableNameMetaPrefix +
                HConstants::META_ROW_DELIMITER.chr).to_java_bytes)
scanner = metaTable.getScanner(scan)

# Use java.lang.String doing compares.  Ruby String is a bit odd.
tableNameStr = java.lang.String.new(tableName)
while (result = scanner.next())
  rowid = Bytes.toString(result.getRow())
  rowidStr = java.lang.String.new(rowid)
  if not rowidStr.startsWith(tableNameMetaPrefix)
    # Gone too far, break
    break
  end
  LOG.info("Found row in catalog: " + rowid)
  LOG.info("Deleting row from catalog: " + rowid);
  d = Delete.new(result.getRow())
  metaTable.delete(d)
end

scanner.close()
