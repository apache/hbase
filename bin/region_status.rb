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
# View the current status of all regions on an HBase cluster.  This is
# predominantly used to determined if all the regions in META have been
# onlined yet on startup.
#
# To use this script, run:
#
#  ${HBASE_HOME}/bin/hbase org.jruby.Main region_status.rb [wait] [--table <table_name>]

require 'optparse'

usage = 'Usage : ./hbase org.jruby.Main region_status.rb [wait]' \
        '[--table <table_name>]\n'
OptionParser.new do |o|
  o.banner = usage
  o.on('-t', '--table TABLENAME', 'Only process TABLENAME') do |tablename|
    $tablename = tablename
  end
  o.on('-h', '--help', 'Display help message') { puts o; exit }
  o.parse!
end

SHOULD_WAIT = ARGV[0] == 'wait'
if ARGV[0] && !SHOULD_WAIT
  print usage
  exit 1
end

require 'java'

java_import org.apache.hadoop.hbase.HBaseConfiguration
java_import org.apache.hadoop.hbase.TableName
java_import org.apache.hadoop.hbase.HConstants
java_import org.apache.hadoop.hbase.MasterNotRunningException
java_import org.apache.hadoop.hbase.client.HBaseAdmin
java_import org.apache.hadoop.hbase.client.Table
java_import org.apache.hadoop.hbase.client.Scan
java_import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter
java_import org.apache.hadoop.hbase.util.Bytes
java_import org.apache.hadoop.hbase.HRegionInfo
java_import org.apache.hadoop.hbase.MetaTableAccessor
java_import org.apache.hadoop.hbase.HTableDescriptor
java_import org.apache.hadoop.hbase.client.ConnectionFactory

# disable debug logging on this script for clarity
log_level = org.apache.log4j.Level::ERROR
org.apache.log4j.Logger.getLogger('org.apache.zookeeper').setLevel(log_level)
org.apache.log4j.Logger.getLogger('org.apache.hadoop.hbase').setLevel(log_level)

config = HBaseConfiguration.create
config.set 'fs.defaultFS', config.get(HConstants::HBASE_DIR)
connection = ConnectionFactory.createConnection(config)
# wait until the master is running
admin = nil
loop do
  begin
    admin = connection.getAdmin
    break
  rescue MasterNotRunningException => e
    print 'Waiting for master to start...\n'
    sleep 1
  end
end

meta_count = 0
server_count = 0

# scan META to see how many regions we should have
if $tablename.nil?
  scan = Scan.new
else
  tableNameMetaPrefix = $tablename + HConstants::META_ROW_DELIMITER.chr
  scan = Scan.new(
    (tableNameMetaPrefix + HConstants::META_ROW_DELIMITER.chr).to_java_bytes
  )
end
scan.setCacheBlocks(false)
scan.setCaching(10)
scan.setFilter(FirstKeyOnlyFilter.new)
INFO = 'info'.to_java_bytes
REGION_INFO = 'regioninfo'.to_java_bytes
scan.addColumn INFO, REGION_INFO
table = nil
iter = nil
loop do
  begin
    table = connection.getTable(TableName.valueOf('hbase:meta'))
    scanner = table.getScanner(scan)
    iter = scanner.iterator
    break
  rescue IOException => ioe
    print "Exception trying to scan META: #{ioe}"
    sleep 1
  end
end
while iter.hasNext
  result = iter.next
  rowid = Bytes.toString(result.getRow)
  rowidStr = java.lang.String.new(rowid)
  if !$tablename.nil? && !rowidStr.startsWith(tableNameMetaPrefix)
    # Gone too far, break
    break
  end
  region = MetaTableAccessor.getHRegionInfo(result)
  unless region.isOffline
    # only include regions that should be online
    meta_count += 1
  end
end
scanner.close
# If we're trying to see the status of all HBase tables, we need to include the
# hbase:meta table, that is not included in our scan
meta_count += 1 if $tablename.nil?

# query the master to see how many regions are on region servers
$TableName = TableName.valueOf($tablename.to_java_bytes) unless $tablename.nil?
loop do
  if $tablename.nil?
    server_count = admin.getClusterStatus.getRegionsCount
  else
    connection = ConnectionFactory.createConnection(config)
    server_count = MetaTableAccessor.allTableRegions(connection, $TableName).size
  end
  print "Region Status: #{server_count} / #{meta_count}\n"
  if SHOULD_WAIT && server_count < meta_count
    # continue this loop until server & meta count match
    sleep 10
  else
    break
  end
end
admin.close
connection.close

exit server_count == meta_count ? 0 : 1
