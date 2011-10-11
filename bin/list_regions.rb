# List regions belonging to a table + what server they're on
# To use this script, run:
#
#  ${HBASE_HOME}/bin/hbase org.jruby.Main list_regions.rb

TABLE_NAME=ARGV[0]

unless TABLE_NAME
  print "USAGE: #{$0} TABLE \n"
  exit 1
end

require 'java'

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.util.Writables

config = HBaseConfiguration.new
config.set 'fs.default.name', config.get(HConstants::HBASE_DIR)
JAVA_TABLE_NAME=TABLE_NAME.to_java_bytes
INFO_FAMILY = 'info'.to_java_bytes
REGION_INFO_COLUMN = 'regioninfo'.to_java_bytes
SERVER_COLUMN = 'server'.to_java_bytes

table = HTable.new config, '.META.'.to_java_bytes

scan = Scan.new
scan.addColumn INFO_FAMILY, REGION_INFO_COLUMN
scan.addColumn INFO_FAMILY, SERVER_COLUMN
scanner = table.getScanner scan

print "Finding regions in %s...\n" % [TABLE_NAME]
  while row = scanner.next
  region = Writables.getHRegionInfo row.getValue(INFO_FAMILY, REGION_INFO_COLUMN)
  next unless Bytes.equals(region.getTableDesc.getName, JAVA_TABLE_NAME)
  server_bytes = row.getValue(INFO_FAMILY, SERVER_COLUMN)
  server = server_bytes ? String.from_java_bytes(server_bytes) : 'no server'

  table =  String.from_java_bytes region.getTableDesc.getName
  print "%s - %s\n"  % [region.getRegionNameAsString, server]
end
scanner.close
print "Finished."
