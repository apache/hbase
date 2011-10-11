# View the current status of all regions on an HBase cluster.  This is
# predominantly used to determined if all the regions in META have been
# onlined yet on startup.
#
# To use this script, run:
#
#  ${HBASE_HOME}/bin/hbase org.jruby.Main region_status.rb [wait]

SHOULD_WAIT=ARGV[0] == 'wait'

if ARGV[0] and not SHOULD_WAIT
  print "USAGE: #{$0} [wait] \n"
  exit 1
end

require 'java'

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.MasterNotRunningException
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.util.Writables

# disable debug logging on this script for clarity
log_level = org.apache.log4j.Level::ERROR
org.apache.log4j.Logger.getLogger("org.apache.zookeeper").setLevel(log_level)
org.apache.log4j.Logger.getLogger("org.apache.hadoop.hbase").setLevel(log_level)

config = HBaseConfiguration.create
config.set 'fs.default.name', config.get(HConstants::HBASE_DIR)

# wait until the master is running
admin = nil
while true
  begin
    admin = HBaseAdmin.new config
    break
  rescue MasterNotRunningException => e
    print 'Waiting for master to start...\n'
    sleep 1
  end
end

meta_count = 0
server_count = 0

# scan META to see how many regions we should have
scan = Scan.new
scan.cache_blocks = false
scan.caching = 10
scan.setFilter(FirstKeyOnlyFilter.new)
INFO = 'info'.to_java_bytes
REGION_INFO = 'regioninfo'.to_java_bytes
scan.addColumn INFO, REGION_INFO
table = nil
iter = nil
while true
  begin
    table = HTable.new config, '.META.'.to_java_bytes
    scanner = table.getScanner(scan)
    iter = scanner.iterator
    break
  rescue IOException => ioe
    print "Exception trying to scan META: #{ioe}"
    sleep 1
  end
end
while iter.hasNext
  row = iter.next
  region = Writables.getHRegionInfo row.getValue(INFO, REGION_INFO)
  if not region.isOffline
    # only include regions that should be online
    meta_count += 1
  end
end
scanner.close
# META count does not include the -ROOT- and .META. regions *doh*
meta_count += 2

# query the master to see how many regions are on region servers
while true
  server_count = admin.getClusterStatus().getRegionsCount()
  print "Region Status: #{server_count} / #{meta_count}\n"
  if SHOULD_WAIT and server_count < meta_count
    #continue this loop until server & meta count match
    sleep 10
  else
    break
  end
end

exit server_count == meta_count ? 0 : 1
