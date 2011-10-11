#!/usr/bin/env hbase-jruby
# Prints the hostname of the machine running the active master.

include Java

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper

# disable debug/info logging on this script for clarity
log_level = org.apache.log4j.Level::ERROR
org.apache.log4j.Logger.getLogger('org.apache.zookeeper').setLevel(log_level)
org.apache.log4j.Logger.getLogger('org.apache.hadoop.hbase').setLevel(log_level)

config = HBaseConfiguration.create
config.set 'fs.default.name', config.get(HConstants::HBASE_DIR)

zk = ZooKeeperWrapper.createInstance(config, 'get_active_master')
begin
  master_address = zk.readMasterAddress(nil)
  if master_address
    puts master_address.getHostname()
  else
    puts 'Master not running'
  end
ensure
  zk.close()
end
