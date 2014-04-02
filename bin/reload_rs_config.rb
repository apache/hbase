#
# Copyright 2013 The Apache Software Foundation
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
# This script can be used to make either the region server running on the
# current host reload, or make all the region servers reload the configuration.
#
# To see usage for this script, run:
#
#  ${HBASE_HOME}/bin/hbase org.jruby.Main ${HBASE_HOME}/bin/reload_rs_config.rb usage
#
include Java
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.ClusterStatus
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HServerInfo
import org.apache.hadoop.hbase.HServerAddress
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.HConstants
import org.apache.commons.logging.LogFactory
import java.net.InetAddress

# Name of this script
NAME = "reload_rs_config"

def usage
  puts 'Usage: %s.rb [reloadAll | hostname]' % NAME
  puts 'Use reloadAll to make all the Region Servers reload their configurations, or,'
  puts 'Use a specific hostname to make a specific region server reload its configuration'
  exit!
end

reloadAll = false

if ARGV.size != 1
  usage
else
  if ARGV[0] == 'reloadAll'
    reloadAll = true
  else
    hostname = ARGV[0]
  end
end

# Get configuration to use.
c = HBaseConfiguration.create()

# Taken from add_table.rb script
# Set hadoop filesystem configuration using the hbase.rootdir.
# Otherwise, we'll always use localhost though the hbase.rootdir
# might be pointing at hdfs location.
c.set("fs.default.name", c.get(HConstants::HBASE_DIR))

# Get a logger instance.
LOG = LogFactory.getLog(NAME)

# get the admin interface
admin = HBaseAdmin.new(c)

if c.getBoolean(HConstants::REGIONSERVER_USE_THRIFT, HConstants::DEFAULT_REGIONSERVER_USE_THRIFT)
  puts "Swift server seems to be enabled. Trying to connect on the thrift port."
  port = c.getInt(HConstants::REGIONSERVER_SWIFT_PORT, HConstants::DEFAULT_REGIONSERVER_SWIFT_PORT)
else
  puts "Swift server seems to be disabled. Trying to connect on the Hadoop RPC port."
  port = c.getInt(HConstants::REGIONSERVER_PORT, HConstants::DEFAULT_REGIONSERVER_PORT)
end

if reloadAll
  # get the cluster servers
  servers = admin.getClusterStatus().getServerInfo()

  servers.each do |server|
    address = server.getServerAddress()
    puts "Updating the configuration at host " + address.getHostname()
    admin.updateConfiguration(address)
  end
else
  if port > 0
    address = HServerAddress.new(hostname, port)
  else
    address = nil
  
    # get the cluster servers
    servers = admin.getClusterStatus().getServerInfo()

    servers.each do |server|
      if server.getServerAddress().getHostname() == InetAddress.getLocalHost().getHostName()
        address = server.getServerAddress()
        break
      end
    end
  end

  if address == nil
    puts "invalid server"
    exit
  end

  puts "Updating the configuration at host " + address.getHostname()
  admin.updateConfiguration(address)
end

