#
# Copyright 2009 The Apache Software Foundation
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
# Script will be used to stop a regionserver and flag to the master that it is going down for a restart
#
# To see usage for this script, run:
#
#  ${HBASE_HOME}/bin/hbase org.jruby.Main stop_regionserver_for_restart.rb
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
NAME = "manage_dfs_quorum_reads"

# Print usage for this script
def usage
  puts 'Usage: %s.rb <setNumThreads | setTimeout>    <value>' % NAME
  exit!
end

# Check arguments
if ARGV.size != 2
  usage
end

command = ARGV[0]
if command != 'setNumThreads' && command != 'setTimeout'
  usage
end
value = ARGV[1]

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

hostname = InetAddress.getLocalHost().getHostName()
port = c.getInt("hbase.regionserver.port", 0)

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

if command == 'setNumThreads'
  admin.setNumHDFSQuorumReadThreads(address, value.to_i)
else
  admin.setHDFSQuorumReadTimeoutMillis(address, value.to_i)
end
