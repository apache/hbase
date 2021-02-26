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

# Add or remove servers from draining mode via zookeeper
# Deprecated in 2.0, and will be removed in 3.0. Use Admin decommission
# API instead.

require 'optparse'
include Java

java_import org.apache.hadoop.hbase.HBaseConfiguration
java_import org.apache.hadoop.hbase.client.ConnectionFactory
java_import org.apache.hadoop.hbase.client.HBaseAdmin
java_import org.apache.hadoop.hbase.zookeeper.ZKUtil
java_import org.apache.hadoop.hbase.zookeeper.ZNodePaths
java_import org.slf4j.LoggerFactory

# Name of this script
NAME = 'draining_servers'.freeze

# Do command-line parsing
options = {}
optparse = OptionParser.new do |opts|
  opts.banner = "Usage: ./hbase org.jruby.Main #{NAME}.rb [options] add|remove|list <hostname>|<host:port>|<servername> ..."
  opts.separator 'Add remove or list servers in draining mode. Can accept either hostname to drain all region servers' \
                 'in that host, a host:port pair or a host,port,startCode triplet. More than one server can be given separated by space'
  opts.on('-h', '--help', 'Display usage information') do
    puts opts
    exit
  end
end
optparse.parse!

# Return array of servernames where servername is hostname+port+startcode
# comma-delimited
def getServers(admin)
  serverInfos = admin.getClusterStatus.getServers
  servers = []
  serverInfos.each do |server|
    servers << server.getServerName
  end
  servers
end

# rubocop:disable Metrics/AbcSize
def getServerNames(hostOrServers, config)
  ret = []
  connection = ConnectionFactory.createConnection(config)

  hostOrServers.each do |host_or_server|
    # check whether it is already serverName. No need to connect to cluster
    parts = host_or_server.split(',')
    if parts.size == 3
      ret << host_or_server
    else
      admin ||= connection.getAdmin
      servers = getServers(admin)

      host_or_server = host_or_server.tr(':', ',')
      servers.each do |server|
        ret << server if server.start_with?(host_or_server)
      end
    end
  end

  admin.close if admin
  connection.close
  ret
end

def addServers(_options, hostOrServers)
  config = HBaseConfiguration.create
  servers = getServerNames(hostOrServers, config)

  zkw = org.apache.hadoop.hbase.zookeeper.ZKWatcher.new(config, 'draining_servers', nil)

  begin
    parentZnode = zkw.getZNodePaths.drainingZNode
    servers.each do |server|
      node = ZNodePaths.joinZNode(parentZnode, server)
      ZKUtil.createAndFailSilent(zkw, node)
    end
  ensure
    zkw.close
  end
end

def removeServers(_options, hostOrServers)
  config = HBaseConfiguration.create
  servers = getServerNames(hostOrServers, config)

  zkw = org.apache.hadoop.hbase.zookeeper.ZKWatcher.new(config, 'draining_servers', nil)

  begin
    parentZnode = zkw.getZNodePaths.drainingZNode
    servers.each do |server|
      node = ZNodePaths.joinZNode(parentZnode, server)
      ZKUtil.deleteNodeFailSilent(zkw, node)
    end
  ensure
    zkw.close
  end
end
# rubocop:enable Metrics/AbcSize

# list servers in draining mode
def listServers(_options)
  config = HBaseConfiguration.create

  zkw = org.apache.hadoop.hbase.zookeeper.ZKWatcher.new(config, 'draining_servers', nil)

  begin
    parentZnode = zkw.getZNodePaths.drainingZNode
    servers = ZKUtil.listChildrenNoWatch(zkw, parentZnode)
    servers.each { |server| puts server }
  ensure
    zkw.close
  end
end

hostOrServers = ARGV[1..ARGV.size]

# Create a logger and save it to ruby global
$LOG = LoggerFactory.getLogger(NAME)
case ARGV[0]
when 'add'
  if ARGV.length < 2
    puts optparse
    exit 1
  end
  addServers(options, hostOrServers)
when 'remove'
  if ARGV.length < 2
    puts optparse
    exit 1
  end
  removeServers(options, hostOrServers)
when 'list'
  listServers(options)
else
  puts optparse
  exit 3
end
