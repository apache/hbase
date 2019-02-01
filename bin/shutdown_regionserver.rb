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

# This script is used to issue a stop command to a regionserver via RPC.
# Intended for use in environments where sshing around is inappropriate
# Run it like this by passing it to a jruby interpreter:
#
#  ./bin/hbase org.jruby.Main bin/shutdown_regionserver.rb c2021:16020

include Java
java_import org.apache.hadoop.hbase.HBaseConfiguration
java_import org.apache.hadoop.hbase.client.HBaseAdmin
java_import org.apache.hadoop.hbase.client.ConnectionFactory

def usage(msg = nil)
  warn 'Usage: shutdown_regionserver.rb <host:port>..'
  warn
  warn 'Stops the specified regionservers via RPC'
  warn format('Error: %<msg>s') if msg
  abort
end

usage if ARGV.empty?

ARGV.each do |x|
  usage format('Invalid host:port: %<x>s') unless x.include? ':'
end

config = HBaseConfiguration.create
connection = ConnectionFactory.createConnection(config)
begin
  admin = connection.getAdmin
rescue StandardError
  abort "Error: Couldn't instantiate HBaseAdmin"
end

ARGV.each do |hostport|
  admin.stopRegionServer(hostport)
end
admin.close
connection.close
