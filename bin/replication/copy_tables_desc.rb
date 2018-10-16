#
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

# Script to recreate all tables from one cluster to another
# To see usage for this script, run:
#
#  ${HBASE_HOME}/bin/hbase org.jruby.Main copy_tables_desc.rb
#

include Java
java_import org.apache.hadoop.conf.Configuration
java_import org.apache.hadoop.hbase.HBaseConfiguration
java_import org.apache.hadoop.hbase.HConstants
java_import org.apache.hadoop.hbase.HTableDescriptor
java_import org.apache.hadoop.hbase.TableName
java_import org.apache.hadoop.hbase.client.ConnectionFactory
java_import org.apache.hadoop.hbase.client.HBaseAdmin
java_import org.slf4j.LoggerFactory

# Name of this script
NAME = 'copy_tables_desc'.freeze

# Print usage for this script
def usage
  puts format('Usage: %s.rb master_zookeeper.quorum.peers:clientport:znode_parent slave_zookeeper.quorum.peers:clientport:znode_parent [table1,table2,table3,...]', NAME)
  exit!
end

def copy(src, dst, table)
  # verify if table exists in source cluster
  begin
    t = src.getTableDescriptor(TableName.valueOf(table))
  rescue org.apache.hadoop.hbase.TableNotFoundException
    puts format("Source table \"%s\" doesn't exist, skipping.", table)
    return
  end

  # verify if table *doesn't* exists in the target cluster
  begin
    dst.createTable(t)
  rescue org.apache.hadoop.hbase.TableExistsException
    puts format('Destination table "%s" exists in remote cluster, skipping.', table)
    return
  end

  puts format('Schema for table "%s" was succesfully copied to remote cluster.', table)
end

usage if ARGV.size < 2 || ARGV.size > 3

LOG = LoggerFactory.getLogger(NAME)

parts1 = ARGV[0].split(':')

parts2 = ARGV[1].split(':')

parts3 = ARGV[2].split(',') unless ARGV[2].nil?

c1 = HBaseConfiguration.create
c1.set(HConstants::ZOOKEEPER_QUORUM, parts1[0])
c1.set('hbase.zookeeper.property.clientPort', parts1[1])
c1.set(HConstants::ZOOKEEPER_ZNODE_PARENT, parts1[2])

connection1 = ConnectionFactory.createConnection(c1)
admin1 = connection1.getAdmin

c2 = HBaseConfiguration.create
c2.set(HConstants::ZOOKEEPER_QUORUM, parts2[0])
c2.set('hbase.zookeeper.property.clientPort', parts2[1])
c2.set(HConstants::ZOOKEEPER_ZNODE_PARENT, parts2[2])

connection2 = ConnectionFactory.createConnection(c2)
admin2 = connection2.getAdmin

if parts3.nil?
  admin1.listTableNames.each do |t|
    copy(admin1, admin2, t.nameAsString)
  end
else
  parts3.each do |t|
    copy(admin1, admin2, t)
  end
end

admin1.close
admin2.close
connection1.close
connection2.close
