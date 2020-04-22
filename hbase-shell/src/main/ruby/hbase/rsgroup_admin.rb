#
# Copyright The Apache Software Foundation
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

include Java
java_import org.apache.hadoop.hbase.util.Pair

# Wrapper for org.apache.hadoop.hbase.group.GroupAdminClient
# Which is an API to manage region server groups

module Hbase
  class RSGroupAdmin
    include HBaseConstants

    def initialize(connection)
      @connection = connection
      @admin = org.apache.hadoop.hbase.rsgroup.RSGroupAdminClient.new(connection)
    end

    def close
      @admin.close
    end

    #--------------------------------------------------------------------------
    # Returns a list of groups in hbase
    def list_rs_groups
      @admin.listRSGroups.map { |g| g.getName }
    end

    #--------------------------------------------------------------------------
    # get a group's information
    def get_rsgroup(group_name)
      group = @admin.getRSGroupInfo(group_name)
      if group.nil?
        raise(ArgumentError, 'Group does not exist: ' + group_name)
      end

      res = {}
      if block_given?
        yield('Servers:')
      end

      servers = []
      group.getServers.each do |v|
        if block_given?
          yield(v.toString)
        else
          servers << v.toString
        end
      end
      res[:servers] = servers

      tables = []
      if block_given?
        yield('Tables:')
      end
      group.getTables.each do |v|
        if block_given?
          yield(v.toString)
        else
          tables << v.toString
        end
      end
      res[:tables] = tables

      if !block_given?
        res
      else
        nil
      end
    end

    #--------------------------------------------------------------------------
    # add a group
    def add_rs_group(group_name)
      @admin.addRSGroup(group_name)
    end

    #--------------------------------------------------------------------------
    # remove a group
    def remove_rs_group(group_name)
      @admin.removeRSGroup(group_name)
    end

    #--------------------------------------------------------------------------
    # balance a group
    def balance_rs_group(group_name)
      @admin.balanceRSGroup(group_name)
    end

    #--------------------------------------------------------------------------
    # move server to a group
    def move_servers(dest, *args)
      servers = java.util.HashSet.new
      args[0].each do |s|
        servers.add(org.apache.hadoop.hbase.net.Address.fromString(s))
      end
      @admin.moveServers(servers, dest)
    end

    #--------------------------------------------------------------------------
    # move server to a group
    def move_tables(dest, *args)
      tables = java.util.HashSet.new;
      args[0].each do |s|
        tables.add(org.apache.hadoop.hbase.TableName.valueOf(s))
      end
      @admin.moveTables(tables, dest)
    end

    #--------------------------------------------------------------------------
    # get group of server
    def get_rsgroup_of_server(server)
      res = @admin.getRSGroupOfServer(
          org.apache.hadoop.hbase.net.Address.fromString(server))
      if res.nil?
        raise(ArgumentError,'Server has no group: ' + server)
      end
      res
    end

    #--------------------------------------------------------------------------
    # get group of table
    def get_rsgroup_of_table(table)
      res = @admin.getRSGroupInfoOfTable(
          org.apache.hadoop.hbase.TableName.valueOf(table))
      if res.nil?
        raise(ArgumentError,'Table has no group: ' + table)
      end
      res
    end

    #--------------------------------------------------------------------------
    # move server and table to a group
    def move_servers_tables(dest, *args)
      servers = java.util.HashSet.new
      tables = java.util.HashSet.new;
      args[0].each do |s|
        servers.add(org.apache.hadoop.hbase.net.Address.fromString(s))
      end
      args[1].each do |t|
        tables.add(org.apache.hadoop.hbase.TableName.valueOf(t))
      end
      @admin.moveServersAndTables(servers, tables, dest)
    end

    #--------------------------------------------------------------------------
    # remove decommissioned server from rsgroup
    def remove_servers(*args)
      # Flatten params array
      args = args.flatten.compact
      servers = java.util.HashSet.new
      args.each do |s|
        servers.add(org.apache.hadoop.hbase.net.Address.fromString(s))
      end
      @admin.removeServers(servers)
    end

    #--------------------------------------------------------------------------
    # rename rsgroup
    def rename_rsgroup(oldname, newname)
      @admin.renameRSGroup(oldname, newname)
    end
  end
end
