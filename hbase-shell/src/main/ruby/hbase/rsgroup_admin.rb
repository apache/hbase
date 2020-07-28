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
    def initialize(connection)
      @connection = connection
      @admin = org.apache.hadoop.hbase.rsgroup.RSGroupAdminClient.new(connection)
      @hb_admin = @connection.getAdmin
    end

    def close
      @admin.close
    end

    #--------------------------------------------------------------------------
    # Returns a list of groups in hbase
    def list_rs_groups
      @admin.listRSGroups
    end

    #--------------------------------------------------------------------------
    # get a group's information
    def get_rsgroup(group_name)
      group = @admin.getRSGroupInfo(group_name)
      raise(ArgumentError, 'Group does not exist: ' + group_name) if group.nil?
      group
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
    # move tables to a group
    def move_tables(dest, *args)
      tables = java.util.HashSet.new
      args[0].each do |s|
        tables.add(org.apache.hadoop.hbase.TableName.valueOf(s))
      end
      @admin.moveTables(tables, dest)
    end

    #--------------------------------------------------------------------------
    # move namespaces to a group
    def move_namespaces(dest, *args)
      tables = get_tables(args[0])
      @admin.moveTables(tables, dest)
    end

    #--------------------------------------------------------------------------
    # get group of server
    def get_rsgroup_of_server(server)
      res = @admin.getRSGroupOfServer(
        org.apache.hadoop.hbase.net.Address.fromString(server)
      )
      raise(ArgumentError, 'Server has no group: ' + server) if res.nil?
      res
    end

    #--------------------------------------------------------------------------
    # get group of table
    def get_rsgroup_of_table(table)
      res = @admin.getRSGroupInfoOfTable(
        org.apache.hadoop.hbase.TableName.valueOf(table)
      )
      raise(ArgumentError, 'Table has no group: ' + table) if res.nil?
      res
    end

    #--------------------------------------------------------------------------
    # move server and table to a group
    def move_servers_tables(dest, *args)
      servers = get_servers(args[0])
      tables = java.util.HashSet.new
      args[1].each do |t|
        tables.add(org.apache.hadoop.hbase.TableName.valueOf(t))
      end
      @admin.moveServersAndTables(servers, tables, dest)
    end

    #--------------------------------------------------------------------------
    # move server and namespace to a group
    def move_servers_namespaces(dest, *args)
      servers = get_servers(args[0])
      tables = get_tables(args[1])
      @admin.moveServersAndTables(servers, tables, dest)
    end

    def get_servers(servers)
      server_set = java.util.HashSet.new
      servers.each do |s|
        server_set.add(org.apache.hadoop.hbase.net.Address.fromString(s))
      end
      server_set
    end

    def get_tables(namespaces)
      table_set = java.util.HashSet.new
      error = "Can't find a namespace: "
      namespaces.each do |ns|
        raise(ArgumentError, "#{error}#{ns}") unless namespace_exists?(ns)
        table_set.addAll(get_tables_by_namespace(ns))
      end
      table_set
    end

    # Get tables by namespace
    def get_tables_by_namespace(ns)
      tables = java.util.HashSet.new
      tablelist = @hb_admin.listTableNamesByNamespace(ns).map(&:getNameAsString)
      tablelist.each do |table|
        tables.add(org.apache.hadoop.hbase.TableName.valueOf(table))
      end
      tables
    end

    # Does Namespace exist
    def namespace_exists?(ns)
      return !@hb_admin.getNamespaceDescriptor(ns).nil?
    rescue org.apache.hadoop.hbase.NamespaceNotFoundException
      return false
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

    #----------------------------------------------------------------------------------------------
    # modify a rsgroup configuration
    def alter_rsgroup_config(rsgroup_name, *args)
      # Fail if table name is not a string
      raise(ArgumentError, 'RSGroup name must be of type String') unless rsgroup_name.is_a?(String)

      group = @admin.getRSGroupInfo(rsgroup_name)

      raise(ArgumentError, 'RSGroup does not exist') unless group

      configuration = java.util.HashMap.new
      configuration.putAll(group.getConfiguration)

      # Flatten params array
      args = args.flatten.compact

      # Start defining the table
      args.each do |arg|
        unless arg.is_a?(Hash)
          raise(ArgumentError, "#{arg.class} of #{arg.inspect} is not of Hash type")
        end
        method = arg[::HBaseConstants::METHOD]
        if method == 'unset'
          configuration.remove(arg[::HBaseConstants::NAME])
        elsif method == 'set'
          arg.delete(::HBaseConstants::METHOD)
          for k, v in arg
            v = v.to_s unless v.nil?
            configuration.put(k, v)
          end
        else
          raise(ArgumentError, "Unknown method #{method}")
        end
      end
      @admin.updateRSGroupConfig(rsgroup_name, configuration)
    end
  end
end
