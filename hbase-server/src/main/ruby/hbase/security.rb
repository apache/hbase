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

# Wrapper for org.apache.hadoop.hbase.client.HBaseAdmin

module Hbase
  class SecurityAdmin
    include HBaseConstants

    def initialize(configuration, formatter)
      @config = configuration
      @admin = org.apache.hadoop.hbase.client.HBaseAdmin.new(configuration)
      @formatter = formatter
    end

    #----------------------------------------------------------------------------------------------
    def grant(user, permissions, table_name=nil, family=nil, qualifier=nil)
      security_available?

      # TODO: need to validate user name

      if (table_name != nil)
        # Table should exist
        raise(ArgumentError, "Can't find a table: #{table_name}") unless exists?(table_name)

        tablebytes=table_name.to_java_bytes
        htd = @admin.getTableDescriptor(tablebytes)

        if (family != nil)
          raise(ArgumentError, "Can't find a family: #{family}") unless htd.hasFamily(family.to_java_bytes)
        end

        fambytes = family.to_java_bytes if (family != nil)
        qualbytes = qualifier.to_java_bytes if (qualifier != nil)
      end

      begin
        meta_table = org.apache.hadoop.hbase.client.HTable.new(@config,
          org.apache.hadoop.hbase.security.access.AccessControlLists::ACL_TABLE_NAME)
        service = meta_table.coprocessorService(
          org.apache.hadoop.hbase.HConstants::EMPTY_START_ROW)

        protocol = org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos::
          AccessControlService.newBlockingStub(service)
        perm = org.apache.hadoop.hbase.security.access.Permission.new(
          permissions.to_java_bytes)

        # invoke cp endpoint to perform access controlse
        org.apache.hadoop.hbase.protobuf.ProtobufUtil.grant(
          protocol, user, tablebytes, fambytes,
          qualbytes, perm.getActions())
      ensure
        meta_table.close()
      end
    end

    #----------------------------------------------------------------------------------------------
    def revoke(user, table_name=nil, family=nil, qualifier=nil)
      security_available?

      # TODO: need to validate user name

      if (table_name != nil)
        # Table should exist
        raise(ArgumentError, "Can't find a table: #{table_name}") unless exists?(table_name)

        tablebytes=table_name.to_java_bytes
        htd = @admin.getTableDescriptor(tablebytes)

        if (family != nil)
          raise(ArgumentError, "Can't find family: #{family}") unless htd.hasFamily(family.to_java_bytes)
        end

        fambytes = family.to_java_bytes if (family != nil)
        qualbytes = qualifier.to_java_bytes if (qualifier != nil)
      end

      begin
        meta_table = org.apache.hadoop.hbase.client.HTable.new(@config,
          org.apache.hadoop.hbase.security.access.AccessControlLists::ACL_TABLE_NAME)
        service = meta_table.coprocessorService(
          org.apache.hadoop.hbase.HConstants::EMPTY_START_ROW)

        protocol = org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos::
          AccessControlService.newBlockingStub(service)

        # invoke cp endpoint to perform access controlse
        org.apache.hadoop.hbase.protobuf.ProtobufUtil.revoke(
          protocol, user, tablebytes, fambytes, qualbytes)
      ensure
        meta_table.close()
      end
    end

    #----------------------------------------------------------------------------------------------
    def user_permission(table_name=nil)
      security_available?

      if (table_name != nil)
        raise(ArgumentError, "Can't find table: #{table_name}") unless exists?(table_name)

        tablebytes=table_name.to_java_bytes
      end

      begin
        meta_table = org.apache.hadoop.hbase.client.HTable.new(@config,
          org.apache.hadoop.hbase.security.access.AccessControlLists::ACL_TABLE_NAME)
        service = meta_table.coprocessorService(
          org.apache.hadoop.hbase.HConstants::EMPTY_START_ROW)

        protocol = org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos::
          AccessControlService.newBlockingStub(service)

        # invoke cp endpoint to perform access controlse
        perms = org.apache.hadoop.hbase.protobuf.ProtobufUtil.getUserPermissions(
          protocol, tablebytes)
      ensure
        meta_table.close()
      end

      res = {}
      count  = 0
      perms.each do |value|
        user_name = String.from_java_bytes(value.getUser)
        table = (value.getTable != nil) ? org.apache.hadoop.hbase.util.Bytes::toStringBinary(value.getTable) : ''
        family = (value.getFamily != nil) ? org.apache.hadoop.hbase.util.Bytes::toStringBinary(value.getFamily) : ''
        qualifier = (value.getQualifier != nil) ? org.apache.hadoop.hbase.util.Bytes::toStringBinary(value.getQualifier) : ''

        action = org.apache.hadoop.hbase.security.access.Permission.new value.getActions

        if block_given?
          yield(user_name, "#{table},#{family},#{qualifier}: #{action.to_s}")
        else
          res[user_name] ||= {}
          res[user_name][family + ":" +qualifier] = action
        end
        count += 1
      end
      
      return ((block_given?) ? count : res)
    end

    # Does table exist?
    def exists?(table_name)
      @admin.tableExists(table_name)
    end

    # Make sure that security tables are available
    def security_available?()
      raise(ArgumentError, "DISABLED: Security features are not available") \
        unless exists?(org.apache.hadoop.hbase.security.access.AccessControlLists::ACL_TABLE_NAME)
    end

  end
end
