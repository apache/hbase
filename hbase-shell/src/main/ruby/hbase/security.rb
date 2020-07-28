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
    def initialize(admin)
      @admin = admin
      @connection = @admin.getConnection
    end

    def close
      @admin.close
    end

    #----------------------------------------------------------------------------------------------
    def grant(user, permissions, table_name = nil, family = nil, qualifier = nil)
      security_available?

      # TODO: need to validate user name

      begin
        # Verify that the specified permission is valid
        if permissions.nil? || permissions.empty?
          raise(ArgumentError, 'Invalid permission: no actions associated with user')
        end

        perm = org.apache.hadoop.hbase.security.access.Permission.new(
          permissions.to_java_bytes
        )

        if !table_name.nil?
          tablebytes = table_name.to_java_bytes
          # check if the tablename passed is actually a namespace
          if isNamespace?(table_name)
            # Namespace should exist first.
            namespace_name = table_name[1...table_name.length]
            raise(ArgumentError, "Can't find a namespace: #{namespace_name}") unless
              namespace_exists?(namespace_name)

            org.apache.hadoop.hbase.security.access.AccessControlClient.grant(
              @connection, namespace_name, user, perm.getActions
            )
          else
            # Table should exist
            raise(ArgumentError, "Can't find a table: #{table_name}") unless exists?(table_name)

            tableName = org.apache.hadoop.hbase.TableName.valueOf(table_name)
            htd = @admin.getTableDescriptor(tableName)

            unless family.nil?
              raise(ArgumentError, "Can't find a family: #{family}") unless htd.hasFamily(family.to_java_bytes)
            end

            fambytes = family.to_java_bytes unless family.nil?
            qualbytes = qualifier.to_java_bytes unless qualifier.nil?

            org.apache.hadoop.hbase.security.access.AccessControlClient.grant(
              @connection, tableName, user, fambytes, qualbytes, perm.getActions
            )
          end
        else
          # invoke cp endpoint to perform access controls
          org.apache.hadoop.hbase.security.access.AccessControlClient.grant(
            @connection, user, perm.getActions
          )
        end
      end
    end

    #----------------------------------------------------------------------------------------------
    def revoke(user, table_name = nil, family = nil, qualifier = nil)
      security_available?

      # TODO: need to validate user name

      begin
        if !table_name.nil?
          # check if the tablename passed is actually a namespace
          if isNamespace?(table_name)
            # Namespace should exist first.
            namespace_name = table_name[1...table_name.length]
            raise(ArgumentError, "Can't find a namespace: #{namespace_name}") unless namespace_exists?(namespace_name)

            tablebytes = table_name.to_java_bytes
            org.apache.hadoop.hbase.security.access.AccessControlClient.revoke(
              @connection, namespace_name, user
            )
          else
            # Table should exist
            raise(ArgumentError, "Can't find a table: #{table_name}") unless exists?(table_name)

            tableName = org.apache.hadoop.hbase.TableName.valueOf(table_name)
            htd = @admin.getTableDescriptor(tableName)

            unless family.nil?
              raise(ArgumentError, "Can't find a family: #{family}") unless htd.hasFamily(family.to_java_bytes)
            end

            fambytes = family.to_java_bytes unless family.nil?
            qualbytes = qualifier.to_java_bytes unless qualifier.nil?

            org.apache.hadoop.hbase.security.access.AccessControlClient.revoke(
              @connection, tableName, user, fambytes, qualbytes
            )
          end
        else
          perm = org.apache.hadoop.hbase.security.access.Permission.new(''.to_java_bytes)
          org.apache.hadoop.hbase.security.access.AccessControlClient.revoke(
            @connection, user, perm.getActions
          )
        end
      end
    end

    #----------------------------------------------------------------------------------------------
    def user_permission(table_regex = nil)
      security_available?
      all_perms = org.apache.hadoop.hbase.security.access.AccessControlClient.getUserPermissions(
        @connection, table_regex
      )
      res = {}
      count = 0
      all_perms.each do |value|
        user_name = value.getUser
        permission = value.getPermission
        table = ''
        family = ''
        qualifier = ''
        if !table_regex.nil? && isNamespace?(table_regex)
          nsPerm = permission.to_java(org.apache.hadoop.hbase.security.access.NamespacePermission)
          namespace = nsPerm.getNamespace
        elsif !table_regex.nil? && isTablePermission?(permission)
          tblPerm = permission.to_java(org.apache.hadoop.hbase.security.access.TablePermission)
          namespace = tblPerm.getNamespace
          table = !tblPerm.getTableName.nil? ? tblPerm.getTableName.getNameAsString : ''
          family = !tblPerm.getFamily.nil? ?
                    org.apache.hadoop.hbase.util.Bytes.toStringBinary(tblPerm.getFamily) : ''
          qualifier = !tblPerm.getQualifier.nil? ?
                       org.apache.hadoop.hbase.util.Bytes.toStringBinary(tblPerm.getQualifier) : ''
        end

        action = org.apache.hadoop.hbase.security.access.Permission.new permission.getActions

        if block_given?
          yield(user_name, "#{namespace},#{table},#{family},#{qualifier}: #{action}")
        else
          res[user_name] ||= {}
          res[user_name]["#{family}:#{qualifier}"] = action
        end
        count += 1
      end

      (block_given? ? count : res)
    end

    # Does table exist?
    def exists?(table_name)
      @admin.tableExists(TableName.valueOf(table_name))
    end

    def isNamespace?(table_name)
      table_name.start_with?('@')
    end

    def isTablePermission?(permission)
      permission.java_kind_of?(org.apache.hadoop.hbase.security.access.TablePermission)
    end

    # Does Namespace exist
    def namespace_exists?(namespace_name)
      return !@admin.getNamespaceDescriptor(namespace_name).nil?
    rescue org.apache.hadoop.hbase.NamespaceNotFoundException => e
      return false
    end

    # Make sure that security features are available
    def security_available?
      caps = []
      begin
        # Try the getSecurityCapabilities API where supported.
        # We only need to look at AUTHORIZATION, the AccessController doesn't support
        # CELL_AUTHORIZATION without AUTHORIZATION also available.
        caps = @admin.getSecurityCapabilities
      rescue
        # If we are unable to use getSecurityCapabilities, fall back with a check for
        # deployment of the ACL table
        raise(ArgumentError, 'DISABLED: Security features are not available') unless \
          exists?(org.apache.hadoop.hbase.security.access.PermissionStorage::ACL_TABLE_NAME.getNameAsString)
        return
      end
      raise(ArgumentError, 'DISABLED: Security features are not available') unless \
        caps.include? org.apache.hadoop.hbase.client.security.SecurityCapability::AUTHORIZATION
    end
  end
end
