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

include Java
java_import java.util.concurrent.TimeUnit
java_import org.apache.hadoop.hbase.TableName
java_import org.apache.hadoop.hbase.ServerName
java_import org.apache.hadoop.hbase.quotas.ThrottleType
java_import org.apache.hadoop.hbase.quotas.QuotaFilter
java_import org.apache.hadoop.hbase.quotas.QuotaRetriever
java_import org.apache.hadoop.hbase.quotas.QuotaSettingsFactory
java_import org.apache.hadoop.hbase.quotas.QuotaTableUtil
java_import org.apache.hadoop.hbase.quotas.SpaceViolationPolicy

module HBaseQuotasConstants
  # RPC Quota constants
  GLOBAL_BYPASS = 'GLOBAL_BYPASS'.freeze
  THROTTLE_TYPE = 'THROTTLE_TYPE'.freeze
  THROTTLE = 'THROTTLE'.freeze
  REQUEST = 'REQUEST'.freeze
  WRITE = 'WRITE'.freeze
  READ = 'READ'.freeze
  # Space quota constants
  SPACE = 'SPACE'.freeze
  NO_INSERTS = 'NO_INSERTS'.freeze
  NO_WRITES = 'NO_WRITES'.freeze
  NO_WRITES_COMPACTIONS = 'NO_WRITES_COMPACTIONS'.freeze
  DISABLE = 'DISABLE'.freeze
end

module Hbase
  # rubocop:disable Metrics/ClassLength
  class QuotasAdmin
    def initialize(admin)
      @admin = admin
    end

    def close
      @admin.close
    end

    def throttle(args)
      raise(ArgumentError, 'Arguments should be a Hash') unless args.is_a?(Hash)
      type = args.fetch(THROTTLE_TYPE, REQUEST)
      args.delete(THROTTLE_TYPE)
      type, limit, time_unit = _parse_limit(args.delete(LIMIT), ThrottleType, type)
      if args.key?(USER)
        user = args.delete(USER)
        if args.key?(TABLE)
          table = TableName.valueOf(args.delete(TABLE))
          raise(ArgumentError, 'Unexpected arguments: ' + args.inspect) unless args.empty?
          settings = QuotaSettingsFactory.throttleUser(user, table, type, limit, time_unit)
        elsif args.key?(NAMESPACE)
          namespace = args.delete(NAMESPACE)
          raise(ArgumentError, 'Unexpected arguments: ' + args.inspect) unless args.empty?
          settings = QuotaSettingsFactory.throttleUser(user, namespace, type, limit, time_unit)
        else
          raise(ArgumentError, 'Unexpected arguments: ' + args.inspect) unless args.empty?
          settings = QuotaSettingsFactory.throttleUser(user, type, limit, time_unit)
        end
      elsif args.key?(TABLE)
        table = TableName.valueOf(args.delete(TABLE))
        raise(ArgumentError, 'Unexpected arguments: ' + args.inspect) unless args.empty?
        settings = QuotaSettingsFactory.throttleTable(table, type, limit, time_unit)
      elsif args.key?(NAMESPACE)
        namespace = args.delete(NAMESPACE)
        raise(ArgumentError, 'Unexpected arguments: ' + args.inspect) unless args.empty?
        settings = QuotaSettingsFactory.throttleNamespace(namespace, type, limit, time_unit)
      else
        raise 'One of USER, TABLE or NAMESPACE must be specified'
      end
      @admin.setQuota(settings)
    end

    def unthrottle(args)
      raise(ArgumentError, 'Arguments should be a Hash') unless args.is_a?(Hash)
      if args.key?(USER)
        user = args.delete(USER)
        if args.key?(TABLE)
          table = TableName.valueOf(args.delete(TABLE))
          raise(ArgumentError, 'Unexpected arguments: ' + args.inspect) unless args.empty?
          settings = QuotaSettingsFactory.unthrottleUser(user, table)
        elsif args.key?(NAMESPACE)
          namespace = args.delete(NAMESPACE)
          raise(ArgumentError, 'Unexpected arguments: ' + args.inspect) unless args.empty?
          settings = QuotaSettingsFactory.unthrottleUser(user, namespace)
        else
          raise(ArgumentError, 'Unexpected arguments: ' + args.inspect) unless args.empty?
          settings = QuotaSettingsFactory.unthrottleUser(user)
        end
      elsif args.key?(TABLE)
        table = TableName.valueOf(args.delete(TABLE))
        raise(ArgumentError, 'Unexpected arguments: ' + args.inspect) unless args.empty?
        settings = QuotaSettingsFactory.unthrottleTable(table)
      elsif args.key?(NAMESPACE)
        namespace = args.delete(NAMESPACE)
        raise(ArgumentError, 'Unexpected arguments: ' + args.inspect) unless args.empty?
        settings = QuotaSettingsFactory.unthrottleNamespace(namespace)
      else
        raise 'One of USER, TABLE or NAMESPACE must be specified'
      end
      @admin.setQuota(settings)
    end

    # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity
    # rubocop:disable Metrics/MethodLength, Metrics/PerceivedComplexity
    def limit_space(args)
      raise(ArgumentError, 'Argument should be a Hash') unless !args.nil? && args.is_a?(Hash)
      # Let the user provide a raw number
      limit = if args[LIMIT].is_a?(Numeric)
                args[LIMIT]
              else
                # Parse a string a 1K, 2G, etc.
                _parse_size(args[LIMIT])
              end
      if limit <= 0
        raise(ArgumentError, 'Invalid space limit, must be greater than 0')
      end

      # Extract the policy, failing if something bogus was provided
      policy = SpaceViolationPolicy.valueOf(args[POLICY])
      # Create a table or namespace quota
      if args.key?(TABLE)
        if args.key?(NAMESPACE)
          raise(ArgumentError, 'Only one of TABLE or NAMESPACE can be specified.')
        end
        settings = QuotaSettingsFactory.limitTableSpace(TableName.valueOf(args.delete(TABLE)), limit, policy)
      elsif args.key?(NAMESPACE)
        if args.key?(TABLE)
          raise(ArgumentError, 'Only one of TABLE or NAMESPACE can be specified.')
        end
        settings = QuotaSettingsFactory.limitNamespaceSpace(args.delete(NAMESPACE), limit, policy)
      else
        raise(ArgumentError, 'One of TABLE or NAMESPACE must be specified.')
      end
      # Apply the quota
      @admin.setQuota(settings)
    end
    # rubocop:enable Metrics/AbcSize, Metrics/CyclomaticComplexity
    # rubocop:enable Metrics/MethodLength, Metrics/PerceivedComplexity

    def remove_space_limit(args)
      raise(ArgumentError, 'Argument should be a Hash') unless !args.nil? && args.is_a?(Hash)
      if args.key?(TABLE)
        if args.key?(NAMESPACE)
          raise(ArgumentError, 'Only one of TABLE or NAMESPACE can be specified.')
        end
        table = TableName.valueOf(args.delete(TABLE))
        settings = QuotaSettingsFactory.removeTableSpaceLimit(table)
      elsif args.key?(NAMESPACE)
        if args.key?(TABLE)
          raise(ArgumentError, 'Only one of TABLE or NAMESPACE can be specified.')
        end
        settings = QuotaSettingsFactory.removeNamespaceSpaceLimit(args.delete(NAMESPACE))
      else
        raise(ArgumentError, 'One of TABLE or NAMESPACE must be specified.')
      end
      @admin.setQuota(settings)
    end

    def get_master_table_sizes
      @admin.getSpaceQuotaTableSizes
    end

    def get_quota_snapshots(regionserver = nil)
      # Ask a regionserver if we were given one
      return get_rs_quota_snapshots(regionserver) if regionserver
      # Otherwise, read from the quota table
      get_quota_snapshots_from_table
    end

    def get_quota_snapshots_from_table
      # Reads the snapshots from the hbase:quota table
      QuotaTableUtil.getSnapshots(@admin.getConnection)
    end

    def get_rs_quota_snapshots(rs)
      # Reads the snapshots from a specific regionserver
      @admin.getRegionServerSpaceQuotaSnapshots(ServerName.valueOf(rs))
    end

    def set_global_bypass(bypass, args)
      raise(ArgumentError, 'Arguments should be a Hash') unless args.is_a?(Hash)

      if args.key?(USER)
        user = args.delete(USER)
        raise(ArgumentError, 'Unexpected arguments: ' + args.inspect) unless args.empty?
        settings = QuotaSettingsFactory.bypassGlobals(user, bypass)
      else
        raise 'Expected USER'
      end
      @admin.setQuota(settings)
    end

    def list_quotas(args = {})
      raise(ArgumentError, 'Arguments should be a Hash') unless args.is_a?(Hash)

      limit = args.delete('LIMIT') || -1
      count = 0

      filter = QuotaFilter.new
      filter.setUserFilter(args.delete(USER)) if args.key?(USER)
      filter.setTableFilter(args.delete(TABLE)) if args.key?(TABLE)
      filter.setNamespaceFilter(args.delete(NAMESPACE)) if args.key?(NAMESPACE)
      raise(ArgumentError, 'Unexpected arguments: ' + args.inspect) unless args.empty?

      # Start the scanner
      scanner = @admin.getQuotaRetriever(filter)
      begin
        iter = scanner.iterator

        # Iterate results
        while iter.hasNext
          break if limit > 0 && count >= limit

          settings = iter.next
          owner = {
            USER => settings.getUserName,
            TABLE => settings.getTableName,
            NAMESPACE => settings.getNamespace
          }.delete_if { |_k, v| v.nil? }.map { |k, v| k.to_s + ' => ' + v.to_s } * ', '

          yield owner, settings.to_s

          count += 1
        end
      ensure
        scanner.close
      end

      count
    end

    def list_snapshot_sizes
      QuotaTableUtil.getObservedSnapshotSizes(@admin.getConnection)
    end

    def switch_rpc_throttle(enabled)
      @admin.switchRpcThrottle(java.lang.Boolean.valueOf(enabled))
    end

    def _parse_size(str_limit)
      str_limit = str_limit.downcase
      match = /^(\d+)([bkmgtp%]?)$/.match(str_limit)
      if match
        if match[2] == '%'
          return match[1].to_i
        else
          return _size_from_str(match[1].to_i, match[2])
        end
      else
        raise(ArgumentError, 'Invalid size limit syntax')
      end
    end

    # rubocop:disable Metrics/AbcSize, Metrics/MethodLength
    def _parse_limit(str_limit, type_cls, type)
      str_limit = str_limit.downcase
      match = /^(\d+)(req|cu|[bkmgtp])\/(sec|min|hour|day)$/.match(str_limit)
      if match
        limit = match[1].to_i
        if match[2] == 'req'
          type = type_cls.valueOf(type + '_NUMBER')
        elsif match[2] == 'cu'
          type = type_cls.valueOf(type + '_CAPACITY_UNIT')
        else
          limit = _size_from_str(limit, match[2])
          type = type_cls.valueOf(type + '_SIZE')
        end

        if limit <= 0
          raise(ArgumentError, 'Invalid throttle limit, must be greater than 0')
        end

        case match[3]
        when 'sec'  then time_unit = TimeUnit::SECONDS
        when 'min'  then time_unit = TimeUnit::MINUTES
        when 'hour' then time_unit = TimeUnit::HOURS
        when 'day'  then time_unit = TimeUnit::DAYS
        end

        return type, limit, time_unit
      else
        raise(ArgumentError, 'Invalid throttle limit syntax')
      end
    end
    # rubocop:enable Metrics/AbcSize, Metrics/MethodLength

    def _size_from_str(value, suffix)
      case suffix
      when 'k' then value <<= 10
      when 'm' then value <<= 20
      when 'g' then value <<= 30
      when 't' then value <<= 40
      when 'p' then value <<= 50
      end
      value
    end
  end
  # rubocop:enable Metrics/ClassLength
end
