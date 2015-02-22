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
java_import org.apache.hadoop.hbase.quotas.ThrottleType
java_import org.apache.hadoop.hbase.quotas.QuotaFilter
java_import org.apache.hadoop.hbase.quotas.QuotaRetriever
java_import org.apache.hadoop.hbase.quotas.QuotaSettingsFactory

module HBaseQuotasConstants
  GLOBAL_BYPASS = 'GLOBAL_BYPASS'
  THROTTLE_TYPE = 'THROTTLE_TYPE'
  THROTTLE = 'THROTTLE'
  REQUEST = 'REQUEST'
end

module Hbase
  class QuotasAdmin
    def initialize(configuration, formatter)
      @config = configuration
      @connection = org.apache.hadoop.hbase.client.ConnectionFactory.createConnection(configuration)
      @admin = @connection.getAdmin()
      @formatter = formatter
    end

    def throttle(args)
      raise(ArgumentError, "Arguments should be a Hash") unless args.kind_of?(Hash)
      type = args.fetch(THROTTLE_TYPE, REQUEST)
      type, limit, time_unit = _parse_limit(args.delete(LIMIT), ThrottleType, type)
      if args.has_key?(USER)
        user = args.delete(USER)
        if args.has_key?(TABLE)
          table = TableName.valueOf(args.delete(TABLE))
          raise(ArgumentError, "Unexpected arguments: " + args.inspect) unless args.empty?
          settings = QuotaSettingsFactory.throttleUser(user, table, type, limit, time_unit)
        elsif args.has_key?(NAMESPACE)
          namespace = args.delete(NAMESPACE)
          raise(ArgumentError, "Unexpected arguments: " + args.inspect) unless args.empty?
          settings = QuotaSettingsFactory.throttleUser(user, namespace, type, limit, time_unit)
        else
          raise(ArgumentError, "Unexpected arguments: " + args.inspect) unless args.empty?
          settings = QuotaSettingsFactory.throttleUser(user, type, limit, time_unit)
        end
      elsif args.has_key?(TABLE)
        table = TableName.valueOf(args.delete(TABLE))
        raise(ArgumentError, "Unexpected arguments: " + args.inspect) unless args.empty?
        settings = QuotaSettingsFactory.throttleTable(table, type, limit, time_unit)
      elsif args.has_key?(NAMESPACE)
        namespace = args.delete(NAMESPACE)
        raise(ArgumentError, "Unexpected arguments: " + args.inspect) unless args.empty?
        settings = QuotaSettingsFactory.throttleNamespace(namespace, type, limit, time_unit)
      else
        raise "One of USER, TABLE or NAMESPACE must be specified"
      end
      @admin.setQuota(settings)
    end

    def unthrottle(args)
      raise(ArgumentError, "Arguments should be a Hash") unless args.kind_of?(Hash)
      if args.has_key?(USER)
        user = args.delete(USER)
        if args.has_key?(TABLE)
          table = TableName.valueOf(args.delete(TABLE))
          raise(ArgumentError, "Unexpected arguments: " + args.inspect) unless args.empty?
          settings = QuotaSettingsFactory.unthrottleUser(user, table)
        elsif args.has_key?(NAMESPACE)
          namespace = args.delete(NAMESPACE)
          raise(ArgumentError, "Unexpected arguments: " + args.inspect) unless args.empty?
          settings = QuotaSettingsFactory.unthrottleUser(user, namespace)
        else
          raise(ArgumentError, "Unexpected arguments: " + args.inspect) unless args.empty?
          settings = QuotaSettingsFactory.unthrottleUser(user)
        end
      elsif args.has_key?(TABLE)
        table = TableName.valueOf(args.delete(TABLE))
        raise(ArgumentError, "Unexpected arguments: " + args.inspect) unless args.empty?
        settings = QuotaSettingsFactory.unthrottleTable(table)
      elsif args.has_key?(NAMESPACE)
        namespace = args.delete(NAMESPACE)
        raise(ArgumentError, "Unexpected arguments: " + args.inspect) unless args.empty?
        settings = QuotaSettingsFactory.unthrottleNamespace(namespace)
      else
        raise "One of USER, TABLE or NAMESPACE must be specified"
      end
      @admin.setQuota(settings)
    end

    def set_global_bypass(bypass, args)
      raise(ArgumentError, "Arguments should be a Hash") unless args.kind_of?(Hash)

      if args.has_key?(USER)
        user = args.delete(USER)
        raise(ArgumentError, "Unexpected arguments: " + args.inspect) unless args.empty?
        settings = QuotaSettingsFactory.bypassGlobals(user, bypass)
      else
        raise "Expected USER"
      end
      @admin.setQuota(settings)
    end

    def list_quotas(args = {})
      raise(ArgumentError, "Arguments should be a Hash") unless args.kind_of?(Hash)

      limit = args.delete("LIMIT") || -1
      count = 0

      filter = QuotaFilter.new()
      filter.setUserFilter(args.delete(USER)) if args.has_key?(USER)
      filter.setTableFilter(args.delete(TABLE)) if args.has_key?(TABLE)
      filter.setNamespaceFilter(args.delete(NAMESPACE)) if args.has_key?(NAMESPACE)
      raise(ArgumentError, "Unexpected arguments: " + args.inspect) unless args.empty?

      # Start the scanner
      scanner = @admin.getQuotaRetriever(filter)
      begin
        iter = scanner.iterator

        # Iterate results
        while iter.hasNext
          if limit > 0 && count >= limit
            break
          end

          settings = iter.next
          owner = {
            USER => settings.getUserName(),
            TABLE => settings.getTableName(),
            NAMESPACE => settings.getNamespace(),
          }.delete_if { |k, v| v.nil? }.map {|k, v| k.to_s + " => " + v.to_s} * ', '

          yield owner, settings.to_s

          count += 1
        end
      ensure
        scanner.close()
      end

      return count
    end

    def _parse_size(str_limit)
      str_limit = str_limit.downcase
      match = /(\d+)([bkmgtp%]*)/.match(str_limit)
      if match
        if match[2] == '%'
          return match[1].to_i
        else
          return _size_from_str(match[1].to_i, match[2])
        end
      else
        raise "Invalid size limit syntax"
      end
    end

    def _parse_limit(str_limit, type_cls, type)
      str_limit = str_limit.downcase
      match = /(\d+)(req|[bkmgtp])\/(sec|min|hour|day)/.match(str_limit)
      if match
        if match[2] == 'req'
          limit = match[1].to_i
          type = type_cls.valueOf(type + "_NUMBER")
        else
          limit = _size_from_str(match[1].to_i, match[2])
          type = type_cls.valueOf(type + "_SIZE")
        end

        if limit <= 0
          raise "Invalid throttle limit, must be greater then 0"
        end

        case match[3]
          when 'sec'  then time_unit = TimeUnit::SECONDS
          when 'min'  then time_unit = TimeUnit::MINUTES
          when 'hour' then time_unit = TimeUnit::HOURS
          when 'day'  then time_unit = TimeUnit::DAYS
        end

        return type, limit, time_unit
      else
        raise "Invalid throttle limit syntax"
      end
    end

    def _size_from_str(value, suffix)
      case suffix
        when 'k' then value <<= 10
        when 'm' then value <<= 20
        when 'g' then value <<= 30
        when 't' then value <<= 40
        when 'p' then value <<= 50
      end
      return value
    end
  end
end