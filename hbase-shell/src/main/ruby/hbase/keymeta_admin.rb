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
# frozen_string_literal: true

require 'java'
java_import org.apache.hadoop.hbase.io.crypto.ManagedKeyData
java_import org.apache.hadoop.hbase.keymeta.KeymetaAdminClient

module Hbase
  # KeymetaAdmin is a class that provides a Ruby interface to the HBase Key Management API.
  # It is used to interface with the HBase Key Management API.
  class KeymetaAdmin
    def initialize(connection)
      @connection = connection
      @admin = KeymetaAdminClient.new(connection)
      @hb_admin = @connection.getAdmin
    end

    def close
      @admin.close
    end

    def enable_key_management(key_info)
      cust, namespace = extract_cust_info(key_info)
      @admin.enableKeyManagement(cust, namespace)
    end

    def get_key_statuses(key_info)
      cust, namespace = extract_cust_info(key_info)
      @admin.getManagedKeys(cust, namespace)
    end

    def extract_cust_info(key_info)
      cust_info = key_info.split(':')
      raise(ArgumentError, 'Invalid cust:namespace format') unless [1, 2].include?(cust_info.length)

      [cust_info[0], cust_info.length > 1 ? cust_info[1] : ManagedKeyData::KEY_SPACE_GLOBAL]
    end
  end
end
