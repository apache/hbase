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
java_import org.apache.hadoop.hbase.io.crypto.ManagedKeyData
java_import org.apache.hadoop.hbase.keymeta.KeymetaAdminClient

module Hbase
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
      @admin.enableManagedKeys(cust, namespace)
    end

    def get_key_statuses(key_info)
      cust, namespace = extract_cust_info(key_info)
      @admin.getManagedKeys(cust, namespace)
    end

    def extract_cust_info(key_info)
      custInfo = key_info.split(':')
      raise(ArgumentError, 'Invalid cust:namespace format') unless (custInfo.length == 1 ||
        custInfo.length == 2)
      return custInfo[0], custInfo.length > 1 ? custInfo[1] :
        ManagedKeyData::KEY_NAMESPACE_GLOBAL
    end
  end
end
