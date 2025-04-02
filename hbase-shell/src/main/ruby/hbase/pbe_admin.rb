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
  class PBEAdmin
    def initialize(connection)
      @connection = connection
      @admin = PBEKeymetaAdminClient.new(connection)
      @hb_admin = @connection.getAdmin
    end

    def close
      @admin.close
    end

    def pbe_enable(pbe_prefix)
      prefix, namespace = extract_prefix_info(pbe_prefix)
      @admin.enablePBE(prefix, namespace)
    end

    def show_pbe_status(pbe_prefix)
      prefix, namespace = extract_prefix_info(pbe_prefix)
      @admin.getPBEKeyStatuses(prefix, namespace)
    end

    def extract_prefix_info(pbe_prefix)
      prefixInfo = pbe_prefix.split(':')
      raise(ArgumentError, 'Invalid prefix:namespace format') unless (prefixInfo.length == 1 ||
        prefixInfo.length == 2)
      return prefixInfo[0], prefixInfo.length > 1 ? prefixInfo[1] :
        PBEKeyData::KEY_NAMESPACE_GLOBAL
    end
  end
end
