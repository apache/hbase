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

module Hbase
  class PBEAdmin
    def initialize(connection)
      @connection = connection
      @admin = org.apache.hadoop.hbase.keymeta.PBEKeymetaAdminClient.new(connection)
      @hb_admin = @connection.getAdmin
    end

    def close
      @admin.close
    end

    def pbe_enable(pbe_prefix)
      prefixInfo = pbe_prefix.split(':')
      assert prefixInfo.length <= 2, 'Invalid prefix:namespace format'
      @admin.enablePBE(prefixInfo[0], prefixInfo.length > 1? prefixInfo[1] :
        org.apache.hadoop.hbase.io.crypto.PBEKeyData.KEY_NAMESPACE_GLOBAL)
    end
  end
end
