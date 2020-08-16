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
java_import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil
java_import org.apache.hadoop.hbase.util.FutureUtils
java_import org.apache.hadoop.hbase.client.ConnectionFactory
java_import org.apache.hadoop.hbase.security.UserProvider
java_import org.apache.hadoop.hbase.client.ClusterConnectionFactory

module Shell
  module Commands
    class Regioninfo < Command
      def help
        <<-EOF
Return RegionInfo. Takes Region name or an encoded Region name
(Of use when all you have is an encoded Region name).

Examples:
Below we pass first encoded region name and then full region name.

  hbase(main):002:0>  regioninfo '1588230740'
  {ENCODED => 1588230740, NAME => 'hbase:meta,,1', STARTKEY => '', ENDKEY => ''}
  hbase(main):002:0>  regioninfo 'hbase:meta,,1'
  {ENCODED => 1588230740, NAME => 'hbase:meta,,1', STARTKEY => '', ENDKEY => ''}

EOF
      end

      def command(region_name)
        admin = @shell.hbase.connection.getAdmin()
        sn = admin.getMaster()

        conf = @shell.hbase.configuration
        user = UserProvider.instantiate(conf).getCurrent()
        clusterConnection = ClusterConnectionFactory.createAsyncClusterConnection(conf, nil, user)
        regionInfo = ProtobufUtil.toRegionInfo(FutureUtils.get(
          clusterConnection.getRegionServerAdmin(sn).getRegionInfo(
            ProtobufUtil.getGetRegionInfoRequest(region_name.to_java_bytes))).getRegionInfo())
        if clusterConnection != nil
          clusterConnection.close()
        end
        puts regionInfo
      end
    end
  end
end
