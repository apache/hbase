/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.rsgroup;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupProtos;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
final class RSGroupProtobufUtil {
  private RSGroupProtobufUtil() {
  }

  static RSGroupInfo toGroupInfo(RSGroupProtos.RSGroupInfo proto) {
    RSGroupInfo RSGroupInfo = new RSGroupInfo(proto.getName());
    for(HBaseProtos.ServerName el: proto.getServersList()) {
      RSGroupInfo.addServer(Address.fromParts(el.getHostName(), el.getPort()));
    }
    for(HBaseProtos.TableName pTableName: proto.getTablesList()) {
      RSGroupInfo.addTable(ProtobufUtil.toTableName(pTableName));
    }
    return RSGroupInfo;
  }

  static RSGroupProtos.RSGroupInfo toProtoGroupInfo(RSGroupInfo pojo) {
    List<HBaseProtos.TableName> tables = new ArrayList<>(pojo.getTables().size());
    for(TableName arg: pojo.getTables()) {
      tables.add(ProtobufUtil.toProtoTableName(arg));
    }
    List<HBaseProtos.ServerName> hostports = new ArrayList<>(pojo.getServers().size());
    for(Address el: pojo.getServers()) {
      hostports.add(HBaseProtos.ServerName.newBuilder()
          .setHostName(el.getHostname())
          .setPort(el.getPort())
          .build());
    }
    return RSGroupProtos.RSGroupInfo.newBuilder().setName(pojo.getName())
        .addAllServers(hostports)
        .addAllTables(tables).build();
  }
}
