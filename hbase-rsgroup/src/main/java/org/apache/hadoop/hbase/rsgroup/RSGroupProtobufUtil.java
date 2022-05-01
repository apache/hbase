/*
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
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BalanceRequest;
import org.apache.hadoop.hbase.client.BalanceResponse;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.NameStringPair;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.BalanceRSGroupRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.BalanceRSGroupResponse;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupProtos;
import org.apache.hadoop.hbase.protobuf.generated.TableProtos;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
final class RSGroupProtobufUtil {
  private RSGroupProtobufUtil() {
  }

  static void populateBalanceRSGroupResponse(BalanceRSGroupResponse.Builder responseBuilder,
    BalanceResponse response) {
    responseBuilder.setBalanceRan(response.isBalancerRan())
      .setMovesCalculated(response.getMovesCalculated())
      .setMovesExecuted(response.getMovesExecuted());
  }

  static BalanceResponse toBalanceResponse(BalanceRSGroupResponse response) {
    return BalanceResponse.newBuilder().setBalancerRan(response.getBalanceRan())
      .setMovesExecuted(response.hasMovesExecuted() ? response.getMovesExecuted() : 0)
      .setMovesCalculated(response.hasMovesCalculated() ? response.getMovesCalculated() : 0)
      .build();
  }

  static BalanceRSGroupRequest createBalanceRSGroupRequest(String groupName,
    BalanceRequest request) {
    return BalanceRSGroupRequest.newBuilder().setRSGroupName(groupName)
      .setDryRun(request.isDryRun()).setIgnoreRit(request.isIgnoreRegionsInTransition()).build();
  }

  static BalanceRequest toBalanceRequest(BalanceRSGroupRequest request) {
    return BalanceRequest.newBuilder().setDryRun(request.hasDryRun() && request.getDryRun())
      .setIgnoreRegionsInTransition(request.hasIgnoreRit() && request.getIgnoreRit()).build();
  }

  static RSGroupInfo toGroupInfo(RSGroupProtos.RSGroupInfo proto) {
    RSGroupInfo rsGroupInfo = new RSGroupInfo(proto.getName());
    for (HBaseProtos.ServerName el : proto.getServersList()) {
      rsGroupInfo.addServer(Address.fromParts(el.getHostName(), el.getPort()));
    }
    for (TableProtos.TableName pTableName : proto.getTablesList()) {
      rsGroupInfo.addTable(ProtobufUtil.toTableName(pTableName));
    }
    proto.getConfigurationList()
      .forEach(pair -> rsGroupInfo.setConfiguration(pair.getName(), pair.getValue()));
    return rsGroupInfo;
  }

  static RSGroupProtos.RSGroupInfo toProtoGroupInfo(RSGroupInfo pojo) {
    List<TableProtos.TableName> tables = new ArrayList<>(pojo.getTables().size());
    for (TableName arg : pojo.getTables()) {
      tables.add(ProtobufUtil.toProtoTableName(arg));
    }
    List<HBaseProtos.ServerName> hostports = new ArrayList<>(pojo.getServers().size());
    for (Address el : pojo.getServers()) {
      hostports.add(HBaseProtos.ServerName.newBuilder().setHostName(el.getHostname())
        .setPort(el.getPort()).build());
    }
    List<
      NameStringPair> configuration =
        pojo
          .getConfiguration().entrySet().stream().map(entry -> NameStringPair.newBuilder()
            .setName(entry.getKey()).setValue(entry.getValue()).build())
          .collect(Collectors.toList());
    return RSGroupProtos.RSGroupInfo.newBuilder().setName(pojo.getName()).addAllServers(hostports)
      .addAllTables(tables).addAllConfiguration(configuration).build();
  }
}
