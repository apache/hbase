/**
 * Copyright The Apache Software Foundation
 *
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

import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;
import com.google.protobuf.ServiceException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.GetRSGroupInfoOfServerResponse;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.GetRSGroupInfoOfTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupProtos;


/**
 * Client used for managing region server group information.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
class RSGroupAdminClient extends RSGroupAdmin {
  private RSGroupAdminProtos.RSGroupAdminService.BlockingInterface proxy;
  private static final Log LOG = LogFactory.getLog(RSGroupAdminClient.class);

  public RSGroupAdminClient(Connection conn) throws IOException {
    proxy = RSGroupAdminProtos.RSGroupAdminService.newBlockingStub(
        conn.getAdmin().coprocessorService());
  }

  @Override
  public RSGroupInfo getRSGroupInfo(String groupName) throws IOException {
    try {
      RSGroupAdminProtos.GetRSGroupInfoResponse resp =
        proxy.getRSGroupInfo(null,
            RSGroupAdminProtos.GetRSGroupInfoRequest.newBuilder()
                .setRSGroupName(groupName).build());
      if(resp.hasRSGroupInfo()) {
        return ProtobufUtil.toGroupInfo(resp.getRSGroupInfo());
      }
      return null;
    } catch (ServiceException e) {
      throw ProtobufUtil.getRemoteException(e);
    }
  }

  @Override
  public RSGroupInfo getRSGroupInfoOfTable(TableName tableName) throws IOException {
    RSGroupAdminProtos.GetRSGroupInfoOfTableRequest request =
        RSGroupAdminProtos.GetRSGroupInfoOfTableRequest.newBuilder()
            .setTableName(ProtobufUtil.toProtoTableName(tableName)).build();

    try {
      GetRSGroupInfoOfTableResponse resp = proxy.getRSGroupInfoOfTable(null, request);
      if (resp.hasRSGroupInfo()) {
        return ProtobufUtil.toGroupInfo(resp.getRSGroupInfo());
      }
      return null;
    } catch (ServiceException e) {
      throw ProtobufUtil.getRemoteException(e);
    }
  }

  @Override
  public void moveServers(Set<HostAndPort> servers, String targetGroup) throws IOException {
    Set<HBaseProtos.ServerName> hostPorts = Sets.newHashSet();
    for(HostAndPort el: servers) {
      hostPorts.add(HBaseProtos.ServerName.newBuilder()
        .setHostName(el.getHostText())
        .setPort(el.getPort())
        .build());
    }
    RSGroupAdminProtos.MoveServersRequest request =
        RSGroupAdminProtos.MoveServersRequest.newBuilder()
            .setTargetGroup(targetGroup)
            .addAllServers(hostPorts).build();

    try {
      proxy.moveServers(null, request);
    } catch (ServiceException e) {
      throw ProtobufUtil.getRemoteException(e);
    }
  }

  @Override
  public void moveTables(Set<TableName> tables, String targetGroup) throws IOException {
    RSGroupAdminProtos.MoveTablesRequest.Builder builder =
        RSGroupAdminProtos.MoveTablesRequest.newBuilder()
            .setTargetGroup(targetGroup);
    for(TableName tableName: tables) {
      builder.addTableName(ProtobufUtil.toProtoTableName(tableName));
    }
    try {
      proxy.moveTables(null, builder.build());
    } catch (ServiceException e) {
      throw ProtobufUtil.getRemoteException(e);
    }
  }

  @Override
  public void addRSGroup(String groupName) throws IOException {
    RSGroupAdminProtos.AddRSGroupRequest request =
        RSGroupAdminProtos.AddRSGroupRequest.newBuilder()
            .setRSGroupName(groupName).build();
    try {
      proxy.addRSGroup(null, request);
    } catch (ServiceException e) {
      throw ProtobufUtil.getRemoteException(e);
    }
  }

  @Override
  public void removeRSGroup(String name) throws IOException {
    RSGroupAdminProtos.RemoveRSGroupRequest request =
        RSGroupAdminProtos.RemoveRSGroupRequest.newBuilder()
            .setRSGroupName(name).build();
    try {
      proxy.removeRSGroup(null, request);
    } catch (ServiceException e) {
      throw ProtobufUtil.getRemoteException(e);
    }
  }

  @Override
  public boolean balanceRSGroup(String name) throws IOException {
    RSGroupAdminProtos.BalanceRSGroupRequest request =
        RSGroupAdminProtos.BalanceRSGroupRequest.newBuilder()
            .setRSGroupName(name).build();

    try {
      return proxy.balanceRSGroup(null, request).getBalanceRan();
    } catch (ServiceException e) {
      throw ProtobufUtil.getRemoteException(e);
    }
  }

  @Override
  public List<RSGroupInfo> listRSGroups() throws IOException {
    try {
      List<RSGroupProtos.RSGroupInfo> resp =
          proxy.listRSGroupInfos(null,
              RSGroupAdminProtos.ListRSGroupInfosRequest.newBuilder().build()).getRSGroupInfoList();
      List<RSGroupInfo> result = new ArrayList<RSGroupInfo>(resp.size());
      for(RSGroupProtos.RSGroupInfo entry: resp) {
        result.add(ProtobufUtil.toGroupInfo(entry));
      }
      return result;
    } catch (ServiceException e) {
      throw ProtobufUtil.getRemoteException(e);
    }
  }

  @Override
  public RSGroupInfo getRSGroupOfServer(HostAndPort hostPort) throws IOException {
    RSGroupAdminProtos.GetRSGroupInfoOfServerRequest request =
        RSGroupAdminProtos.GetRSGroupInfoOfServerRequest.newBuilder()
            .setServer(HBaseProtos.ServerName.newBuilder()
                .setHostName(hostPort.getHostText())
                .setPort(hostPort.getPort())
                .build())
            .build();
    try {
      GetRSGroupInfoOfServerResponse resp = proxy.getRSGroupInfoOfServer(null, request);
      if (resp.hasRSGroupInfo()) {
        return ProtobufUtil.toGroupInfo(resp.getRSGroupInfo());
      }
      return null;
    } catch (ServiceException e) {
      throw ProtobufUtil.getRemoteException(e);
    }
  }

  @Override
  public void close() throws IOException {
  }
}
