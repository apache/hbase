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

import com.google.protobuf.ServiceException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.NameStringPair;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.AddRSGroupRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.BalanceRSGroupRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.GetRSGroupInfoOfServerRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.GetRSGroupInfoOfServerResponse;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.GetRSGroupInfoOfTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.GetRSGroupInfoOfTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.GetRSGroupInfoRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.GetRSGroupInfoResponse;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.ListRSGroupInfosRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.MoveServersAndTablesRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.MoveServersRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.MoveTablesRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.RSGroupAdminService;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.RemoveRSGroupRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.RemoveServersRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.RenameRSGroupRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupAdminProtos.UpdateRSGroupConfigRequest;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupProtos;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

/**
 * Client used for managing region server group information.
 */
@InterfaceAudience.Private
public class RSGroupAdminClient implements RSGroupAdmin {
  private RSGroupAdminService.BlockingInterface stub;
  private Admin admin;

  public RSGroupAdminClient(Connection conn) throws IOException {
    admin = conn.getAdmin();
    stub = RSGroupAdminService.newBlockingStub(admin.coprocessorService());
  }

  @Override
  public RSGroupInfo getRSGroupInfo(String groupName) throws IOException {
    try {
      GetRSGroupInfoResponse resp = stub.getRSGroupInfo(null,
          GetRSGroupInfoRequest.newBuilder().setRSGroupName(groupName).build());
      if(resp.hasRSGroupInfo()) {
        return RSGroupProtobufUtil.toGroupInfo(resp.getRSGroupInfo());
      }
      return null;
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  @Override
  public RSGroupInfo getRSGroupInfoOfTable(TableName tableName) throws IOException {
    GetRSGroupInfoOfTableRequest request = GetRSGroupInfoOfTableRequest.newBuilder().setTableName(
        ProtobufUtil.toProtoTableName(tableName)).build();
    try {
      GetRSGroupInfoOfTableResponse resp = stub.getRSGroupInfoOfTable(null, request);
      if (resp.hasRSGroupInfo()) {
        return RSGroupProtobufUtil.toGroupInfo(resp.getRSGroupInfo());
      }
      return null;
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  @Override
  public void moveServers(Set<Address> servers, String targetGroup) throws IOException {
    Set<HBaseProtos.ServerName> hostPorts = Sets.newHashSet();
    for(Address el: servers) {
      hostPorts.add(HBaseProtos.ServerName.newBuilder()
        .setHostName(el.getHostname())
        .setPort(el.getPort())
        .build());
    }
    MoveServersRequest request = MoveServersRequest.newBuilder()
            .setTargetGroup(targetGroup)
            .addAllServers(hostPorts)
            .build();
    try {
      stub.moveServers(null, request);
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  @Override
  public void moveTables(Set<TableName> tables, String targetGroup) throws IOException {
    MoveTablesRequest.Builder builder = MoveTablesRequest.newBuilder().setTargetGroup(targetGroup);
    for(TableName tableName: tables) {
      builder.addTableName(ProtobufUtil.toProtoTableName(tableName));
      if (!admin.tableExists(tableName)) {
        throw new TableNotFoundException(tableName);
      }
    }
    try {
      stub.moveTables(null, builder.build());
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  @Override
  public void addRSGroup(String groupName) throws IOException {
    AddRSGroupRequest request = AddRSGroupRequest.newBuilder().setRSGroupName(groupName).build();
    try {
      stub.addRSGroup(null, request);
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  @Override
  public void removeRSGroup(String name) throws IOException {
    RemoveRSGroupRequest request = RemoveRSGroupRequest.newBuilder().setRSGroupName(name).build();
    try {
      stub.removeRSGroup(null, request);
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  @Override
  public boolean balanceRSGroup(String groupName) throws IOException {
    BalanceRSGroupRequest request = BalanceRSGroupRequest.newBuilder()
        .setRSGroupName(groupName).build();
    try {
      return stub.balanceRSGroup(null, request).getBalanceRan();
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  @Override
  public List<RSGroupInfo> listRSGroups() throws IOException {
    try {
      List<RSGroupProtos.RSGroupInfo> resp = stub.listRSGroupInfos(null,
          ListRSGroupInfosRequest.getDefaultInstance()).getRSGroupInfoList();
      List<RSGroupInfo> result = new ArrayList<>(resp.size());
      for(RSGroupProtos.RSGroupInfo entry : resp) {
        result.add(RSGroupProtobufUtil.toGroupInfo(entry));
      }
      return result;
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  @Override
  public RSGroupInfo getRSGroupOfServer(Address hostPort) throws IOException {
    GetRSGroupInfoOfServerRequest request = GetRSGroupInfoOfServerRequest.newBuilder()
            .setServer(HBaseProtos.ServerName.newBuilder()
                .setHostName(hostPort.getHostname())
                .setPort(hostPort.getPort())
                .build())
            .build();
    try {
      GetRSGroupInfoOfServerResponse resp = stub.getRSGroupInfoOfServer(null, request);
      if (resp.hasRSGroupInfo()) {
        return RSGroupProtobufUtil.toGroupInfo(resp.getRSGroupInfo());
      }
      return null;
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  @Override
  public void moveServersAndTables(Set<Address> servers, Set<TableName> tables, String targetGroup)
      throws IOException {
    MoveServersAndTablesRequest.Builder builder =
            MoveServersAndTablesRequest.newBuilder().setTargetGroup(targetGroup);
    for(Address el: servers) {
      builder.addServers(HBaseProtos.ServerName.newBuilder()
              .setHostName(el.getHostname())
              .setPort(el.getPort())
              .build());
    }
    for(TableName tableName: tables) {
      builder.addTableName(ProtobufUtil.toProtoTableName(tableName));
      if (!admin.tableExists(tableName)) {
        throw new TableNotFoundException(tableName);
      }
    }
    try {
      stub.moveServersAndTables(null, builder.build());
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  @Override
  public void removeServers(Set<Address> servers) throws IOException {
    Set<HBaseProtos.ServerName> hostPorts = Sets.newHashSet();
    for(Address el: servers) {
      hostPorts.add(HBaseProtos.ServerName.newBuilder()
          .setHostName(el.getHostname())
          .setPort(el.getPort())
          .build());
    }
    RemoveServersRequest request = RemoveServersRequest.newBuilder()
        .addAllServers(hostPorts)
        .build();
    try {
      stub.removeServers(null, request);
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  @Override
  public void renameRSGroup(String oldName, String newName) throws IOException {
    RenameRSGroupRequest request = RenameRSGroupRequest.newBuilder()
      .setOldRsgroupName(oldName)
      .setNewRsgroupName(newName).build();
    try {
      stub.renameRSGroup(null, request);
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  @Override
  public void updateRSGroupConfig(String groupName, Map<String, String> configuration)
      throws IOException {
    UpdateRSGroupConfigRequest.Builder builder = UpdateRSGroupConfigRequest.newBuilder()
        .setGroupName(groupName);
    if (configuration != null) {
      configuration.entrySet().forEach(e ->
          builder.addConfiguration(NameStringPair.newBuilder().setName(e.getKey())
              .setValue(e.getValue()).build()));
    }
    try {
      stub.updateRSGroupConfig(null, builder.build());
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }
}
