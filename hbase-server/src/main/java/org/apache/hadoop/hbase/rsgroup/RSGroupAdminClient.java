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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BalanceRequest;
import org.apache.hadoop.hbase.client.BalanceResponse;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.net.Address;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.collect.Sets;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupAdminProtos.AddRSGroupRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupAdminProtos.BalanceRSGroupRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupAdminProtos.GetRSGroupInfoOfServerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupAdminProtos.GetRSGroupInfoOfServerResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupAdminProtos.GetRSGroupInfoOfTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupAdminProtos.GetRSGroupInfoOfTableResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupAdminProtos.GetRSGroupInfoRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupAdminProtos.GetRSGroupInfoResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupAdminProtos.ListRSGroupInfosRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupAdminProtos.MoveServersAndTablesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupAdminProtos.MoveServersRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupAdminProtos.MoveTablesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupAdminProtos.RSGroupAdminService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupAdminProtos.RemoveRSGroupRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupAdminProtos.RemoveServersRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RSGroupProtos;

/**
 * Client used for managing region server group information.
 * @deprecated Keep it here only for tests, using {@link Admin} instead.
 */
@Deprecated
@InterfaceAudience.Private
public class RSGroupAdminClient {
  private RSGroupAdminService.BlockingInterface stub;
  private Admin admin;

  public RSGroupAdminClient(Connection conn) throws IOException {
    admin = conn.getAdmin();
    stub = RSGroupAdminService.newBlockingStub(admin.coprocessorService());
  }

  /**
   * Gets {@code RSGroupInfo} for given group name.
   */
  public RSGroupInfo getRSGroupInfo(String groupName) throws IOException {
    try {
      GetRSGroupInfoResponse resp = stub.getRSGroupInfo(null,
        GetRSGroupInfoRequest.newBuilder().setRSGroupName(groupName).build());
      if (resp.hasRSGroupInfo()) {
        return ProtobufUtil.toGroupInfo(resp.getRSGroupInfo());
      }
      return null;
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  /**
   * Gets {@code RSGroupInfo} for the given table's group.
   */
  public RSGroupInfo getRSGroupInfoOfTable(TableName tableName) throws IOException {
    GetRSGroupInfoOfTableRequest request = GetRSGroupInfoOfTableRequest.newBuilder()
      .setTableName(ProtobufUtil.toProtoTableName(tableName)).build();
    try {
      GetRSGroupInfoOfTableResponse resp = stub.getRSGroupInfoOfTable(null, request);
      if (resp.hasRSGroupInfo()) {
        return ProtobufUtil.toGroupInfo(resp.getRSGroupInfo());
      }
      return null;
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  /**
   * Move given set of servers to the specified target RegionServer group.
   */
  public void moveServers(Set<Address> servers, String targetGroup) throws IOException {
    Set<HBaseProtos.ServerName> hostPorts = Sets.newHashSet();
    for (Address el : servers) {
      hostPorts.add(HBaseProtos.ServerName.newBuilder().setHostName(el.getHostname())
        .setPort(el.getPort()).build());
    }
    MoveServersRequest request =
      MoveServersRequest.newBuilder().setTargetGroup(targetGroup).addAllServers(hostPorts).build();
    try {
      stub.moveServers(null, request);
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  /**
   * Move all servers from source to target RegionServer group.
   */
  public void moveAllServers(String sourceGroup, String targetGroup) throws IOException {
    Set<HBaseProtos.ServerName> hostPorts = Sets.newHashSet();
    Set<Address> servers = getRSGroupInfo(sourceGroup).getServers();
    for (Address el : servers) {
      hostPorts.add(HBaseProtos.ServerName.newBuilder().setHostName(el.getHostname())
        .setPort(el.getPort()).build());
    }
    MoveServersRequest request =
      MoveServersRequest.newBuilder().setTargetGroup(targetGroup).addAllServers(hostPorts).build();
    try {
      stub.moveServers(null, request);
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  /**
   * Move given set of tables to the specified target RegionServer group. This will unassign all of
   * a table's region so it can be reassigned to the correct group.
   */
  public void moveTables(Set<TableName> tables, String targetGroup) throws IOException {
    MoveTablesRequest.Builder builder = MoveTablesRequest.newBuilder().setTargetGroup(targetGroup);
    for (TableName tableName : tables) {
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

  /**
   * Creates a new RegionServer group with the given name.
   */
  public void addRSGroup(String groupName) throws IOException {
    AddRSGroupRequest request = AddRSGroupRequest.newBuilder().setRSGroupName(groupName).build();
    try {
      stub.addRSGroup(null, request);
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  /**
   * Removes RegionServer group associated with the given name.
   */
  public void removeRSGroup(String name) throws IOException {
    RemoveRSGroupRequest request = RemoveRSGroupRequest.newBuilder().setRSGroupName(name).build();
    try {
      stub.removeRSGroup(null, request);
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  /**
   * Balance regions in the given RegionServer group.
   * @return BalanceResponse details about the balancer run
   */
  public BalanceResponse balanceRSGroup(String groupName, BalanceRequest request)
    throws IOException {
    try {
      BalanceRSGroupRequest req = ProtobufUtil.createBalanceRSGroupRequest(groupName, request);
      return ProtobufUtil.toBalanceResponse(stub.balanceRSGroup(null, req));
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  /**
   * Lists current set of RegionServer groups.
   */
  public List<RSGroupInfo> listRSGroups() throws IOException {
    try {
      List<RSGroupProtos.RSGroupInfo> resp = stub
        .listRSGroupInfos(null, ListRSGroupInfosRequest.getDefaultInstance()).getRSGroupInfoList();
      List<RSGroupInfo> result = new ArrayList<>(resp.size());
      for (RSGroupProtos.RSGroupInfo entry : resp) {
        result.add(ProtobufUtil.toGroupInfo(entry));
      }
      return result;
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  /**
   * Retrieve the RSGroupInfo a server is affiliated to
   * @param hostPort HostPort to get RSGroupInfo for
   */
  public RSGroupInfo getRSGroupOfServer(Address hostPort) throws IOException {
    GetRSGroupInfoOfServerRequest request =
      GetRSGroupInfoOfServerRequest.newBuilder().setServer(HBaseProtos.ServerName.newBuilder()
        .setHostName(hostPort.getHostname()).setPort(hostPort.getPort()).build()).build();
    try {
      GetRSGroupInfoOfServerResponse resp = stub.getRSGroupInfoOfServer(null, request);
      if (resp.hasRSGroupInfo()) {
        return ProtobufUtil.toGroupInfo(resp.getRSGroupInfo());
      }
      return null;
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }

  /**
   * Move given set of servers and tables to the specified target RegionServer group.
   * @param servers     set of servers to move
   * @param tables      set of tables to move
   * @param targetGroup the target group name
   * @throws IOException if moving the server and tables fail
   */
  public void moveServersAndTables(Set<Address> servers, Set<TableName> tables, String targetGroup)
    throws IOException {
    MoveServersAndTablesRequest.Builder builder =
      MoveServersAndTablesRequest.newBuilder().setTargetGroup(targetGroup);
    for (Address el : servers) {
      builder.addServers(HBaseProtos.ServerName.newBuilder().setHostName(el.getHostname())
        .setPort(el.getPort()).build());
    }
    for (TableName tableName : tables) {
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

  /**
   * Remove decommissioned servers from rsgroup. 1. Sometimes we may find the server aborted due to
   * some hardware failure and we must offline the server for repairing. Or we need to move some
   * servers to join other clusters. So we need to remove these servers from the rsgroup. 2.
   * Dead/recovering/live servers will be disallowed.
   * @param servers set of servers to remove
   */
  public void removeServers(Set<Address> servers) throws IOException {
    Set<HBaseProtos.ServerName> hostPorts = Sets.newHashSet();
    for (Address el : servers) {
      hostPorts.add(HBaseProtos.ServerName.newBuilder().setHostName(el.getHostname())
        .setPort(el.getPort()).build());
    }
    RemoveServersRequest request =
      RemoveServersRequest.newBuilder().addAllServers(hostPorts).build();
    try {
      stub.removeServers(null, request);
    } catch (ServiceException e) {
      throw ProtobufUtil.handleRemoteException(e);
    }
  }
}
