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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupProtos;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;

import org.apache.hbase.thirdparty.com.google.common.collect.Maps;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

@InterfaceAudience.Private
public class VerifyingRSGroupAdminClient extends RSGroupAdminClient {
  private Connection conn;
  private ZKWatcher zkw;
  private RSGroupAdminClient wrapped;

  public VerifyingRSGroupAdminClient(RSGroupAdminClient RSGroupAdmin, Configuration conf)
      throws IOException {
    wrapped = RSGroupAdmin;
    conn = ConnectionFactory.createConnection(conf);
    zkw = new ZKWatcher(conf, this.getClass().getSimpleName(), null);
  }

  @Override
  public void addRSGroup(String groupName) throws IOException {
    wrapped.addRSGroup(groupName);
    verify();
  }

  @Override
  public RSGroupInfo getRSGroupInfo(String groupName) throws IOException {
    return wrapped.getRSGroupInfo(groupName);
  }

  @Override
  public RSGroupInfo getRSGroupInfoOfTable(TableName tableName) throws IOException {
    return wrapped.getRSGroupInfoOfTable(tableName);
  }

  @Override
  public void moveServers(Set<Address> servers, String targetGroup) throws IOException {
    wrapped.moveServers(servers, targetGroup);
    verify();
  }

  @Override
  public void moveTables(Set<TableName> tables, String targetGroup) throws IOException {
    wrapped.moveTables(tables, targetGroup);
    verify();
  }

  @Override
  public void removeRSGroup(String name) throws IOException {
    wrapped.removeRSGroup(name);
    verify();
  }

  @Override
  public boolean balanceRSGroup(String groupName) throws IOException {
    return wrapped.balanceRSGroup(groupName);
  }

  @Override
  public List<RSGroupInfo> listRSGroups() throws IOException {
    return wrapped.listRSGroups();
  }

  @Override
  public RSGroupInfo getRSGroupOfServer(Address hostPort) throws IOException {
    return wrapped.getRSGroupOfServer(hostPort);
  }

  @Override
  public void moveServersAndTables(Set<Address> servers, Set<TableName> tables, String targetGroup)
          throws IOException {
    wrapped.moveServersAndTables(servers, tables, targetGroup);
    verify();
  }

  @Override
  public void removeServers(Set<Address> servers) throws IOException {
    wrapped.removeServers(servers);
    verify();
  }

  public void verify() throws IOException {
    Map<String, RSGroupInfo> groupMap = Maps.newHashMap();
    Set<RSGroupInfo> zList = Sets.newHashSet();
    List<TableDescriptor> tds = new ArrayList<>();
    try (Admin admin = conn.getAdmin()) {
      tds.addAll(admin.listTableDescriptors());
      tds.addAll(admin.listTableDescriptorsByNamespace(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME));
    }
    try (Table table = conn.getTable(RSGroupInfoManagerImpl.RSGROUP_TABLE_NAME);
        ResultScanner scanner = table.getScanner(new Scan())) {
      for (;;) {
        Result result = scanner.next();
        if (result == null) {
          break;
        }
        RSGroupProtos.RSGroupInfo proto = RSGroupProtos.RSGroupInfo.parseFrom(result.getValue(
          RSGroupInfoManagerImpl.META_FAMILY_BYTES, RSGroupInfoManagerImpl.META_QUALIFIER_BYTES));
        RSGroupInfo rsGroupInfo = ProtobufUtil.toGroupInfo(proto);
        groupMap.put(proto.getName(), RSGroupUtil.fillTables(rsGroupInfo, tds));
      }
    }
    assertEquals(Sets.newHashSet(groupMap.values()), Sets.newHashSet(wrapped.listRSGroups()));
    try {
      String groupBasePath = ZNodePaths.joinZNode(zkw.getZNodePaths().baseZNode, "rsgroup");
      for (String znode : ZKUtil.listChildrenNoWatch(zkw, groupBasePath)) {
        byte[] data = ZKUtil.getData(zkw, ZNodePaths.joinZNode(groupBasePath, znode));
        if (data.length > 0) {
          ProtobufUtil.expectPBMagicPrefix(data);
          ByteArrayInputStream bis =
              new ByteArrayInputStream(data, ProtobufUtil.lengthOfPBMagic(), data.length);
          RSGroupInfo rsGroupInfo =
              ProtobufUtil.toGroupInfo(RSGroupProtos.RSGroupInfo.parseFrom(bis));
          zList.add(RSGroupUtil.fillTables(rsGroupInfo, tds));
        }
      }
      assertEquals(zList.size(), groupMap.size());
      for (RSGroupInfo rsGroupInfo : zList) {
        assertTrue(groupMap.get(rsGroupInfo.getName()).equals(rsGroupInfo));
      }
    } catch (KeeperException e) {
      throw new IOException("ZK verification failed", e);
    } catch (DeserializationException e) {
      throw new IOException("ZK verification failed", e);
    } catch (InterruptedException e) {
      throw new IOException("ZK verification failed", e);
    }
  }
}
