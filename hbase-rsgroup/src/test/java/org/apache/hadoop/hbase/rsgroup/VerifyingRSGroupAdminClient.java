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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
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
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.junit.Assert;

import org.apache.hbase.thirdparty.com.google.common.collect.Maps;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

@InterfaceAudience.Private
public class VerifyingRSGroupAdminClient implements RSGroupAdmin {
  private Table table;
  private ZKWatcher zkw;
  private RSGroupAdmin wrapped;
  private Connection conn;

  public VerifyingRSGroupAdminClient(RSGroupAdmin RSGroupAdmin, Configuration conf)
      throws IOException {
    wrapped = RSGroupAdmin;
    conn = ConnectionFactory.createConnection(conf);
    table = conn.getTable(RSGroupInfoManager.RSGROUP_TABLE_NAME);
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

  @Override
  public void renameRSGroup(String oldName, String newName) throws IOException {
    wrapped.renameRSGroup(oldName, newName);
    verify();
  }

  @Override
  public void updateRSGroupConfig(String groupName, Map<String, String> configuration)
      throws IOException {
    wrapped.updateRSGroupConfig(groupName, configuration);
    verify();
  }

  public void verify() throws IOException {
    Map<String, RSGroupInfo> groupMap = Maps.newHashMap();
    Set<RSGroupInfo> zList = Sets.newHashSet();

    SortedSet<Address> lives = Sets.newTreeSet();
    for (ServerName sn : conn.getAdmin().getClusterMetrics().getLiveServerMetrics().keySet()) {
      lives.add(sn.getAddress());
    }
    for (ServerName sn : conn.getAdmin().listDecommissionedRegionServers()) {
      lives.remove(sn.getAddress());
    }

    List<RSGroupInfo> rsGroupInfos = retrieveGroupListFromGroupTable();
    for (RSGroupInfo rsGroupInfo : rsGroupInfos) {
      groupMap.put(rsGroupInfo.getName(), rsGroupInfo);
      for (Address address : rsGroupInfo.getServers()) {
        lives.remove(address);
      }
    }

    groupMap.put(RSGroupInfo.DEFAULT_GROUP,
        new RSGroupInfo(RSGroupInfo.DEFAULT_GROUP, lives, getDefaultRSGroupTables(groupMap)));

      Assert.assertEquals(Sets.newHashSet(groupMap.values()),
        Sets.newHashSet(wrapped.listRSGroups()));
    try {
      String groupBasePath = ZNodePaths.joinZNode(zkw.getZNodePaths().baseZNode, "rsgroup");
      for(String znode: ZKUtil.listChildrenNoWatch(zkw, groupBasePath)) {
        byte[] data = ZKUtil.getData(zkw, ZNodePaths.joinZNode(groupBasePath, znode));
        if(data.length > 0) {
          ProtobufUtil.expectPBMagicPrefix(data);
          ByteArrayInputStream bis = new ByteArrayInputStream(
              data, ProtobufUtil.lengthOfPBMagic(), data.length);
          zList.add(RSGroupProtobufUtil.toGroupInfo(RSGroupProtos.RSGroupInfo.parseFrom(bis)));
        }
      }
      groupMap.remove(RSGroupInfo.DEFAULT_GROUP);
      Assert.assertEquals(zList.size(), groupMap.size());
      for(RSGroupInfo RSGroupInfo : zList) {
        Assert.assertTrue(groupMap.get(RSGroupInfo.getName()).equals(RSGroupInfo));
      }
    } catch (KeeperException e) {
      throw new IOException("ZK verification failed", e);
    } catch (DeserializationException e) {
      throw new IOException("ZK verification failed", e);
    } catch (InterruptedException e) {
      throw new IOException("ZK verification failed", e);
    }
  }

  private NavigableSet<TableName> getDefaultRSGroupTables(Map<String, RSGroupInfo> groupMap)
      throws IOException {
    NavigableSet<TableName> orphanTables = new TreeSet<>();
    for (TableDescriptor td : conn.getAdmin().listTableDescriptors(Pattern.compile(".*"),
        true)) {
      orphanTables.add(td.getTableName());
    }
    for (RSGroupInfo group : groupMap.values()) {
      if (!group.getName().equals(RSGroupInfo.DEFAULT_GROUP)) {
        orphanTables.removeAll(group.getTables());
      }
    }
    return orphanTables;
  }

  List<RSGroupInfo> retrieveGroupListFromGroupTable() throws IOException {
    List<RSGroupInfo> rsGroupInfoList = Lists.newArrayList();
    try (Table table = conn.getTable(RSGroupInfoManager.RSGROUP_TABLE_NAME);
        ResultScanner scanner = table.getScanner(new Scan())) {
      for (Result result; ; ) {
        result = scanner.next();
        if (result == null) {
          break;
        }
        RSGroupProtos.RSGroupInfo proto = RSGroupProtos.RSGroupInfo.parseFrom(result
            .getValue(RSGroupInfoManager.META_FAMILY_BYTES,
                RSGroupInfoManager.META_QUALIFIER_BYTES));
        rsGroupInfoList.add(RSGroupProtobufUtil.toGroupInfo(proto));
      }
    }
    return rsGroupInfoList;
  }
}
