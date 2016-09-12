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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.RSGroupProtos;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.junit.Assert;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

@InterfaceAudience.Private
public class VerifyingRSGroupAdminClient extends RSGroupAdmin {
  private Table table;
  private ZooKeeperWatcher zkw;
  private RSGroupSerDe serDe;
  private RSGroupAdmin wrapped;

  public VerifyingRSGroupAdminClient(RSGroupAdmin RSGroupAdmin, Configuration conf)
      throws IOException {
    wrapped = RSGroupAdmin;
    table = ConnectionFactory.createConnection(conf).getTable(RSGroupInfoManager.RSGROUP_TABLE_NAME);
    zkw = new ZooKeeperWatcher(conf, this.getClass().getSimpleName(), null);
    serDe = new RSGroupSerDe();
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
  public void moveServers(Set<HostAndPort> servers, String targetGroup) throws IOException {
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
  public boolean balanceRSGroup(String name) throws IOException {
    return wrapped.balanceRSGroup(name);
  }

  @Override
  public List<RSGroupInfo> listRSGroups() throws IOException {
    return wrapped.listRSGroups();
  }

  @Override
  public RSGroupInfo getRSGroupOfServer(HostAndPort hostPort) throws IOException {
    return wrapped.getRSGroupOfServer(hostPort);
  }

  public void verify() throws IOException {
    Map<String, RSGroupInfo> groupMap = Maps.newHashMap();
    Set<RSGroupInfo> zList = Sets.newHashSet();

    for (Result result : table.getScanner(new Scan())) {
      RSGroupProtos.RSGroupInfo proto =
          RSGroupProtos.RSGroupInfo.parseFrom(
              result.getValue(
                  RSGroupInfoManager.META_FAMILY_BYTES,
                  RSGroupInfoManager.META_QUALIFIER_BYTES));
      groupMap.put(proto.getName(), ProtobufUtil.toGroupInfo(proto));
    }
    Assert.assertEquals(Sets.newHashSet(groupMap.values()),
        Sets.newHashSet(wrapped.listRSGroups()));
    try {
      String groupBasePath = ZKUtil.joinZNode(zkw.baseZNode, "rsgroup");
      for(String znode: ZKUtil.listChildrenNoWatch(zkw, groupBasePath)) {
        byte[] data = ZKUtil.getData(zkw, ZKUtil.joinZNode(groupBasePath, znode));
        if(data.length > 0) {
          ProtobufUtil.expectPBMagicPrefix(data);
          ByteArrayInputStream bis = new ByteArrayInputStream(
              data, ProtobufUtil.lengthOfPBMagic(), data.length);
          zList.add(ProtobufUtil.toGroupInfo(RSGroupProtos.RSGroupInfo.parseFrom(bis)));
        }
      }
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

  @Override
  public void close() throws IOException {
  }
}
