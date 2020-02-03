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
package org.apache.hadoop.hbase.replication;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;

@RunWith(Parameterized.class)
@Category({ LargeTests.class })
public class TestNamespaceReplication extends TestReplicationBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestNamespaceReplication.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestNamespaceReplication.class);

  private static String ns1 = "ns1";
  private static String ns2 = "ns2";

  private static final TableName tabAName = TableName.valueOf("ns1:TA");
  private static final TableName tabBName = TableName.valueOf("ns2:TB");

  private static final byte[] f1Name = Bytes.toBytes("f1");
  private static final byte[] f2Name = Bytes.toBytes("f2");

  private static final byte[] val = Bytes.toBytes("myval");

  private static Connection connection1;
  private static Connection connection2;
  private static Admin admin1;
  private static Admin admin2;

  @Parameter
  public boolean serialPeer;

  @Override
  protected boolean isSerialPeer() {
    return serialPeer;
  }

  @Parameters(name = "{index}: serialPeer={0}")
  public static List<Boolean> parameters() {
    return ImmutableList.of(true, false);
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TestReplicationBase.setUpBeforeClass();

    connection1 = ConnectionFactory.createConnection(CONF1);
    connection2 = ConnectionFactory.createConnection(CONF2);
    admin1 = connection1.getAdmin();
    admin2 = connection2.getAdmin();

    admin1.createNamespace(NamespaceDescriptor.create(ns1).build());
    admin1.createNamespace(NamespaceDescriptor.create(ns2).build());
    admin2.createNamespace(NamespaceDescriptor.create(ns1).build());
    admin2.createNamespace(NamespaceDescriptor.create(ns2).build());

    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tabAName);
    builder.setColumnFamily(ColumnFamilyDescriptorBuilder
      .newBuilder(f1Name).setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build());
    builder.setColumnFamily(ColumnFamilyDescriptorBuilder
      .newBuilder(f2Name).setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build());
    TableDescriptor tabA = builder.build();
    admin1.createTable(tabA);
    admin2.createTable(tabA);

    builder = TableDescriptorBuilder.newBuilder(tabBName);
    builder.setColumnFamily(ColumnFamilyDescriptorBuilder
      .newBuilder(f1Name).setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build());
    builder.setColumnFamily(ColumnFamilyDescriptorBuilder
      .newBuilder(f2Name).setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build());
    TableDescriptor tabB = builder.build();
    admin1.createTable(tabB);
    admin2.createTable(tabB);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    admin1.disableTable(tabAName);
    admin1.deleteTable(tabAName);
    admin1.disableTable(tabBName);
    admin1.deleteTable(tabBName);
    admin2.disableTable(tabAName);
    admin2.deleteTable(tabAName);
    admin2.disableTable(tabBName);
    admin2.deleteTable(tabBName);

    admin1.deleteNamespace(ns1);
    admin1.deleteNamespace(ns2);
    admin2.deleteNamespace(ns1);
    admin2.deleteNamespace(ns2);

    connection1.close();
    connection2.close();
    TestReplicationBase.tearDownAfterClass();
  }

  @Test
  public void testNamespaceReplication() throws Exception {
    String peerId = "2";

    Table htab1A = connection1.getTable(tabAName);
    Table htab2A = connection2.getTable(tabAName);

    Table htab1B = connection1.getTable(tabBName);
    Table htab2B = connection2.getTable(tabBName);

    ReplicationPeerConfig rpc = admin1.getReplicationPeerConfig(peerId);
    admin1.updateReplicationPeerConfig(peerId,
      ReplicationPeerConfig.newBuilder(rpc).setReplicateAllUserTables(false).build());

    // add ns1 to peer config which replicate to cluster2
    rpc = admin1.getReplicationPeerConfig(peerId);
    Set<String> namespaces = new HashSet<>();
    namespaces.add(ns1);
    admin1.updateReplicationPeerConfig(peerId,
      ReplicationPeerConfig.newBuilder(rpc).setNamespaces(namespaces).build());
    LOG.info("update peer config");

    // Table A can be replicated to cluster2
    put(htab1A, row, f1Name, f2Name);
    ensureRowExisted(htab2A, row, f1Name, f2Name);
    delete(htab1A, row, f1Name, f2Name);
    ensureRowNotExisted(htab2A, row, f1Name, f2Name);

    // Table B can not be replicated to cluster2
    put(htab1B, row, f1Name, f2Name);
    ensureRowNotExisted(htab2B, row, f1Name, f2Name);

    // add ns1:TA => 'f1' and ns2 to peer config which replicate to cluster2
    rpc = admin1.getReplicationPeerConfig(peerId);
    namespaces = new HashSet<>();
    namespaces.add(ns2);
    Map<TableName, List<String>> tableCfs = new HashMap<>();
    tableCfs.put(tabAName, new ArrayList<>());
    tableCfs.get(tabAName).add("f1");
    admin1.updateReplicationPeerConfig(peerId, ReplicationPeerConfig.newBuilder(rpc)
        .setNamespaces(namespaces).setTableCFsMap(tableCfs).build());
    LOG.info("update peer config");

    // Only family f1 of Table A can replicated to cluster2
    put(htab1A, row, f1Name, f2Name);
    ensureRowExisted(htab2A, row, f1Name);
    delete(htab1A, row, f1Name, f2Name);
    ensureRowNotExisted(htab2A, row, f1Name);

    // All cfs of table B can replicated to cluster2
    put(htab1B, row, f1Name, f2Name);
    ensureRowExisted(htab2B, row, f1Name, f2Name);
    delete(htab1B, row, f1Name, f2Name);
    ensureRowNotExisted(htab2B, row, f1Name, f2Name);

    admin1.removeReplicationPeer(peerId);
  }

  private void put(Table source, byte[] row, byte[]... families)
      throws Exception {
    for (byte[] fam : families) {
      Put put = new Put(row);
      put.addColumn(fam, row, val);
      source.put(put);
    }
  }

  private void delete(Table source, byte[] row, byte[]... families)
      throws Exception {
    for (byte[] fam : families) {
      Delete del = new Delete(row);
      del.addFamily(fam);
      source.delete(del);
    }
  }

  private void ensureRowExisted(Table target, byte[] row, byte[]... families)
      throws Exception {
    for (byte[] fam : families) {
      Get get = new Get(row);
      get.addFamily(fam);
      for (int i = 0; i < NB_RETRIES; i++) {
        if (i == NB_RETRIES - 1) {
          fail("Waited too much time for put replication");
        }
        Result res = target.get(get);
        if (res.isEmpty()) {
          LOG.info("Row not available");
        } else {
          assertEquals(1, res.size());
          assertArrayEquals(val, res.value());
          break;
        }
        Thread.sleep(10 * SLEEP_TIME);
      }
    }
  }

  private void ensureRowNotExisted(Table target, byte[] row, byte[]... families)
      throws Exception {
    for (byte[] fam : families) {
      Get get = new Get(row);
      get.addFamily(fam);
      for (int i = 0; i < NB_RETRIES; i++) {
        if (i == NB_RETRIES - 1) {
          fail("Waited too much time for delete replication");
        }
        Result res = target.get(get);
        if (res.size() >= 1) {
          LOG.info("Row not deleted");
        } else {
          break;
        }
        Thread.sleep(10 * SLEEP_TIME);
      }
    }
  }
}
