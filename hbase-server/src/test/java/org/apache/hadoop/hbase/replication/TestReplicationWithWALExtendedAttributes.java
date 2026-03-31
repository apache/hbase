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
package org.apache.hadoop.hbase.replication;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.codec.KeyValueCodecWithTags;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerObserver;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;

@Category({ ReplicationTests.class, MediumTests.class })
public class TestReplicationWithWALExtendedAttributes {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReplicationWithWALExtendedAttributes.class);

  private static final Logger LOG =
    LoggerFactory.getLogger(TestReplicationWithWALExtendedAttributes.class);

  private static Configuration conf1 = HBaseConfiguration.create();

  private static Admin replicationAdmin;

  private static Connection connection1;

  private static Table htable1;
  private static Table htable2;

  private static HBaseTestingUtil utility1;
  private static HBaseTestingUtil utility2;
  private static final long SLEEP_TIME = 500;
  private static final int NB_RETRIES = 10;

  private static final TableName TABLE_NAME = TableName.valueOf("TestReplicationWithWALAnnotation");
  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final byte[] ROW = Bytes.toBytes("row");
  private static final byte[] ROW2 = Bytes.toBytes("row2");

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf1.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/1");
    conf1.setInt("replication.source.size.capacity", 10240);
    conf1.setLong("replication.source.sleepforretries", 100);
    conf1.setInt("hbase.regionserver.maxlogs", 10);
    conf1.setLong("hbase.master.logcleaner.ttl", 10);
    conf1.setInt("zookeeper.recovery.retry", 1);
    conf1.setInt("zookeeper.recovery.retry.intervalmill", 10);
    conf1.setLong(HConstants.THREAD_WAKE_FREQUENCY, 100);
    conf1.setInt("replication.stats.thread.period.seconds", 5);
    conf1.setBoolean("hbase.tests.use.shortcircuit.reads", false);
    conf1.setStrings(HConstants.REPLICATION_CODEC_CONF_KEY, KeyValueCodecWithTags.class.getName());
    conf1.setStrings(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY,
      TestCoprocessorForWALAnnotationAtSource.class.getName());

    utility1 = new HBaseTestingUtil(conf1);
    utility1.startMiniZKCluster();
    MiniZooKeeperCluster miniZK = utility1.getZkCluster();
    // Have to reget conf1 in case zk cluster location different
    // than default
    conf1 = utility1.getConfiguration();
    LOG.info("Setup first Zk");

    // Base conf2 on conf1 so it gets the right zk cluster.
    Configuration conf2 = HBaseConfiguration.create(conf1);
    conf2.setInt("hfile.format.version", 3);
    conf2.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/2");
    conf2.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 6);
    conf2.setBoolean("hbase.tests.use.shortcircuit.reads", false);
    conf2.setStrings(HConstants.REPLICATION_CODEC_CONF_KEY, KeyValueCodecWithTags.class.getName());
    conf2.setStrings(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY,
      TestCoprocessorForWALAnnotationAtSink.class.getName());
    conf2.setStrings(CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY,
      TestReplicationSinkRegionServerEndpoint.class.getName());

    utility2 = new HBaseTestingUtil(conf2);
    utility2.setZkCluster(miniZK);

    LOG.info("Setup second Zk");
    utility1.startMiniCluster(2);
    utility2.startMiniCluster(2);

    connection1 = ConnectionFactory.createConnection(conf1);
    replicationAdmin = connection1.getAdmin();
    ReplicationPeerConfig rpc =
      ReplicationPeerConfig.newBuilder().setClusterKey(utility2.getRpcConnnectionURI()).build();
    replicationAdmin.addReplicationPeer("2", rpc);

    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TABLE_NAME)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(FAMILY).setMaxVersions(3)
        .setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build())
      .build();
    try (Connection conn = ConnectionFactory.createConnection(conf1);
      Admin admin = conn.getAdmin()) {
      admin.createTable(tableDescriptor, HBaseTestingUtil.KEYS_FOR_HBA_CREATE_TABLE);
    }
    try (Connection conn = ConnectionFactory.createConnection(conf2);
      Admin admin = conn.getAdmin()) {
      admin.createTable(tableDescriptor, HBaseTestingUtil.KEYS_FOR_HBA_CREATE_TABLE);
    }
    htable1 = utility1.getConnection().getTable(TABLE_NAME);
    htable2 = utility2.getConnection().getTable(TABLE_NAME);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    Closeables.close(replicationAdmin, true);
    Closeables.close(connection1, true);
    utility2.shutdownMiniCluster();
    utility1.shutdownMiniCluster();
  }

  @Test
  public void testReplicationWithWALExtendedAttributes() throws Exception {
    Put put = new Put(ROW);
    put.addColumn(FAMILY, ROW, ROW);

    htable1 = utility1.getConnection().getTable(TABLE_NAME);
    htable1.put(put);

    Put put2 = new Put(ROW2);
    put2.addColumn(FAMILY, ROW2, ROW2);

    htable1.batch(Collections.singletonList(put2), new Object[1]);

    assertGetValues(new Get(ROW), ROW);
    assertGetValues(new Get(ROW2), ROW2);
  }

  private static void assertGetValues(Get get, byte[] value)
    throws IOException, InterruptedException {
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i == NB_RETRIES - 1) {
        fail("Waited too much time for put replication");
      }
      Result res = htable2.get(get);
      if (res.isEmpty()) {
        LOG.info("Row not available");
        Thread.sleep(SLEEP_TIME);
      } else {
        assertArrayEquals(value, res.value());
        break;
      }
    }
  }

  public static class TestCoprocessorForWALAnnotationAtSource
    implements RegionCoprocessor, RegionObserver {

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void preWALAppend(ObserverContext<? extends RegionCoprocessorEnvironment> ctx,
      WALKey key, WALEdit edit) throws IOException {
      key.addExtendedAttribute("extendedAttr1", Bytes.toBytes("Value of Extended attribute 01"));
      key.addExtendedAttribute("extendedAttr2", Bytes.toBytes("Value of Extended attribute 02"));
    }
  }

  public static class TestCoprocessorForWALAnnotationAtSink
    implements RegionCoprocessor, RegionObserver {

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void prePut(ObserverContext<? extends RegionCoprocessorEnvironment> c, Put put,
      WALEdit edit) throws IOException {
      String attrVal1 = Bytes.toString(put.getAttribute("extendedAttr1"));
      String attrVal2 = Bytes.toString(put.getAttribute("extendedAttr2"));
      if (attrVal1 == null || attrVal2 == null) {
        throw new IOException("Failed to retrieve WAL annotations");
      }
      if (
        attrVal1.equals("Value of Extended attribute 01")
          && attrVal2.equals("Value of Extended attribute 02")
      ) {
        return;
      }
      throw new IOException("Failed to retrieve WAL annotations..");
    }

    @Override
    public void preBatchMutate(ObserverContext<? extends RegionCoprocessorEnvironment> c,
      MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
      String attrVal1 = Bytes.toString(miniBatchOp.getOperation(0).getAttribute("extendedAttr1"));
      String attrVal2 = Bytes.toString(miniBatchOp.getOperation(0).getAttribute("extendedAttr2"));
      if (attrVal1 == null || attrVal2 == null) {
        throw new IOException("Failed to retrieve WAL annotations");
      }
      if (
        attrVal1.equals("Value of Extended attribute 01")
          && attrVal2.equals("Value of Extended attribute 02")
      ) {
        return;
      }
      throw new IOException("Failed to retrieve WAL annotations..");
    }
  }

  public static final class TestReplicationSinkRegionServerEndpoint
    implements RegionServerCoprocessor, RegionServerObserver {

    @Override
    public Optional<RegionServerObserver> getRegionServerObserver() {
      return Optional.of(this);
    }

    @Override
    public void preReplicationSinkBatchMutate(
      ObserverContext<RegionServerCoprocessorEnvironment> ctx, AdminProtos.WALEntry walEntry,
      Mutation mutation) throws IOException {
      RegionServerObserver.super.preReplicationSinkBatchMutate(ctx, walEntry, mutation);
      List<WALProtos.Attribute> attributeList = walEntry.getKey().getExtendedAttributesList();
      attachWALExtendedAttributesToMutation(mutation, attributeList);
    }

    @Override
    public void postReplicationSinkBatchMutate(
      ObserverContext<RegionServerCoprocessorEnvironment> ctx, AdminProtos.WALEntry walEntry,
      Mutation mutation) throws IOException {
      RegionServerObserver.super.postReplicationSinkBatchMutate(ctx, walEntry, mutation);
      LOG.info("WALEntry extended attributes: {}", walEntry.getKey().getExtendedAttributesList());
      LOG.info("Mutation attributes: {}", mutation.getAttributesMap());
    }

    private void attachWALExtendedAttributesToMutation(Mutation mutation,
      List<WALProtos.Attribute> attributeList) {
      if (attributeList != null) {
        for (WALProtos.Attribute attribute : attributeList) {
          mutation.setAttribute(attribute.getKey(), attribute.getValue().toByteArray());
        }
      }
    }
  }

}
