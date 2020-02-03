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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ArrayBackedTag;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.codec.KeyValueCodecWithTags;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ReplicationTests.class, MediumTests.class})
public class TestReplicationWithTags {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestReplicationWithTags.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestReplicationWithTags.class);
  private static final byte TAG_TYPE = 1;

  private static Configuration conf1 = HBaseConfiguration.create();
  private static Configuration conf2;

  private static ReplicationAdmin replicationAdmin;

  private static Connection connection1;
  private static Connection connection2;

  private static Table htable1;
  private static Table htable2;

  private static HBaseTestingUtility utility1;
  private static HBaseTestingUtility utility2;
  private static final long SLEEP_TIME = 500;
  private static final int NB_RETRIES = 10;

  private static final TableName TABLE_NAME = TableName.valueOf("TestReplicationWithTags");
  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final byte[] ROW = Bytes.toBytes("row");

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf1.setInt("hfile.format.version", 3);
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
        TestCoprocessorForTagsAtSource.class.getName());

    utility1 = new HBaseTestingUtility(conf1);
    utility1.startMiniZKCluster();
    MiniZooKeeperCluster miniZK = utility1.getZkCluster();
    // Have to reget conf1 in case zk cluster location different
    // than default
    conf1 = utility1.getConfiguration();
    LOG.info("Setup first Zk");

    // Base conf2 on conf1 so it gets the right zk cluster.
    conf2 = HBaseConfiguration.create(conf1);
    conf2.setInt("hfile.format.version", 3);
    conf2.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/2");
    conf2.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 6);
    conf2.setBoolean("hbase.tests.use.shortcircuit.reads", false);
    conf2.setStrings(HConstants.REPLICATION_CODEC_CONF_KEY, KeyValueCodecWithTags.class.getName());
    conf2.setStrings(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY,
            TestCoprocessorForTagsAtSink.class.getName());

    utility2 = new HBaseTestingUtility(conf2);
    utility2.setZkCluster(miniZK);

    LOG.info("Setup second Zk");
    utility1.startMiniCluster(2);
    utility2.startMiniCluster(2);

    replicationAdmin = new ReplicationAdmin(conf1);
    ReplicationPeerConfig rpc = new ReplicationPeerConfig();
    rpc.setClusterKey(utility2.getClusterKey());
    replicationAdmin.addPeer("2", rpc, null);

    HTableDescriptor table = new HTableDescriptor(TABLE_NAME);
    HColumnDescriptor fam = new HColumnDescriptor(FAMILY);
    fam.setMaxVersions(3);
    fam.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    table.addFamily(fam);
    try (Connection conn = ConnectionFactory.createConnection(conf1);
        Admin admin = conn.getAdmin()) {
      admin.createTable(table, HBaseTestingUtility.KEYS_FOR_HBA_CREATE_TABLE);
    }
    try (Connection conn = ConnectionFactory.createConnection(conf2);
        Admin admin = conn.getAdmin()) {
      admin.createTable(table, HBaseTestingUtility.KEYS_FOR_HBA_CREATE_TABLE);
    }
    htable1 = utility1.getConnection().getTable(TABLE_NAME);
    htable2 = utility2.getConnection().getTable(TABLE_NAME);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    utility2.shutdownMiniCluster();
    utility1.shutdownMiniCluster();
  }

  @Test
  public void testReplicationWithCellTags() throws Exception {
    LOG.info("testSimplePutDelete");
    Put put = new Put(ROW);
    put.setAttribute("visibility", Bytes.toBytes("myTag3"));
    put.addColumn(FAMILY, ROW, ROW);

    htable1 = utility1.getConnection().getTable(TABLE_NAME);
    htable1.put(put);

    Get get = new Get(ROW);
    try {
      for (int i = 0; i < NB_RETRIES; i++) {
        if (i == NB_RETRIES - 1) {
          fail("Waited too much time for put replication");
        }
        Result res = htable2.get(get);
        if (res.isEmpty()) {
          LOG.info("Row not available");
          Thread.sleep(SLEEP_TIME);
        } else {
          assertArrayEquals(ROW, res.value());
          assertEquals(1, TestCoprocessorForTagsAtSink.TAGS.size());
          Tag tag = TestCoprocessorForTagsAtSink.TAGS.get(0);
          assertEquals(TAG_TYPE, tag.getType());
          break;
        }
      }
    } finally {
      TestCoprocessorForTagsAtSink.TAGS = null;
    }
  }

  public static class TestCoprocessorForTagsAtSource implements RegionCoprocessor, RegionObserver {
    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void prePut(final ObserverContext<RegionCoprocessorEnvironment> e, final Put put,
        final WALEdit edit, final Durability durability) throws IOException {
      byte[] attribute = put.getAttribute("visibility");
      byte[] cf = null;
      List<Cell> updatedCells = new ArrayList<>();
      if (attribute != null) {
        for (List<? extends Cell> edits : put.getFamilyCellMap().values()) {
          for (Cell cell : edits) {
            KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
            if (cf == null) {
              cf = CellUtil.cloneFamily(kv);
            }
            Tag tag = new ArrayBackedTag(TAG_TYPE, attribute);
            List<Tag> tagList = new ArrayList<>(1);
            tagList.add(tag);

            KeyValue newKV = new KeyValue(CellUtil.cloneRow(kv), 0, kv.getRowLength(),
                CellUtil.cloneFamily(kv), 0, kv.getFamilyLength(), CellUtil.cloneQualifier(kv), 0,
                kv.getQualifierLength(), kv.getTimestamp(),
                KeyValue.Type.codeToType(kv.getTypeByte()), CellUtil.cloneValue(kv), 0,
                kv.getValueLength(), tagList);
            ((List<Cell>) updatedCells).add(newKV);
          }
        }
        put.getFamilyCellMap().remove(cf);
        // Update the family map
        put.getFamilyCellMap().put(cf, updatedCells);
      }
    }
  }

  public static class TestCoprocessorForTagsAtSink implements RegionCoprocessor, RegionObserver {
    private static List<Tag> TAGS = null;

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void postGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get,
        List<Cell> results) throws IOException {
      if (results.size() > 0) {
        // Check tag presence in the 1st cell in 1st Result
        if (!results.isEmpty()) {
          Cell cell = results.get(0);
          TAGS = PrivateCellUtil.getTags(cell);
        }
      }
    }
  }
}
