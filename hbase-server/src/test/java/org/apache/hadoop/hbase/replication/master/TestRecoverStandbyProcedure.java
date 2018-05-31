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
package org.apache.hadoop.hbase.replication.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.replication.RecoverStandbyProcedure;
import org.apache.hadoop.hbase.master.replication.SyncReplicationReplayWALManager;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.regionserver.wal.ProtobufLogWriter;
import org.apache.hadoop.hbase.regionserver.wal.WALUtil;
import org.apache.hadoop.hbase.replication.ReplicationUtils;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils.StreamLacksCapabilityException;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MasterTests.class, LargeTests.class})
public class TestRecoverStandbyProcedure {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRecoverStandbyProcedure.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRecoverStandbyProcedure.class);

  private static final TableName tableName = TableName.valueOf("TestRecoverStandbyProcedure");

  private static final RegionInfo regionInfo = RegionInfoBuilder.newBuilder(tableName).build();

  private static final byte[] family = Bytes.toBytes("CF");

  private static final byte[] qualifier = Bytes.toBytes("q");

  private static final long timestamp = System.currentTimeMillis();

  private static final int ROW_COUNT = 1000;

  private static final int WAL_NUMBER = 10;

  private static final int RS_NUMBER = 3;

  private static final String PEER_ID = "1";

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static SyncReplicationReplayWALManager syncReplicationReplayWALManager;

  private static ProcedureExecutor<MasterProcedureEnv> procExec;

  private static FileSystem fs;

  private static Configuration conf;

  @BeforeClass
  public static void setupCluster() throws Exception {
    UTIL.startMiniCluster(RS_NUMBER);
    UTIL.getHBaseCluster().waitForActiveAndReadyMaster();
    conf = UTIL.getConfiguration();
    HMaster master = UTIL.getHBaseCluster().getMaster();
    fs = master.getMasterFileSystem().getWALFileSystem();
    syncReplicationReplayWALManager = master.getSyncReplicationReplayWALManager();
    procExec = master.getMasterProcedureExecutor();
  }

  @AfterClass
  public static void cleanupTest() throws Exception {
    try {
      UTIL.shutdownMiniCluster();
    } catch (Exception e) {
      LOG.warn("failure shutting down cluster", e);
    }
  }

  @Before
  public void setupBeforeTest() throws IOException {
    UTIL.createTable(tableName, family);
  }

  @After
  public void tearDownAfterTest() throws IOException {
    try (Admin admin = UTIL.getAdmin()) {
      if (admin.isTableEnabled(tableName)) {
        admin.disableTable(tableName);
      }
      admin.deleteTable(tableName);
    }
  }

  @Test
  public void testRecoverStandby() throws IOException, StreamLacksCapabilityException {
    setupSyncReplicationWALs();
    long procId = procExec.submitProcedure(new RecoverStandbyProcedure(PEER_ID, false));
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);

    try (Table table = UTIL.getConnection().getTable(tableName)) {
      for (int i = 0; i < WAL_NUMBER * ROW_COUNT; i++) {
        Result result = table.get(new Get(Bytes.toBytes(i)).setTimestamp(timestamp));
        assertNotNull(result);
        assertEquals(i, Bytes.toInt(result.getValue(family, qualifier)));
      }
    }
  }

  private void setupSyncReplicationWALs() throws IOException, StreamLacksCapabilityException {
    Path peerRemoteWALDir = ReplicationUtils
      .getPeerRemoteWALDir(syncReplicationReplayWALManager.getRemoteWALDir(), PEER_ID);
    if (!fs.exists(peerRemoteWALDir)) {
      fs.mkdirs(peerRemoteWALDir);
    }
    for (int i = 0; i < WAL_NUMBER; i++) {
      try (ProtobufLogWriter writer = new ProtobufLogWriter()) {
        Path wal = new Path(peerRemoteWALDir, "srv1,8888." + i + ".syncrep");
        writer.init(fs, wal, conf, true, WALUtil.getWALBlockSize(conf, fs, peerRemoteWALDir));
        List<Entry> entries = setupWALEntries(i * ROW_COUNT, (i + 1) * ROW_COUNT);
        for (Entry entry : entries) {
          writer.append(entry);
        }
        writer.sync(false);
        LOG.info("Created wal {} to replay for peer id={}", wal, PEER_ID);
      }
    }
  }

  private List<Entry> setupWALEntries(int startRow, int endRow) {
    return IntStream.range(startRow, endRow)
        .mapToObj(i -> createWALEntry(Bytes.toBytes(i), Bytes.toBytes(i)))
        .collect(Collectors.toList());
  }

  private Entry createWALEntry(byte[] row, byte[] value) {
    WALKeyImpl key = new WALKeyImpl(regionInfo.getEncodedNameAsBytes(), tableName, 1);
    WALEdit edit = new WALEdit();
    edit.add(new KeyValue(row, family, qualifier, timestamp, value));
    return new Entry(key, edit);
  }
}
