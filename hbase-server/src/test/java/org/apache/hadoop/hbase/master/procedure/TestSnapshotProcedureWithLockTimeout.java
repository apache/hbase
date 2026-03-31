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
package org.apache.hadoop.hbase.master.procedure;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.SnapshotType;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.snapshot.TakeSnapshotHandler;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos;

/**
 * Snapshot creation with master lock timeout test.
 */
@Category({ MasterTests.class, MediumTests.class })
public class TestSnapshotProcedureWithLockTimeout {

  private static final Logger LOG =
    LoggerFactory.getLogger(TestSnapshotProcedureWithLockTimeout.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSnapshotProcedureWithLockTimeout.class);

  private static HBaseTestingUtil TEST_UTIL;
  private HMaster master;
  private TableName TABLE_NAME;
  private byte[] CF;
  private String SNAPSHOT_NAME;

  @Before
  public void setup() throws Exception {
    TEST_UTIL = new HBaseTestingUtil();
    Configuration config = TEST_UTIL.getConfiguration();
    config.setInt("hbase.snapshot.remote.verify.threshold", 1);
    config.setLong(TakeSnapshotHandler.HBASE_SNAPSHOT_MASTER_LOCK_ACQUIRE_TIMEOUT, 1L);
    TEST_UTIL.startMiniCluster(3);
    master = TEST_UTIL.getHBaseCluster().getMaster();
    TABLE_NAME = TableName.valueOf(Bytes.toBytes("TestSnapshotProcedureWithLockTimeout"));
    CF = Bytes.toBytes("cf");
    SNAPSHOT_NAME = "SnapshotProcLockTimeout";
    final byte[][] splitKeys = new RegionSplitter.HexStringSplit().split(10);
    Table table = TEST_UTIL.createTable(TABLE_NAME, CF, splitKeys);
    TEST_UTIL.loadTable(table, CF, false);
  }

  @After
  public void teardown() throws Exception {
    if (this.master != null) {
      ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(master.getMasterProcedureExecutor(),
        false);
    }
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testTakeZkCoordinatedSnapshot() {
    for (int i = 0; i < 10; i++) {
      try {
        // Verify that snapshot creation is not possible because lock could not be
        // acquired on time. This can be flaky behavior because even though we provide 1ms
        // as lock timeout, it could still be fast enough and eventually lead to successful
        // snapshot creation. If that happens, retry again.
        testTakeZkCoordinatedSnapshot(i);
        break;
      } catch (Exception e) {
        LOG.error("Error because of faster lock acquisition. retrying....", e);
      }
      assertNotEquals("Retries exhausted", 9, i);
    }
  }

  private void testTakeZkCoordinatedSnapshot(int i) throws Exception {
    SnapshotDescription snapshotOnSameTable =
      new SnapshotDescription(SNAPSHOT_NAME + i, TABLE_NAME, SnapshotType.SKIPFLUSH);
    SnapshotProtos.SnapshotDescription snapshotOnSameTableProto =
      ProtobufUtil.createHBaseProtosSnapshotDesc(snapshotOnSameTable);
    Thread second = new Thread("zk-snapshot") {
      @Override
      public void run() {
        try {
          master.getSnapshotManager().takeSnapshot(snapshotOnSameTableProto);
        } catch (IOException e) {
          LOG.error("zk snapshot failed", e);
          fail("zk snapshot failed");
        }
      }
    };
    second.start();

    Thread.sleep(5000);
    boolean snapshotCreated = false;
    try {
      SnapshotTestingUtils.confirmSnapshotValid(TEST_UTIL, snapshotOnSameTableProto, TABLE_NAME,
        CF);
      snapshotCreated = true;
    } catch (AssertionError e) {
      LOG.error("Assertion error..", e);
      if (
        e.getMessage() != null && e.getMessage().contains("target snapshot directory")
          && e.getMessage().contains("doesn't exist.")
      ) {
        LOG.debug("Expected behaviour - snapshot could not be created");
      } else {
        throw new IOException(e);
      }
    }

    if (snapshotCreated) {
      throw new IOException("Snapshot created successfully");
    }

    // ensure all scheduled procedures are successfully completed
    TEST_UTIL.waitFor(4000, 400,
      () -> master.getMasterProcedureExecutor().getProcedures().stream()
        .filter(masterProcedureEnvProcedure -> masterProcedureEnvProcedure.getState()
            == ProcedureProtos.ProcedureState.SUCCESS)
        .count() == master.getMasterProcedureExecutor().getProcedures().size());
  }
}
