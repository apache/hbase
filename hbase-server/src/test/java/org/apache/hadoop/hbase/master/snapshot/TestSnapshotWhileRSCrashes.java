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
package org.apache.hadoop.hbase.master.snapshot;

import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.locking.LockManager.MasterLock;
import org.apache.hadoop.hbase.master.locking.LockProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.LockType;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, MediumTests.class })
public class TestSnapshotWhileRSCrashes {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSnapshotWhileRSCrashes.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static TableName NAME = TableName.valueOf("Cleanup");

  private static byte[] CF = Bytes.toBytes("cf");

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.startMiniCluster(3);
    UTIL.createMultiRegionTable(NAME, CF);
    UTIL.waitTableAvailable(NAME);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void test() throws InterruptedException, IOException {
    String snName = "sn";
    MasterLock lock = UTIL.getMiniHBaseCluster().getMaster().getLockManager().createMasterLock(NAME,
      LockType.EXCLUSIVE, "for testing");
    lock.acquire();
    Thread t = new Thread(() -> {
      try {
        UTIL.getAdmin().snapshot(snName, NAME);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    });
    t.setDaemon(true);
    t.start();
    ProcedureExecutor<MasterProcedureEnv> procExec =
      UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor();
    UTIL.waitFor(10000,
      () -> procExec.getProcedures().stream().filter(p -> !p.isFinished())
        .filter(p -> p instanceof LockProcedure).map(p -> (LockProcedure) p)
        .filter(p -> NAME.equals(p.getTableName())).anyMatch(p -> !p.isLocked()));
    UTIL.getMiniHBaseCluster().stopRegionServer(0);
    lock.release();
    // the snapshot can not work properly when there are rs crashes, so here we just want to make
    // sure that the regions could online
    try (Table table = UTIL.getConnection().getTable(NAME);
        ResultScanner scanner = table.getScanner(CF)) {
      assertNull(scanner.next());
    }
  }
}
