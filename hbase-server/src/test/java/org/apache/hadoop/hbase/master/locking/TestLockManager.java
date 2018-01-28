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
package org.apache.hadoop.hbase.master.locking;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureConstants;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.LockType;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MasterTests.class, MediumTests.class })
public class TestLockManager {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestLockManager.class);

  @Rule
  public TestName testName = new TestName();
  // crank this up if this test turns out to be flaky.
  private static final int LOCAL_LOCKS_TIMEOUT = 1000;

  private static final Logger LOG = LoggerFactory.getLogger(TestLockManager.class);
  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static MasterServices masterServices;

  private static String namespace = "namespace";
  private static TableName tableName = TableName.valueOf(namespace, "table");
  private static HRegionInfo[] tableRegions;

  private static void setupConf(Configuration conf) {
    conf.setInt(MasterProcedureConstants.MASTER_PROCEDURE_THREADS, 1);
    conf.setBoolean("hbase.procedure.check.owner.set", false);  // since rpc user will be null
    conf.setInt(LockProcedure.LOCAL_MASTER_LOCKS_TIMEOUT_MS_CONF, LOCAL_LOCKS_TIMEOUT);
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    setupConf(UTIL.getConfiguration());
    UTIL.startMiniCluster(1);
    masterServices = UTIL.getMiniHBaseCluster().getMaster();
    UTIL.getAdmin().createNamespace(NamespaceDescriptor.create(namespace).build());
    UTIL.createTable(tableName, new byte[][]{"fam".getBytes()}, new byte[][] {"1".getBytes()});
    List<HRegionInfo> regions = UTIL.getAdmin().getTableRegions(tableName);
    assert regions.size() > 0;
    tableRegions = new HRegionInfo[regions.size()];
    regions.toArray(tableRegions);
  }

  @AfterClass
  public static void cleanupTest() throws Exception {
    try {
      UTIL.shutdownMiniCluster();
    } catch (Exception e) {
      LOG.warn("failure shutting down cluster", e);
    }
  }

  @After
  public void tearDown() throws Exception {
    for (Procedure<?> proc : getMasterProcedureExecutor().getProcedures()) {
      if (proc instanceof LockProcedure) {
        ((LockProcedure) proc).unlock(getMasterProcedureExecutor().getEnvironment());
        ProcedureTestingUtility.waitProcedure(getMasterProcedureExecutor(), proc);
      }
    }
    assertEquals(0, getMasterProcedureExecutor().getEnvironment().getProcedureScheduler().size());
  }

  private ProcedureExecutor<MasterProcedureEnv> getMasterProcedureExecutor() {
    return UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor();
  }

  /**
   * Tests that basic lock functionality works.
   */
  @Test
  public void testMasterLockAcquire() throws Exception {
    LockManager.MasterLock lock = masterServices.getLockManager().createMasterLock(namespace,
        LockType.EXCLUSIVE, "desc");
    assertTrue(lock.tryAcquire(2000));
    assertTrue(lock.getProc().isLocked());
    lock.release();
    assertEquals(null, lock.getProc());
  }

  /**
   * Two locks try to acquire lock on same table, assert that later one times out.
   */
  @Test
  public void testMasterLockAcquireTimeout() throws Exception {
    LockManager.MasterLock lock = masterServices.getLockManager().createMasterLock(
        tableName, LockType.EXCLUSIVE, "desc");
    LockManager.MasterLock lock2 = masterServices.getLockManager().createMasterLock(
        tableName, LockType.EXCLUSIVE, "desc");
    assertTrue(lock.tryAcquire(2000));
    assertFalse(lock2.tryAcquire(LOCAL_LOCKS_TIMEOUT/2));  // wait less than other lock's timeout
    assertEquals(null, lock2.getProc());
    lock.release();
    assertTrue(lock2.tryAcquire(2000));
    assertTrue(lock2.getProc().isLocked());
    lock2.release();
  }

  /**
   * Take region lock, they try table exclusive lock, later one should time out.
   */
  @Test
  public void testMasterLockAcquireTimeoutRegionVsTableExclusive() throws Exception {
    LockManager.MasterLock lock = masterServices.getLockManager().createMasterLock(
        tableRegions, "desc");
    LockManager.MasterLock lock2 = masterServices.getLockManager().createMasterLock(
        tableName, LockType.EXCLUSIVE, "desc");
    assertTrue(lock.tryAcquire(2000));
    assertFalse(lock2.tryAcquire(LOCAL_LOCKS_TIMEOUT/2));  // wait less than other lock's timeout
    assertEquals(null, lock2.getProc());
    lock.release();
    assertTrue(lock2.tryAcquire(2000));
    assertTrue(lock2.getProc().isLocked());
    lock2.release();
  }
}
