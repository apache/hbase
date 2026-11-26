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
package org.apache.hadoop.hbase.backup;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.backup.impl.BackupManager;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.CheckAndMutateResult;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.ToolRunner;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * If the response to taking the backup exclusive lock is lost after the lock was taken, the backup
 * fails before it records a running session. The repair command must release that lock.
 */
@Tag(LargeTests.TAG)
public class TestRepairAfterLostBackupLockResponse extends TestBackupBase {

  private static final byte[] ACTIVE_SESSION_ROW = Bytes.toBytes("activesession:");

  /** When set, the next successful update of the exclusive lock fails after it is applied. */
  private static final AtomicBoolean DROP_LOCK_RESPONSE = new AtomicBoolean();

  private static final AtomicInteger DROPPED_RESPONSES = new AtomicInteger();

  public static class LockResponseDropper implements RegionCoprocessor, RegionObserver {

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public CheckAndMutateResult postCheckAndMutate(
      ObserverContext<? extends RegionCoprocessorEnvironment> c, CheckAndMutate checkAndMutate,
      CheckAndMutateResult result) throws IOException {
      if (
        result.isSuccess() && Bytes.equals(ACTIVE_SESSION_ROW, checkAndMutate.getRow())
          && c.getEnvironment().getRegionInfo().getTable()
            .equals(BackupSystemTable.getTableName(c.getEnvironment().getConfiguration()))
          && DROP_LOCK_RESPONSE.compareAndSet(true, false)
      ) {
        DROPPED_RESPONSES.incrementAndGet();
        // the lock update has been applied, but the client gets an error instead of the result
        throw new IOException("Injected failure after taking the backup exclusive lock");
      }
      return result;
    }
  }

  @BeforeAll
  public static void setUp() throws Exception {
    TEST_UTIL = new HBaseTestingUtil();
    conf1 = TEST_UTIL.getConfiguration();
    conf1.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, LockResponseDropper.class.getName());
    // do not wait an hour for the lock that is never released
    conf1.setInt(BackupManager.BACKUP_EXCLUSIVE_OPERATION_TIMEOUT_SECONDS_KEY, 5);
    autoRestoreOnFailure = true;
    useSecondCluster = false;
    setUpHelper();
  }

  private int runBackup() throws Exception {
    String[] args =
      new String[] { "create", "full", BACKUP_ROOT_DIR, "-t", table1.getNameAsString() };
    return ToolRunner.run(conf1, new BackupDriver(), args);
  }

  @Test
  public void testRepairReleasesLockWithoutRunningBackup() throws Exception {
    try (BackupSystemTable table = new BackupSystemTable(TEST_UTIL.getConnection())) {
      int before = table.getBackupHistory().size();

      DROP_LOCK_RESPONSE.set(true);
      assertNotEquals(0, runBackup(), "Backup should fail when the lock response is lost");
      assertEquals(1, DROPPED_RESPONSES.get());
      assertFalse(DROP_LOCK_RESPONSE.get());
      // the backup failed before recording a session, so there is nothing for repair to fail
      assertEquals(before, table.getBackupHistory().size());

      assertTrue(table.isBackupExclusiveOperationActive());

      assertEquals(0, ToolRunner.run(conf1, new BackupDriver(), new String[] { "repair" }));
      assertFalse(table.isBackupExclusiveOperationActive());

      assertEquals(0, runBackup(), "Backup should succeed after repair");
      assertEquals(before + 1, table.getBackupHistory().size());
      assertTrue(checkSucceeded(table.getBackupHistory().get(0).getBackupId()));
    }
  }
}
