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

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.impl.BackupSystemTable;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.util.ToolRunner;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * This class is only a base for other integration-level backup tests. Do not add tests here.
 * TestBackupSmallTests is where tests that don't require bring machines up/down should go All other
 * tests should have their own classes and extend this one
 */
@Category(LargeTests.class)
public class TestBackupDeleteWithFailures extends TestBackupBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBackupDeleteWithFailures.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestBackupDeleteWithFailures.class);

  public enum Failure {
    NO_FAILURES,
    PRE_SNAPSHOT_FAILURE,
    PRE_DELETE_SNAPSHOT_FAILURE,
    POST_DELETE_SNAPSHOT_FAILURE
  }

  public static class MasterSnapshotObserver implements MasterCoprocessor, MasterObserver {
    List<Failure> failures = new ArrayList<>();

    public void setFailures(Failure... f) {
      failures.clear();
      for (int i = 0; i < f.length; i++) {
        failures.add(f[i]);
      }
    }

    @Override
    public Optional<MasterObserver> getMasterObserver() {
      return Optional.of(this);
    }

    @Override
    public void preSnapshot(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final SnapshotDescription snapshot, final TableDescriptor hTableDescriptor)
      throws IOException {
      if (failures.contains(Failure.PRE_SNAPSHOT_FAILURE)) {
        throw new IOException("preSnapshot");
      }
    }

    @Override
    public void preDeleteSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
      SnapshotDescription snapshot) throws IOException {
      if (failures.contains(Failure.PRE_DELETE_SNAPSHOT_FAILURE)) {
        throw new IOException("preDeleteSnapshot");
      }
    }

    @Override
    public void postDeleteSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
      SnapshotDescription snapshot) throws IOException {
      if (failures.contains(Failure.POST_DELETE_SNAPSHOT_FAILURE)) {
        throw new IOException("postDeleteSnapshot");
      }
    }
  }

  /**
   * Setup Cluster with appropriate configurations before running tests.
   * @throws Exception if starting the mini cluster or setting up the tables fails
   */
  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL = new HBaseTestingUtil();
    conf1 = TEST_UTIL.getConfiguration();
    conf1.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, MasterSnapshotObserver.class.getName());
    conf1.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    setUpHelper();
  }

  private MasterSnapshotObserver getMasterSnapshotObserver() {
    return TEST_UTIL.getHBaseCluster().getMaster().getMasterCoprocessorHost()
      .findCoprocessor(MasterSnapshotObserver.class);
  }

  @Test
  public void testBackupDeleteWithFailures() throws Exception {
    testBackupDeleteWithFailuresAfter(1, Failure.PRE_DELETE_SNAPSHOT_FAILURE);
    testBackupDeleteWithFailuresAfter(0, Failure.POST_DELETE_SNAPSHOT_FAILURE);
    testBackupDeleteWithFailuresAfter(1, Failure.PRE_SNAPSHOT_FAILURE);
  }

  private void testBackupDeleteWithFailuresAfter(int expected, Failure... failures)
    throws Exception {
    LOG.info("test repair backup delete on a single table with data and failures " + failures[0]);
    List<TableName> tableList = Lists.newArrayList(table1);
    String backupId = fullTableBackup(tableList).getBackupId();
    assertTrue(checkSucceeded(backupId));
    LOG.info("backup complete");
    String[] backupIds = new String[] { backupId };
    BackupSystemTable table = new BackupSystemTable(TEST_UTIL.getConnection());
    BackupInfo info = table.readBackupInfo(backupId);
    Path path = new Path(info.getBackupRootDir(), backupId);
    FileSystem fs = FileSystem.get(path.toUri(), conf1);
    assertTrue(fs.exists(path));

    Connection conn = TEST_UTIL.getConnection();
    Admin admin = conn.getAdmin();
    MasterSnapshotObserver observer = getMasterSnapshotObserver();

    observer.setFailures(failures);
    try {
      getBackupAdmin().deleteBackups(backupIds);
    } catch (IOException e) {
      if (expected != 1) {
        assertTrue(false);
      }
    }

    // Verify that history length == expected after delete failure
    assertTrue(table.getBackupHistory().size() == expected);

    String[] ids = table.getListOfBackupIdsFromDeleteOperation();

    // Verify that we still have delete record in backup system table
    if (expected == 1) {
      assertTrue(ids.length == 1);
      assertTrue(ids[0].equals(backupId));
    } else {
      assertNull(ids);
    }

    // Now run repair command to repair "failed" delete operation
    String[] args = new String[] { "repair" };

    observer.setFailures(Failure.NO_FAILURES);

    // Run repair
    int ret = ToolRunner.run(conf1, new BackupDriver(), args);
    assertTrue(ret == 0);
    // Verify that history length == 0
    assertTrue(table.getBackupHistory().size() == 0);
    ids = table.getListOfBackupIdsFromDeleteOperation();

    // Verify that we do not have delete record in backup system table
    assertNull(ids);

    table.close();
    admin.close();
  }
}
