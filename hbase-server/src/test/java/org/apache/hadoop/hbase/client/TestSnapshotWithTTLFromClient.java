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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.snapshot.SnapshotTTLExpiredException;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test restore/clone snapshots with TTL from the client
 */
@Category({ LargeTests.class, ClientTests.class })
public class TestSnapshotWithTTLFromClient {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSnapshotWithTTLFromClient.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestSnapshotWithTTLFromClient.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();
  private static final int NUM_RS = 2;
  private static final String STRING_TABLE_NAME = "test";
  private static final byte[] TEST_FAM = Bytes.toBytes("fam");
  private static final TableName TABLE_NAME = TableName.valueOf(STRING_TABLE_NAME);
  private static final TableName CLONED_TABLE_NAME = TableName.valueOf("clonedTable");
  private static final String TTL_KEY = "TTL";
  private static final int CHORE_INTERVAL_SECS = 30;

  /**
   * Setup the config for the cluster
   * @throws Exception on failure
   */
  @BeforeClass
  public static void setupCluster() throws Exception {
    setupConf(UTIL.getConfiguration());
    UTIL.startMiniCluster(NUM_RS);
  }

  protected static void setupConf(Configuration conf) {
    // Enable snapshot
    conf.setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);

    // Set this to high value so that cleaner chore is not triggered
    conf.setInt("hbase.master.cleaner.snapshot.interval", CHORE_INTERVAL_SECS * 60 * 1000);
  }

  @Before
  public void setup() throws Exception {
    createTable();
  }

  protected void createTable() throws Exception {
    UTIL.createTable(TABLE_NAME, new byte[][] { TEST_FAM });
  }

  @After
  public void tearDown() throws Exception {
    UTIL.deleteTableIfAny(TABLE_NAME);
    UTIL.deleteTableIfAny(CLONED_TABLE_NAME);
    SnapshotTestingUtils.deleteAllSnapshots(UTIL.getAdmin());
    SnapshotTestingUtils.deleteArchiveDirectory(UTIL);
  }

  @AfterClass
  public static void cleanupTest() throws Exception {
    try {
      UTIL.shutdownMiniCluster();
    } catch (Exception e) {
      LOG.warn("failure shutting down cluster", e);
    }
  }

  @Test
  public void testRestoreSnapshotWithTTLSuccess() throws Exception {
    String snapshotName = "nonExpiredTTLRestoreSnapshotTest";

    // table should exist
    assertTrue(UTIL.getAdmin().tableExists(TABLE_NAME));

    // create snapshot fo given table with specified ttl
    createSnapshotWithTTL(TABLE_NAME, snapshotName, CHORE_INTERVAL_SECS * 2);
    Admin admin = UTIL.getAdmin();

    // Disable and drop table
    admin.disableTable(TABLE_NAME);
    admin.deleteTable(TABLE_NAME);
    assertFalse(UTIL.getAdmin().tableExists(TABLE_NAME));

    // restore snapshot
    admin.restoreSnapshot(snapshotName);

    // table should be created
    assertTrue(UTIL.getAdmin().tableExists(TABLE_NAME));
  }

  @Test
  public void testRestoreSnapshotFailsDueToTTLExpired() throws Exception {
    String snapshotName = "expiredTTLRestoreSnapshotTest";

    // table should exist
    assertTrue(UTIL.getAdmin().tableExists(TABLE_NAME));

    // create snapshot fo given table with specified ttl
    createSnapshotWithTTL(TABLE_NAME, snapshotName, 5);
    Admin admin = UTIL.getAdmin();

    // Disable and drop table
    admin.disableTable(TABLE_NAME);
    admin.deleteTable(TABLE_NAME);
    assertFalse(UTIL.getAdmin().tableExists(TABLE_NAME));

    // Sleep so that TTL may expire
    Threads.sleep(10000);

    // restore snapshot which has expired
    try {
      admin.restoreSnapshot(snapshotName);
      fail("Restore snapshot succeeded even though TTL has expired.");
    } catch (SnapshotTTLExpiredException e) {
      LOG.info("Correctly failed to restore a TTL expired snapshot table:" + e.getMessage());
    }

    // table should not be created
    assertFalse(UTIL.getAdmin().tableExists(TABLE_NAME));
  }

  @Test
  public void testCloneSnapshotWithTTLSuccess() throws Exception {
    String snapshotName = "nonExpiredTTLCloneSnapshotTest";

    // table should exist
    assertTrue(UTIL.getAdmin().tableExists(TABLE_NAME));

    // create snapshot fo given table with specified ttl
    createSnapshotWithTTL(TABLE_NAME, snapshotName, CHORE_INTERVAL_SECS * 2);
    Admin admin = UTIL.getAdmin();

    // restore snapshot
    admin.cloneSnapshot(snapshotName, CLONED_TABLE_NAME);

    // table should be created
    assertTrue(UTIL.getAdmin().tableExists(CLONED_TABLE_NAME));
  }

  @Test
  public void testCloneSnapshotFailsDueToTTLExpired() throws Exception {
    String snapshotName = "expiredTTLCloneSnapshotTest";

    // table should exist
    assertTrue(UTIL.getAdmin().tableExists(TABLE_NAME));

    // create snapshot fo given table with specified ttl
    createSnapshotWithTTL(TABLE_NAME, snapshotName, 5);
    Admin admin = UTIL.getAdmin();

    assertTrue(UTIL.getAdmin().tableExists(TABLE_NAME));

    // Sleep so that TTL may expire
    Threads.sleep(10000);

    // clone snapshot which has expired
    try {
      admin.cloneSnapshot(snapshotName, CLONED_TABLE_NAME);
      fail("Clone snapshot succeeded even though TTL has expired.");
    } catch (SnapshotTTLExpiredException e) {
      LOG.info("Correctly failed to clone a TTL expired snapshot table:" + e.getMessage());
    }

    // table should not be created
    assertFalse(UTIL.getAdmin().tableExists(CLONED_TABLE_NAME));
  }

  private void createSnapshotWithTTL(TableName tableName, final String snapshotName,
    final int snapshotTTL) throws IOException {
    Admin admin = UTIL.getAdmin();

    // make sure we don't fail on listing snapshots
    SnapshotTestingUtils.assertNoSnapshots(admin);

    // put some stuff in the table
    Table table = UTIL.getConnection().getTable(tableName);
    UTIL.loadTable(table, TEST_FAM);

    Map<String, Object> props = new HashMap<>();
    props.put(TTL_KEY, snapshotTTL);

    // take a snapshot of the table
    SnapshotTestingUtils.snapshot(UTIL.getAdmin(), snapshotName, tableName, SnapshotType.FLUSH, 3,
      props);
    LOG.debug("Snapshot completed.");

    // make sure we have the snapshot with expectd TTL
    List<SnapshotDescription> snapshots =
      SnapshotTestingUtils.assertOneSnapshotThatMatches(admin, snapshotName, tableName);
    assertEquals(1, snapshots.size());
    assertEquals(snapshotTTL, snapshots.get(0).getTtl());
  }
}
