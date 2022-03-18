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
package org.apache.hadoop.hbase.master.cleaner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.Waiter.ExplainingPredicate;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.SnapshotType;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.snapshot.DisabledTableSnapshotHandler;
import org.apache.hadoop.hbase.master.snapshot.SnapshotHFileCleaner;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.regionserver.CompactedHFilesDischarger;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotReferenceUtil;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.snapshot.UnknownSnapshotException;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.Uninterruptibles;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteSnapshotRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetCompletedSnapshotsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetCompletedSnapshotsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos
    .IsSnapshotCleanupEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos
    .IsSnapshotCleanupEnabledResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsSnapshotDoneRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsSnapshotDoneResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos
    .SetSnapshotCleanupRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;

/**
 * Test the master-related aspects of a snapshot
 */
@Category({MasterTests.class, MediumTests.class})
public class TestSnapshotFromMaster {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSnapshotFromMaster.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestSnapshotFromMaster.class);
  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();
  private static final int NUM_RS = 2;
  private static Path rootDir;
  private static FileSystem fs;
  private static HMaster master;

  // for hfile archiving test.
  private static Path archiveDir;
  private static final byte[] TEST_FAM = Bytes.toBytes("fam");
  private static final TableName TABLE_NAME =
      TableName.valueOf("test");
  // refresh the cache every 1/2 second
  private static final long cacheRefreshPeriod = 500;
  private static final int blockingStoreFiles = 12;

  /**
   * Setup the config for the cluster
   */
  @BeforeClass
  public static void setupCluster() throws Exception {
    setupConf(UTIL.getConfiguration());
    UTIL.startMiniCluster(NUM_RS);
    fs = UTIL.getDFSCluster().getFileSystem();
    master = UTIL.getMiniHBaseCluster().getMaster();
    rootDir = master.getMasterFileSystem().getRootDir();
    archiveDir = new Path(rootDir, HConstants.HFILE_ARCHIVE_DIRECTORY);
  }

  private static void setupConf(Configuration conf) {
    // disable the ui
    conf.setInt("hbase.regionsever.info.port", -1);
    // change the flush size to a small amount, regulating number of store files
    conf.setInt("hbase.hregion.memstore.flush.size", 25000);
    // so make sure we get a compaction when doing a load, but keep around some
    // files in the store
    conf.setInt("hbase.hstore.compaction.min", 2);
    conf.setInt("hbase.hstore.compactionThreshold", 5);
    // block writes if we get to 12 store files
    conf.setInt("hbase.hstore.blockingStoreFiles", blockingStoreFiles);
    // Ensure no extra cleaners on by default (e.g. TimeToLiveHFileCleaner)
    conf.set(HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS, "");
    conf.set(HConstants.HBASE_MASTER_LOGCLEANER_PLUGINS, "");
    // Enable snapshot
    conf.setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
    conf.setLong(SnapshotManager.HBASE_SNAPSHOT_SENTINELS_CLEANUP_TIMEOUT_MILLIS, 3 * 1000L);
    conf.setLong(SnapshotHFileCleaner.HFILE_CACHE_REFRESH_PERIOD_CONF_KEY, cacheRefreshPeriod);
    conf.set(HConstants.HBASE_REGION_SPLIT_POLICY_KEY,
      ConstantSizeRegionSplitPolicy.class.getName());
    conf.setInt("hbase.hfile.compactions.cleaner.interval", 20 * 1000);
    conf.setInt("hbase.master.cleaner.snapshot.interval", 500);
  }

  @Before
  public void setup() throws Exception {
    UTIL.createTable(TABLE_NAME, TEST_FAM);
    master.getSnapshotManager().setSnapshotHandlerForTesting(TABLE_NAME, null);
  }

  @After
  public void tearDown() throws Exception {
    UTIL.deleteTable(TABLE_NAME);
    SnapshotTestingUtils.deleteAllSnapshots(UTIL.getAdmin());
    SnapshotTestingUtils.deleteArchiveDirectory(UTIL);
  }

  @AfterClass
  public static void cleanupTest() throws Exception {
    try {
      UTIL.shutdownMiniCluster();
    } catch (Exception e) {
      // NOOP;
    }
  }

  /**
   * Test that the contract from the master for checking on a snapshot are valid.
   * <p>
   * <ol>
   * <li>If a snapshot fails with an error, we expect to get the source error.</li>
   * <li>If there is no snapshot name supplied, we should get an error.</li>
   * <li>If asking about a snapshot has hasn't occurred, you should get an error.</li>
   * </ol>
   */
  @Test
  public void testIsDoneContract() throws Exception {

    IsSnapshotDoneRequest.Builder builder = IsSnapshotDoneRequest.newBuilder();

    String snapshotName = "asyncExpectedFailureTest";

    // check that we get an exception when looking up snapshot where one hasn't happened
    SnapshotTestingUtils.expectSnapshotDoneException(master, builder.build(),
      UnknownSnapshotException.class);

    // and that we get the same issue, even if we specify a name
    SnapshotDescription desc = SnapshotDescription.newBuilder()
      .setName(snapshotName).setTable(TABLE_NAME.getNameAsString()).build();
    builder.setSnapshot(desc);
    SnapshotTestingUtils.expectSnapshotDoneException(master, builder.build(),
      UnknownSnapshotException.class);

    // set a mock handler to simulate a snapshot
    DisabledTableSnapshotHandler mockHandler = Mockito.mock(DisabledTableSnapshotHandler.class);
    Mockito.when(mockHandler.getException()).thenReturn(null);
    Mockito.when(mockHandler.getSnapshot()).thenReturn(desc);
    Mockito.when(mockHandler.isFinished()).thenReturn(Boolean.TRUE);
    Mockito.when(mockHandler.getCompletionTimestamp())
      .thenReturn(EnvironmentEdgeManager.currentTime());

    master.getSnapshotManager()
        .setSnapshotHandlerForTesting(TABLE_NAME, mockHandler);

    // if we do a lookup without a snapshot name, we should fail - you should always know your name
    builder = IsSnapshotDoneRequest.newBuilder();
    SnapshotTestingUtils.expectSnapshotDoneException(master, builder.build(),
      UnknownSnapshotException.class);

    // then do the lookup for the snapshot that it is done
    builder.setSnapshot(desc);
    IsSnapshotDoneResponse response =
      master.getMasterRpcServices().isSnapshotDone(null, builder.build());
    assertTrue("Snapshot didn't complete when it should have.", response.getDone());

    // now try the case where we are looking for a snapshot we didn't take
    builder.setSnapshot(SnapshotDescription.newBuilder().setName("Not A Snapshot").build());
    SnapshotTestingUtils.expectSnapshotDoneException(master, builder.build(),
      UnknownSnapshotException.class);

    // then create a snapshot to the fs and make sure that we can find it when checking done
    snapshotName = "completed";
    desc = createSnapshot(snapshotName);

    builder.setSnapshot(desc);
    response = master.getMasterRpcServices().isSnapshotDone(null, builder.build());
    assertTrue("Completed, on-disk snapshot not found", response.getDone());
  }

  @Test
  public void testGetCompletedSnapshots() throws Exception {
    // first check when there are no snapshots
    GetCompletedSnapshotsRequest request = GetCompletedSnapshotsRequest.newBuilder().build();
    GetCompletedSnapshotsResponse response =
      master.getMasterRpcServices().getCompletedSnapshots(null, request);
    assertEquals("Found unexpected number of snapshots", 0, response.getSnapshotsCount());

    // write one snapshot to the fs
    String snapshotName = "completed";
    SnapshotDescription snapshot = createSnapshot(snapshotName);

    // check that we get one snapshot
    response = master.getMasterRpcServices().getCompletedSnapshots(null, request);
    assertEquals("Found unexpected number of snapshots", 1, response.getSnapshotsCount());
    List<SnapshotDescription> snapshots = response.getSnapshotsList();
    List<SnapshotDescription> expected = Lists.newArrayList(snapshot);
    assertEquals("Returned snapshots don't match created snapshots", expected, snapshots);

    // write a second snapshot
    snapshotName = "completed_two";
    snapshot = createSnapshot(snapshotName);
    expected.add(snapshot);

    // check that we get one snapshot
    response = master.getMasterRpcServices().getCompletedSnapshots(null, request);
    assertEquals("Found unexpected number of snapshots", 2, response.getSnapshotsCount());
    snapshots = response.getSnapshotsList();
    assertEquals("Returned snapshots don't match created snapshots", expected, snapshots);
  }

  @Test
  public void testDeleteSnapshot() throws Exception {

    String snapshotName = "completed";
    SnapshotDescription snapshot = SnapshotDescription.newBuilder().setName(snapshotName).build();

    DeleteSnapshotRequest request = DeleteSnapshotRequest.newBuilder().setSnapshot(snapshot)
        .build();
    try {
      master.getMasterRpcServices().deleteSnapshot(null, request);
      fail("Master didn't throw exception when attempting to delete snapshot that doesn't exist");
    } catch (org.apache.hbase.thirdparty.com.google.protobuf.ServiceException e) {
      // Expected
    }

    // write one snapshot to the fs
    createSnapshot(snapshotName);

    // then delete the existing snapshot,which shouldn't cause an exception to be thrown
    master.getMasterRpcServices().deleteSnapshot(null, request);
  }

  @Test
  public void testGetCompletedSnapshotsWithCleanup() throws Exception {
    // Enable auto snapshot cleanup for the cluster
    SetSnapshotCleanupRequest setSnapshotCleanupRequest =
        SetSnapshotCleanupRequest.newBuilder().setEnabled(true).build();
    master.getMasterRpcServices().switchSnapshotCleanup(null, setSnapshotCleanupRequest);

    // first check when there are no snapshots
    GetCompletedSnapshotsRequest request = GetCompletedSnapshotsRequest.newBuilder().build();
    GetCompletedSnapshotsResponse response =
        master.getMasterRpcServices().getCompletedSnapshots(null, request);
    assertEquals("Found unexpected number of snapshots", 0, response.getSnapshotsCount());

    // NOTE: This is going to be flakey. Its timing based. For now made it more coarse
    // so more likely to pass though we have to hang around longer.

    // write one snapshot to the fs
    createSnapshotWithTtl("snapshot_01", 5L);
    createSnapshotWithTtl("snapshot_02", 100L);

    // check that we get one snapshot
    response = master.getMasterRpcServices().getCompletedSnapshots(null, request);
    assertEquals("Found unexpected number of snapshots", 2, response.getSnapshotsCount());

    // Check that 1 snapshot is auto cleaned after 5 sec of TTL expiration. Wait 10 seconds
    // just in case.
    Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
    response = master.getMasterRpcServices().getCompletedSnapshots(null, request);
    assertEquals("Found unexpected number of snapshots", 1, response.getSnapshotsCount());
  }

  @Test
  public void testGetCompletedSnapshotsWithoutCleanup() throws Exception {
    // Disable auto snapshot cleanup for the cluster
    SetSnapshotCleanupRequest setSnapshotCleanupRequest =
        SetSnapshotCleanupRequest.newBuilder().setEnabled(false).build();
    master.getMasterRpcServices().switchSnapshotCleanup(null, setSnapshotCleanupRequest);

    // first check when there are no snapshots
    GetCompletedSnapshotsRequest request = GetCompletedSnapshotsRequest.newBuilder().build();
    GetCompletedSnapshotsResponse response =
        master.getMasterRpcServices().getCompletedSnapshots(null, request);
    assertEquals("Found unexpected number of snapshots", 0, response.getSnapshotsCount());

    // write one snapshot to the fs
    createSnapshotWithTtl("snapshot_02", 1L);
    createSnapshotWithTtl("snapshot_03", 1L);

    // check that we get one snapshot
    response = master.getMasterRpcServices().getCompletedSnapshots(null, request);
    assertEquals("Found unexpected number of snapshots", 2, response.getSnapshotsCount());

    // check that no snapshot is auto cleaned even after 1 sec of TTL expiration
    Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
    response = master.getMasterRpcServices().getCompletedSnapshots(null, request);
    assertEquals("Found unexpected number of snapshots", 2, response.getSnapshotsCount());
  }

  @Test
  public void testSnapshotCleanupStatus() throws Exception {
    // Enable auto snapshot cleanup for the cluster
    SetSnapshotCleanupRequest setSnapshotCleanupRequest =
        SetSnapshotCleanupRequest.newBuilder().setEnabled(true).build();
    master.getMasterRpcServices().switchSnapshotCleanup(null, setSnapshotCleanupRequest);

    // Check if auto snapshot cleanup is enabled
    IsSnapshotCleanupEnabledRequest isSnapshotCleanupEnabledRequest =
        IsSnapshotCleanupEnabledRequest.newBuilder().build();
    IsSnapshotCleanupEnabledResponse isSnapshotCleanupEnabledResponse =
        master.getMasterRpcServices().isSnapshotCleanupEnabled(null,
            isSnapshotCleanupEnabledRequest);
    Assert.assertTrue(isSnapshotCleanupEnabledResponse.getEnabled());

    // Disable auto snapshot cleanup for the cluster
    setSnapshotCleanupRequest = SetSnapshotCleanupRequest.newBuilder()
        .setEnabled(false).build();
    master.getMasterRpcServices().switchSnapshotCleanup(null, setSnapshotCleanupRequest);

    // Check if auto snapshot cleanup is disabled
    isSnapshotCleanupEnabledRequest = IsSnapshotCleanupEnabledRequest
        .newBuilder().build();
    isSnapshotCleanupEnabledResponse =
        master.getMasterRpcServices().isSnapshotCleanupEnabled(null,
            isSnapshotCleanupEnabledRequest);
    Assert.assertFalse(isSnapshotCleanupEnabledResponse.getEnabled());
  }

  /**
   * Test that the snapshot hfile archive cleaner works correctly. HFiles that are in snapshots
   * should be retained, while those that are not in a snapshot should be deleted.
   * @throws Exception on failure
   */
  @Test
  public void testSnapshotHFileArchiving() throws Exception {
    Admin admin = UTIL.getAdmin();
    // make sure we don't fail on listing snapshots
    SnapshotTestingUtils.assertNoSnapshots(admin);

    // recreate test table with disabled compactions; otherwise compaction may happen before
    // snapshot, the call after snapshot will be a no-op and checks will fail
    UTIL.deleteTable(TABLE_NAME);
    TableDescriptor td = TableDescriptorBuilder.newBuilder(TABLE_NAME)
            .setColumnFamily(ColumnFamilyDescriptorBuilder.of(TEST_FAM))
            .setCompactionEnabled(false)
            .build();
    UTIL.getAdmin().createTable(td);

    // load the table
    for (int i = 0; i < blockingStoreFiles / 2; i ++) {
      UTIL.loadTable(UTIL.getConnection().getTable(TABLE_NAME), TEST_FAM);
      UTIL.flush(TABLE_NAME);
    }

    // disable the table so we can take a snapshot
    admin.disableTable(TABLE_NAME);

    // take a snapshot of the table
    String snapshotName = "snapshot";
    String snapshotNameBytes = snapshotName;
    admin.snapshot(snapshotName, TABLE_NAME);

    LOG.info("After snapshot File-System state");
    CommonFSUtils.logFileSystemState(fs, rootDir, LOG);

    // ensure we only have one snapshot
    SnapshotTestingUtils.assertOneSnapshotThatMatches(admin, snapshotNameBytes, TABLE_NAME);

    td = TableDescriptorBuilder.newBuilder(td)
            .setCompactionEnabled(true)
            .build();
    // enable compactions now
    admin.modifyTable(td);

    // renable the table so we can compact the regions
    admin.enableTable(TABLE_NAME);

    // compact the files so we get some archived files for the table we just snapshotted
    List<HRegion> regions = UTIL.getHBaseCluster().getRegions(TABLE_NAME);
    for (HRegion region : regions) {
      region.waitForFlushesAndCompactions(); // enable can trigger a compaction, wait for it.
      region.compactStores(); // min is 2 so will compact and archive
    }
    List<RegionServerThread> regionServerThreads = UTIL.getMiniHBaseCluster()
        .getRegionServerThreads();
    HRegionServer hrs = null;
    for (RegionServerThread rs : regionServerThreads) {
      if (!rs.getRegionServer().getRegions(TABLE_NAME).isEmpty()) {
        hrs = rs.getRegionServer();
        break;
      }
    }
    CompactedHFilesDischarger cleaner = new CompactedHFilesDischarger(100, null, hrs, false);
    cleaner.chore();
    LOG.info("After compaction File-System state");
    CommonFSUtils.logFileSystemState(fs, rootDir, LOG);

    // make sure the cleaner has run
    LOG.debug("Running hfile cleaners");
    ensureHFileCleanersRun();
    LOG.info("After cleaners File-System state: " + rootDir);
    CommonFSUtils.logFileSystemState(fs, rootDir, LOG);

    // get the snapshot files for the table
    Path snapshotTable = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);
    Set<String> snapshotHFiles = SnapshotReferenceUtil.getHFileNames(
        UTIL.getConfiguration(), fs, snapshotTable);
    // check that the files in the archive contain the ones that we need for the snapshot
    LOG.debug("Have snapshot hfiles:");
    for (String fileName : snapshotHFiles) {
      LOG.debug(fileName);
    }
    // get the archived files for the table
    Collection<String> archives = getHFiles(archiveDir, fs, TABLE_NAME);

    // get the hfiles for the table
    Collection<String> hfiles = getHFiles(rootDir, fs, TABLE_NAME);

    // and make sure that there is a proper subset
    for (String fileName : snapshotHFiles) {
      boolean exist = archives.contains(fileName) || hfiles.contains(fileName);
      assertTrue("Archived hfiles " + archives
        + " and table hfiles " + hfiles + " is missing snapshot file:" + fileName, exist);
    }

    // delete the existing snapshot
    admin.deleteSnapshot(snapshotNameBytes);
    SnapshotTestingUtils.assertNoSnapshots(admin);

    // make sure that we don't keep around the hfiles that aren't in a snapshot
    // make sure we wait long enough to refresh the snapshot hfile
    List<BaseHFileCleanerDelegate> delegates = UTIL.getMiniHBaseCluster().getMaster()
        .getHFileCleaner().cleanersChain;
    for (BaseHFileCleanerDelegate delegate: delegates) {
      if (delegate instanceof SnapshotHFileCleaner) {
        ((SnapshotHFileCleaner)delegate).getFileCacheForTesting().triggerCacheRefreshForTesting();
      }
    }
    // run the cleaner again
    // In SnapshotFileCache, we use lock.tryLock to avoid being blocked by snapshot operation, if we
    // fail to get the lock, we will not archive any files. This is not a big deal in real world, so
    // here we will try to run the cleaner multiple times to make the test more stable.
    // See HBASE-26861
    UTIL.waitFor(60000, 1000, new ExplainingPredicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        LOG.debug("Running hfile cleaners");
        ensureHFileCleanersRun();
        LOG.info("After delete snapshot cleaners run File-System state");
        CommonFSUtils.logFileSystemState(fs, rootDir, LOG);
        return getHFiles(archiveDir, fs, TABLE_NAME).isEmpty();
      }

      @Override
      public String explainFailure() throws Exception {
        return "Still have some hfiles in the archive, when their snapshot has been deleted: "
          + getHFiles(archiveDir, fs, TABLE_NAME);
      }
    });
  }

  /**
   * @return all the HFiles for a given table in the specified dir
   * @throws IOException on expected failure
   */
  private final Collection<String> getHFiles(Path dir, FileSystem fs, TableName tableName) throws IOException {
    Path tableDir = CommonFSUtils.getTableDir(dir, tableName);
    return SnapshotTestingUtils.listHFileNames(fs, tableDir);
  }

  /**
   * Make sure the {@link HFileCleaner HFileCleaners} run at least once
   */
  private static void ensureHFileCleanersRun() {
    UTIL.getHBaseCluster().getMaster().getHFileCleaner().chore();
  }

  private SnapshotDescription createSnapshot(final String snapshotName) throws IOException {
    SnapshotTestingUtils.SnapshotMock snapshotMock =
      new SnapshotTestingUtils.SnapshotMock(UTIL.getConfiguration(), fs, rootDir);
    SnapshotTestingUtils.SnapshotMock.SnapshotBuilder builder =
      snapshotMock.createSnapshotV2(snapshotName, "test", 0);
    builder.commit();
    return builder.getSnapshotDescription();
  }

  private SnapshotDescription createSnapshotWithTtl(final String snapshotName, final long ttl)
      throws IOException {
    SnapshotTestingUtils.SnapshotMock snapshotMock =
        new SnapshotTestingUtils.SnapshotMock(UTIL.getConfiguration(), fs, rootDir);
    SnapshotTestingUtils.SnapshotMock.SnapshotBuilder builder =
        snapshotMock.createSnapshotV2(snapshotName, "test", 0, ttl);
    builder.commit();
    return builder.getSnapshotDescription();
  }

  @Test
  public void testAsyncSnapshotWillNotBlockSnapshotHFileCleaner() throws Exception {
    // Write some data
    Table table = UTIL.getConnection().getTable(TABLE_NAME);
    for (int i = 0; i < 10; i++) {
      Put put = new Put(Bytes.toBytes(i)).addColumn(TEST_FAM, Bytes.toBytes("q"), Bytes.toBytes(i));
      table.put(put);
    }
    String snapshotName = "testAsyncSnapshotWillNotBlockSnapshotHFileCleaner01";
    Future<Void> future =
      UTIL.getAdmin().snapshotAsync(new org.apache.hadoop.hbase.client.SnapshotDescription(
        snapshotName, TABLE_NAME, SnapshotType.FLUSH));
    Waiter.waitFor(UTIL.getConfiguration(), 10 * 1000L, 200L,
      () -> UTIL.getAdmin().listSnapshots(Pattern.compile(snapshotName)).size() == 1);
    UTIL.waitFor(30000, () -> !master.getSnapshotManager().isTakingAnySnapshot());
  }
}
