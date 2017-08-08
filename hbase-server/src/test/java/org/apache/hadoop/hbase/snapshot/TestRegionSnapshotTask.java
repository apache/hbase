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
package org.apache.hadoop.hbase.snapshot;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.regionserver.snapshot.FlushSnapshotSubprocedure;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

/**
 * Testing the region snapshot task on a cluster.
 * @see org.apache.hadoop.hbase.regionserver.snapshot.FlushSnapshotSubprocedure.RegionSnapshotTask
 */
@Category({ MediumTests.class, RegionServerTests.class})
public class TestRegionSnapshotTask {
  private final Log LOG = LogFactory.getLog(getClass());

  private static HBaseTestingUtility TEST_UTIL;
  private static Configuration conf;
  private static FileSystem fs;
  private static Path rootDir;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();

    conf = TEST_UTIL.getConfiguration();

    // Try to frequently clean up compacted files
    conf.setInt("hbase.hfile.compaction.discharger.interval", 1000);
    conf.setInt("hbase.master.hfilecleaner.ttl", 1000);

    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.getHBaseCluster().waitForActiveAndReadyMaster();
    TEST_UTIL.waitUntilAllRegionsAssigned(TableName.META_TABLE_NAME);

    rootDir = FSUtils.getRootDir(conf);
    fs = TEST_UTIL.getTestFileSystem();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Tests adding a region to the snapshot manifest while compactions are running on the region.
   * The idea is to slow down the process of adding a store file to the manifest while
   * triggering compactions on the region, allowing the store files to be marked for archival while
   * snapshot operation is running.
   * This test checks for the correct behavior in such a case that the compacted files should
   * not be moved around if a snapshot operation is in progress.
   * See HBASE-18398
   */
  @Test(timeout = 30000)
  public void testAddRegionWithCompactions() throws Exception {
    final TableName tableName = TableName.valueOf("test_table");
    Table table = setupTable(tableName);

    List<HRegion> hRegions = TEST_UTIL.getHBaseCluster().getRegions(tableName);

    final HBaseProtos.SnapshotDescription snapshot = HBaseProtos.SnapshotDescription.newBuilder()
        .setTable(tableName.getNameAsString())
        .setType(HBaseProtos.SnapshotDescription.Type.FLUSH)
        .setName("test_table_snapshot")
        .setVersion(SnapshotManifestV2.DESCRIPTOR_VERSION)
        .build();
    ForeignExceptionDispatcher monitor = new ForeignExceptionDispatcher(snapshot.getName());

    final HRegion region = spy(hRegions.get(0));

    Path workingDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(snapshot, rootDir);
    final SnapshotManifest manifest =
        SnapshotManifest.create(conf, fs, workingDir, snapshot, monitor);
    manifest.addTableDescriptor(table.getTableDescriptor());

    if (!fs.exists(workingDir)) {
      fs.mkdirs(workingDir);
    }
    assertTrue(fs.exists(workingDir));
    SnapshotDescriptionUtils.writeSnapshotInfo(snapshot, workingDir, fs);

    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
        addRegionToSnapshot(snapshot, region, manifest);
        return null;
      }
    }).when(region).addRegionToSnapshot(snapshot, monitor);

    FlushSnapshotSubprocedure.RegionSnapshotTask snapshotTask =
        new FlushSnapshotSubprocedure.RegionSnapshotTask(region, snapshot, true, monitor);
    ExecutorService executor = Executors.newFixedThreadPool(1);
    Future f = executor.submit(snapshotTask);

    // Trigger major compaction and wait for snaphot operation to finish
    LOG.info("Starting major compaction");
    region.compact(true);
    LOG.info("Finished major compaction");
    f.get();

    // Consolidate region manifests into a single snapshot manifest
    manifest.consolidate();

    // Make sure that the region manifest exists, which means the snapshot operation succeeded
    assertNotNull(manifest.getRegionManifests());
    // Sanity check, there should be only one region
    assertEquals(1, manifest.getRegionManifests().size());

    // Make sure that no files went missing after the snapshot operation
    SnapshotReferenceUtil.verifySnapshot(conf, fs, manifest);
  }

  private void addRegionToSnapshot(HBaseProtos.SnapshotDescription snapshot,
      HRegion region, SnapshotManifest manifest) throws Exception {
    LOG.info("Adding region to snapshot: " + region.getRegionInfo().getRegionNameAsString());
    Path workingDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(snapshot, rootDir);
    SnapshotManifest.RegionVisitor visitor = createRegionVisitorWithDelay(snapshot, workingDir);
    manifest.addRegion(region, visitor);
    LOG.info("Added the region to snapshot: " + region.getRegionInfo().getRegionNameAsString());
  }

  private SnapshotManifest.RegionVisitor createRegionVisitorWithDelay(
      HBaseProtos.SnapshotDescription desc, Path workingDir) {
    return new SnapshotManifestV2.ManifestBuilder(conf, fs, workingDir) {
      @Override
      public void storeFile(final SnapshotProtos.SnapshotRegionManifest.Builder region,
          final SnapshotProtos.SnapshotRegionManifest.FamilyFiles.Builder family,
          final StoreFileInfo storeFile) throws IOException {
        try {
          LOG.debug("Introducing delay before adding store file to manifest");
          Thread.sleep(2000);
        } catch (InterruptedException ex) {
          LOG.error("Interrupted due to error: " + ex);
        }
        super.storeFile(region, family, storeFile);
      }
    };
  }

  private Table setupTable(TableName tableName) throws Exception {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    // Flush many files, but do not compact immediately
    htd.setMemStoreFlushSize(5000).setConfiguration("hbase.hstore.compactionThreshold", "250");
    // Make sure the region does not split
    htd.setRegionSplitPolicyClassName(ConstantSizeRegionSplitPolicy.class.getName());
    htd.setMaxFileSize(100 * 1024 * 1024);

    byte[] fam = Bytes.toBytes("fam");
    Table table = TEST_UTIL.createTable(htd, new byte[][] {fam},
        TEST_UTIL.getConfiguration());
    TEST_UTIL.loadTable(table, fam);
    return table;
  }
}
