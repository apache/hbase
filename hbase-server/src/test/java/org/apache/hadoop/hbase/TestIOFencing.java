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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.regionserver.CompactingMemStore;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.regionserver.wal.WALUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.wal.WAL;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.CompactionDescriptor;

/**
 * Test for the case where a regionserver going down has enough cycles to do damage to regions that
 * have actually been assigned elsehwere.
 * <p>
 * If we happen to assign a region before it fully done with in its old location -- i.e. it is on
 * two servers at the same time -- all can work fine until the case where the region on the dying
 * server decides to compact or otherwise change the region file set. The region in its new location
 * will then get a surprise when it tries to do something w/ a file removed by the region in its old
 * location on dying server.
 * <p>
 * Making a test for this case is a little tough in that even if a file is deleted up on the
 * namenode, if the file was opened before the delete, it will continue to let reads happen until
 * something changes the state of cached blocks in the dfsclient that was already open (a block from
 * the deleted file is cleaned from the datanode by NN).
 * <p>
 * What we will do below is do an explicit check for existence on the files listed in the region
 * that has had some files removed because of a compaction. This sort of hurry's along and makes
 * certain what is a chance occurance.
 */
@Category({MiscTests.class, LargeTests.class})
public class TestIOFencing {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestIOFencing.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestIOFencing.class);
  static {
    // Uncomment the following lines if more verbosity is needed for
    // debugging (see HBASE-12285 for details).
    //((Log4JLogger)FSNamesystem.LOG).getLogger().setLevel(Level.ALL);
    //((Log4JLogger)DataNode.LOG).getLogger().setLevel(Level.ALL);
    //((Log4JLogger)LeaseManager.LOG).getLogger().setLevel(Level.ALL);
    //((Log4JLogger)LogFactory.getLog("org.apache.hadoop.hdfs.server.namenode.FSNamesystem"))
    //    .getLogger().setLevel(Level.ALL);
    //((Log4JLogger)DFSClient.LOG).getLogger().setLevel(Level.ALL);
  }

  public abstract static class CompactionBlockerRegion extends HRegion {
    AtomicInteger compactCount = new AtomicInteger();
    volatile CountDownLatch compactionsBlocked = new CountDownLatch(0);
    volatile CountDownLatch compactionsWaiting = new CountDownLatch(0);

    @SuppressWarnings("deprecation")
    public CompactionBlockerRegion(Path tableDir, WAL log,
        FileSystem fs, Configuration confParam, RegionInfo info,
        TableDescriptor htd, RegionServerServices rsServices) {
      super(tableDir, log, fs, confParam, info, htd, rsServices);
    }

    public void stopCompactions() {
      compactionsBlocked = new CountDownLatch(1);
      compactionsWaiting = new CountDownLatch(1);
    }

    public void allowCompactions() {
      LOG.debug("allowing compactions");
      compactionsBlocked.countDown();
    }
    public void waitForCompactionToBlock() throws IOException {
      try {
        LOG.debug("waiting for compaction to block");
        compactionsWaiting.await();
        LOG.debug("compaction block reached");
      } catch (InterruptedException ex) {
        throw new IOException(ex);
      }
    }

    @Override
    public boolean compact(CompactionContext compaction, HStore store,
        ThroughputController throughputController) throws IOException {
      try {
        return super.compact(compaction, store, throughputController);
      } finally {
        compactCount.getAndIncrement();
      }
    }

    @Override
    public boolean compact(CompactionContext compaction, HStore store,
        ThroughputController throughputController, User user) throws IOException {
      try {
        return super.compact(compaction, store, throughputController, user);
      } finally {
        compactCount.getAndIncrement();
      }
    }

    public int countStoreFiles() {
      int count = 0;
      for (HStore store : stores.values()) {
        count += store.getStorefilesCount();
      }
      return count;
    }
  }

  /**
   * An override of HRegion that allows us park compactions in a holding pattern and
   * then when appropriate for the test, allow them proceed again.
   */
  public static class BlockCompactionsInPrepRegion extends CompactionBlockerRegion {

    public BlockCompactionsInPrepRegion(Path tableDir, WAL log,
        FileSystem fs, Configuration confParam, RegionInfo info,
        TableDescriptor htd, RegionServerServices rsServices) {
      super(tableDir, log, fs, confParam, info, htd, rsServices);
    }
    @Override
    protected void doRegionCompactionPrep() throws IOException {
      compactionsWaiting.countDown();
      try {
        compactionsBlocked.await();
      } catch (InterruptedException ex) {
        throw new IOException();
      }
      super.doRegionCompactionPrep();
    }
  }

  /**
   * An override of HRegion that allows us park compactions in a holding pattern and
   * then when appropriate for the test, allow them proceed again. This allows the compaction
   * entry to go the WAL before blocking, but blocks afterwards
   */
  public static class BlockCompactionsInCompletionRegion extends CompactionBlockerRegion {
    public BlockCompactionsInCompletionRegion(Path tableDir, WAL log,
        FileSystem fs, Configuration confParam, RegionInfo info,
        TableDescriptor htd, RegionServerServices rsServices) {
      super(tableDir, log, fs, confParam, info, htd, rsServices);
    }
    @Override
    protected HStore instantiateHStore(final ColumnFamilyDescriptor family) throws IOException {
      return new BlockCompactionsInCompletionHStore(this, family, this.conf);
    }
  }

  public static class BlockCompactionsInCompletionHStore extends HStore {
    CompactionBlockerRegion r;
    protected BlockCompactionsInCompletionHStore(HRegion region, ColumnFamilyDescriptor family,
        Configuration confParam) throws IOException {
      super(region, family, confParam);
      r = (CompactionBlockerRegion) region;
    }

    @Override
    protected void completeCompaction(Collection<HStoreFile> compactedFiles) throws IOException {
      try {
        r.compactionsWaiting.countDown();
        r.compactionsBlocked.await();
      } catch (InterruptedException ex) {
        throw new IOException(ex);
      }
      super.completeCompaction(compactedFiles);
    }
  }

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static TableName TABLE_NAME =
      TableName.valueOf("tabletest");
  private final static byte[] FAMILY = Bytes.toBytes("family");
  private static final int FIRST_BATCH_COUNT = 4000;
  private static final int SECOND_BATCH_COUNT = FIRST_BATCH_COUNT;

  /**
   * Test that puts up a regionserver, starts a compaction on a loaded region but holds the
   * compaction until after we have killed the server and the region has come up on
   * a new regionserver altogether.  This fakes the double assignment case where region in one
   * location changes the files out from underneath a region being served elsewhere.
   */
  @Test
  public void testFencingAroundCompaction() throws Exception {
    for(MemoryCompactionPolicy policy : MemoryCompactionPolicy.values()) {
      doTest(BlockCompactionsInPrepRegion.class, policy);
    }
  }

  /**
   * Test that puts up a regionserver, starts a compaction on a loaded region but holds the
   * compaction completion until after we have killed the server and the region has come up on
   * a new regionserver altogether.  This fakes the double assignment case where region in one
   * location changes the files out from underneath a region being served elsewhere.
   */
  @Test
  public void testFencingAroundCompactionAfterWALSync() throws Exception {
    for(MemoryCompactionPolicy policy : MemoryCompactionPolicy.values()) {
      doTest(BlockCompactionsInCompletionRegion.class, policy);
    }
  }

  public void doTest(Class<?> regionClass, MemoryCompactionPolicy policy) throws Exception {
    Configuration c = TEST_UTIL.getConfiguration();
    // Insert our custom region
    c.setClass(HConstants.REGION_IMPL, regionClass, HRegion.class);
    // Encourage plenty of flushes
    c.setLong("hbase.hregion.memstore.flush.size", 25000);
    c.set(HConstants.HBASE_REGION_SPLIT_POLICY_KEY, ConstantSizeRegionSplitPolicy.class.getName());
    // Only run compaction when we tell it to
    c.setInt("hbase.hstore.compaction.min",1);
    c.setInt("hbase.hstore.compactionThreshold", 1000);
    c.setLong("hbase.hstore.blockingStoreFiles", 1000);
    // Compact quickly after we tell it to!
    c.setInt("hbase.regionserver.thread.splitcompactcheckfrequency", 1000);
    c.set(CompactingMemStore.COMPACTING_MEMSTORE_TYPE_KEY, String.valueOf(policy));
    LOG.info("Starting mini cluster");
    TEST_UTIL.startMiniCluster(1);
    CompactionBlockerRegion compactingRegion = null;
    Admin admin = null;
    try {
      LOG.info("Creating admin");
      admin = TEST_UTIL.getConnection().getAdmin();
      LOG.info("Creating table");
      TEST_UTIL.createTable(TABLE_NAME, FAMILY);
      Table table = TEST_UTIL.getConnection().getTable(TABLE_NAME);
      LOG.info("Loading test table");
      // Find the region
      List<HRegion> testRegions = TEST_UTIL.getMiniHBaseCluster().findRegionsForTable(TABLE_NAME);
      assertEquals(1, testRegions.size());
      compactingRegion = (CompactionBlockerRegion)testRegions.get(0);
      LOG.info("Blocking compactions");
      compactingRegion.stopCompactions();
      long lastFlushTime = compactingRegion.getEarliestFlushTimeForAllStores();
      // Load some rows
      TEST_UTIL.loadNumericRows(table, FAMILY, 0, FIRST_BATCH_COUNT);

      // add a compaction from an older (non-existing) region to see whether we successfully skip
      // those entries
      HRegionInfo oldHri = new HRegionInfo(table.getName(),
        HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
      CompactionDescriptor compactionDescriptor = ProtobufUtil.toCompactionDescriptor(oldHri,
        FAMILY, Lists.newArrayList(new Path("/a")), Lists.newArrayList(new Path("/b")),
        new Path("store_dir"));
      WALUtil.writeCompactionMarker(compactingRegion.getWAL(),
          ((HRegion)compactingRegion).getReplicationScope(),
        oldHri, compactionDescriptor, compactingRegion.getMVCC());

      // Wait till flush has happened, otherwise there won't be multiple store files
      long startWaitTime = System.currentTimeMillis();
      while (compactingRegion.getEarliestFlushTimeForAllStores() <= lastFlushTime ||
          compactingRegion.countStoreFiles() <= 1) {
        LOG.info("Waiting for the region to flush " +
          compactingRegion.getRegionInfo().getRegionNameAsString());
        Thread.sleep(1000);
        admin.flush(table.getName());
        assertTrue("Timed out waiting for the region to flush",
          System.currentTimeMillis() - startWaitTime < 30000);
      }
      assertTrue(compactingRegion.countStoreFiles() > 1);
      final byte REGION_NAME[] = compactingRegion.getRegionInfo().getRegionName();
      LOG.info("Asking for compaction");
      admin.majorCompact(TABLE_NAME);
      LOG.info("Waiting for compaction to be about to start");
      compactingRegion.waitForCompactionToBlock();
      LOG.info("Starting a new server");
      RegionServerThread newServerThread = TEST_UTIL.getMiniHBaseCluster().startRegionServer();
      final HRegionServer newServer = newServerThread.getRegionServer();
      LOG.info("Killing region server ZK lease");
      TEST_UTIL.expireRegionServerSession(0);
      CompactionBlockerRegion newRegion = null;
      startWaitTime = System.currentTimeMillis();
      LOG.info("Waiting for the new server to pick up the region " + Bytes.toString(REGION_NAME));

      // wait for region to be assigned and to go out of log replay if applicable
      Waiter.waitFor(c, 60000, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          Region newRegion = newServer.getOnlineRegion(REGION_NAME);
          return newRegion != null;
        }
      });

      newRegion = (CompactionBlockerRegion)newServer.getOnlineRegion(REGION_NAME);

      // After compaction of old region finishes on the server that was going down, make sure that
      // all the files we expect are still working when region is up in new location.
      FileSystem fs = newRegion.getFilesystem();
      for (String f: newRegion.getStoreFileList(new byte [][] {FAMILY})) {
        assertTrue("After compaction, does not exist: " + f, fs.exists(new Path(f)));
      }
      LOG.info("Allowing compaction to proceed");
      compactingRegion.allowCompactions();
      while (compactingRegion.compactCount.get() == 0) {
        Thread.sleep(1000);
      }
      // The server we killed stays up until the compaction that was started before it was killed
      // completes. In logs you should see the old regionserver now going down.
      LOG.info("Compaction finished");

      // If we survive the split keep going...
      // Now we make sure that the region isn't totally confused.  Load up more rows.
      TEST_UTIL.loadNumericRows(table, FAMILY, FIRST_BATCH_COUNT,
        FIRST_BATCH_COUNT + SECOND_BATCH_COUNT);
      admin.majorCompact(TABLE_NAME);
      startWaitTime = System.currentTimeMillis();
      while (newRegion.compactCount.get() == 0) {
        Thread.sleep(1000);
        assertTrue("New region never compacted",
          System.currentTimeMillis() - startWaitTime < 180000);
      }
      int count;
      for (int i = 0;; i++) {
        try {
          count = TEST_UTIL.countRows(table);
          break;
        } catch (DoNotRetryIOException e) {
          // wait up to 30s
          if (i >= 30 || !e.getMessage().contains("File does not exist")) {
            throw e;
          }
          Thread.sleep(1000);
        }
      }
      if (policy == MemoryCompactionPolicy.EAGER || policy == MemoryCompactionPolicy.ADAPTIVE) {
        assertTrue(FIRST_BATCH_COUNT + SECOND_BATCH_COUNT >= count);
      } else {
        assertEquals(FIRST_BATCH_COUNT + SECOND_BATCH_COUNT, count);
      }
    } finally {
      if (compactingRegion != null) {
        compactingRegion.allowCompactions();
      }
      admin.close();
      TEST_UTIL.shutdownMiniCluster();
    }
  }
}
