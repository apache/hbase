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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.Mockito;

/**
 * Tests a race condition between archiving of compacted files in CompactedHFilesDischarger chore
 * and HRegion.close();
 */
@Category({RegionServerTests.class, SmallTests.class})
public class TestCompactionArchiveConcurrentClose {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCompactionArchiveConcurrentClose.class);

  private HBaseTestingUtility testUtil;

  private Path testDir;
  private AtomicBoolean archived = new AtomicBoolean();

  @Rule
  public TestName name = new TestName();

  @Before
  public void setup() throws Exception {
    testUtil = HBaseTestingUtility.createLocalHTU();
    testDir = testUtil.getDataTestDir("TestStoreFileRefresherChore");
    CommonFSUtils.setRootDir(testUtil.getConfiguration(), testDir);
  }

  @After
  public void tearDown() throws Exception {
    testUtil.cleanupTestDir();
  }

  @Test
  public void testStoreCloseAndDischargeRunningInParallel() throws Exception {
    byte[] fam = Bytes.toBytes("f");
    byte[] col = Bytes.toBytes("c");
    byte[] val = Bytes.toBytes("val");

    TableName tableName = TableName.valueOf(name.getMethodName());
    TableDescriptor htd = TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(fam)).build();
    RegionInfo info = RegionInfoBuilder.newBuilder(tableName).build();
    HRegion region = initHRegion(htd, info);
    RegionServerServices rss = mock(RegionServerServices.class);
    List<HRegion> regions = new ArrayList<>();
    regions.add(region);
    Mockito.doReturn(regions).when(rss).getRegions();

    // Create the cleaner object
    CompactedHFilesDischarger cleaner =
        new CompactedHFilesDischarger(1000, (Stoppable) null, rss, false);
    // Add some data to the region and do some flushes
    int batchSize = 10;
    int fileCount = 10;
    for (int f = 0; f < fileCount; f++) {
      int start = f * batchSize;
      for (int i = start; i < start + batchSize; i++) {
        Put p = new Put(Bytes.toBytes("row" + i));
        p.addColumn(fam, col, val);
        region.put(p);
      }
      // flush them
      region.flush(true);
    }

    HStore store = region.getStore(fam);
    assertEquals(fileCount, store.getStorefilesCount());

    Collection<HStoreFile> storefiles = store.getStorefiles();
    // None of the files should be in compacted state.
    for (HStoreFile file : storefiles) {
      assertFalse(file.isCompactedAway());
    }
    // Do compaction
    region.compact(true);

    // now run the cleaner with a concurrent close
    Thread cleanerThread = new Thread() {
      @Override
      public void run() {
        cleaner.chore();
      }
    };
    cleanerThread.start();
    // wait for cleaner to pause
    synchronized (archived) {
      if (!archived.get()) {
        archived.wait();
      }
    }
    final AtomicReference<Exception> closeException = new AtomicReference<>();
    Thread closeThread = new Thread() {
      @Override
      public void run() {
        // wait for the chore to complete and call close
        try {
          ((HRegion) region).close();
        } catch (IOException e) {
          closeException.set(e);
        }
      }
    };
    closeThread.start();
    // no error should occur after the execution of the test
    closeThread.join();
    cleanerThread.join();

    if (closeException.get() != null) {
      throw closeException.get();
    }
  }

  private HRegion initHRegion(TableDescriptor htd, RegionInfo info) throws IOException {
    Configuration conf = testUtil.getConfiguration();
    Path tableDir = CommonFSUtils.getTableDir(testDir, htd.getTableName());

    HRegionFileSystem fs =
        new WaitingHRegionFileSystem(conf, tableDir.getFileSystem(conf), tableDir, info);
    ChunkCreator.initialize(MemStoreLABImpl.CHUNK_SIZE_DEFAULT, false, 0, 0, 0, null);
    final Configuration walConf = new Configuration(conf);
    CommonFSUtils.setRootDir(walConf, tableDir);
    final WALFactory wals = new WALFactory(walConf, "log_" + info.getEncodedName());
    HRegion region = new HRegion(fs, wals.getWAL(info), conf, htd, null);

    region.initialize();

    return region;
  }

  private class WaitingHRegionFileSystem extends HRegionFileSystem {

    public WaitingHRegionFileSystem(final Configuration conf, final FileSystem fs,
        final Path tableDir, final RegionInfo regionInfo) {
      super(conf, fs, tableDir, regionInfo);
    }

    @Override
    public void removeStoreFiles(String familyName, Collection<HStoreFile> storeFiles)
        throws IOException {
      super.removeStoreFiles(familyName, storeFiles);
      archived.set(true);
      synchronized (archived) {
        archived.notifyAll();
      }
      try {
        // unfortunately we can't use a stronger barrier here as the fix synchronizing
        // the race condition will then block
        Thread.sleep(100);
      } catch (InterruptedException ie) {
        throw new InterruptedIOException("Interrupted waiting for latch");
      }
    }
  }
}
