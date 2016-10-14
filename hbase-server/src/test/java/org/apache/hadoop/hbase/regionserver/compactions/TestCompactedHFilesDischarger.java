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
package org.apache.hadoop.hbase.regionserver.compactions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.CompactedHFilesDischarger;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MediumTests.class, RegionServerTests.class })
public class TestCompactedHFilesDischarger {
  private final HBaseTestingUtility testUtil = new HBaseTestingUtility();
  private Region region;
  private final static byte[] fam = Bytes.toBytes("cf_1");
  private final static byte[] qual1 = Bytes.toBytes("qf_1");
  private final static byte[] val = Bytes.toBytes("val");
  private static CountDownLatch latch = new CountDownLatch(3);
  private static AtomicInteger counter = new AtomicInteger(0);
  private static AtomicInteger scanCompletedCounter = new AtomicInteger(0);
  private RegionServerServices rss;

  @Before
  public void setUp() throws Exception {
    TableName tableName = TableName.valueOf(getClass().getSimpleName());
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor(fam));
    HRegionInfo info = new HRegionInfo(tableName, null, null, false);
    Path path = testUtil.getDataTestDir(getClass().getSimpleName());
//    region = HBaseTestingUtility.createRegionAndWAL(info, path, testUtil.getConfiguration(), htd);
    rss = mock(RegionServerServices.class);
    List<Region> regions = new ArrayList<Region>();
    regions.add(region);
    when(rss.getOnlineRegions()).thenReturn(regions);
  }

  @After
  public void tearDown() throws IOException {
    counter.set(0);
    scanCompletedCounter.set(0);
    latch = new CountDownLatch(3);
    HBaseTestingUtility.closeRegionAndWAL(region);
    testUtil.cleanupTestDir();
  }

  @Test
  public void testCompactedHFilesCleaner() throws Exception {
    // Create the cleaner object
    CompactedHFilesDischarger cleaner =
        new CompactedHFilesDischarger(1000, (Stoppable) null, rss, false);
    // Add some data to the region and do some flushes
    for (int i = 1; i < 10; i++) {
      Put p = new Put(Bytes.toBytes("row" + i));
      p.addColumn(fam, qual1, val);
      region.put(p);
    }
    // flush them
    region.flush(true);
    for (int i = 11; i < 20; i++) {
      Put p = new Put(Bytes.toBytes("row" + i));
      p.addColumn(fam, qual1, val);
      region.put(p);
    }
    // flush them
    region.flush(true);
    for (int i = 21; i < 30; i++) {
      Put p = new Put(Bytes.toBytes("row" + i));
      p.addColumn(fam, qual1, val);
      region.put(p);
    }
    // flush them
    region.flush(true);

    Store store = region.getStore(fam);
    assertEquals(3, store.getStorefilesCount());

    Collection<StoreFile> storefiles = store.getStorefiles();
    Collection<StoreFile> compactedfiles =
        ((HStore) store).getStoreEngine().getStoreFileManager().getCompactedfiles();
    // None of the files should be in compacted state.
    for (StoreFile file : storefiles) {
      assertFalse(file.isCompactedAway());
    }
    // Try to run the cleaner without compaction. there should not be any change
    cleaner.chore();
    storefiles = store.getStorefiles();
    // None of the files should be in compacted state.
    for (StoreFile file : storefiles) {
      assertFalse(file.isCompactedAway());
    }
    // now do some compaction
    region.compact(true);
    // Still the flushed files should be present until the cleaner runs. But the state of it should
    // be in COMPACTED state
    assertEquals(1, store.getStorefilesCount());
    assertEquals(3,
      ((HStore) store).getStoreEngine().getStoreFileManager().getCompactedfiles().size());

    // Run the cleaner
    cleaner.chore();
    assertEquals(1, store.getStorefilesCount());
    storefiles = store.getStorefiles();
    for (StoreFile file : storefiles) {
      // Should not be in compacted state
      assertFalse(file.isCompactedAway());
    }
    compactedfiles = ((HStore) store).getStoreEngine().getStoreFileManager().getCompactedfiles();
    assertTrue(compactedfiles.size() == 0);
    
  }

  @Test
  public void testCleanerWithParallelScannersAfterCompaction() throws Exception {
    // Create the cleaner object
    CompactedHFilesDischarger cleaner =
        new CompactedHFilesDischarger(1000, (Stoppable) null, rss, false);
    // Add some data to the region and do some flushes
    for (int i = 1; i < 10; i++) {
      Put p = new Put(Bytes.toBytes("row" + i));
      p.addColumn(fam, qual1, val);
      region.put(p);
    }
    // flush them
    region.flush(true);
    for (int i = 11; i < 20; i++) {
      Put p = new Put(Bytes.toBytes("row" + i));
      p.addColumn(fam, qual1, val);
      region.put(p);
    }
    // flush them
    region.flush(true);
    for (int i = 21; i < 30; i++) {
      Put p = new Put(Bytes.toBytes("row" + i));
      p.addColumn(fam, qual1, val);
      region.put(p);
    }
    // flush them
    region.flush(true);

    Store store = region.getStore(fam);
    assertEquals(3, store.getStorefilesCount());

    Collection<StoreFile> storefiles = store.getStorefiles();
    Collection<StoreFile> compactedfiles =
        ((HStore) store).getStoreEngine().getStoreFileManager().getCompactedfiles();
    // None of the files should be in compacted state.
    for (StoreFile file : storefiles) {
      assertFalse(file.isCompactedAway());
    }
    // Do compaction
    region.compact(true);
    startScannerThreads();

    storefiles = store.getStorefiles();
    int usedReaderCount = 0;
    int unusedReaderCount = 0;
    for (StoreFile file : storefiles) {
      if (file.getRefCount() == 3) {
        usedReaderCount++;
      }
    }
    compactedfiles = ((HStore) store).getStoreEngine().getStoreFileManager().getCompactedfiles();
    for(StoreFile file : compactedfiles) {
      assertEquals("Refcount should be 3", 0, file.getRefCount());
      unusedReaderCount++;
    }
    // Though there are files we are not using them for reads
    assertEquals("unused reader count should be 3", 3, unusedReaderCount);
    assertEquals("used reader count should be 1", 1, usedReaderCount);
    // now run the cleaner
    cleaner.chore();
    countDown();
    assertEquals(1, store.getStorefilesCount());
    storefiles = store.getStorefiles();
    for (StoreFile file : storefiles) {
      // Should not be in compacted state
      assertFalse(file.isCompactedAway());
    }
    compactedfiles = ((HStore) store).getStoreEngine().getStoreFileManager().getCompactedfiles();
    assertTrue(compactedfiles.size() == 0);
  }

  @Test
  public void testCleanerWithParallelScanners() throws Exception {
    // Create the cleaner object
    CompactedHFilesDischarger cleaner =
        new CompactedHFilesDischarger(1000, (Stoppable) null, rss, false);
    // Add some data to the region and do some flushes
    for (int i = 1; i < 10; i++) {
      Put p = new Put(Bytes.toBytes("row" + i));
      p.addColumn(fam, qual1, val);
      region.put(p);
    }
    // flush them
    region.flush(true);
    for (int i = 11; i < 20; i++) {
      Put p = new Put(Bytes.toBytes("row" + i));
      p.addColumn(fam, qual1, val);
      region.put(p);
    }
    // flush them
    region.flush(true);
    for (int i = 21; i < 30; i++) {
      Put p = new Put(Bytes.toBytes("row" + i));
      p.addColumn(fam, qual1, val);
      region.put(p);
    }
    // flush them
    region.flush(true);

    Store store = region.getStore(fam);
    assertEquals(3, store.getStorefilesCount());

    Collection<StoreFile> storefiles = store.getStorefiles();
    Collection<StoreFile> compactedfiles =
        ((HStore) store).getStoreEngine().getStoreFileManager().getCompactedfiles();
    // None of the files should be in compacted state.
    for (StoreFile file : storefiles) {
      assertFalse(file.isCompactedAway());
    }
    startScannerThreads();
    // Do compaction
    region.compact(true);

    storefiles = store.getStorefiles();
    int usedReaderCount = 0;
    int unusedReaderCount = 0;
    for (StoreFile file : storefiles) {
      if (file.getRefCount() == 0) {
        unusedReaderCount++;
      }
    }
    compactedfiles =
        ((HStore) store).getStoreEngine().getStoreFileManager().getCompactedfiles();
    for(StoreFile file : compactedfiles) {
      assertEquals("Refcount should be 3", 3, file.getRefCount());
      usedReaderCount++;
    }
    // The newly compacted file will not be used by any scanner
    assertEquals("unused reader count should be 1", 1, unusedReaderCount);
    assertEquals("used reader count should be 3", 3, usedReaderCount);
    // now run the cleaner
    cleaner.chore();
    countDown();
    // No change in the number of store files as none of the compacted files could be cleaned up
    assertEquals(1, store.getStorefilesCount());
    assertEquals(3,
      ((HStore) store).getStoreEngine().getStoreFileManager().getCompactedfiles().size());
    while (scanCompletedCounter.get() != 3) {
      Thread.sleep(100);
    }
    // reset
    latch = new CountDownLatch(3);
    scanCompletedCounter.set(0);
    counter.set(0);
    // Try creating a new scanner and it should use only the new file created after compaction
    startScannerThreads();
    storefiles = store.getStorefiles();
    usedReaderCount = 0;
    unusedReaderCount = 0;
    for (StoreFile file : storefiles) {
      if (file.getRefCount() == 3) {
        usedReaderCount++;
      }
    }
    compactedfiles = ((HStore) store).getStoreEngine().getStoreFileManager().getCompactedfiles();
    for(StoreFile file : compactedfiles) {
      assertEquals("Refcount should be 0", 0, file.getRefCount());
      unusedReaderCount++;
    }
    // Though there are files we are not using them for reads
    assertEquals("unused reader count should be 3", 3, unusedReaderCount);
    assertEquals("used reader count should be 1", 1, usedReaderCount);
    countDown();
    while (scanCompletedCounter.get() != 3) {
      Thread.sleep(100);
    }
    // Run the cleaner again
    cleaner.chore();
    // Now the cleaner should be able to clear it up because there are no active readers
    assertEquals(1, store.getStorefilesCount());
    storefiles = store.getStorefiles();
    for (StoreFile file : storefiles) {
      // Should not be in compacted state
      assertFalse(file.isCompactedAway());
    }
    compactedfiles = ((HStore) store).getStoreEngine().getStoreFileManager().getCompactedfiles();
    assertTrue(compactedfiles.size() == 0);
  }

  protected void countDown() {
    // count down 3 times
    latch.countDown();
    latch.countDown();
    latch.countDown();
  }

  protected void startScannerThreads() throws InterruptedException {
    // Start parallel scan threads
    ScanThread[] scanThreads = new ScanThread[3];
    for (int i = 0; i < 3; i++) {
      scanThreads[i] = new ScanThread((HRegion) region);
    }
    for (ScanThread thread : scanThreads) {
      thread.start();
    }
    while (counter.get() != 3) {
      Thread.sleep(100);
    }
  }

  private static class ScanThread extends Thread {
    private final HRegion region;

    public ScanThread(HRegion region) {
      this.region = region;
    }

    @Override
    public void run() {
      try {
        initiateScan(region);
      } catch (IOException e) {
        // do nothing
      }
    }

    private void initiateScan(HRegion region) throws IOException {
      Scan scan = new Scan();
      scan.setCaching(1);
      RegionScanner resScanner = null;
      try {
        resScanner = region.getScanner(scan);
        List<Cell> results = new ArrayList<Cell>();
        boolean next = resScanner.next(results);
        try {
          counter.incrementAndGet();
          latch.await();
        } catch (InterruptedException e) {
        }
        while (!next) {
          resScanner.next(results);
        }
      } finally {
        scanCompletedCounter.incrementAndGet();
        resScanner.close();
      }
    }
  }
}
