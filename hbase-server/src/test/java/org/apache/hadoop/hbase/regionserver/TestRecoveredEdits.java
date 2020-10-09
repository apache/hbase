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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MemoryCompactionPolicy;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.hfile.BlockCache;
import org.apache.hadoop.hbase.io.hfile.BlockCacheFactory;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALSplitUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests around replay of recovered.edits content.
 */
@Category({MediumTests.class})
public class TestRecoveredEdits {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRecoveredEdits.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final Logger LOG = LoggerFactory.getLogger(TestRecoveredEdits.class);

  private static BlockCache blockCache;

  @Rule public TestName testName = new TestName();

  /**
   * Path to a recovered.edits file in hbase-server test resources folder.
   * This is a little fragile getting this path to a file of 10M of edits.
   */
  @SuppressWarnings("checkstyle:VisibilityModifier")
  public static final Path RECOVEREDEDITS_PATH = new Path(
    System.getProperty("test.build.classes", "target/test-classes"),
    "0000000000000016310");

  /**
   * Name of table referenced by edits in the recovered.edits file.
   */
  public static final String RECOVEREDEDITS_TABLENAME = "IntegrationTestBigLinkedList";

  /**
   * Column family referenced by edits in the recovered.edits file.
   */
  public static final byte [] RECOVEREDEDITS_COLUMNFAMILY = Bytes.toBytes("meta");
  public static final byte[][] RECOVEREDITS_COLUMNFAMILY_ARRAY =
    new byte[][] {RECOVEREDEDITS_COLUMNFAMILY};
  public static final ColumnFamilyDescriptor RECOVEREDEDITS_CFD =
    ColumnFamilyDescriptorBuilder.newBuilder(RECOVEREDEDITS_COLUMNFAMILY).build();

  /**
   * Name of table mentioned edits from recovered.edits
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    blockCache = BlockCacheFactory.createBlockCache(TEST_UTIL.getConfiguration());
  }

  /**
   * HBASE-12782 ITBLL fails for me if generator does anything but 5M per maptask.
   * Create a region. Close it. Then copy into place a file to replay, one that is bigger than
   * configured flush size so we bring on lots of flushes.  Then reopen and confirm all edits
   * made it in.
   */
  @Test
  public void testReplayWorksThoughLotsOfFlushing() throws
      IOException {
    for(MemoryCompactionPolicy policy : MemoryCompactionPolicy.values()) {
      testReplayWorksWithMemoryCompactionPolicy(policy);
    }
  }

  private void testReplayWorksWithMemoryCompactionPolicy(MemoryCompactionPolicy policy) throws
    IOException {
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    // Set it so we flush every 1M or so.  Thats a lot.
    conf.setInt(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 1024*1024);
    conf.set(CompactingMemStore.COMPACTING_MEMSTORE_TYPE_KEY, String.valueOf(policy).toLowerCase());
    TableDescriptor tableDescriptor = TableDescriptorBuilder.
      newBuilder(TableName.valueOf(testName.getMethodName())).
      setColumnFamily(RECOVEREDEDITS_CFD) .build();
    RegionInfo hri = RegionInfoBuilder.newBuilder(tableDescriptor.getTableName()).build();
    final String encodedRegionName = hri.getEncodedName();
    Path hbaseRootDir = TEST_UTIL.getDataTestDir();
    FileSystem fs = FileSystem.get(TEST_UTIL.getConfiguration());
    Path tableDir = CommonFSUtils.getTableDir(hbaseRootDir, tableDescriptor.getTableName());
    HRegionFileSystem hrfs =
        new HRegionFileSystem(TEST_UTIL.getConfiguration(), fs, tableDir, hri);
    if (fs.exists(hrfs.getRegionDir())) {
      LOG.info("Region directory already exists. Deleting.");
      fs.delete(hrfs.getRegionDir(), true);
    }
    HRegion region = HBaseTestingUtility
        .createRegionAndWAL(hri, hbaseRootDir, conf, tableDescriptor, blockCache);
    assertEquals(encodedRegionName, region.getRegionInfo().getEncodedName());
    List<String> storeFiles = region.getStoreFileList(RECOVEREDITS_COLUMNFAMILY_ARRAY);
    // There should be no store files.
    assertTrue(storeFiles.isEmpty());
    region.close();
    Path regionDir = FSUtils.getRegionDirFromRootDir(hbaseRootDir, hri);
    Path recoveredEditsDir = WALSplitUtil.getRegionDirRecoveredEditsDir(regionDir);
    // Copy this file under the region's recovered.edits dir so it is replayed on reopen.
    Path destination = new Path(recoveredEditsDir, RECOVEREDEDITS_PATH.getName());
    fs.copyToLocalFile(RECOVEREDEDITS_PATH, destination);
    assertTrue(fs.exists(destination));
    // Now the file 0000000000000016310 is under recovered.edits, reopen the region to replay.
    region = HRegion.openHRegion(region, null);
    assertEquals(encodedRegionName, region.getRegionInfo().getEncodedName());
    storeFiles = region.getStoreFileList(RECOVEREDITS_COLUMNFAMILY_ARRAY);
    // Our 0000000000000016310 is 10MB. Most of the edits are for one region. Lets assume that if
    // we flush at 1MB, that there are at least 3 flushed files that are there because of the
    // replay of edits.
    if(policy == MemoryCompactionPolicy.EAGER || policy == MemoryCompactionPolicy.ADAPTIVE) {
      assertTrue("Files count=" + storeFiles.size(), storeFiles.size() >= 1);
    } else {
      assertTrue("Files count=" + storeFiles.size(), storeFiles.size() > 10);
    }
    // Now verify all edits made it into the region.
    int count = verifyAllEditsMadeItIn(fs, conf, RECOVEREDEDITS_PATH, region);
    assertTrue(count > 0);
    LOG.info("Checked " + count + " edits made it in");
  }

  /**
   * @return Return how many edits seen.
   */
  // Used by TestWALPlayer over in hbase-mapreduce too.
  public static int verifyAllEditsMadeItIn(final FileSystem fs, final Configuration conf,
      final Path edits, final HRegion region) throws IOException {
    int count = 0;
    // Read all cells from recover edits
    List<Cell> walCells = new ArrayList<>();
    try (WAL.Reader reader = WALFactory.createReader(fs, edits, conf)) {
      WAL.Entry entry;
      while ((entry = reader.next()) != null) {
        WALKey key = entry.getKey();
        WALEdit val = entry.getEdit();
        count++;
        // Check this edit is for this region.
        if (!Bytes.equals(key.getEncodedRegionName(),
            region.getRegionInfo().getEncodedNameAsBytes())) {
          continue;
        }
        Cell previous = null;
        for (Cell cell : val.getCells()) {
          if (WALEdit.isMetaEditFamily(cell)) {
            continue;
          }
          if (previous != null && CellComparatorImpl.COMPARATOR.compareRows(previous, cell) == 0) {
            continue;
          }
          previous = cell;
          walCells.add(cell);
        }
      }
    }

    // Read all cells from region
    List<Cell> regionCells = new ArrayList<>();
    try (RegionScanner scanner = region.getScanner(new Scan())) {
      List<Cell> tmpCells;
      do {
        tmpCells = new ArrayList<>();
        scanner.nextRaw(tmpCells);
        regionCells.addAll(tmpCells);
      } while (!tmpCells.isEmpty());
    }

    Collections.sort(walCells, CellComparatorImpl.COMPARATOR);
    int found = 0;
    for (int i = 0, j = 0; i < walCells.size() && j < regionCells.size(); ) {
      int compareResult = PrivateCellUtil
          .compareKeyIgnoresMvcc(CellComparatorImpl.COMPARATOR, walCells.get(i),
              regionCells.get(j));
      if (compareResult == 0) {
        i++;
        j++;
        found++;
      } else if (compareResult > 0) {
        j++;
      } else {
        i++;
      }
    }
    assertEquals("Only found " + found + " cells in region, but there are " + walCells.size() +
        " cells in recover edits", found, walCells.size());
    return count;
  }
}
