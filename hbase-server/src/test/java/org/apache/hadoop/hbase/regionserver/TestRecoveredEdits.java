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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALSplitter;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Tests around replay of recovered.edits content.
 */
@Category({MediumTests.class})
public class TestRecoveredEdits {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final Log LOG = LogFactory.getLog(TestRecoveredEdits.class);
  @Rule public TestName testName = new TestName();

  /**
   * HBASE-12782 ITBLL fails for me if generator does anything but 5M per maptask.
   * Create a region. Close it. Then copy into place a file to replay, one that is bigger than
   * configured flush size so we bring on lots of flushes.  Then reopen and confirm all edits
   * made it in.
   * @throws IOException
   */
  @Test (timeout=60000)
  public void testReplayWorksThoughLotsOfFlushing() throws IOException {
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    // Set it so we flush every 1M or so.  Thats a lot.
    conf.setInt(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 1024*1024);
    // The file of recovered edits has a column family of 'meta'. Also has an encoded regionname
    // of 4823016d8fca70b25503ee07f4c6d79f which needs to match on replay.
    final String encodedRegionName = "4823016d8fca70b25503ee07f4c6d79f";
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(testName.getMethodName()));
    final String columnFamily = "meta";
    byte [][] columnFamilyAsByteArray = new byte [][] {Bytes.toBytes(columnFamily)};
    htd.addFamily(new HColumnDescriptor(columnFamily));
    HRegionInfo hri = new HRegionInfo(htd.getTableName()) {
      @Override
      public synchronized String getEncodedName() {
        return encodedRegionName;
      }

      // Cache the name because lots of lookups.
      private byte [] encodedRegionNameAsBytes = null;
      @Override
      public synchronized byte[] getEncodedNameAsBytes() {
        if (encodedRegionNameAsBytes == null) {
          this.encodedRegionNameAsBytes = Bytes.toBytes(getEncodedName());
        }
        return this.encodedRegionNameAsBytes;
      }
    };
    Path hbaseRootDir = TEST_UTIL.getDataTestDir();
    FileSystem fs = FileSystem.get(TEST_UTIL.getConfiguration());
    Path tableDir = FSUtils.getTableDir(hbaseRootDir, htd.getTableName());
    HRegionFileSystem hrfs =
        new HRegionFileSystem(TEST_UTIL.getConfiguration(), fs, tableDir, hri);
    if (fs.exists(hrfs.getRegionDir())) {
      LOG.info("Region directory already exists. Deleting.");
      fs.delete(hrfs.getRegionDir(), true);
    }
    HRegion region = HRegion.createHRegion(hri, hbaseRootDir, conf, htd, null);
    assertEquals(encodedRegionName, region.getRegionInfo().getEncodedName());
    List<String> storeFiles = region.getStoreFileList(columnFamilyAsByteArray);
    // There should be no store files.
    assertTrue(storeFiles.isEmpty());
    region.close();
    Path regionDir = region.getRegionDir(hbaseRootDir, hri);
    Path recoveredEditsDir = WALSplitter.getRegionDirRecoveredEditsDir(regionDir);
    // This is a little fragile getting this path to a file of 10M of edits.
    Path recoveredEditsFile = new Path(new Path(
      System.getProperty("project.build.testSourceDirectory", "src" + Path.SEPARATOR + "test"),
      "data"), "0000000000000016310");
    // Copy this file under the region's recovered.edits dir so it is replayed on reopen.
    Path destination = new Path(recoveredEditsDir, recoveredEditsFile.getName());
    fs.copyToLocalFile(recoveredEditsFile, destination);
    assertTrue(fs.exists(destination));
    // Now the file 0000000000000016310 is under recovered.edits, reopen the region to replay.
    region = HRegion.openHRegion(region, null);
    assertEquals(encodedRegionName, region.getRegionInfo().getEncodedName());
    storeFiles = region.getStoreFileList(columnFamilyAsByteArray);
    // Our 0000000000000016310 is 10MB. Most of the edits are for one region. Lets assume that if
    // we flush at 1MB, that there are at least 3 flushed files that are there because of the
    // replay of edits.
    assertTrue("Files count=" + storeFiles.size(), storeFiles.size() > 10);
    // Now verify all edits made it into the region.
    int count = verifyAllEditsMadeItIn(fs, conf, recoveredEditsFile, region);
    LOG.info("Checked " + count + " edits made it in");
  }

  /**
   * @param fs
   * @param conf
   * @param edits
   * @param region
   * @return Return how many edits seen.
   * @throws IOException
   */
  private int verifyAllEditsMadeItIn(final FileSystem fs, final Configuration conf,
      final Path edits, final HRegion region)
  throws IOException {
    int count = 0;
    // Based on HRegion#replayRecoveredEdits
    WAL.Reader reader = null;
    try {
      reader = WALFactory.createReader(fs, edits, conf);
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
        for (Cell cell: val.getCells()) {
          if (CellUtil.matchingFamily(cell, WALEdit.METAFAMILY)) continue;
          if (previous != null && CellComparator.COMPARATOR.compareRows(previous, cell) == 0)
            continue;
          previous = cell;
          Get g = new Get(CellUtil.cloneRow(cell));
          Result r = region.get(g);
          boolean found = false;
          for (CellScanner scanner = r.cellScanner(); scanner.advance();) {
            Cell current = scanner.current();
            if (CellComparator.COMPARATOR.compareKeyIgnoresMvcc(cell, current) == 0) {
              found = true;
              break;
            }
          }
          assertTrue("Failed to find " + cell, found);
        }
      }
    } finally {
      if (reader != null) reader.close();
    }
    return count;
  }
}
