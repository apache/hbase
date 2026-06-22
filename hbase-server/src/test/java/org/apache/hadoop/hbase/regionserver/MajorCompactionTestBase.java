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

import static org.apache.hadoop.hbase.HBaseTestingUtility.START_KEY;
import static org.apache.hadoop.hbase.HBaseTestingUtility.START_KEY_BYTES;
import static org.apache.hadoop.hbase.HBaseTestingUtility.fam1;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTestConst;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.wal.WAL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.provider.Arguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for testing major compactions
 */
public abstract class MajorCompactionTestBase {

  public static Stream<Arguments> parameters() {
    return Stream.of("NONE", "BASIC", "EAGER").map(Arguments::of);
  }

  private static final Logger LOG =
    LoggerFactory.getLogger(MajorCompactionTestBase.class.getName());
  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  protected Configuration conf = UTIL.getConfiguration();

  protected String name;
  protected final String compType;

  protected HRegion r = null;
  protected TableDescriptor htd = null;
  protected static final byte[] COLUMN_FAMILY = fam1;
  protected final byte[] STARTROW = Bytes.toBytes(START_KEY);
  protected static final byte[] COLUMN_FAMILY_TEXT = COLUMN_FAMILY;
  protected int compactionThreshold;
  protected byte[] secondRowBytes, thirdRowBytes;
  protected static final long MAX_FILES_TO_COMPACT = 10;

  /** constructor */
  protected MajorCompactionTestBase(String compType) {
    this.compType = compType;
  }

  @BeforeEach
  public void setUp(TestInfo testInfo) throws Exception {
    this.name = testInfo.getTestMethod().get().getName();
    // Set cache flush size to 1MB
    conf.setInt(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 1024 * 1024);
    conf.setInt(HConstants.HREGION_MEMSTORE_BLOCK_MULTIPLIER, 100);
    compactionThreshold = conf.getInt("hbase.hstore.compactionThreshold", 3);
    conf.set(CompactingMemStore.COMPACTING_MEMSTORE_TYPE_KEY, String.valueOf(compType));

    secondRowBytes = START_KEY_BYTES.clone();
    // Increment the least significant character so we get to next row.
    secondRowBytes[START_KEY_BYTES.length - 1]++;
    thirdRowBytes = START_KEY_BYTES.clone();
    thirdRowBytes[START_KEY_BYTES.length - 1] =
      (byte) (thirdRowBytes[START_KEY_BYTES.length - 1] + 2);
    this.htd = UTIL.createTableDescriptor(
      TableName.valueOf((name + "-" + compType).replace('[', 'i').replace(']', 'i')),
      ColumnFamilyDescriptorBuilder.DEFAULT_MIN_VERSIONS, 3, HConstants.FOREVER,
      ColumnFamilyDescriptorBuilder.DEFAULT_KEEP_DELETED);
    this.r = UTIL.createLocalHRegion(htd, null, null);
  }

  @AfterEach
  public void tearDown() throws Exception {
    WAL wal = ((HRegion) r).getWAL();
    ((HRegion) r).close();
    wal.close();
  }

  protected final void majorCompaction() throws Exception {
    createStoreFile(r);
    for (int i = 0; i < compactionThreshold; i++) {
      createStoreFile(r);
    }
    // Add more content.
    HTestConst.addContent(new RegionAsTable(r), Bytes.toString(COLUMN_FAMILY));

    // Now there are about 5 versions of each column.
    // Default is that there only 3 (MAXVERSIONS) versions allowed per column.
    //
    // Assert == 3 when we ask for versions.
    Result result = r.get(new Get(STARTROW).addFamily(COLUMN_FAMILY_TEXT).readVersions(100));
    assertEquals(compactionThreshold, result.size());

    r.flush(true);
    r.compact(true);

    // look at the second row
    // Increment the least significant character so we get to next row.
    byte[] secondRowBytes = START_KEY_BYTES.clone();
    secondRowBytes[START_KEY_BYTES.length - 1]++;

    // Always 3 versions if that is what max versions is.
    result = r.get(new Get(secondRowBytes).addFamily(COLUMN_FAMILY_TEXT).readVersions(100));
    LOG.debug(
      "Row " + Bytes.toStringBinary(secondRowBytes) + " after " + "initial compaction: " + result);
    assertEquals(compactionThreshold, result.size(),
      "Invalid number of versions of row " + Bytes.toStringBinary(secondRowBytes) + ".");

    // Now add deletes to memstore and then flush it.
    // That will put us over
    // the compaction threshold of 3 store files. Compacting these store files
    // should result in a compacted store file that has no references to the
    // deleted row.
    LOG.debug("Adding deletes to memstore and flushing");
    Delete delete = new Delete(secondRowBytes, EnvironmentEdgeManager.currentTime());
    byte[][] famAndQf = { COLUMN_FAMILY, null };
    delete.addFamily(famAndQf[0]);
    r.delete(delete);

    // Assert deleted.
    result = r.get(new Get(secondRowBytes).addFamily(COLUMN_FAMILY_TEXT).readVersions(100));
    assertTrue(result.isEmpty(), "Second row should have been deleted");

    r.flush(true);

    result = r.get(new Get(secondRowBytes).addFamily(COLUMN_FAMILY_TEXT).readVersions(100));
    assertTrue(result.isEmpty(), "Second row should have been deleted");

    // Add a bit of data and flush. Start adding at 'bbb'.
    createSmallerStoreFile(this.r);
    r.flush(true);
    // Assert that the second row is still deleted.
    result = r.get(new Get(secondRowBytes).addFamily(COLUMN_FAMILY_TEXT).readVersions(100));
    assertTrue(result.isEmpty(), "Second row should still be deleted");

    // Force major compaction.
    r.compact(true);
    assertEquals(1, r.getStore(COLUMN_FAMILY_TEXT).getStorefiles().size());

    result = r.get(new Get(secondRowBytes).addFamily(COLUMN_FAMILY_TEXT).readVersions(100));
    assertTrue(result.isEmpty(), "Second row should still be deleted");

    // Make sure the store files do have some 'aaa' keys in them -- exactly 3.
    // Also, that compacted store files do not have any secondRowBytes because
    // they were deleted.
    verifyCounts(3, 0);

    // Multiple versions allowed for an entry, so the delete isn't enough
    // Lower TTL and expire to ensure that all our entries have been wiped
    final int ttl = 1000;
    for (HStore store : r.getStores()) {
      ScanInfo old = store.getScanInfo();
      ScanInfo si = old.customize(old.getMaxVersions(), ttl, old.getKeepDeletedCells());
      store.setScanInfo(si);
    }
    Thread.sleep(1000);

    r.compact(true);
    int count = count();
    assertEquals(0, count, "Should not see anything after TTL has expired");
  }

  private void verifyCounts(int countRow1, int countRow2) throws Exception {
    int count1 = 0;
    int count2 = 0;
    for (HStoreFile f : r.getStore(COLUMN_FAMILY_TEXT).getStorefiles()) {
      try (StoreFileScanner scanner = f.getPreadScanner(false, Long.MAX_VALUE, 0, false)) {
        scanner.seek(KeyValue.LOWESTKEY);
        for (Cell cell;;) {
          cell = scanner.next();
          if (cell == null) {
            break;
          }
          byte[] row = CellUtil.cloneRow(cell);
          if (Bytes.equals(row, STARTROW)) {
            count1++;
          } else if (Bytes.equals(row, secondRowBytes)) {
            count2++;
          }
        }
      }
    }
    assertEquals(countRow1, count1);
    assertEquals(countRow2, count2);
  }

  private int count() throws IOException {
    int count = 0;
    for (HStoreFile f : r.getStore(COLUMN_FAMILY_TEXT).getStorefiles()) {
      try (StoreFileScanner scanner = f.getPreadScanner(false, Long.MAX_VALUE, 0, false)) {
        scanner.seek(KeyValue.LOWESTKEY);
        while (scanner.next() != null) {
          count++;
        }
      }
    }
    return count;
  }

  protected final void createStoreFile(final HRegion region) throws IOException {
    createStoreFile(region, Bytes.toString(COLUMN_FAMILY));
  }

  protected final void createStoreFile(final HRegion region, String family) throws IOException {
    Table loader = new RegionAsTable(region);
    HTestConst.addContent(loader, family);
    region.flush(true);
  }

  protected final void createSmallerStoreFile(final HRegion region) throws IOException {
    Table loader = new RegionAsTable(region);
    HTestConst.addContent(loader, Bytes.toString(COLUMN_FAMILY), Bytes.toBytes("" + "bbb"), null);
    region.flush(true);
  }
}
