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

import static org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder.NEW_VERSION_BEHAVIOR;
import static org.apache.hadoop.hbase.regionserver.StoreFileWriter.ENABLE_HISTORICAL_COMPACTION_FILES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.MemoryCompactionPolicy;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionConfiguration;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Store file writer does not do any compaction. Each cell written to either the live or historical
 * file. Regular (i.e., not-raw) scans that reads the latest put cells scans only live files. To
 * ensure the correctness of store file writer, we need to verify that live files includes all live
 * cells. This test indirectly verify this as follows. The test creates two tables, each with one
 * region and one store. The dual file writing (live vs historical) is configured on only one of the
 * tables. The test generates exact set of mutations on both tables. These mutations include all
 * types of cells and these cells are written to multiple files using multiple memstore flushes.
 * After writing all cells, the test first verify that both tables return the same set of cells for
 * regular and raw scans. Then the same verification is done after tables are minor and finally
 * major compacted. The test also verifies that flushes do not generate historical files and the
 * historical files are generated only when historical file generation is enabled (by the config
 * hbase.enable.historical.compaction.files).
 */
@Category({ MediumTests.class, RegionServerTests.class })
@RunWith(Parameterized.class)
public class TestStoreFileWriter {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestStoreFileWriter.class);
  private final int ROW_NUM = 100;
  private final Random RANDOM = new Random(11);
  private final HBaseTestingUtil testUtil = new HBaseTestingUtil();
  private HRegion[] regions = new HRegion[2];
  private final byte[][] qualifiers =
    { Bytes.toBytes("0"), Bytes.toBytes("1"), Bytes.toBytes("2") };
  // This keeps track of all cells. It is a list of rows, each row is a list of columns, each
  // column is a list of CellInfo object
  private ArrayList<ArrayList<ArrayList<CellInfo>>> insertedCells;
  private TableName[] tableName = new TableName[2];
  private final Configuration conf = testUtil.getConfiguration();
  private int flushCount = 0;

  @Parameterized.Parameter(0)
  public KeepDeletedCells keepDeletedCells;
  @Parameterized.Parameter(1)
  public int maxVersions;
  @Parameterized.Parameter(2)
  public boolean newVersionBehavior;

  @Parameterized.Parameters(name = "keepDeletedCells={0}, maxVersions={1}, newVersionBehavior={2}")
  public static synchronized Collection<Object[]> data() {
    return Arrays.asList(
      new Object[][] { { KeepDeletedCells.FALSE, 1, true }, { KeepDeletedCells.FALSE, 2, false },
        { KeepDeletedCells.FALSE, 3, true }, { KeepDeletedCells.TRUE, 1, false },
        // { KeepDeletedCells.TRUE, 2, true }, see HBASE-28442
        { KeepDeletedCells.TRUE, 3, false } });
  }

  // In memory representation of a cell. We only need to know timestamp and type field for our
  // testing for cell. Please note the row for the cell is implicit in insertedCells.
  private static class CellInfo {
    long timestamp;
    Cell.Type type;

    CellInfo(long timestamp, Cell.Type type) {
      this.timestamp = timestamp;
      this.type = type;
    }
  }

  private void createTable(int index, boolean enableDualFileWriter) throws IOException {
    tableName[index] = TableName.valueOf(getClass().getSimpleName() + "_" + index);
    ColumnFamilyDescriptor familyDescriptor =
      ColumnFamilyDescriptorBuilder.newBuilder(HBaseTestingUtil.fam1).setMaxVersions(maxVersions)
        .setKeepDeletedCells(keepDeletedCells)
        .setValue(NEW_VERSION_BEHAVIOR, Boolean.toString(newVersionBehavior)).build();
    TableDescriptorBuilder builder =
      TableDescriptorBuilder.newBuilder(tableName[index]).setColumnFamily(familyDescriptor)
        .setValue(ENABLE_HISTORICAL_COMPACTION_FILES, Boolean.toString(enableDualFileWriter));
    testUtil.createTable(builder.build(), null);
    regions[index] = testUtil.getMiniHBaseCluster().getRegions(tableName[index]).get(0);
  }

  @Before
  public void setUp() throws Exception {
    conf.setInt(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MAX_KEY, 6);
    conf.set(CompactingMemStore.COMPACTING_MEMSTORE_TYPE_KEY,
      String.valueOf(MemoryCompactionPolicy.NONE));
    testUtil.startMiniCluster();
    createTable(0, false);
    createTable(1, true);
    insertedCells = new ArrayList<>(ROW_NUM);
    for (int r = 0; r < ROW_NUM; r++) {
      insertedCells.add(new ArrayList<>(qualifiers.length));
      for (int q = 0; q < qualifiers.length; q++) {
        insertedCells.get(r).add(new ArrayList<>(10));
      }
    }
  }

  @After
  public void tearDown() throws Exception {
    this.testUtil.shutdownMiniCluster();
    testUtil.cleanupTestDir();
  }

  @Test
  public void testCompactedFiles() throws Exception {
    for (int i = 0; i < 10; i++) {
      insertRows(ROW_NUM * maxVersions);
      deleteRows(ROW_NUM / 8);
      deleteRowVersions(ROW_NUM / 8);
      deleteColumns(ROW_NUM / 8);
      deleteColumnVersions(ROW_NUM / 8);
      flushRegion();
    }

    verifyCells();

    HStore[] stores = new HStore[2];

    stores[0] = regions[0].getStore(HBaseTestingUtil.fam1);
    assertEquals(flushCount, stores[0].getStorefilesCount());

    stores[1] = regions[1].getStore(HBaseTestingUtil.fam1);
    assertEquals(flushCount, stores[1].getStorefilesCount());

    regions[0].compact(false);
    assertEquals(flushCount - stores[0].getCompactedFiles().size() + 1,
      stores[0].getStorefilesCount());

    regions[1].compact(false);
    assertEquals(flushCount - stores[1].getCompactedFiles().size() + 2,
      stores[1].getStorefilesCount());

    verifyCells();

    regions[0].compact(true);
    assertEquals(1, stores[0].getStorefilesCount());

    regions[1].compact(true);
    assertEquals(keepDeletedCells == KeepDeletedCells.FALSE ? 1 : 2,
      stores[1].getStorefilesCount());

    verifyCells();
  }

  private void verifyCells() throws Exception {
    scanAndCompare(false);
    scanAndCompare(true);
  }

  private void flushRegion() throws Exception {
    regions[0].flush(true);
    regions[1].flush(true);
    flushCount++;
  }

  private Long getRowTimestamp(int row) {
    Long maxTimestamp = null;
    for (int q = 0; q < qualifiers.length; q++) {
      int size = insertedCells.get(row).get(q).size();
      if (size > 0) {
        CellInfo mostRecentCellInfo = insertedCells.get(row).get(q).get(size - 1);
        if (mostRecentCellInfo.type == Cell.Type.Put) {
          if (maxTimestamp == null || maxTimestamp < mostRecentCellInfo.timestamp) {
            maxTimestamp = mostRecentCellInfo.timestamp;
          }
        }
      }
    }
    return maxTimestamp;
  }

  private long getNewTimestamp(long timestamp) throws Exception {
    long newTimestamp = System.currentTimeMillis();
    if (timestamp == newTimestamp) {
      Thread.sleep(1);
      newTimestamp = System.currentTimeMillis();
      assertTrue(timestamp < newTimestamp);
    }
    return newTimestamp;
  }

  private void insertRows(int rowCount) throws Exception {
    int row;
    long timestamp = System.currentTimeMillis();
    for (int r = 0; r < rowCount; r++) {
      row = RANDOM.nextInt(ROW_NUM);
      Put put = new Put(Bytes.toBytes(String.valueOf(row)), timestamp);
      for (int q = 0; q < qualifiers.length; q++) {
        put.addColumn(HBaseTestingUtil.fam1, qualifiers[q],
          Bytes.toBytes(String.valueOf(timestamp)));
        insertedCells.get(row).get(q).add(new CellInfo(timestamp, Cell.Type.Put));
      }
      regions[0].put(put);
      regions[1].put(put);
      timestamp = getNewTimestamp(timestamp);
    }
  }

  private void deleteRows(int rowCount) throws Exception {
    int row;
    for (int r = 0; r < rowCount; r++) {
      long timestamp = System.currentTimeMillis();
      row = RANDOM.nextInt(ROW_NUM);
      Delete delete = new Delete(Bytes.toBytes(String.valueOf(row)));
      regions[0].delete(delete);
      regions[1].delete(delete);
      // For simplicity, the family delete markers are inserted for all columns (instead of
      // allocating a separate column for them) in the memory representation of the data stored
      // to HBase
      for (int q = 0; q < qualifiers.length; q++) {
        insertedCells.get(row).get(q).add(new CellInfo(timestamp, Cell.Type.DeleteFamily));
      }
    }
  }

  private void deleteSingleRowVersion(int row, long timestamp) throws IOException {
    Delete delete = new Delete(Bytes.toBytes(String.valueOf(row)));
    delete.addFamilyVersion(HBaseTestingUtil.fam1, timestamp);
    regions[0].delete(delete);
    regions[1].delete(delete);
    // For simplicity, the family delete version markers are inserted for all columns (instead of
    // allocating a separate column for them) in the memory representation of the data stored
    // to HBase
    for (int q = 0; q < qualifiers.length; q++) {
      insertedCells.get(row).get(q).add(new CellInfo(timestamp, Cell.Type.DeleteFamilyVersion));
    }
  }

  private void deleteRowVersions(int rowCount) throws Exception {
    int row;
    for (int r = 0; r < rowCount; r++) {
      row = RANDOM.nextInt(ROW_NUM);
      Long timestamp = getRowTimestamp(row);
      if (timestamp != null) {
        deleteSingleRowVersion(row, timestamp);
      }
    }
    // Just insert one more delete marker possibly does not delete any row version
    row = RANDOM.nextInt(ROW_NUM);
    deleteSingleRowVersion(row, System.currentTimeMillis());
  }

  private void deleteColumns(int rowCount) throws Exception {
    int row;
    for (int r = 0; r < rowCount; r++) {
      long timestamp = System.currentTimeMillis();
      row = RANDOM.nextInt(ROW_NUM);
      int q = RANDOM.nextInt(qualifiers.length);
      Delete delete = new Delete(Bytes.toBytes(String.valueOf(row)), timestamp);
      delete.addColumns(HBaseTestingUtil.fam1, qualifiers[q], timestamp);
      regions[0].delete(delete);
      regions[1].delete(delete);
      insertedCells.get(row).get(q).add(new CellInfo(timestamp, Cell.Type.DeleteColumn));
    }
  }

  private void deleteColumnVersions(int rowCount) throws Exception {
    int row;
    for (int r = 0; r < rowCount; r++) {
      row = RANDOM.nextInt(ROW_NUM);
      Long timestamp = getRowTimestamp(row);
      if (timestamp != null) {
        Delete delete = new Delete(Bytes.toBytes(String.valueOf(row)));
        int q = RANDOM.nextInt(qualifiers.length);
        delete.addColumn(HBaseTestingUtil.fam1, qualifiers[q], timestamp);
        regions[0].delete(delete);
        regions[1].delete(delete);
        insertedCells.get(row).get(q).add(new CellInfo(timestamp, Cell.Type.Delete));
      }
    }
  }

  private Scan createScan(boolean raw) {
    Scan scan = new Scan();
    scan.readAllVersions();
    scan.setRaw(raw);
    return scan;
  }

  private void scanAndCompare(boolean raw) throws Exception {
    try (RegionScanner firstRS = regions[0].getScanner(createScan(raw))) {
      try (RegionScanner secondRS = regions[1].getScanner(createScan(raw))) {
        boolean firstHasMore;
        boolean secondHasMore;
        do {
          List<Cell> firstRowList = new ArrayList<>();
          List<Cell> secondRowList = new ArrayList<>();
          firstHasMore = firstRS.nextRaw(firstRowList);
          secondHasMore = secondRS.nextRaw(secondRowList);
          assertEquals(firstRowList.size(), secondRowList.size());
          int size = firstRowList.size();
          for (int i = 0; i < size; i++) {
            Cell firstCell = firstRowList.get(i);
            Cell secondCell = secondRowList.get(i);
            assert (CellUtil.matchingRowColumn(firstCell, secondCell));
            assert (firstCell.getType() == secondCell.getType());
            assert (Bytes.equals(CellUtil.cloneValue(firstCell), CellUtil.cloneValue(firstCell)));
          }
        } while (firstHasMore && secondHasMore);
        assertEquals(firstHasMore, secondHasMore);
      }
    }
  }
}
