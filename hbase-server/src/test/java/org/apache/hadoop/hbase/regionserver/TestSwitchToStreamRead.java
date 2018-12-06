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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Scan.ReadType;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.regionserver.HRegion.RegionScannerImpl;
import org.apache.hadoop.hbase.regionserver.ScannerContext.LimitScope;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestSwitchToStreamRead {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSwitchToStreamRead.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static TableName TABLE_NAME = TableName.valueOf("stream");

  private static byte[] FAMILY = Bytes.toBytes("cf");

  private static byte[] QUAL = Bytes.toBytes("cq");

  private static String VALUE_PREFIX;

  private static HRegion REGION;

  @Before
  public void setUp() throws IOException {
    UTIL.getConfiguration().setLong(StoreScanner.STORESCANNER_PREAD_MAX_BYTES, 2048);
    StringBuilder sb = new StringBuilder(256);
    for (int i = 0; i < 255; i++) {
      sb.append((char) ThreadLocalRandom.current().nextInt('A', 'z' + 1));
    }
    VALUE_PREFIX = sb.append("-").toString();
    REGION = UTIL.createLocalHRegion(
      TableDescriptorBuilder.newBuilder(TABLE_NAME)
        .setColumnFamily(
          ColumnFamilyDescriptorBuilder.newBuilder(FAMILY).setBlocksize(1024).build())
        .build(),
      null, null);
    for (int i = 0; i < 900; i++) {
      REGION
        .put(new Put(Bytes.toBytes(i)).addColumn(FAMILY, QUAL, Bytes.toBytes(VALUE_PREFIX + i)));
    }
    REGION.flush(true);
    for (int i = 900; i < 1000; i++) {
      REGION
        .put(new Put(Bytes.toBytes(i)).addColumn(FAMILY, QUAL, Bytes.toBytes(VALUE_PREFIX + i)));
    }
  }

  @After
  public void tearDown() throws IOException {
    REGION.close(true);
    UTIL.cleanupTestDir();
  }

  private Set<StoreFileReader> getStreamReaders() {
    List<HStore> stores = REGION.getStores();
    Assert.assertEquals(1, stores.size());
    HStore firstStore = stores.get(0);
    Assert.assertNotNull(firstStore);
    Collection<HStoreFile> storeFiles = firstStore.getStorefiles();
    Assert.assertEquals(1, storeFiles.size());
    HStoreFile firstSToreFile = storeFiles.iterator().next();
    Assert.assertNotNull(firstSToreFile);
    return Collections.unmodifiableSet(firstSToreFile.streamReaders);
  }

  /**
   * Test Case for HBASE-21551
   */
  @Test
  public void testStreamReadersCleanup() throws IOException {
    Set<StoreFileReader> streamReaders = getStreamReaders();
    Assert.assertEquals(0, getStreamReaders().size());
    try (RegionScannerImpl scanner = REGION.getScanner(new Scan().setReadType(ReadType.STREAM))) {
      StoreScanner storeScanner =
          (StoreScanner) (scanner).getStoreHeapForTesting().getCurrentForTesting();
      List<StoreFileScanner> sfScanners = storeScanner.getAllScannersForTesting().stream()
          .filter(kvs -> kvs instanceof StoreFileScanner).map(kvs -> (StoreFileScanner) kvs)
          .collect(Collectors.toList());
      Assert.assertEquals(1, sfScanners.size());
      StoreFileScanner sfScanner = sfScanners.get(0);
      Assert.assertFalse(sfScanner.getReader().shared);

      // There should be a stream reader
      Assert.assertEquals(1, getStreamReaders().size());
    }
    Assert.assertEquals(0, getStreamReaders().size());

    // The streamsReader should be clear after region close even if there're some opened stream
    // scanner.
    RegionScannerImpl scanner = REGION.getScanner(new Scan().setReadType(ReadType.STREAM));
    Assert.assertNotNull(scanner);
    Assert.assertEquals(1, getStreamReaders().size());
    REGION.close();
    Assert.assertEquals(0, streamReaders.size());
  }

  @Test
  public void test() throws IOException {
    try (RegionScannerImpl scanner = REGION.getScanner(new Scan())) {
      StoreScanner storeScanner =
        (StoreScanner) (scanner).getStoreHeapForTesting().getCurrentForTesting();
      for (KeyValueScanner kvs : storeScanner.getAllScannersForTesting()) {
        if (kvs instanceof StoreFileScanner) {
          StoreFileScanner sfScanner = (StoreFileScanner) kvs;
          // starting from pread so we use shared reader here.
          assertTrue(sfScanner.getReader().shared);
        }
      }
      List<Cell> cells = new ArrayList<>();
      for (int i = 0; i < 500; i++) {
        assertTrue(scanner.next(cells));
        Result result = Result.create(cells);
        assertEquals(VALUE_PREFIX + i, Bytes.toString(result.getValue(FAMILY, QUAL)));
        cells.clear();
        scanner.shipped();
      }
      for (KeyValueScanner kvs : storeScanner.getAllScannersForTesting()) {
        if (kvs instanceof StoreFileScanner) {
          StoreFileScanner sfScanner = (StoreFileScanner) kvs;
          // we should have convert to use stream read now.
          assertFalse(sfScanner.getReader().shared);
        }
      }
      for (int i = 500; i < 1000; i++) {
        assertEquals(i != 999, scanner.next(cells));
        Result result = Result.create(cells);
        assertEquals(VALUE_PREFIX + i, Bytes.toString(result.getValue(FAMILY, QUAL)));
        cells.clear();
        scanner.shipped();
      }
    }
    // make sure all scanners are closed.
    for (HStoreFile sf : REGION.getStore(FAMILY).getStorefiles()) {
      assertFalse(sf.isReferencedInReads());
    }
  }

  public static final class MatchLastRowKeyFilter extends FilterBase {

    @Override
    public boolean filterRowKey(Cell cell) throws IOException {
      return Bytes.toInt(cell.getRowArray(), cell.getRowOffset()) != 999;
    }
  }

  private void testFilter(Filter filter) throws IOException {
    try (RegionScannerImpl scanner = REGION.getScanner(new Scan().setFilter(filter))) {
      StoreScanner storeScanner =
        (StoreScanner) (scanner).getStoreHeapForTesting().getCurrentForTesting();
      for (KeyValueScanner kvs : storeScanner.getAllScannersForTesting()) {
        if (kvs instanceof StoreFileScanner) {
          StoreFileScanner sfScanner = (StoreFileScanner) kvs;
          // starting from pread so we use shared reader here.
          assertTrue(sfScanner.getReader().shared);
        }
      }
      List<Cell> cells = new ArrayList<>();
      // should return before finishing the scan as we want to switch from pread to stream
      assertTrue(scanner.next(cells,
        ScannerContext.newBuilder().setTimeLimit(LimitScope.BETWEEN_CELLS, -1).build()));
      assertTrue(cells.isEmpty());
      scanner.shipped();

      for (KeyValueScanner kvs : storeScanner.getAllScannersForTesting()) {
        if (kvs instanceof StoreFileScanner) {
          StoreFileScanner sfScanner = (StoreFileScanner) kvs;
          // we should have convert to use stream read now.
          assertFalse(sfScanner.getReader().shared);
        }
      }
      assertFalse(scanner.next(cells,
        ScannerContext.newBuilder().setTimeLimit(LimitScope.BETWEEN_CELLS, -1).build()));
      Result result = Result.create(cells);
      assertEquals(VALUE_PREFIX + 999, Bytes.toString(result.getValue(FAMILY, QUAL)));
      cells.clear();
      scanner.shipped();
    }
    // make sure all scanners are closed.
    for (HStoreFile sf : REGION.getStore(FAMILY).getStorefiles()) {
      assertFalse(sf.isReferencedInReads());
    }
  }

  // We use a different logic to implement filterRowKey, where we will keep calling kvHeap.next
  // until the row key is changed. And there we can only use NoLimitScannerContext so we can not
  // make the upper layer return immediately. Simply do not use NoLimitScannerContext will lead to
  // an infinite loop. Need to dig more, the code are way too complicated...
  @Ignore
  @Test
  public void testFilterRowKey() throws IOException {
    testFilter(new MatchLastRowKeyFilter());
  }

  public static final class MatchLastRowCellNextColFilter extends FilterBase {

    @Override
    public ReturnCode filterCell(Cell c) throws IOException {
      if (Bytes.toInt(c.getRowArray(), c.getRowOffset()) == 999) {
        return ReturnCode.INCLUDE;
      } else {
        return ReturnCode.NEXT_COL;
      }
    }
  }

  @Test
  public void testFilterCellNextCol() throws IOException {
    testFilter(new MatchLastRowCellNextColFilter());
  }

  public static final class MatchLastRowCellNextRowFilter extends FilterBase {

    @Override
    public ReturnCode filterCell(Cell c) throws IOException {
      if (Bytes.toInt(c.getRowArray(), c.getRowOffset()) == 999) {
        return ReturnCode.INCLUDE;
      } else {
        return ReturnCode.NEXT_ROW;
      }
    }
  }

  @Test
  public void testFilterCellNextRow() throws IOException {
    testFilter(new MatchLastRowCellNextRowFilter());
  }

  public static final class MatchLastRowFilterRowFilter extends FilterBase {

    private boolean exclude;

    @Override
    public void filterRowCells(List<Cell> kvs) throws IOException {
      Cell c = kvs.get(0);
      exclude = Bytes.toInt(c.getRowArray(), c.getRowOffset()) != 999;
    }

    @Override
    public void reset() throws IOException {
      exclude = false;
    }

    @Override
    public boolean filterRow() throws IOException {
      return exclude;
    }

    @Override
    public boolean hasFilterRow() {
      return true;
    }
  }

  @Test
  public void testFilterRow() throws IOException {
    testFilter(new MatchLastRowFilterRowFilter());
  }
}
