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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
import org.apache.hadoop.hbase.wal.WAL;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test the {@link MemStoreCompactorSegmentsIterator} and {@link MemStoreMergerSegmentsIterator}
 * class, Test for bug : HBASE-22324
 */
@Category({ RegionServerTests.class, SmallTests.class })
public class TestMemStoreSegmentsIterator {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMemStoreSegmentsIterator.class);

  protected static String TABLE = "test_mscsi";
  protected static String FAMILY = "f";
  protected static String COLUMN = "c";
  protected static String ROOT_SUB_PATH = "testMemStoreSegmentsIterator";
  protected static long LESS_THAN_INTEGER_MAX_VALUE_SEQ_ID = Long.valueOf(Integer.MAX_VALUE) - 1;
  protected static long GREATER_THAN_INTEGER_MAX_VALUE_SEQ_ID = Long.valueOf(Integer.MAX_VALUE) + 1;

  protected CellComparator comparator;
  protected int compactionKVMax;
  protected WAL wal;
  protected HRegion region;
  protected HStore store;

  @Before
  public void setup() throws IOException {
    Configuration conf = new Configuration();
    HBaseTestingUtility hbaseUtility = HBaseTestingUtility.createLocalHTU(conf);
    HColumnDescriptor hcd = new HColumnDescriptor(FAMILY);
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(TABLE));
    htd.addFamily(hcd);
    HRegionInfo info = new HRegionInfo(TableName.valueOf(TABLE), null, null, false);
    Path rootPath = hbaseUtility.getDataTestDir(ROOT_SUB_PATH);
    this.wal = hbaseUtility.createWal(conf, rootPath, info);
    this.region = HRegion.createHRegion(info, rootPath, conf, htd, this.wal, true);
    this.store = new HStore(this.region, hcd, conf, false);
    this.comparator = CellComparator.getInstance();
    this.compactionKVMax = HConstants.COMPACTION_KV_MAX_DEFAULT;
  }

  @Test
  public void testMemStoreCompactorSegmentsIteratorNext() throws IOException {
    List<ImmutableSegment> segments = Arrays.asList(createTestImmutableSegment());
    MemStoreCompactorSegmentsIterator iterator = new MemStoreCompactorSegmentsIterator(segments,
        this.comparator, this.compactionKVMax, this.store);
    verifyNext(iterator);
    closeTestSegments(segments);
  }

  @Test
  public void testMemStoreMergerSegmentsIteratorNext() throws IOException {
    List<ImmutableSegment> segments = Arrays.asList(createTestImmutableSegment());
    MemStoreMergerSegmentsIterator iterator =
        new MemStoreMergerSegmentsIterator(segments, this.comparator, this.compactionKVMax);
    verifyNext(iterator);
    closeTestSegments(segments);
  }

  protected ImmutableSegment createTestImmutableSegment() {
    ImmutableSegment segment1 = SegmentFactory.instance().createImmutableSegment(this.comparator);
    final byte[] one = Bytes.toBytes(1);
    final byte[] two = Bytes.toBytes(2);
    final byte[] f = Bytes.toBytes(FAMILY);
    final byte[] q = Bytes.toBytes(COLUMN);
    final byte[] v = Bytes.toBytes(3);
    final KeyValue kv1 = new KeyValue(one, f, q, System.currentTimeMillis(), v);
    final KeyValue kv2 = new KeyValue(two, f, q, System.currentTimeMillis(), v);
    // the seqId of first cell less than Integer.MAX_VALUE,
    // the seqId of second cell greater than integer.MAX_VALUE
    kv1.setSequenceId(LESS_THAN_INTEGER_MAX_VALUE_SEQ_ID);
    kv2.setSequenceId(GREATER_THAN_INTEGER_MAX_VALUE_SEQ_ID);
    segment1.internalAdd(kv1, false, null, true);
    segment1.internalAdd(kv2, false, null, true);
    return segment1;
  }

  protected void closeTestSegments(List<ImmutableSegment> segments) {
    for (Segment segment : segments) {
      segment.close();
    }
  }

  protected void verifyNext(MemStoreSegmentsIterator iterator) {
    // check first cell
    assertTrue(iterator.hasNext());
    Cell firstCell = iterator.next();
    assertEquals(LESS_THAN_INTEGER_MAX_VALUE_SEQ_ID, firstCell.getSequenceId());

    // check second cell
    assertTrue(iterator.hasNext());
    Cell secondCell = iterator.next();
    assertEquals(GREATER_THAN_INTEGER_MAX_VALUE_SEQ_ID, secondCell.getSequenceId());
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentEdgeManagerTestHelper.reset();
    if (store != null) {
      try {
        store.close();
      } catch (IOException e) {
      }
      store = null;
    }
    if (region != null) {
      region.close();
      region = null;
    }

    if (wal != null) {
      wal.close();
      wal = null;
    }
  }
}
