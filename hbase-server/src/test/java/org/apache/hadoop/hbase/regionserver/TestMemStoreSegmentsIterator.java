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
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
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

  private static String TABLE = "test_mscsi";
  private static String FAMILY = "f";
  private static String COLUMN = "c";
  private static String ROOT_SUB_PATH = "testMemStoreSegmentsIterator";
  private static long LESS_THAN_INTEGER_MAX_VALUE_SEQ_ID = Long.valueOf(Integer.MAX_VALUE) - 1;
  private static long GREATER_THAN_INTEGER_MAX_VALUE_SEQ_ID = Long.valueOf(Integer.MAX_VALUE) + 1;

  private CellComparator comparator;
  private int compactionKVMax;
  private HRegion region;
  private HStore store;

  @Before
  public void setup() throws IOException {
    Configuration conf = new Configuration();
    HBaseTestingUtil hbaseUtility = new HBaseTestingUtil(conf);
    TableDescriptorBuilder tableDescriptorBuilder =
      TableDescriptorBuilder.newBuilder(TableName.valueOf(TABLE));
    ColumnFamilyDescriptor columnFamilyDescriptor =
      ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(FAMILY)).build();
    tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);

    RegionInfo info = RegionInfoBuilder.newBuilder(TableName.valueOf(TABLE)).build();
    Path rootPath = hbaseUtility.getDataTestDir(ROOT_SUB_PATH);
    this.region =
      HBaseTestingUtil.createRegionAndWAL(info, rootPath, conf, tableDescriptorBuilder.build());
    this.store = region.getStore(columnFamilyDescriptor.getName());
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
    final KeyValue kv1 = new KeyValue(one, f, q, EnvironmentEdgeManager.currentTime(), v);
    final KeyValue kv2 = new KeyValue(two, f, q, EnvironmentEdgeManager.currentTime(), v);
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
    ExtendedCell firstCell = iterator.next();
    assertEquals(LESS_THAN_INTEGER_MAX_VALUE_SEQ_ID, firstCell.getSequenceId());

    // check second cell
    assertTrue(iterator.hasNext());
    ExtendedCell secondCell = iterator.next();
    assertEquals(GREATER_THAN_INTEGER_MAX_VALUE_SEQ_ID, secondCell.getSequenceId());
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentEdgeManagerTestHelper.reset();
    if (region != null) {
      HBaseTestingUtil.closeRegionAndWAL(region);
    }
  }
}
