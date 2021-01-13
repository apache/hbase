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

import static org.junit.Assert.assertTrue;

import io.opentelemetry.sdk.testing.junit4.OpenTelemetryRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNameTestRule;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestHRegionTracing {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestHRegionTracing.class);

  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static byte[] FAMILY = Bytes.toBytes("family");

  private static byte[] QUALIFIER = Bytes.toBytes("qual");

  private static byte[] ROW = Bytes.toBytes("row");

  private static byte[] VALUE = Bytes.toBytes("value");

  @Rule
  public final OpenTelemetryRule traceRule = OpenTelemetryRule.create();

  @Rule
  public final TableNameTestRule tableNameRule = new TableNameTestRule();

  private static WAL WAL;

  private HRegion region;

  @BeforeClass
  public static void setUpBeforeClass() throws IOException {
    WAL = HBaseTestingUtility.createWal(UTIL.getConfiguration(), UTIL.getDataTestDir(), null);
  }

  @AfterClass
  public static void tearDownAfterClass() throws IOException {
    Closeables.close(WAL, true);
    UTIL.cleanupTestDir();
  }

  @Before
  public void setUp() throws IOException {
    TableName tableName = tableNameRule.getTableName();
    TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY)).build();
    RegionInfo info = RegionInfoBuilder.newBuilder(tableName).build();
    ChunkCreator.initialize(MemStoreLAB.CHUNK_SIZE_DEFAULT, false, 0, 0, 0, null,
      MemStoreLAB.INDEX_CHUNK_SIZE_PERCENTAGE_DEFAULT);
    region = HRegion.createHRegion(info, UTIL.getDataTestDir(), UTIL.getConfiguration(), desc, WAL);
    region = UTIL.createLocalHRegion(info, desc);
  }

  @After
  public void tearDown() throws IOException {
    if (region != null) {
      region.close();
    }
  }

  private void assertSpan(String spanName) {
    assertTrue(traceRule.getSpans().stream().anyMatch(span -> {
      if (!span.getName().equals(spanName)) {
        return false;
      }
      List<String> regionNames = span.getAttributes().get(TraceUtil.REGION_NAMES_KEY);
      return regionNames != null && regionNames.size() == 1 &&
        regionNames.get(0).equals(region.getRegionInfo().getRegionNameAsString());
    }));
  }

  @Test
  public void testGet() throws IOException {
    region.get(new Get(ROW));
    assertSpan("Region.get");
  }

  @Test
  public void testPut() throws IOException {
    region.put(new Put(ROW).addColumn(FAMILY, QUALIFIER, VALUE));
    assertSpan("Region.put");
    assertSpan("Region.getRowLock");
  }

  @Test
  public void testDelete() throws IOException {
    region.delete(new Delete(ROW).addColumn(FAMILY, QUALIFIER));
    assertSpan("Region.delete");
    assertSpan("Region.getRowLock");
  }

  @Test
  public void testAppend() throws IOException {
    region.append(new Append(ROW).addColumn(FAMILY, QUALIFIER, VALUE));
    assertSpan("Region.append");
    assertSpan("Region.getRowLock");
  }

  @Test
  public void testIncrement() throws IOException {
    region.increment(new Increment(ROW).addColumn(FAMILY, QUALIFIER, 1));
    assertSpan("Region.increment");
    assertSpan("Region.getRowLock");
  }

  @Test
  public void testBatchMutate() throws IOException {
    region.batchMutate(new Mutation[] { new Put(ROW).addColumn(FAMILY, QUALIFIER, VALUE) });
    assertSpan("Region.batchMutate");
    assertSpan("Region.getRowLock");
  }

  @Test
  public void testCheckAndMutate() throws IOException {
    region.checkAndMutate(CheckAndMutate.newBuilder(ROW).ifNotExists(FAMILY, QUALIFIER)
      .build(new Put(ROW).addColumn(FAMILY, QUALIFIER, VALUE)));
    assertSpan("Region.checkAndMutate");
    assertSpan("Region.getRowLock");
  }

  @Test
  public void testScanner() throws IOException {
    try (RegionScanner scanner = region.getScanner(new Scan())) {
      scanner.reseek(ROW);
      scanner.next(new ArrayList<>());
    }
    assertSpan("Region.getScanner");
    assertSpan("RegionScanner.reseek");
    assertSpan("RegionScanner.next");
    assertSpan("RegionScanner.close");
  }
}
