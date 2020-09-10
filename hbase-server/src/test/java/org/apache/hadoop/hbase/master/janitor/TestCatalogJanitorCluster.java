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
package org.apache.hadoop.hbase.master.janitor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.master.assignment.RegionStateStore;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MasterTests.class, LargeTests.class })
public class TestCatalogJanitorCluster {
  private static final Logger LOG = LoggerFactory.getLogger(TestCatalogJanitorCluster.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCatalogJanitorCluster.class);

  @Rule
  public final TestName name = new TestName();

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final TableName T1 = TableName.valueOf("t1");
  private static final TableName T2 = TableName.valueOf("t2");
  private static final TableName T3 = TableName.valueOf("t3");
  private static final TableName T4 = TableName.valueOf("t4");
  private static final TableName T5 = TableName.valueOf("t5");

  @Before
  public void before() throws Exception {
    TEST_UTIL.startMiniCluster();
    TEST_UTIL.createMultiRegionTable(T1, new byte[][] { HConstants.CATALOG_FAMILY });
    TEST_UTIL.createMultiRegionTable(T2, new byte[][] { HConstants.CATALOG_FAMILY });
    TEST_UTIL.createMultiRegionTable(T3, new byte[][] { HConstants.CATALOG_FAMILY });

    final byte[][] keysForT4 =
      { Bytes.toBytes("aa"), Bytes.toBytes("bb"), Bytes.toBytes("cc"), Bytes.toBytes("dd") };

    TEST_UTIL.createTable(T4, HConstants.CATALOG_FAMILY, keysForT4);

    final byte[][] keysForT5 = { Bytes.toBytes("bb"), Bytes.toBytes("cc"), Bytes.toBytes("dd") };

    TEST_UTIL.createTable(T5, HConstants.CATALOG_FAMILY, keysForT5);
  }

  @After
  public void after() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Fat method where we start with a fat hbase:meta and then gradually intro problems running
   * catalogjanitor for each to ensure it triggers complaint. Do one big method because takes a
   * while to build up the context we need. We create three tables and then make holes, overlaps,
   * add unknown servers and empty out regioninfo columns. Each should up counts in the
   * CatalogJanitor.Report produced.
   */
  @Test
  public void testConsistency() throws IOException {
    CatalogJanitor janitor = TEST_UTIL.getHBaseCluster().getMaster().getCatalogJanitor();
    RegionStateStore regionStateStore =
      TEST_UTIL.getHBaseCluster().getMaster().getAssignmentManager().getRegionStateStore();
    janitor.scan();
    Report report = janitor.getLastReport();
    // Assert no problems.
    assertTrue(report.isEmpty());
    // Now remove first region in table t2 to see if catalogjanitor scan notices.
    List<RegionInfo> t2Ris = MetaTableAccessor.getTableRegions(TEST_UTIL.getConnection(), T2);
    regionStateStore.deleteRegion(t2Ris.get(0));
    janitor.scan();
    report = janitor.getLastReport();
    assertFalse(report.isEmpty());
    assertEquals(1, report.getHoles().size());
    assertTrue(report.getHoles().get(0).getFirst().getTable()
      .equals(RegionInfoBuilder.UNDEFINED.getTable()));
    assertTrue(report.getHoles().get(0).getSecond().getTable().equals(T2));
    assertEquals(0, report.getOverlaps().size());
    // Next, add overlaps to first row in t3
    List<RegionInfo> t3Ris = MetaTableAccessor.getTableRegions(TEST_UTIL.getConnection(), T3);
    RegionInfo ri = t3Ris.get(0);
    RegionInfo newRi1 = RegionInfoBuilder.newBuilder(ri.getTable())
      .setStartKey(incrementRow(ri.getStartKey())).setEndKey(incrementRow(ri.getEndKey())).build();
    Put p1 = MetaTableAccessor.makePutFromRegionInfo(newRi1, System.currentTimeMillis());
    RegionInfo newRi2 = RegionInfoBuilder.newBuilder(newRi1.getTable())
      .setStartKey(incrementRow(newRi1.getStartKey())).setEndKey(incrementRow(newRi1.getEndKey()))
      .build();
    Put p2 = MetaTableAccessor.makePutFromRegionInfo(newRi2, System.currentTimeMillis());
    MetaTableAccessor.putsToMetaTable(TEST_UTIL.getConnection(), Arrays.asList(p1, p2));
    janitor.scan();
    report = janitor.getLastReport();
    assertFalse(report.isEmpty());
    // We added two overlaps so total three.
    assertEquals(3, report.getOverlaps().size());
    // Assert hole is still there.
    assertEquals(1, report.getHoles().size());
    // Assert other attributes are empty still.
    assertTrue(report.getEmptyRegionInfo().isEmpty());
    assertTrue(report.getUnknownServers().isEmpty());
    // Now make bad server in t1.
    List<RegionInfo> t1Ris = MetaTableAccessor.getTableRegions(TEST_UTIL.getConnection(), T1);
    RegionInfo t1Ri1 = t1Ris.get(1);
    Put pServer = new Put(t1Ri1.getRegionName());
    pServer.addColumn(HConstants.CATALOG_FAMILY, MetaTableAccessor.getServerColumn(0),
      Bytes.toBytes("bad.server.example.org:1234"));
    MetaTableAccessor.putsToMetaTable(TEST_UTIL.getConnection(), Arrays.asList(pServer));
    janitor.scan();
    report = janitor.getLastReport();
    assertFalse(report.isEmpty());
    assertEquals(1, report.getUnknownServers().size());
    // Test what happens if we blow away an info:server row, if it is null. Should not kill CJ
    // and we should log the row that had the problem. HBASE-23192. Just make sure we don't
    // break if this happens.
    LOG.info("Make null info:server");
    Put emptyInfoServerPut = new Put(t1Ri1.getRegionName());
    emptyInfoServerPut.addColumn(HConstants.CATALOG_FAMILY, MetaTableAccessor.getServerColumn(0),
      Bytes.toBytes(""));
    MetaTableAccessor.putsToMetaTable(TEST_UTIL.getConnection(), Arrays.asList(emptyInfoServerPut));
    janitor.scan();
    report = janitor.getLastReport();
    assertEquals(0, report.getUnknownServers().size());
    // Mke an empty regioninfo in t1.
    RegionInfo t1Ri2 = t1Ris.get(2);
    Put pEmptyRI = new Put(t1Ri2.getRegionName());
    pEmptyRI.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
      HConstants.EMPTY_BYTE_ARRAY);
    MetaTableAccessor.putsToMetaTable(TEST_UTIL.getConnection(), Arrays.asList(pEmptyRI));
    janitor.scan();
    report = janitor.getLastReport();
    assertEquals(1, report.getEmptyRegionInfo().size());

    int holesReported = report.getHoles().size();
    int overlapsReported = report.getOverlaps().size();

    // Test the case for T4
    // r1: [aa, bb), r2: [cc, dd), r3: [a, cc)
    // Make sure only overlaps and no holes are reported.
    List<RegionInfo> t4Ris = MetaTableAccessor.getTableRegions(TEST_UTIL.getConnection(), T4);
    // delete the region [bb, cc)
    MetaTableAccessor.deleteRegionInfo(TEST_UTIL.getConnection(), t4Ris.get(2));

    // add a new region [a, cc)
    RegionInfo newRiT4 = RegionInfoBuilder.newBuilder(T4).setStartKey("a".getBytes())
      .setEndKey("cc".getBytes()).build();
    Put putForT4 = MetaTableAccessor.makePutFromRegionInfo(newRiT4, System.currentTimeMillis());
    MetaTableAccessor.putsToMetaTable(TEST_UTIL.getConnection(), Arrays.asList(putForT4));

    janitor.scan();
    report = janitor.getLastReport();
    // there is no new hole reported, 2 more overLaps added.
    assertEquals(holesReported, report.getHoles().size());
    assertEquals(overlapsReported + 2, report.getOverlaps().size());

    holesReported = report.getHoles().size();
    overlapsReported = report.getOverlaps().size();

    // Test the case for T5
    // r0: [, bb), r1: [a, g), r2: [bb, cc), r3: [dd, )
    // Make sure only overlaps and no holes are reported.
    List<RegionInfo> t5Ris = MetaTableAccessor.getTableRegions(TEST_UTIL.getConnection(), T5);
    // delete the region [cc, dd)
    MetaTableAccessor.deleteRegionInfo(TEST_UTIL.getConnection(), t5Ris.get(2));

    // add a new region [a, g)
    RegionInfo newRiT5 = RegionInfoBuilder.newBuilder(T5).setStartKey("a".getBytes())
      .setEndKey("g".getBytes()).build();
    Put putForT5 = MetaTableAccessor.makePutFromRegionInfo(newRiT5, System.currentTimeMillis());
    MetaTableAccessor.putsToMetaTable(TEST_UTIL.getConnection(), Arrays.asList(putForT5));

    janitor.scan();
    report = janitor.getLastReport();
    // there is no new hole reported, 3 more overLaps added.
    // ([a, g), [, bb)), ([a, g), [bb, cc)), ([a, g), [dd, ))
    assertEquals(holesReported, report.getHoles().size());
    assertEquals(overlapsReported + 3, report.getOverlaps().size());
  }

  /**
   * Take last byte and add one to it.
   */
  private static byte[] incrementRow(byte[] row) {
    if (row.length == 0) {
      return new byte[] { '0' };
    }
    row[row.length - 1] = (byte) (((int) row[row.length - 1]) + 1);
    return row;
  }

  @Test
  public void testHoles() throws IOException {
    CatalogJanitor janitor = TEST_UTIL.getHBaseCluster().getMaster().getCatalogJanitor();

    Report report = janitor.getLastReport();
    // Assert no problems.
    assertTrue(report.isEmpty());
    // Verify start and end region holes
    verifyCornerHoles(janitor, T1);
    // Verify start and end region holes
    verifyCornerHoles(janitor, T2);
    verifyMiddleHole(janitor);
    // Verify that MetaFixer is able to fix these holes
    fixHoles(janitor);
  }

  private void fixHoles(CatalogJanitor janitor) throws IOException {
    MetaFixer metaFixer = new MetaFixer(TEST_UTIL.getHBaseCluster().getMaster());
    janitor.scan();
    Report report = janitor.getLastReport();
    // Verify total number of holes, 2 in t1 and t2 each and one in t3
    assertEquals("Number of holes are not matching", 5, report.getHoles().size());
    metaFixer.fix();
    janitor.scan();
    report = janitor.getLastReport();
    assertEquals("Holes are not fixed", 0, report.getHoles().size());
  }

  private void verifyMiddleHole(CatalogJanitor janitor) throws IOException {
    // Verify middle holes
    RegionInfo firstRegion = getRegionInfo(T3, "".getBytes());
    RegionInfo secondRegion = getRegionInfo(T3, "bbb".getBytes());
    RegionInfo thirdRegion = getRegionInfo(T3, "ccc".getBytes());
    MetaTableAccessor.deleteRegionInfo(TEST_UTIL.getConnection(), secondRegion);
    LinkedList<Pair<RegionInfo, RegionInfo>> holes = getHoles(janitor, T3);
    Pair<RegionInfo, RegionInfo> regionInfoRegionInfoPair = holes.getFirst();
    assertTrue(regionInfoRegionInfoPair.getFirst().getTable().equals(T3));
    assertTrue(regionInfoRegionInfoPair.getSecond().getTable().equals(T3));
    assertTrue(
      regionInfoRegionInfoPair.getFirst().getEncodedName().equals(firstRegion.getEncodedName()));
    assertTrue(
      regionInfoRegionInfoPair.getSecond().getEncodedName().equals(thirdRegion.getEncodedName()));
  }

  private void verifyCornerHoles(CatalogJanitor janitor, TableName tableName) throws IOException {
    RegionInfo firstRegion = getRegionInfo(tableName, "".getBytes());
    RegionInfo secondRegion = getRegionInfo(tableName, "bbb".getBytes());
    MetaTableAccessor.deleteRegionInfo(TEST_UTIL.getConnection(), firstRegion);
    LinkedList<Pair<RegionInfo, RegionInfo>> holes = getHoles(janitor, tableName);

    assertEquals(1, holes.size());
    Pair<RegionInfo, RegionInfo> regionInfoRegionInfoPair = holes.get(0);
    assertTrue(regionInfoRegionInfoPair.getFirst().getTable()
      .equals(RegionInfoBuilder.UNDEFINED.getTable()));
    assertTrue(regionInfoRegionInfoPair.getSecond().getTable().equals(tableName));
    assertTrue(
      regionInfoRegionInfoPair.getSecond().getEncodedName().equals(secondRegion.getEncodedName()));

    RegionInfo lastRegion = getRegionInfo(tableName, "zzz".getBytes());
    RegionInfo secondLastRegion = getRegionInfo(tableName, "yyy".getBytes());
    MetaTableAccessor.deleteRegionInfo(TEST_UTIL.getConnection(), lastRegion);
    holes = getHoles(janitor, tableName);
    assertEquals(2, holes.size());
    regionInfoRegionInfoPair = holes.get(1);
    assertTrue(regionInfoRegionInfoPair.getFirst().getEncodedName()
      .equals(secondLastRegion.getEncodedName()));
    assertTrue(regionInfoRegionInfoPair.getSecond().getTable()
      .equals(RegionInfoBuilder.UNDEFINED.getTable()));
  }

  // Get Holes filter by table
  private LinkedList<Pair<RegionInfo, RegionInfo>> getHoles(CatalogJanitor janitor,
    TableName tableName) throws IOException {
    janitor.scan();
    Report lastReport = janitor.getLastReport();
    assertFalse(lastReport.isEmpty());
    LinkedList<Pair<RegionInfo, RegionInfo>> holes = new LinkedList<>();
    for (Pair<RegionInfo, RegionInfo> hole : lastReport.getHoles()) {
      if (hole.getFirst().getTable().equals(tableName) ||
        hole.getSecond().getTable().equals(tableName)) {
        holes.add(hole);
      }
    }
    return holes;
  }

  private RegionInfo getRegionInfo(TableName tableName, byte[] row) throws IOException {
    RegionInfo regionInfo =
      TEST_UTIL.getConnection().getRegionLocator(tableName).getRegionLocation(row).getRegion();
    assertNotNull(regionInfo);
    return regionInfo;
  }
}
