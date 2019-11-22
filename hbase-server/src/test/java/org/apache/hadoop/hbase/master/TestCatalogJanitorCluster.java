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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MasterTests.class, LargeTests.class})
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

  @Before
  public void before() throws Exception {
    TEST_UTIL.startMiniCluster();
    TEST_UTIL.createMultiRegionTable(T1, new byte [][] {HConstants.CATALOG_FAMILY});
    TEST_UTIL.createMultiRegionTable(T2, new byte [][] {HConstants.CATALOG_FAMILY});
    TEST_UTIL.createMultiRegionTable(T3, new byte [][] {HConstants.CATALOG_FAMILY});
  }

  @After
  public void after() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Fat method where we start with a fat hbase:meta and then gradually intro
   * problems running catalogjanitor for each to ensure it triggers complaint.
   * Do one big method because takes a while to build up the context we need.
   * We create three tables and then make holes, overlaps, add unknown servers
   * and empty out regioninfo columns. Each should up counts in the
   * CatalogJanitor.Report produced.
   */
  @Test
  public void testConsistency() throws IOException {
    CatalogJanitor janitor = TEST_UTIL.getHBaseCluster().getMaster().getCatalogJanitor();
    int gc = janitor.scan();
    CatalogJanitor.Report report = janitor.getLastReport();
    // Assert no problems.
    assertTrue(report.isEmpty());
    // Now remove first region in table t2 to see if catalogjanitor scan notices.
    List<RegionInfo> t2Ris = MetaTableAccessor.getTableRegions(TEST_UTIL.getConnection(), T2);
    MetaTableAccessor.deleteRegionInfo(TEST_UTIL.getConnection(), t2Ris.get(0));
    gc = janitor.scan();
    report = janitor.getLastReport();
    assertFalse(report.isEmpty());
    assertEquals(1, report.getHoles().size());
    assertTrue(report.getHoles().get(0).getFirst().getTable().equals(T1));
    assertTrue(report.getHoles().get(0).getFirst().isLast());
    assertTrue(report.getHoles().get(0).getSecond().getTable().equals(T2));
    assertEquals(0, report.getOverlaps().size());
    // Next, add overlaps to first row in t3
    List<RegionInfo> t3Ris = MetaTableAccessor.getTableRegions(TEST_UTIL.getConnection(), T3);
    RegionInfo ri = t3Ris.get(0);
    RegionInfo newRi1 = RegionInfoBuilder.newBuilder(ri.getTable()).
        setStartKey(incrementRow(ri.getStartKey())).
        setEndKey(incrementRow(ri.getEndKey())).build();
    Put p1 = MetaTableAccessor.makePutFromRegionInfo(newRi1, System.currentTimeMillis());
    RegionInfo newRi2 = RegionInfoBuilder.newBuilder(newRi1.getTable()).
        setStartKey(incrementRow(newRi1.getStartKey())).
        setEndKey(incrementRow(newRi1.getEndKey())).build();
    Put p2 = MetaTableAccessor.makePutFromRegionInfo(newRi2, System.currentTimeMillis());
    MetaTableAccessor.putsToMetaTable(TEST_UTIL.getConnection(), Arrays.asList(p1, p2));
    gc = janitor.scan();
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
    pServer.addColumn(MetaTableAccessor.getCatalogFamily(),
        MetaTableAccessor.getServerColumn(0), Bytes.toBytes("bad.server.example.org:1234"));
    MetaTableAccessor.putsToMetaTable(TEST_UTIL.getConnection(), Arrays.asList(pServer));
    gc = janitor.scan();
    report = janitor.getLastReport();
    assertFalse(report.isEmpty());
    assertEquals(0, report.getUnknownServers().size());
    // Test what happens if we blow away an info:server row, if it is null. Should not kill CJ
    // and we should log the row that had the problem. HBASE-23192. Just make sure we don't
    // break if this happens.
    LOG.info("Make null info:server");
    Put emptyInfoServerPut = new Put(t1Ri1.getRegionName());
    emptyInfoServerPut.addColumn(MetaTableAccessor.getCatalogFamily(),
        MetaTableAccessor.getServerColumn(0), Bytes.toBytes(""));
    MetaTableAccessor.putsToMetaTable(TEST_UTIL.getConnection(), Arrays.asList(emptyInfoServerPut));
    gc = janitor.scan();
    report = janitor.getLastReport();
    assertEquals(0, report.getUnknownServers().size());
    // Finally, make an empty regioninfo in t1.
    RegionInfo t1Ri2 = t1Ris.get(2);
    Put pEmptyRI = new Put(t1Ri2.getRegionName());
    pEmptyRI.addColumn(MetaTableAccessor.getCatalogFamily(),
        MetaTableAccessor.getRegionInfoColumn(), HConstants.EMPTY_BYTE_ARRAY);
    MetaTableAccessor.putsToMetaTable(TEST_UTIL.getConnection(), Arrays.asList(pEmptyRI));
    gc = janitor.scan();
    report = janitor.getLastReport();
    assertEquals(1, report.getEmptyRegionInfo().size());
  }

  /**
   * Take last byte and add one to it.
   */
  private static byte [] incrementRow(byte [] row) {
    if (row.length == 0) {
      return new byte []{'0'};
    }
    row[row.length - 1] = (byte)(((int)row[row.length - 1]) + 1);
    return row;
  }
}
