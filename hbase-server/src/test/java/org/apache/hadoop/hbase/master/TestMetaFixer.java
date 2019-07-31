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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Category({MasterTests.class, LargeTests.class})
public class TestMetaFixer {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMetaFixer.class);
  @Rule
  public TestName name = new TestName();
  private static final Logger LOG = LoggerFactory.getLogger(TestMetaFixer.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  private void deleteRegion(MasterServices services, RegionInfo ri) throws IOException {
    MetaTableAccessor.deleteRegionInfo(TEST_UTIL.getConnection(), ri);
    // Delete it from Master context too else it sticks around.
    services.getAssignmentManager().getRegionStates().deleteRegion(ri);
  }

  @Test
  public void testPlugsHoles() throws IOException {
    TableName tn = TableName.valueOf(this.name.getMethodName());
    TEST_UTIL.createMultiRegionTable(tn, HConstants.CATALOG_FAMILY);
    List<RegionInfo> ris = MetaTableAccessor.getTableRegions(TEST_UTIL.getConnection(), tn);
    MasterServices services = TEST_UTIL.getHBaseCluster().getMaster();
    services.getCatalogJanitor().scan();
    CatalogJanitor.Report report = services.getCatalogJanitor().getLastReport();
    Assert.assertTrue(report.isEmpty());
    int originalCount = ris.size();
    // Remove first, last and middle region. See if hole gets plugged. Table has 26 regions.
    deleteRegion(services, ris.get(ris.size() -1));
    deleteRegion(services, ris.get(3));
    deleteRegion(services, ris.get(0));
    ris = MetaTableAccessor.getTableRegions(TEST_UTIL.getConnection(), tn);
    services.getCatalogJanitor().scan();
    report = services.getCatalogJanitor().getLastReport();
    Assert.assertEquals(report.toString(), 3, report.getHoles().size());
    MetaFixer fixer = new MetaFixer(services);
    Assert.assertTrue(fixer.fixHoles(report));
    services.getCatalogJanitor().scan();
    report = services.getCatalogJanitor().getLastReport();
    Assert.assertTrue(report.toString(), report.isEmpty());
    // Disable and reenable so the added regions get reassigned.
    TEST_UTIL.getAdmin().disableTable(tn);
    TEST_UTIL.getAdmin().enableTable(tn);
    ris = MetaTableAccessor.getTableRegions(TEST_UTIL.getConnection(), tn);
    Assert.assertEquals(originalCount, ris.size());
  }

  /**
   * Just make sure running fixMeta does right thing for the case
   * of a single-region Table where the region gets dropped.
   * There is nothing much we can do. We can't restore what
   * we don't know about (at least from a read of hbase:meta).
   */
  @Test
  public void testOneRegionTable() throws IOException {
    TableName tn = TableName.valueOf(this.name.getMethodName());
    TEST_UTIL.createTable(tn, HConstants.CATALOG_FAMILY);
    List<RegionInfo> ris = MetaTableAccessor.getTableRegions(TEST_UTIL.getConnection(), tn);
    MasterServices services = TEST_UTIL.getHBaseCluster().getMaster();
    services.getCatalogJanitor().scan();
    CatalogJanitor.Report report = services.getCatalogJanitor().getLastReport();
    int originalCount = ris.size();
    deleteRegion(services, ris.get(0));
    services.getCatalogJanitor().scan();
    report = services.getCatalogJanitor().getLastReport();
    ris = MetaTableAccessor.getTableRegions(TEST_UTIL.getConnection(), tn);
    Assert.assertTrue(ris.isEmpty());
    MetaFixer fixer = new MetaFixer(services);
    Assert.assertFalse(fixer.fixHoles(report));
    report = services.getCatalogJanitor().getLastReport();
    Assert.assertTrue(report.isEmpty());
    ris = MetaTableAccessor.getTableRegions(TEST_UTIL.getConnection(), tn);
    Assert.assertEquals(0, ris.size());
  }
}
