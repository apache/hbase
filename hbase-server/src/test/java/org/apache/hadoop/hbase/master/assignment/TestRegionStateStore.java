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
package org.apache.hadoop.hbase.master.assignment;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@Category({ MasterTests.class, MediumTests.class })
public class TestRegionStateStore {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionStateStore.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRegionStateStore.class);

  protected HBaseTestingUtility util;

  @Before
  public void setup() throws Exception {
    util = new HBaseTestingUtility();
    util.startMiniCluster();
  }

  @After
  public void tearDown() throws Exception {
    util.shutdownMiniCluster();
  }

  @Test
  public void testVisitMetaForRegionExistingRegion() throws Exception {
    final TableName tableName = TableName.valueOf("testVisitMetaForRegion");
    util.createTable(tableName, "cf");
    final List<HRegion> regions = util.getHBaseCluster().getRegions(tableName);
    final String encodedName = regions.get(0).getRegionInfo().getEncodedName();
    final RegionStateStore regionStateStore = util.getHBaseCluster().getMaster().
      getAssignmentManager().getRegionStateStore();
    final AtomicBoolean visitorCalled = new AtomicBoolean(false);
    regionStateStore.visitMetaForRegion(encodedName, new RegionStateStore.RegionStateVisitor() {
      @Override
      public void visitRegionState(Result result, RegionInfo regionInfo, RegionState.State state,
        ServerName regionLocation, ServerName lastHost, long openSeqNum) {
        assertEquals(encodedName, regionInfo.getEncodedName());
        visitorCalled.set(true);
      }
    });
    assertTrue("Visitor has not been called.", visitorCalled.get());
  }

  @Test
  public void testVisitMetaForRegionNonExistingRegion() throws Exception {
    final String encodedName = "fakeencodedregionname";
    final RegionStateStore regionStateStore = util.getHBaseCluster().getMaster().
      getAssignmentManager().getRegionStateStore();
    final AtomicBoolean visitorCalled = new AtomicBoolean(false);
    regionStateStore.visitMetaForRegion(encodedName, new RegionStateStore.RegionStateVisitor() {
      @Override
      public void visitRegionState(Result result, RegionInfo regionInfo, RegionState.State state,
        ServerName regionLocation, ServerName lastHost, long openSeqNum) {
        visitorCalled.set(true);
      }
    });
    assertFalse("Visitor has been called, but it shouldn't.", visitorCalled.get());
  }

}
