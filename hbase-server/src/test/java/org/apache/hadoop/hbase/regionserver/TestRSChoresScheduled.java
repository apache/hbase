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

import java.lang.reflect.Field;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests to validate if HRegionServer default chores are scheduled
 */
@Category({RegionServerTests.class, MediumTests.class})
public class TestRSChoresScheduled {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRSChoresScheduled.class);

  private static HRegionServer hRegionServer;

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.startMiniCluster(StartMiniClusterOption.builder().numRegionServers(1).build());
    hRegionServer = UTIL.getMiniHBaseCluster().getRegionServer(0);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  private static class TestChoreField<E extends ScheduledChore> {

    private E getChoreObj(String fieldName) throws NoSuchFieldException,
      IllegalAccessException {
      Field hRegionServerField = HRegionServer.class.getDeclaredField(fieldName);
      hRegionServerField.setAccessible(true);
      E choreFieldVal = (E) hRegionServerField.get(hRegionServer);
      return choreFieldVal;
    }

    private void testIfChoreScheduled(E choreObj) {
      Assert.assertNotNull(choreObj);
      Assert.assertTrue(hRegionServer.getChoreService().isChoreScheduled(choreObj));
    }

  }

  @Test
  public void testDefaultScheduledChores() throws Exception {
    // test if compactedHFilesDischarger chore is scheduled by default in HRegionServer init
    TestChoreField<CompactedHFilesDischarger> compactedHFilesDischargerTestChoreField =
      new TestChoreField<>();
    CompactedHFilesDischarger compactedHFilesDischarger =
      compactedHFilesDischargerTestChoreField.getChoreObj("compactedFileDischarger");
    compactedHFilesDischargerTestChoreField.testIfChoreScheduled(compactedHFilesDischarger);

    // test if compactionChecker chore is scheduled by default in HRegionServer init
    TestChoreField<ScheduledChore> compactionCheckerTestChoreField = new TestChoreField<>();
    ScheduledChore compactionChecker =
      compactionCheckerTestChoreField.getChoreObj("compactionChecker");
    compactionCheckerTestChoreField.testIfChoreScheduled(compactionChecker);

    // test if periodicFlusher chore is scheduled by default in HRegionServer init
    TestChoreField<ScheduledChore> periodicMemstoreFlusherTestChoreField =
      new TestChoreField<>();
    ScheduledChore periodicFlusher =
      periodicMemstoreFlusherTestChoreField.getChoreObj("periodicFlusher");
    periodicMemstoreFlusherTestChoreField.testIfChoreScheduled(periodicFlusher);

    // test if nonceManager chore is scheduled by default in HRegionServer init
    TestChoreField<ScheduledChore> nonceManagerTestChoreField = new TestChoreField<>();
    ScheduledChore nonceManagerChore =
      nonceManagerTestChoreField.getChoreObj("nonceManagerChore");
    nonceManagerTestChoreField.testIfChoreScheduled(nonceManagerChore);

  }

}
