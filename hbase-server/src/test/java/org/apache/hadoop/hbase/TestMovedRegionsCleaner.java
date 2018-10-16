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
package org.apache.hadoop.hbase;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test whether background cleanup of MovedRegion entries is happening
 */
@Category({ MiscTests.class, MediumTests.class })
public class TestMovedRegionsCleaner {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMovedRegionsCleaner.class);

  private final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  public static int numCalls = 0;

  private static class TestMockRegionServer extends MiniHBaseCluster.MiniHBaseClusterRegionServer {

    public TestMockRegionServer(Configuration conf) throws IOException, InterruptedException {
      super(conf);
    }

    @Override
    protected int movedRegionCleanerPeriod() {
      return 500;
    }

    @Override protected void cleanMovedRegions() {
      // count the number of calls that are being made to this
      //
      numCalls++;
      super.cleanMovedRegions();
    }
  }

  @After public void after() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Before public void before() throws Exception {
    UTIL.getConfiguration()
        .setStrings(HConstants.REGION_SERVER_IMPL, TestMockRegionServer.class.getName());
    UTIL.startMiniCluster(1);
  }

  /**
   * Start the cluster, wait for some time and verify that the background
   * MovedRegion cleaner indeed gets called
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Test public void testMovedRegionsCleaner() throws IOException, InterruptedException {
    // We need to sleep long enough to trigger at least one round of background calls
    // to MovedRegionCleaner happen. Currently the period is set to 500ms.
    // Setting the sleep here for 2s just to be safe
    //
    UTIL.waitFor(2000, new Waiter.Predicate<IOException>() {
      @Override
      public boolean evaluate() throws IOException {

        // verify that there was at least one call to the cleanMovedRegions function
        //
        return numCalls > 0;
      }
    });
  }
}
