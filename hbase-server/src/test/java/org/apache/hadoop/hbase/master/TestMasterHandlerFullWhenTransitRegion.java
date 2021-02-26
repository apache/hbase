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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.Optional;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MasterTests.class, LargeTests.class })
public class TestMasterHandlerFullWhenTransitRegion {

  private static Logger LOG = LoggerFactory
      .getLogger(TestMasterHandlerFullWhenTransitRegion.class.getName());

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMasterHandlerFullWhenTransitRegion.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static final String TABLENAME = "table";

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.getConfiguration().setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        DelayOpenCP.class.getName());
    //set handler number to 1.
    UTIL.getConfiguration().setInt(HConstants.REGION_SERVER_HANDLER_COUNT, 1);
    UTIL.startMiniCluster(2);
    UTIL.createTable(TableName.valueOf(TABLENAME), "fa");
  }

  @Test
  public void test() throws Exception {
    RegionInfo regionInfo = UTIL.getAdmin().getRegions(TableName.valueOf(TABLENAME)).get(0);
    //See HBASE-21754
    //There is Only one handler, if ReportRegionStateTransitionRequest executes in the same kind
    // of thread with moveRegion, it will lock each other. Making the move operation can not finish.
    UTIL.getAdmin().move(regionInfo.getEncodedNameAsBytes());
    LOG.info("Region move complete");
  }


  /**
   * Make open region very slow
   */
  public static class DelayOpenCP implements RegionCoprocessor, RegionObserver {

    @Override
    public void preOpen(ObserverContext<RegionCoprocessorEnvironment> c) throws IOException {
      try {
        if (!c.getEnvironment().getRegion().getRegionInfo().getTable().isSystemTable()) {
          LOG.info("begin to sleep");
          Thread.sleep(10000);
          LOG.info("finish sleep");
        }
      } catch (Throwable t) {

      }
    }

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }
  }

}
