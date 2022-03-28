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
package org.apache.hadoop.hbase.master.region;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Make sure we do not loss data after changing SFT implementation
 */
@Category({ MasterTests.class, MediumTests.class })
public class TestChangeSFTForMasterRegion {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestChangeSFTForMasterRegion.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  private static TableName NAME = TableName.valueOf("test");

  private static byte[] FAMILY = Bytes.toBytes("family");

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.getConfiguration().set(MasterRegionFactory.TRACKER_IMPL,
      StoreFileTrackerFactory.Trackers.DEFAULT.name());
    // use zk connection registry, as we will shutdown the only master instance which will likely to
    // lead to dead loop
    UTIL.getConfiguration().set(HConstants.CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY,
      HConstants.ZK_CONNECTION_REGISTRY_CLASS);
    UTIL.startMiniCluster(1);
    UTIL.createTable(NAME, FAMILY).close();
  }

  @AfterClass
  public static void tearDown() throws IOException {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void test() throws Exception {
    // shutdown master
    UTIL.getMiniHBaseCluster().stopMaster(0).join();
    UTIL.getMiniHBaseCluster().getConf().set(MasterRegionFactory.TRACKER_IMPL,
      StoreFileTrackerFactory.Trackers.FILE.name());
    UTIL.getMiniHBaseCluster().startMaster();
    // make sure that the table still exists
    UTIL.waitTableAvailable(NAME);
    // confirm that we have changed the SFT to FILE
    TableDescriptor td =
      UTIL.getMiniHBaseCluster().getMaster().getMasterRegion().region.getTableDescriptor();
    assertEquals(StoreFileTrackerFactory.Trackers.FILE.name(),
      td.getValue(StoreFileTrackerFactory.TRACKER_IMPL));
  }
}
