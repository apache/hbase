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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertNotEquals;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

// Categorized as a large test so not run as part of general 'test' suite (which is small
// and mediums). This test fails if networking is odd -- say if you are connected to a
// VPN... See HBASE-23850
@Category({ LargeTests.class, ClientTests.class })
public class TestAsyncTableRSCrashPublish {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncTableRSCrashPublish.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static TableName TABLE_NAME = TableName.valueOf("Publish");

  private static byte[] FAMILY = Bytes.toBytes("family");

  @BeforeClass
  public static void beforeClass() throws Exception {
    UTIL.getConfiguration().setBoolean(HConstants.STATUS_PUBLISHED, true);
    /* Below is code for choosing a NetworkInterface and then setting it into
      configs so can be picked up by the client and server.
    String niName = UTIL.getConfiguration().get(HConstants.STATUS_MULTICAST_NI_NAME);
    NetworkInterface ni;
    if (niName != null) {
      ni = NetworkInterface.getByName(niName);
    } else {
      String mcAddress = UTIL.getConfiguration().get(HConstants.STATUS_MULTICAST_ADDRESS,
        HConstants.DEFAULT_STATUS_MULTICAST_ADDRESS);
      InetAddress ina = InetAddress.getByName(mcAddress);
      boolean inet6Address = ina instanceof Inet6Address;
      ni = NetworkInterface.getByInetAddress(inet6Address?
        Addressing.getIp6Address(): Addressing.getIp4Address());
    }
    UTIL.getConfiguration().set(HConstants.STATUS_MULTICAST_NI_NAME, ni.getName());
    */
    UTIL.startMiniCluster(2);
    UTIL.createTable(TABLE_NAME, FAMILY);
    UTIL.waitTableAvailable(TABLE_NAME);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Ignore @Test
  public void test() throws IOException, ExecutionException, InterruptedException {
    Configuration conf = UTIL.getHBaseCluster().getMaster().getConfiguration();
    try (AsyncConnection connection = ConnectionFactory.createAsyncConnection(conf).get()) {
      AsyncNonMetaRegionLocator locator =
        ((AsyncConnectionImpl) connection).getLocator().getNonMetaRegionLocator();
      connection.getTable(TABLE_NAME).get(new Get(Bytes.toBytes(0))).join();
      ServerName serverName =
        locator.getRegionLocationInCache(TABLE_NAME, HConstants.EMPTY_START_ROW)
        .getDefaultRegionLocation().getServerName();
      UTIL.getMiniHBaseCluster().stopRegionServer(serverName);
      UTIL.waitFor(60000,
        () -> locator.getRegionLocationInCache(TABLE_NAME, HConstants.EMPTY_START_ROW) == null);
      connection.getTable(TABLE_NAME).get(new Get(Bytes.toBytes(0))).join();
      assertNotEquals(serverName,
        locator.getRegionLocationInCache(TABLE_NAME, HConstants.EMPTY_START_ROW)
          .getDefaultRegionLocation().getServerName());
    }
  }
}
