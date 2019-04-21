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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertNotEquals;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

@Category({ MediumTests.class, ClientTests.class })
public class TestAsyncTableRSCrashPublish {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncTableRSCrashPublish.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static AsyncConnectionImpl CONN;

  private static TableName TABLE_NAME = TableName.valueOf("Publish");

  private static byte[] FAMILY = Bytes.toBytes("family");

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.getConfiguration().setBoolean(HConstants.STATUS_PUBLISHED, true);
    UTIL.startMiniCluster(2);
    UTIL.createTable(TABLE_NAME, FAMILY);
    UTIL.waitTableAvailable(TABLE_NAME);
    CONN =
      (AsyncConnectionImpl) ConnectionFactory.createAsyncConnection(UTIL.getConfiguration()).get();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    Closeables.close(CONN, true);
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void test() throws IOException {
    AsyncNonMetaRegionLocator locator = CONN.getLocator().getNonMetaRegionLocator();
    CONN.getTable(TABLE_NAME).get(new Get(Bytes.toBytes(0))).join();
    ServerName serverName = locator.getRegionLocationInCache(TABLE_NAME, HConstants.EMPTY_START_ROW)
      .getDefaultRegionLocation().getServerName();
    UTIL.getMiniHBaseCluster().stopRegionServer(serverName);
    UTIL.waitFor(60000,
      () -> locator.getRegionLocationInCache(TABLE_NAME, HConstants.EMPTY_START_ROW) == null);
    CONN.getTable(TABLE_NAME).get(new Get(Bytes.toBytes(0))).join();
    assertNotEquals(serverName,
      locator.getRegionLocationInCache(TABLE_NAME, HConstants.EMPTY_START_ROW)
        .getDefaultRegionLocation().getServerName());
  }
}
