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

import static org.junit.Assert.assertNotNull;

import java.util.concurrent.ExecutionException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
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
public class TestAsyncTableLocatePrefetch {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncTableLocatePrefetch.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static TableName TABLE_NAME = TableName.valueOf("async");

  private static byte[] FAMILY = Bytes.toBytes("cf");

  private static AsyncConnection CONN;

  private static AsyncNonMetaRegionLocator LOCATOR;

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.getConfiguration().setInt(AsyncNonMetaRegionLocator.LOCATE_PREFETCH_LIMIT, 100);
    TEST_UTIL.startMiniCluster(3);
    TEST_UTIL.createMultiRegionTable(TABLE_NAME, FAMILY);
    TEST_UTIL.waitTableAvailable(TABLE_NAME);
    CONN = ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration()).get();
    LOCATOR = new AsyncNonMetaRegionLocator((AsyncConnectionImpl) CONN);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    Closeables.close(CONN, true);
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void test() throws InterruptedException, ExecutionException {
    assertNotNull(LOCATOR.getRegionLocations(TABLE_NAME, Bytes.toBytes("zzz"),
      RegionReplicaUtil.DEFAULT_REPLICA_ID, RegionLocateType.CURRENT, false).get());
    // we finish the request before we adding the remaining results to cache so sleep a bit here
    Thread.sleep(1000);
    // confirm that the locations of all the regions have been cached.
    assertNotNull(LOCATOR.getRegionLocationInCache(TABLE_NAME, Bytes.toBytes("aaa")));
    for (byte[] row : HBaseTestingUtility.KEYS_FOR_HBA_CREATE_TABLE) {
      assertNotNull(LOCATOR.getRegionLocationInCache(TABLE_NAME, row));
    }
  }
}
