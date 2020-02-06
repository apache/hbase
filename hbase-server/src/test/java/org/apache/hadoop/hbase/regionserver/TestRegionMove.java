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

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.DoNotRetryRegionException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;

/**
 * Test move fails when table disabled
 */
@Category({MediumTests.class})
public class TestRegionMove {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionMove.class);

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Rule
  public TestName name = new TestName();
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  public static Configuration CONF ;
  protected static final String F1 = "f1";

  // Test names
  protected TableName tableName;
  protected String method;

  @BeforeClass
  public static void startCluster() throws Exception {
    TEST_UTIL.startMiniCluster(2);
  }

  @AfterClass
  public static void stopCluster() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setup() throws IOException {
    CONF = TEST_UTIL.getConfiguration();
    method = name.getMethodName();
    tableName = TableName.valueOf(method);
  }

  @Test
  public void testDisableAndMove() throws Exception {
    Admin admin = TEST_UTIL.getAdmin();

    // Create a table with more than one region
    Table t = TEST_UTIL.createMultiRegionTable(tableName, Bytes.toBytes(F1), 10);
    TEST_UTIL.waitUntilAllRegionsAssigned(tableName);

    // Write an update to each region
    for (RegionInfo regionInfo : admin.getRegions(tableName)) {
      byte[] startKey = regionInfo.getStartKey();
      // The startKey of the first region is "empty", which would throw an error if we try to
      // Put that.
      byte[] rowKey = org.apache.hbase.thirdparty.com.google.common.primitives.Bytes.concat(
          startKey, Bytes.toBytes("1"));
      Put p = new Put(rowKey);
      p.addColumn(Bytes.toBytes(F1), Bytes.toBytes("q1"), Bytes.toBytes("value"));
      t.put(p);
    }

    // Get a Region which is on the first RS
    HRegionServer rs1 = TEST_UTIL.getRSForFirstRegionInTable(tableName);
    HRegionServer rs2 = TEST_UTIL.getOtherRegionServer(rs1);
    List<RegionInfo> regionsOnRS1ForTable = admin.getRegions(rs1.getServerName()).stream()
        .filter((regionInfo) -> regionInfo.getTable().equals(tableName))
        .collect(Collectors.toList());
    assertTrue(
        "Expected to find at least one region for " + tableName + " on " + rs1.getServerName()
        + ", but found none", !regionsOnRS1ForTable.isEmpty());
    final RegionInfo regionToMove = regionsOnRS1ForTable.get(0);

    // Offline the region and then try to move it. Should fail.
    admin.unassign(regionToMove.getRegionName(), true);
    try {
      admin.move(regionToMove.getEncodedNameAsBytes(), rs2.getServerName());
      fail();
    } catch (DoNotRetryRegionException e) {
      // We got expected exception
    }
    // Reassign for next stage of test.
    admin.assign(regionToMove.getRegionName());

    // Disable the table
    admin.disableTable(tableName);

    try {
      // Move the region to the other RS -- should fail
      admin.move(regionToMove.getEncodedNameAsBytes(), rs2.getServerName());
      fail();
    } catch (DoNotRetryIOException e) {
      // We got expected exception
    }
  }
}
