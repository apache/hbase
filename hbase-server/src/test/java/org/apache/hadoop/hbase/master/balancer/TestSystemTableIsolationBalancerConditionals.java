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
package org.apache.hadoop.hbase.master.balancer;

import static org.apache.hadoop.hbase.master.balancer.BalancerConditionalsTestUtil.getTableToServers;
import static org.apache.hadoop.hbase.master.balancer.BalancerConditionalsTestUtil.validateAssertionsWithRetries;
import static org.apache.hadoop.hbase.master.balancer.BalancerConditionalsTestUtil.validateRegionLocations;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.quotas.QuotaUtil;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableSet;

@Category(LargeTests.class)
public class TestSystemTableIsolationBalancerConditionals {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSystemTableIsolationBalancerConditionals.class);

  private static final Logger LOG =
    LoggerFactory.getLogger(TestSystemTableIsolationBalancerConditionals.class);
  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  // One for product table, one for meta, one for other system tables, and one extra
  private static final int NUM_SERVERS = 3;
  private static final int PRODUCT_TABLE_REGIONS_PER_SERVER = 5;

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(BalancerConditionals.ISOLATE_SYSTEM_TABLES_KEY, true);
    TEST_UTIL.getConfiguration().setBoolean(BalancerConditionals.ISOLATE_META_TABLE_KEY, true);
    TEST_UTIL.getConfiguration().setBoolean(QuotaUtil.QUOTA_CONF_KEY, true);
    TEST_UTIL.getConfiguration().setLong(HConstants.HBASE_BALANCER_PERIOD, 1000L);
    TEST_UTIL.getConfiguration().setBoolean("hbase.master.balancer.stochastic.runMaxSteps", true);

    TEST_UTIL.startMiniCluster(NUM_SERVERS);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testTableIsolation() throws Exception {
    Connection connection = TEST_UTIL.getConnection();
    Admin admin = connection.getAdmin();

    // Create "product" table with 3 regions
    TableName productTableName = TableName.valueOf("product");
    TableDescriptor productTableDescriptor = TableDescriptorBuilder.newBuilder(productTableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("0")).build())
      .build();
    admin.createTable(productTableDescriptor,
      BalancerConditionalsTestUtil.generateSplits(PRODUCT_TABLE_REGIONS_PER_SERVER * NUM_SERVERS));

    Set<TableName> tablesToBeSeparated = ImmutableSet.<TableName> builder()
      .add(TableName.META_TABLE_NAME).add(QuotaUtil.QUOTA_TABLE_NAME).add(productTableName).build();

    // Pause the balancer
    admin.balancerSwitch(false, true);

    // Move all regions (product, meta, and quotas) to one RegionServer
    List<RegionInfo> allRegions = tablesToBeSeparated.stream().map(t -> {
      try {
        return admin.getRegions(t);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }).flatMap(Collection::stream).toList();
    String targetServer =
      TEST_UTIL.getHBaseCluster().getRegionServer(0).getServerName().getServerName();
    for (RegionInfo region : allRegions) {
      admin.move(region.getEncodedNameAsBytes(), Bytes.toBytes(targetServer));
    }

    validateAssertionsWithRetries(TEST_UTIL, false,
      () -> validateRegionLocations(getTableToServers(connection, tablesToBeSeparated),
        productTableName, false));

    // Unpause the balancer and run it
    admin.balancerSwitch(true, true);
    admin.balance();

    validateAssertionsWithRetries(TEST_UTIL, true,
      () -> validateRegionLocations(getTableToServers(connection, tablesToBeSeparated),
        productTableName, true));
  }
}
