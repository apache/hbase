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
package org.apache.hadoop.hbase.master.procedure;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.TableNamespaceManager;
import org.apache.hadoop.hbase.quotas.QuotaUtil;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MasterTests.TAG)
@Tag(LargeTests.TAG)
public class TestTruncateTableNamespaceQuota {
  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  @BeforeAll
  public static void startCluster() throws Exception {
    UTIL.getConfiguration().setBoolean(QuotaUtil.QUOTA_CONF_KEY, true);
    UTIL.startMiniCluster(1);
    UTIL.waitFor(30000,
      () -> UTIL.getMiniHBaseCluster().getMaster().getMasterQuotaManager().isQuotaInitialized());
  }

  @AfterAll
  public static void stopCluster() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testSplitAfterTruncateWithoutPreservingSplits() throws Exception {
    String namespace = "truncate_region_quota";
    TableName tableName = TableName.valueOf(namespace, "t");
    Admin admin = UTIL.getAdmin();
    admin.createNamespace(NamespaceDescriptor.create(namespace)
      .addConfiguration(TableNamespaceManager.KEY_MAX_REGIONS, "2").build());
    TableDescriptor descriptor = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("f")).build();
    admin.createTable(descriptor, new byte[][] { Bytes.toBytes("m") });
    assertEquals(2, admin.getRegions(tableName).size());
    assertEquals(2, cachedRegionCount(tableName));

    admin.disableTable(tableName);
    admin.truncateTable(tableName, false);
    assertEquals(1, admin.getRegions(tableName).size());
    assertEquals(1, cachedRegionCount(tableName));

    RegionInfo region = admin.getRegions(tableName).get(0);
    admin.splitRegionAsync(region.getRegionName(), Bytes.toBytes("m")).get(2, TimeUnit.MINUTES);
    assertEquals(2, admin.getRegions(tableName).size());
    assertEquals(2, cachedRegionCount(tableName));
  }

  @Test
  public void testTruncateStillReducesUsageAfterQuotaIsLowered() throws Exception {
    String namespace = "truncate_lowered_region_quota";
    TableName tableName = TableName.valueOf(namespace, "t");
    Admin admin = UTIL.getAdmin();
    admin.createNamespace(NamespaceDescriptor.create(namespace)
      .addConfiguration(TableNamespaceManager.KEY_MAX_REGIONS, "2").build());
    TableDescriptor descriptor = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of("f")).build();
    admin.createTable(descriptor, new byte[][] { Bytes.toBytes("m") });
    assertEquals(2, cachedRegionCount(tableName));

    NamespaceDescriptor loweredLimit = admin.getNamespaceDescriptor(namespace);
    loweredLimit.setConfiguration(TableNamespaceManager.KEY_MAX_REGIONS, "1");
    admin.modifyNamespace(loweredLimit);
    admin.disableTable(tableName);
    admin.truncateTable(tableName, false);

    assertEquals(1, admin.getRegions(tableName).size());
    assertEquals(1, cachedRegionCount(tableName));
  }

  private static int cachedRegionCount(TableName tableName) throws Exception {
    return UTIL.getMiniHBaseCluster().getMaster().getMasterQuotaManager()
      .getRegionCountOfTable(tableName);
  }
}
