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
package org.apache.hadoop.hbase.namespace;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.TableNamespaceManager;
import org.apache.hadoop.hbase.quotas.QuotaUtil;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Regression test for HBASE-30483. Truncating a table without preserving splits must reconcile the
 * namespace quota cache with the new region layout, otherwise a later split can be rejected with a
 * QuotaExceededException even though the actual region count is within the namespace limit.
 */
@Tag(MediumTests.TAG)
public class TestTruncateTableNamespaceQuota {

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();
  private static Admin ADMIN;
  private static final String PREFIX = "TestTruncateTableNamespaceQuota";
  private static final byte[] FAMILY = Bytes.toBytes("fam");
  private static final byte[] SPLIT_KEY = Bytes.toBytes("m");

  @BeforeAll
  public static void setupBeforeClass() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.setBoolean(QuotaUtil.QUOTA_CONF_KEY, true);
    // Two region servers so that region replicas can be assigned.
    UTIL.startMiniCluster(2);
    TestNamespaceAuditor.waitForQuotaInitialize(UTIL);
    ADMIN = UTIL.getAdmin();
  }

  @AfterAll
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @AfterEach
  public void cleanup() throws Exception {
    for (TableDescriptor table : ADMIN.listTableDescriptors()) {
      if (ADMIN.isTableEnabled(table.getTableName())) {
        ADMIN.disableTable(table.getTableName());
      }
      ADMIN.deleteTable(table.getTableName());
    }
    for (NamespaceDescriptor ns : ADMIN.listNamespaceDescriptors()) {
      if (ns.getName().startsWith(PREFIX)) {
        ADMIN.deleteNamespace(ns.getName());
      }
    }
  }

  @Test
  public void testTruncateReconcilesQuotaAndAllowsSplit() throws Exception {
    String nsp = PREFIX + "_split";
    TableName tableName = createNamespaceAndTwoRegionTable(nsp, "2");
    assertEquals(2, ADMIN.getRegions(tableName).size());
    assertEquals(2, getCachedRegionCount(nsp, tableName));

    ADMIN.disableTable(tableName);
    ADMIN.truncateTable(tableName, false);

    assertEquals(1, ADMIN.getRegions(tableName).size());
    // Before HBASE-30483 the cache still recorded the pre-truncate count of 2 here.
    assertEquals(1, getCachedRegionCount(nsp, tableName));

    // Load rows on both sides of the split key and flush, so the region is splittable.
    try (Table table = UTIL.getConnection().getTable(tableName)) {
      table.put(new Put(Bytes.toBytes("a")).addColumn(FAMILY, FAMILY, FAMILY));
      table.put(new Put(Bytes.toBytes("z")).addColumn(FAMILY, FAMILY, FAMILY));
    }
    ADMIN.flush(tableName);
    HRegion regionToSplit = UTIL.getMiniHBaseCluster().getRegions(tableName).get(0);
    UTIL.waitFor(30000, regionToSplit::isSplittable);

    // Actual usage is 1 of maxregions=2, so this split must be allowed. Before HBASE-30483 it
    // failed with a QuotaExceededException because the cached count was already at the limit.
    ADMIN.splitRegionAsync(regionToSplit.getRegionInfo().getRegionName(), SPLIT_KEY).get(30,
      TimeUnit.SECONDS);

    UTIL.waitFor(30000, () -> ADMIN.getRegions(tableName).size() == 2);
    assertEquals(2, getCachedRegionCount(nsp, tableName));
  }

  @Test
  public void testTruncateSucceedsAfterQuotaLowered() throws Exception {
    String nsp = PREFIX + "_lowered";
    TableName tableName = createNamespaceAndTwoRegionTable(nsp, "2");
    assertEquals(2, getCachedRegionCount(nsp, tableName));

    // Lower the namespace limit below the table's current usage. This pins the design choice that
    // the truncate reconciliation performs no quota limit check: truncate reduces usage, so it
    // must complete and reconcile the cache even though the new limit is below the old count.
    ADMIN.modifyNamespace(NamespaceDescriptor.create(nsp)
      .addConfiguration(TableNamespaceManager.KEY_MAX_REGIONS, "1").build());

    ADMIN.disableTable(tableName);
    ADMIN.truncateTable(tableName, false);

    assertEquals(1, ADMIN.getRegions(tableName).size());
    assertEquals(1, getCachedRegionCount(nsp, tableName));
  }

  @Test
  public void testTruncateCountsOnlyDefaultReplicas() throws Exception {
    String nsp = PREFIX + "_replicas";
    ADMIN.createNamespace(NamespaceDescriptor.create(nsp)
      .addConfiguration(TableNamespaceManager.KEY_MAX_REGIONS, "4").build());
    TableName tableName = TableName.valueOf(nsp + TableName.NAMESPACE_DELIM + "table1");
    TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY)).setRegionReplication(2).build();
    ADMIN.createTable(desc, new byte[][] { SPLIT_KEY });
    UTIL.waitTableAvailable(tableName);
    // Two default replicas; the two non-default replicas do not count toward the quota.
    assertEquals(2, getCachedRegionCount(nsp, tableName));

    ADMIN.disableTable(tableName);
    ADMIN.truncateTable(tableName, false);

    // One default replica plus one non-default replica remain; only the default one is counted.
    assertEquals(2, ADMIN.getRegions(tableName).size());
    assertEquals(1, getCachedRegionCount(nsp, tableName));
  }

  @Test
  public void testTruncatePreservingSplitsKeepsQuotaCount() throws Exception {
    String nsp = PREFIX + "_preserve";
    TableName tableName = createNamespaceAndTwoRegionTable(nsp, "2");
    assertEquals(2, getCachedRegionCount(nsp, tableName));

    ADMIN.disableTable(tableName);
    ADMIN.truncateTable(tableName, true);

    assertEquals(2, ADMIN.getRegions(tableName).size());
    assertEquals(2, getCachedRegionCount(nsp, tableName));
  }

  private TableName createNamespaceAndTwoRegionTable(String nsp, String maxRegions)
    throws Exception {
    ADMIN.createNamespace(NamespaceDescriptor.create(nsp)
      .addConfiguration(TableNamespaceManager.KEY_MAX_REGIONS, maxRegions).build());
    TableName tableName = TableName.valueOf(nsp + TableName.NAMESPACE_DELIM + "table1");
    TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY)).build();
    ADMIN.createTable(desc, new byte[][] { SPLIT_KEY });
    UTIL.waitTableAvailable(tableName);
    return tableName;
  }

  private int getCachedRegionCount(String nsp, TableName tableName) {
    NamespaceTableAndRegionInfo state = UTIL.getHBaseCluster().getMaster().getMasterQuotaManager()
      .getNamespaceQuotaManager().getState(nsp);
    return state.getRegionCountOfTable(tableName);
  }
}
