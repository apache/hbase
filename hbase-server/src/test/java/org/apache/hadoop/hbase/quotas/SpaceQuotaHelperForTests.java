/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.quotas;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.rules.TestName;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

@InterfaceAudience.Private
public class SpaceQuotaHelperForTests {
  private static final Log LOG = LogFactory.getLog(SpaceQuotaHelperForTests.class);

  public static final int SIZE_PER_VALUE = 256;
  public static final String F1 = "f1";
  public static final long ONE_KILOBYTE = 1024L;
  public static final long ONE_MEGABYTE = ONE_KILOBYTE * ONE_KILOBYTE;

  private final HBaseTestingUtility testUtil;
  private final TestName testName;
  private final AtomicLong counter;

  public SpaceQuotaHelperForTests(
      HBaseTestingUtility testUtil, TestName testName, AtomicLong counter) {
    this.testUtil = Objects.requireNonNull(testUtil);
    this.testName = Objects.requireNonNull(testName);
    this.counter = Objects.requireNonNull(counter);
  }

  //
  // Helpers
  //

  void writeData(TableName tn, long sizeInBytes) throws IOException {
    final Connection conn = testUtil.getConnection();
    final Table table = conn.getTable(tn);
    try {
      List<Put> updates = new ArrayList<>();
      long bytesToWrite = sizeInBytes;
      long rowKeyId = 0L;
      final StringBuilder sb = new StringBuilder();
      final Random r = new Random();
      while (bytesToWrite > 0L) {
        sb.setLength(0);
        sb.append(Long.toString(rowKeyId));
        // Use the reverse counter as the rowKey to get even spread across all regions
        Put p = new Put(Bytes.toBytes(sb.reverse().toString()));
        byte[] value = new byte[SIZE_PER_VALUE];
        r.nextBytes(value);
        p.addColumn(Bytes.toBytes(F1), Bytes.toBytes("q1"), value);
        updates.add(p);

        // Batch ~13KB worth of updates
        if (updates.size() > 50) {
          table.put(updates);
          updates.clear();
        }

        // Just count the value size, ignore the size of rowkey + column
        bytesToWrite -= SIZE_PER_VALUE;
        rowKeyId++;
      }

      // Write the final batch
      if (!updates.isEmpty()) {
        table.put(updates);
      }

      LOG.debug("Data was written to HBase");
      // Push the data to disk.
      testUtil.getAdmin().flush(tn);
      LOG.debug("Data flushed to disk");
    } finally {
      table.close();
    }
  }

  Multimap<TableName, QuotaSettings> createTablesWithSpaceQuotas() throws Exception {
    final Admin admin = testUtil.getAdmin();
    final Multimap<TableName, QuotaSettings> tablesWithQuotas = HashMultimap.create();

    final TableName tn1 = createTable();
    final TableName tn2 = createTable();

    NamespaceDescriptor nd = NamespaceDescriptor.create("ns" + counter.getAndIncrement()).build();
    admin.createNamespace(nd);
    final TableName tn3 = createTableInNamespace(nd);
    final TableName tn4 = createTableInNamespace(nd);
    final TableName tn5 = createTableInNamespace(nd);

    final long sizeLimit1 = 1024L * 1024L * 1024L * 1024L * 5L; // 5TB
    final SpaceViolationPolicy violationPolicy1 = SpaceViolationPolicy.NO_WRITES;
    QuotaSettings qs1 = QuotaSettingsFactory.limitTableSpace(tn1, sizeLimit1, violationPolicy1);
    tablesWithQuotas.put(tn1, qs1);
    admin.setQuota(qs1);

    final long sizeLimit2 = 1024L * 1024L * 1024L * 200L; // 200GB
    final SpaceViolationPolicy violationPolicy2 = SpaceViolationPolicy.NO_WRITES_COMPACTIONS;
    QuotaSettings qs2 = QuotaSettingsFactory.limitTableSpace(tn2, sizeLimit2, violationPolicy2);
    tablesWithQuotas.put(tn2, qs2);
    admin.setQuota(qs2);

    final long sizeLimit3 = 1024L * 1024L * 1024L * 1024L * 100L; // 100TB
    final SpaceViolationPolicy violationPolicy3 = SpaceViolationPolicy.NO_INSERTS;
    QuotaSettings qs3 = QuotaSettingsFactory.limitNamespaceSpace(
        nd.getName(), sizeLimit3, violationPolicy3);
    tablesWithQuotas.put(tn3, qs3);
    tablesWithQuotas.put(tn4, qs3);
    tablesWithQuotas.put(tn5, qs3);
    admin.setQuota(qs3);

    final long sizeLimit4 = 1024L * 1024L * 1024L * 5L; // 5GB
    final SpaceViolationPolicy violationPolicy4 = SpaceViolationPolicy.NO_INSERTS;
    QuotaSettings qs4 = QuotaSettingsFactory.limitTableSpace(tn5, sizeLimit4, violationPolicy4);
    // Override the ns quota for tn5, import edge-case to catch table quota taking
    // precedence over ns quota.
    tablesWithQuotas.put(tn5, qs4);
    admin.setQuota(qs4);

    return tablesWithQuotas;
  }

  TableName createTable() throws Exception {
    return createTableWithRegions(1);
  }

  TableName createTableWithRegions(int numRegions) throws Exception {
    return createTableWithRegions(NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR, numRegions);
  }

  TableName createTableWithRegions(String namespace, int numRegions) throws Exception {
    final Admin admin = testUtil.getAdmin();
    final TableName tn = TableName.valueOf(
        namespace, testName.getMethodName() + counter.getAndIncrement());

    // Delete the old table
    if (admin.tableExists(tn)) {
      admin.disableTable(tn);
      admin.deleteTable(tn);
    }

    // Create the table
    HTableDescriptor tableDesc = new HTableDescriptor(tn);
    tableDesc.addFamily(new HColumnDescriptor(F1));
    if (numRegions == 1) {
      admin.createTable(tableDesc);
    } else {
      admin.createTable(tableDesc, Bytes.toBytes("0"), Bytes.toBytes("9"), numRegions);
    }
    return tn;
  }

  TableName createTableInNamespace(NamespaceDescriptor nd) throws Exception {
    final Admin admin = testUtil.getAdmin();
    final TableName tn = TableName.valueOf(nd.getName(),
        testName.getMethodName() + counter.getAndIncrement());

    // Delete the old table
    if (admin.tableExists(tn)) {
      admin.disableTable(tn);
      admin.deleteTable(tn);
    }

    // Create the table
    HTableDescriptor tableDesc = new HTableDescriptor(tn);
    tableDesc.addFamily(new HColumnDescriptor(F1));

    admin.createTable(tableDesc);
    return tn;
  }

  void partitionTablesByQuotaTarget(Multimap<TableName,QuotaSettings> quotas,
      Set<TableName> tablesWithTableQuota, Set<TableName> tablesWithNamespaceQuota) {
    // Partition the tables with quotas by table and ns quota
    for (Entry<TableName, QuotaSettings> entry : quotas.entries()) {
      SpaceLimitSettings settings = (SpaceLimitSettings) entry.getValue();
      TableName tn = entry.getKey();
      if (null != settings.getTableName()) {
        tablesWithTableQuota.add(tn);
      }
      if (null != settings.getNamespace()) {
        tablesWithNamespaceQuota.add(tn);
      }

      if (null == settings.getTableName() && null == settings.getNamespace()) {
        fail("Unexpected table name with null tableName and namespace: " + tn);
      }
    }
  }
}
