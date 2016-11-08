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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.quotas.QuotaObserverChore.TablesWithQuotas;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.SpaceQuota;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;

/**
 * Test class for {@link QuotaObserverChore} that uses a live HBase cluster.
 */
@Category(LargeTests.class)
public class TestQuotaObserverChoreWithMiniCluster {
  private static final Log LOG = LogFactory.getLog(TestQuotaObserverChoreWithMiniCluster.class);
  private static final int SIZE_PER_VALUE = 256;
  private static final String F1 = "f1";
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final AtomicLong COUNTER = new AtomicLong(0);
  private static final long ONE_MEGABYTE = 1024L * 1024L;
  private static final long DEFAULT_WAIT_MILLIS = 500;

  @Rule
  public TestName testName = new TestName();

  private HMaster master;
  private QuotaObserverChore chore;
  private SpaceQuotaViolationNotifierForTest violationNotifier;

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setInt(FileSystemUtilizationChore.FS_UTILIZATION_CHORE_DELAY_KEY, 1000);
    conf.setInt(FileSystemUtilizationChore.FS_UTILIZATION_CHORE_PERIOD_KEY, 1000);
    conf.setInt(QuotaObserverChore.VIOLATION_OBSERVER_CHORE_DELAY_KEY, 1000);
    conf.setInt(QuotaObserverChore.VIOLATION_OBSERVER_CHORE_PERIOD_KEY, 1000);
    conf.setBoolean(QuotaUtil.QUOTA_CONF_KEY, true);
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void removeAllQuotas() throws Exception {
    final Connection conn = TEST_UTIL.getConnection();
    // Wait for the quota table to be created
    if (!conn.getAdmin().tableExists(QuotaUtil.QUOTA_TABLE_NAME)) {
      do {
        LOG.debug("Quota table does not yet exist");
        Thread.sleep(DEFAULT_WAIT_MILLIS);
      } while (!conn.getAdmin().tableExists(QuotaUtil.QUOTA_TABLE_NAME));
    } else {
      // Or, clean up any quotas from previous test runs.
      QuotaRetriever scanner = QuotaRetriever.open(TEST_UTIL.getConfiguration());
      for (QuotaSettings quotaSettings : scanner) {
        final String namespace = quotaSettings.getNamespace();
        final TableName tableName = quotaSettings.getTableName();
        if (null != namespace) {
          LOG.debug("Deleting quota for namespace: " + namespace);
          QuotaUtil.deleteNamespaceQuota(conn, namespace);
        } else {
          assert null != tableName;
          LOG.debug("Deleting quota for table: "+ tableName);
          QuotaUtil.deleteTableQuota(conn, tableName);
        }
      }
    }

    master = TEST_UTIL.getMiniHBaseCluster().getMaster();
    violationNotifier =
        (SpaceQuotaViolationNotifierForTest) master.getSpaceQuotaViolationNotifier();
    violationNotifier.clearTableViolations();
    chore = master.getQuotaObserverChore();
  }

  @Test
  public void testTableViolatesQuota() throws Exception {
    TableName tn = createTableWithRegions(10);

    final long sizeLimit = 2L * ONE_MEGABYTE;
    final SpaceViolationPolicy violationPolicy = SpaceViolationPolicy.NO_INSERTS;
    QuotaSettings settings = QuotaSettingsFactory.limitTableSpace(tn, sizeLimit, violationPolicy);
    TEST_UTIL.getAdmin().setQuota(settings);

    // Write more data than should be allowed
    writeData(tn, 3L * ONE_MEGABYTE);

    Map<TableName,SpaceViolationPolicy> violatedQuotas = violationNotifier.snapshotTablesInViolation();
    while (violatedQuotas.isEmpty()) {
      LOG.info("Found no violated quotas, sleeping and retrying. Current reports: "
          + master.getMasterQuotaManager().snapshotRegionSizes());
      try {
        Thread.sleep(DEFAULT_WAIT_MILLIS);
      } catch (InterruptedException e) {
        LOG.debug("Interrupted while sleeping.", e);
        Thread.currentThread().interrupt();
      }
      violatedQuotas = violationNotifier.snapshotTablesInViolation();
    }

    Entry<TableName,SpaceViolationPolicy> entry = Iterables.getOnlyElement(violatedQuotas.entrySet());
    assertEquals(tn, entry.getKey());
    assertEquals(violationPolicy, entry.getValue());
  }

  @Test
  public void testNamespaceViolatesQuota() throws Exception {
    final String namespace = testName.getMethodName();
    final Admin admin = TEST_UTIL.getAdmin();
    // Ensure the namespace exists
    try {
      admin.getNamespaceDescriptor(namespace);
    } catch (NamespaceNotFoundException e) {
      NamespaceDescriptor desc = NamespaceDescriptor.create(namespace).build();
      admin.createNamespace(desc);
    }

    TableName tn1 = createTableWithRegions(namespace, 5);
    TableName tn2 = createTableWithRegions(namespace, 5);
    TableName tn3 = createTableWithRegions(namespace, 5);

    final long sizeLimit = 5L * ONE_MEGABYTE;
    final SpaceViolationPolicy violationPolicy = SpaceViolationPolicy.DISABLE;
    QuotaSettings settings = QuotaSettingsFactory.limitNamespaceSpace(namespace, sizeLimit, violationPolicy);
    admin.setQuota(settings);

    writeData(tn1, 2L * ONE_MEGABYTE);
    admin.flush(tn1);
    Map<TableName,SpaceViolationPolicy> violatedQuotas = violationNotifier.snapshotTablesInViolation();
    for (int i = 0; i < 5; i++) {
      // Check a few times to make sure we don't prematurely move to violation
      assertEquals("Should not see any quota violations after writing 2MB of data", 0, violatedQuotas.size());
      try {
        Thread.sleep(DEFAULT_WAIT_MILLIS);
      } catch (InterruptedException e) {
        LOG.debug("Interrupted while sleeping." , e);
      }
      violatedQuotas = violationNotifier.snapshotTablesInViolation();
    }

    writeData(tn2, 2L * ONE_MEGABYTE);
    admin.flush(tn2);
    violatedQuotas = violationNotifier.snapshotTablesInViolation();
    for (int i = 0; i < 5; i++) {
      // Check a few times to make sure we don't prematurely move to violation
      assertEquals("Should not see any quota violations after writing 4MB of data", 0,
          violatedQuotas.size());
      try {
        Thread.sleep(DEFAULT_WAIT_MILLIS);
      } catch (InterruptedException e) {
        LOG.debug("Interrupted while sleeping." , e);
      }
      violatedQuotas = violationNotifier.snapshotTablesInViolation();
    }

    // Writing the final 2MB of data will push the namespace over the 5MB limit (6MB in total)
    // and should push all three tables in the namespace into violation.
    writeData(tn3, 2L * ONE_MEGABYTE);
    admin.flush(tn3);
    violatedQuotas = violationNotifier.snapshotTablesInViolation();
    while (violatedQuotas.size() < 3) {
      LOG.debug("Saw fewer violations than desired (expected 3): " + violatedQuotas
          + ". Current reports: " + master.getMasterQuotaManager().snapshotRegionSizes());
      try {
        Thread.sleep(DEFAULT_WAIT_MILLIS);
      } catch (InterruptedException e) {
        LOG.debug("Interrupted while sleeping.", e);
        Thread.currentThread().interrupt();
      }
      violatedQuotas = violationNotifier.snapshotTablesInViolation();
    }

    SpaceViolationPolicy vp1 = violatedQuotas.remove(tn1);
    assertNotNull("tn1 should be in violation", vp1);
    assertEquals(violationPolicy, vp1);
    SpaceViolationPolicy vp2 = violatedQuotas.remove(tn2);
    assertNotNull("tn2 should be in violation", vp2);
    assertEquals(violationPolicy, vp2);
    SpaceViolationPolicy vp3 = violatedQuotas.remove(tn3);
    assertNotNull("tn3 should be in violation", vp3);
    assertEquals(violationPolicy, vp3);
    assertTrue("Unexpected additional quota violations: " + violatedQuotas, violatedQuotas.isEmpty());
  }

  @Test
  public void testTableQuotaOverridesNamespaceQuota() throws Exception {
    final String namespace = testName.getMethodName();
    final Admin admin = TEST_UTIL.getAdmin();
    // Ensure the namespace exists
    try {
      admin.getNamespaceDescriptor(namespace);
    } catch (NamespaceNotFoundException e) {
      NamespaceDescriptor desc = NamespaceDescriptor.create(namespace).build();
      admin.createNamespace(desc);
    }

    TableName tn1 = createTableWithRegions(namespace, 5);
    TableName tn2 = createTableWithRegions(namespace, 5);

    final long namespaceSizeLimit = 3L * ONE_MEGABYTE;
    final SpaceViolationPolicy namespaceViolationPolicy = SpaceViolationPolicy.DISABLE;
    QuotaSettings namespaceSettings = QuotaSettingsFactory.limitNamespaceSpace(namespace,
        namespaceSizeLimit, namespaceViolationPolicy);
    admin.setQuota(namespaceSettings);

    writeData(tn1, 2L * ONE_MEGABYTE);
    admin.flush(tn1);
    Map<TableName,SpaceViolationPolicy> violatedQuotas = violationNotifier.snapshotTablesInViolation();
    for (int i = 0; i < 5; i++) {
      // Check a few times to make sure we don't prematurely move to violation
      assertEquals("Should not see any quota violations after writing 2MB of data", 0,
          violatedQuotas.size());
      try {
        Thread.sleep(DEFAULT_WAIT_MILLIS);
      } catch (InterruptedException e) {
        LOG.debug("Interrupted while sleeping." , e);
      }
      violatedQuotas = violationNotifier.snapshotTablesInViolation();
    }

    writeData(tn2, 2L * ONE_MEGABYTE);
    admin.flush(tn2);
    violatedQuotas = violationNotifier.snapshotTablesInViolation();
    while (violatedQuotas.size() < 2) {
      LOG.debug("Saw fewer violations than desired (expected 2): " + violatedQuotas
          + ". Current reports: " + master.getMasterQuotaManager().snapshotRegionSizes());
      try {
        Thread.sleep(DEFAULT_WAIT_MILLIS);
      } catch (InterruptedException e) {
        LOG.debug("Interrupted while sleeping.", e);
        Thread.currentThread().interrupt();
      }
      violatedQuotas = violationNotifier.snapshotTablesInViolation();
    }

    SpaceViolationPolicy actualPolicyTN1 = violatedQuotas.get(tn1);
    assertNotNull("Expected to see violation policy for tn1", actualPolicyTN1);
    assertEquals(namespaceViolationPolicy, actualPolicyTN1);
    SpaceViolationPolicy actualPolicyTN2 = violatedQuotas.get(tn2);
    assertNotNull("Expected to see violation policy for tn2", actualPolicyTN2);
    assertEquals(namespaceViolationPolicy, actualPolicyTN2);

    // Override the namespace quota with a table quota
    final long tableSizeLimit = ONE_MEGABYTE;
    final SpaceViolationPolicy tableViolationPolicy = SpaceViolationPolicy.NO_INSERTS;
    QuotaSettings tableSettings = QuotaSettingsFactory.limitTableSpace(tn1, tableSizeLimit,
        tableViolationPolicy);
    admin.setQuota(tableSettings);

    // Keep checking for the table quota policy to override the namespace quota
    while (true) {
      violatedQuotas = violationNotifier.snapshotTablesInViolation();
      SpaceViolationPolicy actualTableViolationPolicy = violatedQuotas.get(tn1);
      assertNotNull("Violation policy should never be null", actualTableViolationPolicy);
      if (tableViolationPolicy != actualTableViolationPolicy) {
        LOG.debug("Saw unexpected table violation policy, waiting and re-checking.");
        try {
          Thread.sleep(DEFAULT_WAIT_MILLIS);
        } catch (InterruptedException e) {
          LOG.debug("Interrupted while sleeping");
          Thread.currentThread().interrupt();
        }
        continue;
      }
      assertEquals(tableViolationPolicy, actualTableViolationPolicy);
      break;
    }

    // This should not change with the introduction of the table quota for tn1
    actualPolicyTN2 = violatedQuotas.get(tn2);
    assertNotNull("Expected to see violation policy for tn2", actualPolicyTN2);
    assertEquals(namespaceViolationPolicy, actualPolicyTN2);
  }

  @Test
  public void testGetAllTablesWithQuotas() throws Exception {
    final Multimap<TableName, QuotaSettings> quotas = createTablesWithSpaceQuotas();
    Set<TableName> tablesWithQuotas = new HashSet<>();
    Set<TableName> namespaceTablesWithQuotas = new HashSet<>();
    // Partition the tables with quotas by table and ns quota
    partitionTablesByQuotaTarget(quotas, tablesWithQuotas, namespaceTablesWithQuotas);

    TablesWithQuotas tables = chore.fetchAllTablesWithQuotasDefined();
    assertEquals("Found tables: " + tables, tablesWithQuotas, tables.getTableQuotaTables());
    assertEquals("Found tables: " + tables, namespaceTablesWithQuotas, tables.getNamespaceQuotaTables());
  }

  @Test
  public void testRpcQuotaTablesAreFiltered() throws Exception {
    final Multimap<TableName, QuotaSettings> quotas = createTablesWithSpaceQuotas();
    Set<TableName> tablesWithQuotas = new HashSet<>();
    Set<TableName> namespaceTablesWithQuotas = new HashSet<>();
    // Partition the tables with quotas by table and ns quota
    partitionTablesByQuotaTarget(quotas, tablesWithQuotas, namespaceTablesWithQuotas);

    TableName rpcQuotaTable = createTable();
    TEST_UTIL.getAdmin().setQuota(QuotaSettingsFactory
      .throttleTable(rpcQuotaTable, ThrottleType.READ_NUMBER, 6, TimeUnit.MINUTES));

    // The `rpcQuotaTable` should not be included in this Set
    TablesWithQuotas tables = chore.fetchAllTablesWithQuotasDefined();
    assertEquals("Found tables: " + tables, tablesWithQuotas, tables.getTableQuotaTables());
    assertEquals("Found tables: " + tables, namespaceTablesWithQuotas, tables.getNamespaceQuotaTables());
  }

  @Test
  public void testFilterRegions() throws Exception {
    Map<TableName,Integer> mockReportedRegions = new HashMap<>();
    // Can't mock because of primitive int as a return type -- Mockito
    // can only handle an Integer.
    TablesWithQuotas tables = new TablesWithQuotas(TEST_UTIL.getConnection(),
        TEST_UTIL.getConfiguration()) {
      @Override
      int getNumReportedRegions(TableName table, QuotaViolationStore<TableName> tableStore) {
        Integer i = mockReportedRegions.get(table);
        if (null == i) {
          return 0;
        }
        return i;
      }
    };

    // Create the tables
    TableName tn1 = createTableWithRegions(20);
    TableName tn2 = createTableWithRegions(20);
    TableName tn3 = createTableWithRegions(20);

    // Add them to the Tables with Quotas object
    tables.addTableQuotaTable(tn1);
    tables.addTableQuotaTable(tn2);
    tables.addTableQuotaTable(tn3);

    // Mock the number of regions reported
    mockReportedRegions.put(tn1, 10); // 50%
    mockReportedRegions.put(tn2, 19); // 95%
    mockReportedRegions.put(tn3, 20); // 100%

    // Argument is un-used
    tables.filterInsufficientlyReportedTables(null);
    // The default of 95% reported should prevent tn1 from appearing
    assertEquals(new HashSet<>(Arrays.asList(tn2, tn3)), tables.getTableQuotaTables());
  }

  @Test
  public void testFetchSpaceQuota() throws Exception {
    Multimap<TableName,QuotaSettings> tables = createTablesWithSpaceQuotas();
    // Can pass in an empty map, we're not consulting it.
    chore.initializeViolationStores(Collections.emptyMap());
    // All tables that were created should have a quota defined.
    for (Entry<TableName,QuotaSettings> entry : tables.entries()) {
      final TableName table = entry.getKey();
      final QuotaSettings qs = entry.getValue();

      assertTrue("QuotaSettings was an instance of " + qs.getClass(),
          qs instanceof SpaceLimitSettings);

      SpaceQuota spaceQuota = null;
      if (null != qs.getTableName()) {
        spaceQuota = chore.getTableViolationStore().getSpaceQuota(table);
        assertNotNull("Could not find table space quota for " + table, spaceQuota);
      } else if (null != qs.getNamespace()) {
        spaceQuota = chore.getNamespaceViolationStore().getSpaceQuota(table.getNamespaceAsString());
        assertNotNull("Could not find namespace space quota for " + table.getNamespaceAsString(), spaceQuota);
      } else {
        fail("Expected table or namespace space quota");
      }

      final SpaceLimitSettings sls = (SpaceLimitSettings) qs;
      assertEquals(sls.getProto().getQuota(), spaceQuota);
    }

    TableName tableWithoutQuota = createTable();
    assertNull(chore.getTableViolationStore().getSpaceQuota(tableWithoutQuota));
  }

  //
  // Helpers
  //

  void writeData(TableName tn, long sizeInBytes) throws IOException {
    final Connection conn = TEST_UTIL.getConnection();
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

        // Batch 50K worth of updates
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
      TEST_UTIL.getAdmin().flush(tn);
      LOG.debug("Data flushed to disk");
    } finally {
      table.close();
    }
  }

  Multimap<TableName, QuotaSettings> createTablesWithSpaceQuotas() throws Exception {
    final Admin admin = TEST_UTIL.getAdmin();
    final Multimap<TableName, QuotaSettings> tablesWithQuotas = HashMultimap.create();

    final TableName tn1 = createTable();
    final TableName tn2 = createTable();

    NamespaceDescriptor nd = NamespaceDescriptor.create("ns" + COUNTER.getAndIncrement()).build();
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
    QuotaSettings qs3 = QuotaSettingsFactory.limitNamespaceSpace(nd.getName(), sizeLimit3, violationPolicy3);
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
    final Admin admin = TEST_UTIL.getAdmin();
    final TableName tn = TableName.valueOf(namespace, testName.getMethodName() + COUNTER.getAndIncrement());

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
    final Admin admin = TEST_UTIL.getAdmin();
    final TableName tn = TableName.valueOf(nd.getName(),
        testName.getMethodName() + COUNTER.getAndIncrement());

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
