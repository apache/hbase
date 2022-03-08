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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.Waiter.Predicate;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.ClientServiceCallable;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.SecureBulkLoadClient;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.TestHRegionServerBulkLoad;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.collect.HashMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;
import org.apache.hbase.thirdparty.com.google.common.collect.Multimap;

@InterfaceAudience.Private
public class SpaceQuotaHelperForTests {
  private static final Logger LOG = LoggerFactory.getLogger(SpaceQuotaHelperForTests.class);

  public static final int SIZE_PER_VALUE = 256;
  public static final String F1 = "f1";
  public static final long ONE_KILOBYTE = 1024L;
  public static final long ONE_MEGABYTE = ONE_KILOBYTE * ONE_KILOBYTE;
  public static final long ONE_GIGABYTE = ONE_MEGABYTE * ONE_KILOBYTE;

  private final HBaseTestingUtility testUtil;
  private final TestName testName;
  private final AtomicLong counter;
  private static final int NUM_RETRIES = 10;

  public SpaceQuotaHelperForTests(
      HBaseTestingUtility testUtil, TestName testName, AtomicLong counter) {
    this.testUtil = Objects.requireNonNull(testUtil);
    this.testName = Objects.requireNonNull(testName);
    this.counter = Objects.requireNonNull(counter);
  }

  //
  // Static helpers
  //

  static void updateConfigForQuotas(Configuration conf) {
    // Increase the frequency of some of the chores for responsiveness of the test
    conf.setInt(FileSystemUtilizationChore.FS_UTILIZATION_CHORE_DELAY_KEY, 1000);
    conf.setInt(FileSystemUtilizationChore.FS_UTILIZATION_CHORE_PERIOD_KEY, 1000);
    conf.setInt(QuotaObserverChore.QUOTA_OBSERVER_CHORE_DELAY_KEY, 1000);
    conf.setInt(QuotaObserverChore.QUOTA_OBSERVER_CHORE_PERIOD_KEY, 1000);
    conf.setInt(SpaceQuotaRefresherChore.POLICY_REFRESHER_CHORE_DELAY_KEY, 1000);
    conf.setInt(SpaceQuotaRefresherChore.POLICY_REFRESHER_CHORE_PERIOD_KEY, 1000);
    conf.setInt(SnapshotQuotaObserverChore.SNAPSHOT_QUOTA_CHORE_DELAY_KEY, 1000);
    conf.setInt(SnapshotQuotaObserverChore.SNAPSHOT_QUOTA_CHORE_PERIOD_KEY, 1000);
    conf.setInt(RegionSizeReportingChore.REGION_SIZE_REPORTING_CHORE_PERIOD_KEY, 1000);
    conf.setInt(RegionSizeReportingChore.REGION_SIZE_REPORTING_CHORE_DELAY_KEY, 1000);
    // The period at which we check for compacted files that should be deleted from HDFS
    conf.setInt("hbase.hfile.compaction.discharger.interval", 5 * 1000);
    conf.setBoolean(QuotaUtil.QUOTA_CONF_KEY, true);
  }

  //
  // Helpers
  //

  /**
   * Returns the number of quotas defined in the HBase quota table.
   */
  long listNumDefinedQuotas(Connection conn) throws IOException {
    QuotaRetriever scanner = QuotaRetriever.open(conn.getConfiguration());
    try {
      return Iterables.size(scanner);
    } finally {
      if (scanner != null) {
        scanner.close();
      }
    }
  }

  /**
   * Writes the given mutation into a table until it violates the given policy.
   * Verifies that the policy has been violated & then returns the name of
   * the table created & written into.
   */
  TableName writeUntilViolationAndVerifyViolation(
      SpaceViolationPolicy policyToViolate, Mutation m) throws Exception {
    final TableName tn = writeUntilViolation(policyToViolate);
    verifyViolation(policyToViolate, tn, m);
    return tn;
  }

  /**
   * Writes the given mutation into a table until it violates the given policy.
   * Returns the name of the table created & written into.
   */
  TableName writeUntilViolation(SpaceViolationPolicy policyToViolate) throws Exception {
    TableName tn = createTableWithRegions(10);
    setQuotaLimit(tn, policyToViolate, 2L);
    // Write more data than should be allowed and flush it to disk
    writeData(tn, 3L * SpaceQuotaHelperForTests.ONE_MEGABYTE);

    // This should be sufficient time for the chores to run and see the change.
    Thread.sleep(5000);

    return tn;
  }


  TableName writeUntilViolationAndVerifyViolationInNamespace(
          String ns, SpaceViolationPolicy policyToViolate, Mutation m) throws Exception {
    final TableName tn = writeUntilViolationInNamespace(ns, policyToViolate);
    verifyViolation(policyToViolate, tn, m);
    return tn;
  }

  TableName writeUntilViolationInNamespace(String ns, SpaceViolationPolicy policyToViolate) throws Exception {
    TableName tn = createTableWithRegions(ns,10);

    setQuotaLimit(ns, policyToViolate, 4L);

    // Write more data than should be allowed and flush it to disk
    writeData(tn, 5L * SpaceQuotaHelperForTests.ONE_MEGABYTE);

    // This should be sufficient time for the chores to run and see the change.
    Thread.sleep(5000);

    return tn;
  }

  /**
   * Verifies that the given policy on the given table has been violated
   */
  void verifyViolation(SpaceViolationPolicy policyToViolate, TableName tn, Mutation m)
      throws Exception {
    // But let's try a few times to get the exception before failing
    boolean sawError = false;
    String msg = "";
    for (int i = 0; i < NUM_RETRIES && !sawError; i++) {
      try (Table table = testUtil.getConnection().getTable(tn)) {
        if (m instanceof Put) {
          table.put((Put) m);
        } else if (m instanceof Delete) {
          table.delete((Delete) m);
        } else if (m instanceof Append) {
          table.append((Append) m);
        } else if (m instanceof Increment) {
          table.increment((Increment) m);
        } else {
          fail(
              "Failed to apply " + m.getClass().getSimpleName() +
                  " to the table. Programming error");
        }
        LOG.info("Did not reject the " + m.getClass().getSimpleName() + ", will sleep and retry");
        Thread.sleep(2000);
      } catch (Exception e) {
        msg = StringUtils.stringifyException(e);
        if ((policyToViolate.equals(SpaceViolationPolicy.DISABLE)
            && e instanceof TableNotEnabledException) || msg.contains(policyToViolate.name())) {
          LOG.info("Got the expected exception={}", msg);
          sawError = true;
          break;
        } else {
          LOG.warn("Did not get the expected exception, will sleep and retry", e);
          Thread.sleep(2000);
        }
      }
    }
    if (!sawError) {
      try (Table quotaTable = testUtil.getConnection().getTable(QuotaUtil.QUOTA_TABLE_NAME)) {
        ResultScanner scanner = quotaTable.getScanner(new Scan());
        Result result = null;
        LOG.info("Dumping contents of hbase:quota table");
        while ((result = scanner.next()) != null) {
          LOG.info(Bytes.toString(result.getRow()) + " => " + result.toString());
        }
        scanner.close();
      }
    } else {
      if (policyToViolate.equals(SpaceViolationPolicy.DISABLE)) {
        assertTrue(
            msg.contains("TableNotEnabledException") || msg.contains(policyToViolate.name()));
      } else {
        assertTrue("Expected exception message to contain the word '" + policyToViolate.name()
                + "', but was " + msg,
            msg.contains(policyToViolate.name()));
      }
    }
    assertTrue(
        "Expected to see an exception writing data to a table exceeding its quota", sawError);
  }

  /**
   * Verifies that no policy has been violated on the given table
   */
  void verifyNoViolation(TableName tn, Mutation m) throws Exception {
    // But let's try a few times to write data before failing
    boolean sawSuccess = false;
    for (int i = 0; i < NUM_RETRIES && !sawSuccess; i++) {
      try (Table table = testUtil.getConnection().getTable(tn)) {
        if (m instanceof Put) {
          table.put((Put) m);
        } else if (m instanceof Delete) {
          table.delete((Delete) m);
        } else if (m instanceof Append) {
          table.append((Append) m);
        } else if (m instanceof Increment) {
          table.increment((Increment) m);
        } else {
          fail("Failed to apply " + m.getClass().getSimpleName() + " to the table."
              + " Programming error");
        }
        sawSuccess = true;
      } catch (Exception e) {
        LOG.info("Rejected the " + m.getClass().getSimpleName() + ", will sleep and retry");
        Thread.sleep(2000);
      }
    }
    if (!sawSuccess) {
      try (Table quotaTable = testUtil.getConnection().getTable(QuotaUtil.QUOTA_TABLE_NAME)) {
        ResultScanner scanner = quotaTable.getScanner(new Scan());
        Result result = null;
        LOG.info("Dumping contents of hbase:quota table");
        while ((result = scanner.next()) != null) {
          LOG.info(Bytes.toString(result.getRow()) + " => " + result.toString());
        }
        scanner.close();
      }
    }
    assertTrue("Expected to succeed in writing data to a table not having quota ", sawSuccess);
  }

  /**
   * Verifies that table usage snapshot exists for the table
   */
  void verifyTableUsageSnapshotForSpaceQuotaExist(TableName tn) throws Exception {
    boolean sawUsageSnapshot = false;
    try (Table quotaTable = testUtil.getConnection().getTable(QuotaTableUtil.QUOTA_TABLE_NAME)) {
      Scan s = QuotaTableUtil.makeQuotaSnapshotScanForTable(tn);
      ResultScanner rs = quotaTable.getScanner(s);
      sawUsageSnapshot = (rs.next() != null);
    }
    assertTrue("Expected to succeed in getting table usage snapshots for space quota", sawUsageSnapshot);
  }

  /**
   * Sets the given quota (policy & limit) on the passed table.
   */
  void setQuotaLimit(final TableName tn, SpaceViolationPolicy policy, long sizeInMBs)
      throws Exception {
    final long sizeLimit = sizeInMBs * SpaceQuotaHelperForTests.ONE_MEGABYTE;
    QuotaSettings settings = QuotaSettingsFactory.limitTableSpace(tn, sizeLimit, policy);
    testUtil.getAdmin().setQuota(settings);
    LOG.debug("Quota limit set for table = {}, limit = {}", tn, sizeLimit);
  }

  /**
   * Sets the given quota (policy & limit) on the passed namespace.
   */
  void setQuotaLimit(String ns, SpaceViolationPolicy policy, long sizeInMBs)
          throws Exception {
    final long sizeLimit = sizeInMBs * SpaceQuotaHelperForTests.ONE_MEGABYTE;
    QuotaSettings settings = QuotaSettingsFactory.limitNamespaceSpace(ns, sizeLimit, policy);
    testUtil.getAdmin().setQuota(settings);
    LOG.debug("Quota limit set for namespace = {}, limit = {}", ns, sizeLimit);
  }

  /**
   * Removes the space quota from the given table
   */
  void removeQuotaFromtable(final TableName tn) throws Exception {
    QuotaSettings removeQuota = QuotaSettingsFactory.removeTableSpaceLimit(tn);
    testUtil.getAdmin().setQuota(removeQuota);
    LOG.debug("Space quota settings removed from the table ", tn);
  }

  /**
   *
   * @param tn the tablename
   * @param numFiles number of files
   * @param numRowsPerFile number of rows per file
   * @return a clientServiceCallable which can be used with the Caller factory for bulk load
   * @throws Exception when failed to get connection, table or preparation of the bulk load
   */
  ClientServiceCallable<Void> generateFileToLoad(TableName tn, int numFiles, int numRowsPerFile)
      throws Exception {
    Connection conn = testUtil.getConnection();
    FileSystem fs = testUtil.getTestFileSystem();
    Configuration conf = testUtil.getConfiguration();
    Path baseDir = new Path(fs.getHomeDirectory(), testName.getMethodName() + "_files");
    fs.mkdirs(baseDir);
    final List<Pair<byte[], String>> famPaths = new ArrayList<Pair<byte[], String>>();
    for (int i = 1; i <= numFiles; i++) {
      Path hfile = new Path(baseDir, "file" + i);
      TestHRegionServerBulkLoad
          .createHFile(fs, hfile, Bytes.toBytes(SpaceQuotaHelperForTests.F1), Bytes.toBytes("to"),
              Bytes.toBytes("reject"), numRowsPerFile);
      famPaths.add(new Pair<>(Bytes.toBytes(SpaceQuotaHelperForTests.F1), hfile.toString()));
    }

    // bulk load HFiles
    Table table = conn.getTable(tn);
    final String bulkToken = new SecureBulkLoadClient(conf, table).prepareBulkLoad(conn);
    return new ClientServiceCallable<Void>(conn, tn, Bytes.toBytes("row"),
        new RpcControllerFactory(conf).newController(), HConstants.PRIORITY_UNSET) {
      @Override
      public Void rpcCall() throws Exception {
        SecureBulkLoadClient secureClient = null;
        byte[] regionName = getLocation().getRegionInfo().getRegionName();
        try (Table table = conn.getTable(getTableName())) {
          secureClient = new SecureBulkLoadClient(conf, table);
          secureClient.secureBulkLoadHFiles(getStub(), famPaths, regionName, true, null, bulkToken);
        }
        return null;
      }
    };
  }

  /**
   * Bulk-loads a number of files with a number of rows to the given table.
   */
//  ClientServiceCallable<Boolean> generateFileToLoad(
//      TableName tn, int numFiles, int numRowsPerFile) throws Exception {
//    Connection conn = testUtil.getConnection();
//    FileSystem fs = testUtil.getTestFileSystem();
//    Configuration conf = testUtil.getConfiguration();
//    Path baseDir = new Path(fs.getHomeDirectory(), testName.getMethodName() + "_files");
//    fs.mkdirs(baseDir);
//    final List<Pair<byte[], String>> famPaths = new ArrayList<>();
//    for (int i = 1; i <= numFiles; i++) {
//      Path hfile = new Path(baseDir, "file" + i);
//      TestHRegionServerBulkLoad.createHFile(
//          fs, hfile, Bytes.toBytes(SpaceQuotaHelperForTests.F1), Bytes.toBytes("my"),
//          Bytes.toBytes("file"), numRowsPerFile);
//      famPaths.add(new Pair<>(Bytes.toBytes(SpaceQuotaHelperForTests.F1), hfile.toString()));
//    }
//
//    // bulk load HFiles
//    Table table = conn.getTable(tn);
//    final String bulkToken = new SecureBulkLoadClient(conf, table).prepareBulkLoad(conn);
//    return new ClientServiceCallable<Boolean>(
//        conn, tn, Bytes.toBytes("row"), new RpcControllerFactory(conf).newController(),
//        HConstants.PRIORITY_UNSET) {
//      @Override
//     public Boolean rpcCall() throws Exception {
//        SecureBulkLoadClient secureClient = null;
//        byte[] regionName = getLocation().getRegion().getRegionName();
//        try (Table table = conn.getTable(getTableName())) {
//          secureClient = new SecureBulkLoadClient(conf, table);
//          return secureClient.secureBulkLoadHFiles(getStub(), famPaths, regionName,
//                true, null, bulkToken);
//        }
//      }
//    };
//  }

  /**
   * Removes the space quota from the given namespace
   */
  void removeQuotaFromNamespace(String ns) throws Exception {
    QuotaSettings removeQuota = QuotaSettingsFactory.removeNamespaceSpaceLimit(ns);
    Admin admin = testUtil.getAdmin();
    admin.setQuota(removeQuota);
    LOG.debug("Space quota settings removed from the namespace ", ns);
  }

  /**
   * Removes all quotas defined in the HBase quota table.
   */
  void removeAllQuotas() throws Exception {
    final Connection conn = testUtil.getConnection();
    removeAllQuotas(conn);
    assertEquals(0, listNumDefinedQuotas(conn));
  }

  /**
   * Removes all quotas defined in the HBase quota table.
   */
  void removeAllQuotas(Connection conn) throws IOException {
    // Wait for the quota table to be created
    if (!conn.getAdmin().tableExists(QuotaUtil.QUOTA_TABLE_NAME)) {
      waitForQuotaTable(conn);
    } else {
      // Or, clean up any quotas from previous test runs.
      QuotaRetriever scanner = QuotaRetriever.open(conn.getConfiguration());
      try {
        for (QuotaSettings quotaSettings : scanner) {
          final String namespace = quotaSettings.getNamespace();
          final TableName tableName = quotaSettings.getTableName();
          final String userName = quotaSettings.getUserName();
          if (namespace != null) {
            LOG.debug("Deleting quota for namespace: " + namespace);
            QuotaUtil.deleteNamespaceQuota(conn, namespace);
          } else if (tableName != null) {
            LOG.debug("Deleting quota for table: " + tableName);
            QuotaUtil.deleteTableQuota(conn, tableName);
          } else if (userName != null) {
            LOG.debug("Deleting quota for user: " + userName);
            QuotaUtil.deleteUserQuota(conn, userName);
          }
        }
      } finally {
        if (scanner != null) {
          scanner.close();
        }
      }
    }
  }

  QuotaSettings getTableSpaceQuota(Connection conn, TableName tn) throws IOException {
    try (QuotaRetriever scanner = QuotaRetriever.open(
        conn.getConfiguration(), new QuotaFilter().setTableFilter(tn.getNameAsString()))) {
      for (QuotaSettings setting : scanner) {
        if (setting.getTableName().equals(tn) && setting.getQuotaType() == QuotaType.SPACE) {
          return setting;
        }
      }
      return null;
    }
  }

  /**
   * Waits 30seconds for the HBase quota table to exist.
   */
  public void waitForQuotaTable(Connection conn) throws IOException {
    waitForQuotaTable(conn, 30_000);
  }

  /**
   * Waits {@code timeout} milliseconds for the HBase quota table to exist.
   */
  public void waitForQuotaTable(Connection conn, long timeout) throws IOException {
    testUtil.waitFor(timeout, 1000, new Predicate<IOException>() {
      @Override
      public boolean evaluate() throws IOException {
        return conn.getAdmin().tableExists(QuotaUtil.QUOTA_TABLE_NAME);
      }
    });
  }

  void writeData(TableName tn, long sizeInBytes) throws IOException {
    writeData(testUtil.getConnection(), tn, sizeInBytes);
  }

  void writeData(Connection conn, TableName tn, long sizeInBytes) throws IOException {
    writeData(tn, sizeInBytes, Bytes.toBytes("q1"));
  }

  void writeData(TableName tn, long sizeInBytes, String qual) throws IOException {
    writeData(tn, sizeInBytes, Bytes.toBytes(qual));
  }

  void writeData(TableName tn, long sizeInBytes, byte[] qual) throws IOException {
    final Connection conn = testUtil.getConnection();
    final Table table = conn.getTable(tn);
    try {
      List<Put> updates = new ArrayList<>();
      long bytesToWrite = sizeInBytes;
      long rowKeyId = 0L;
      final StringBuilder sb = new StringBuilder();
      while (bytesToWrite > 0L) {
        sb.setLength(0);
        sb.append(Long.toString(rowKeyId));
        // Use the reverse counter as the rowKey to get even spread across all regions
        Put p = new Put(Bytes.toBytes(sb.reverse().toString()));
        byte[] value = new byte[SIZE_PER_VALUE];
        Bytes.random(value);
        p.addColumn(Bytes.toBytes(F1), qual, value);
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

  NamespaceDescriptor createNamespace() throws Exception {
    return createNamespace(null);
  }

  NamespaceDescriptor createNamespace(String namespace) throws Exception {
    if (namespace == null || namespace.trim().isEmpty())
      namespace = "ns" + counter.getAndIncrement();
    NamespaceDescriptor nd = NamespaceDescriptor.create(namespace).build();
    testUtil.getAdmin().createNamespace(nd);
    return nd;
  }

  Multimap<TableName, QuotaSettings> createTablesWithSpaceQuotas() throws Exception {
    final Admin admin = testUtil.getAdmin();
    final Multimap<TableName, QuotaSettings> tablesWithQuotas = HashMultimap.create();

    final TableName tn1 = createTable();
    final TableName tn2 = createTable();

    NamespaceDescriptor nd = createNamespace();
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

  TableName getNextTableName() {
    return getNextTableName(NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR);
  }

  TableName getNextTableName(String namespace) {
    return TableName.valueOf(namespace, testName.getMethodName() + counter.getAndIncrement());
  }

  TableName createTable() throws Exception {
    return createTableWithRegions(1);
  }

  TableName createTableWithRegions(int numRegions) throws Exception {
    return createTableWithRegions(NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR, numRegions);
  }

  TableName createTableWithRegions(Admin admin, int numRegions) throws Exception {
    return createTableWithRegions(admin, NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR, numRegions,
        0);
  }

  TableName createTableWithRegions(String namespace, int numRegions) throws Exception {
    return createTableWithRegions(testUtil.getAdmin(), namespace, numRegions, 0);
  }

  TableName createTableWithRegions(Admin admin, String namespace, int numRegions,
      int numberOfReplicas) throws Exception {
    final TableName tn = getNextTableName(namespace);

    // Delete the old table
    if (admin.tableExists(tn)) {
      admin.disableTable(tn);
      admin.deleteTable(tn);
    }

    // Create the table
    TableDescriptor tableDesc;
    if (numberOfReplicas > 0) {
      tableDesc = TableDescriptorBuilder.newBuilder(tn).setRegionReplication(numberOfReplicas)
          .setColumnFamily(ColumnFamilyDescriptorBuilder.of(F1)).build();
    } else {
      tableDesc = TableDescriptorBuilder.newBuilder(tn)
          .setColumnFamily(ColumnFamilyDescriptorBuilder.of(F1)).build();
    }
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
    TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(tn)
        .addColumnFamily(ColumnFamilyDescriptorBuilder.of(F1)).build();

    admin.createTable(tableDesc);
    return tn;
  }

  void partitionTablesByQuotaTarget(Multimap<TableName,QuotaSettings> quotas,
      Set<TableName> tablesWithTableQuota, Set<TableName> tablesWithNamespaceQuota) {
    // Partition the tables with quotas by table and ns quota
    for (Entry<TableName, QuotaSettings> entry : quotas.entries()) {
      SpaceLimitSettings settings = (SpaceLimitSettings) entry.getValue();
      TableName tn = entry.getKey();
      if (settings.getTableName() != null) {
        tablesWithTableQuota.add(tn);
      }
      if (settings.getNamespace() != null) {
        tablesWithNamespaceQuota.add(tn);
      }

      if (settings.getTableName() == null && settings.getNamespace() == null) {
        fail("Unexpected table name with null tableName and namespace: " + tn);
      }
    }
  }

  /**
   * Abstraction to simplify the case where a test needs to verify a certain state
   * on a {@code SpaceQuotaSnapshot}. This class fails-fast when there is no such
   * snapshot obtained from the Master. As such, it is not useful to verify the
   * lack of a snapshot.
   */
  static abstract class SpaceQuotaSnapshotPredicate implements Predicate<Exception> {
    private final Connection conn;
    private final TableName tn;
    private final String ns;

    SpaceQuotaSnapshotPredicate(Connection conn, TableName tn) {
      this(Objects.requireNonNull(conn), Objects.requireNonNull(tn), null);
    }

    SpaceQuotaSnapshotPredicate(Connection conn, String ns) {
      this(Objects.requireNonNull(conn), null, Objects.requireNonNull(ns));
    }

    SpaceQuotaSnapshotPredicate(Connection conn, TableName tn, String ns) {
      if ((null != tn && null != ns) || (null == tn && null == ns)) {
        throw new IllegalArgumentException(
            "One of TableName and Namespace must be non-null, and the other null");
      }
      this.conn = conn;
      this.tn = tn;
      this.ns = ns;
    }

    @Override
    public boolean evaluate() throws Exception {
      SpaceQuotaSnapshot snapshot;
      if (null == ns) {
        snapshot = (SpaceQuotaSnapshot) conn.getAdmin().getCurrentSpaceQuotaSnapshot(tn);
      } else {
        snapshot = (SpaceQuotaSnapshot) conn.getAdmin().getCurrentSpaceQuotaSnapshot(ns);
      }

      LOG.debug("Saw quota snapshot for " + (null == tn ? ns : tn) + ": " + snapshot);
      if (null == snapshot) {
        return false;
      }
      return evaluate(snapshot);
    }

    /**
     * Must determine if the given {@code SpaceQuotaSnapshot} meets some criteria.
     *
     * @param snapshot a non-null snapshot obtained from the HBase Master
     * @return true if the criteria is met, false otherwise
     */
    abstract boolean evaluate(SpaceQuotaSnapshot snapshot) throws Exception;
  }

  /**
   * Predicate that waits for all store files in a table to have no compacted files.
   */
  static class NoFilesToDischarge implements Predicate<Exception> {
    private final MiniHBaseCluster cluster;
    private final TableName tn;

    NoFilesToDischarge(MiniHBaseCluster cluster, TableName tn) {
      this.cluster = cluster;
      this.tn = tn;
    }

    @Override
    public boolean evaluate() throws Exception {
      for (HRegion region : cluster.getRegions(tn)) {
        for (HStore store : region.getStores()) {
          Collection<HStoreFile> files =
              store.getStoreEngine().getStoreFileManager().getCompactedfiles();
          if (null != files && !files.isEmpty()) {
            LOG.debug(region.getRegionInfo().getEncodedName() + " still has compacted files");
            return false;
          }
        }
      }
      return true;
    }
  }
}
