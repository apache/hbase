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
package org.apache.hadoop.hbase.io.hfile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for HFile v4 multi-tenant splitting logic using isolated test pattern.
 * <p>
 * Each test method runs independently with its own fresh cluster to ensure complete isolation and
 * avoid connection interference issues between tests.
 * <p>
 * This test validates the complete multi-tenant HFile v4 splitting workflow:
 * <ol>
 * <li><strong>Setup:</strong> Creates table with multi-tenant configuration</li>
 * <li><strong>Data Writing:</strong> Writes large datasets with different tenant distributions</li>
 * <li><strong>Flushing:</strong> Forces memstore flush to create multi-tenant HFile v4 files</li>
 * <li><strong>Splitting:</strong> Tests midkey calculation and file splitting</li>
 * <li><strong>Verification:</strong> Validates split balance and data integrity</li>
 * </ol>
 */
@Category(MediumTests.class)
public class MultiTenantHFileSplittingTest {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(MultiTenantHFileSplittingTest.class);

  private static final Logger LOG = LoggerFactory.getLogger(MultiTenantHFileSplittingTest.class);

  private HBaseTestingUtil testUtil;

  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final byte[] QUALIFIER = Bytes.toBytes("q");
  private static final int TENANT_PREFIX_LENGTH = 3;

  @Before
  public void setUp() throws Exception {
    LOG.info("=== Setting up isolated test environment ===");

    // Create fresh testing utility for each test
    testUtil = new HBaseTestingUtil();

    // Configure test settings
    Configuration conf = testUtil.getConfiguration();

    // Set HFile format version for multi-tenant support
    conf.setInt(HFile.FORMAT_VERSION_KEY, HFile.MIN_FORMAT_VERSION_WITH_MULTI_TENANT);
    conf.setInt(MultiTenantHFileWriter.TENANT_PREFIX_LENGTH, TENANT_PREFIX_LENGTH);

    // Set smaller region size to make splits easier to trigger
    conf.setLong("hbase.hregion.max.filesize", 10 * 1024 * 1024); // 10MB
    conf.setInt("hbase.regionserver.region.split.policy.check.period", 1000);

    // Use policy that allows manual splits
    conf.set("hbase.regionserver.region.split.policy",
      "org.apache.hadoop.hbase.regionserver.IncreasingToUpperBoundRegionSplitPolicy");

    // Configure mini cluster settings for stability
    conf.setInt("hbase.regionserver.msginterval", 100);
    conf.setInt("hbase.client.pause", 250);
    conf.setInt("hbase.client.retries.number", 6);
    conf.setBoolean("hbase.master.enabletable.roundrobin", true);

    // Increase timeouts for split operations
    conf.setLong("hbase.regionserver.fileSplitTimeout", 600000); // 10 minutes
    conf.setInt("hbase.client.operation.timeout", 600000); // 10 minutes

    LOG.info("Configured HFile format version: {}", conf.getInt(HFile.FORMAT_VERSION_KEY, -1));

    // Start fresh mini cluster for this test
    LOG.info("Starting fresh mini cluster for test");
    testUtil.startMiniCluster(1);

    // Wait for cluster to be ready
    testUtil.waitUntilAllRegionsAssigned(TableName.META_TABLE_NAME);

    LOG.info("Fresh cluster ready for test");
  }

  @After
  public void tearDown() throws Exception {
    LOG.info("=== Cleaning up isolated test environment ===");

    if (testUtil != null) {
      try {
        testUtil.shutdownMiniCluster();
        LOG.info("Successfully shut down test cluster");
      } catch (Exception e) {
        LOG.warn("Error during cluster shutdown", e);
      }
    }
  }

  /**
   * Test 1: Single tenant with large amount of data
   */
  @Test(timeout = 600000) // 10 minute timeout
  public void testSingleTenantSplitting() throws Exception {
    String[] tenants = { "T01" };
    int[] rowsPerTenant = { 10000 };

    executeTestScenario(tenants, rowsPerTenant);
  }

  /**
   * Test 2: Three tenants with even distribution
   */
  @Test(timeout = 600000) // 10 minute timeout
  public void testEvenDistributionSplitting() throws Exception {
    String[] tenants = { "T01", "T02", "T03" };
    int[] rowsPerTenant = { 3000, 3000, 3000 };

    executeTestScenario(tenants, rowsPerTenant);
  }

  /**
   * Test 3: Three tenants with uneven distribution
   */
  @Test(timeout = 600000) // 10 minute timeout
  public void testUnevenDistributionSplitting() throws Exception {
    String[] tenants = { "T01", "T02", "T03" };
    int[] rowsPerTenant = { 1000, 2000, 1000 };

    executeTestScenario(tenants, rowsPerTenant);
  }

  /**
   * Test 4: Skewed distribution with one dominant tenant
   */
  @Test(timeout = 600000) // 10 minute timeout
  public void testSkewedDistributionSplitting() throws Exception {
    String[] tenants = { "T01", "T02", "T03", "T04" };
    int[] rowsPerTenant = { 100, 100, 5000, 100 };

    executeTestScenario(tenants, rowsPerTenant);
  }

  /**
   * Test 5: Many small tenants
   */
  @Test(timeout = 600000) // 10 minute timeout
  public void testManySmallTenantsSplitting() throws Exception {
    String[] tenants = { "T01", "T02", "T03", "T04", "T05", "T06", "T07", "T08", "T09", "T10" };
    int[] rowsPerTenant = { 500, 500, 500, 500, 500, 500, 500, 500, 500, 500 };

    executeTestScenario(tenants, rowsPerTenant);
  }

  /**
   * Test 6: Few large tenants
   */
  @Test(timeout = 600000) // 10 minute timeout
  public void testFewLargeTenantsSplitting() throws Exception {
    String[] tenants = { "T01", "T02" };
    int[] rowsPerTenant = { 5000, 5000 };

    executeTestScenario(tenants, rowsPerTenant);
  }

  /**
   * Execute a test scenario with the given configuration. The table will be created fresh for this
   * test.
   */
  private void executeTestScenario(String[] tenants, int[] rowsPerTenant) throws Exception {
    LOG.info("=== Starting test scenario ===");

    // Generate unique table name for this test
    String testName = Thread.currentThread().getStackTrace()[2].getMethodName();
    TableName tableName = TableName.valueOf(testName + "_" + System.currentTimeMillis());

    // Validate input parameters
    if (tenants.length != rowsPerTenant.length) {
      throw new IllegalArgumentException("Tenants and rowsPerTenant arrays must have same length");
    }

    try {
      // Phase 1: Create fresh table
      LOG.info("Phase 1: Creating fresh table {}", tableName);
      createTestTable(tableName);

      // Wait for table to be ready
      Thread.sleep(1000);

      // Phase 2: Write test data
      LOG.info("Phase 2: Writing test data");
      writeTestData(tableName, tenants, rowsPerTenant);

      // Phase 3: Flush memstore to create HFiles
      LOG.info("Phase 3: Flushing table");
      testUtil.flush(tableName);

      // Wait for flush to complete
      Thread.sleep(2000);

      // Phase 4: Verify midkey before split
      LOG.info("Phase 4: Verifying midkey calculation");
      verifyMidkeyCalculation(tableName, tenants, rowsPerTenant);

      // Phase 5: Trigger split
      LOG.info("Phase 5: Triggering region split");
      triggerRegionSplit(tenants, rowsPerTenant, tableName);

      // Phase 6: Compact after split to ensure proper HFile structure
      LOG.info("Phase 6: Compacting table after split");
      testUtil.compact(tableName, true); // Major compaction

      // Wait for compaction to complete
      Thread.sleep(3000);

      // Phase 7: Comprehensive data integrity verification after split
      LOG.info("Phase 7: Starting comprehensive data integrity verification after split");
      verifyDataIntegrityWithScanning(tableName, tenants, rowsPerTenant);
      verifyDataIntegrityAfterSplit(tableName, tenants, rowsPerTenant);

      LOG.info("=== Test scenario completed successfully ===");

    } catch (Exception e) {
      LOG.error("Test scenario failed", e);
      throw e;
    } finally {
      // Clean up table
      try {
        if (testUtil.getAdmin() != null && testUtil.getAdmin().tableExists(tableName)) {
          testUtil.deleteTable(tableName);
        }
      } catch (Exception cleanupException) {
        LOG.warn("Failed to cleanup table {}: {}", tableName, cleanupException.getMessage());
      }
    }
  }

  /**
   * Create test table with multi-tenant configuration.
   */
  private void createTestTable(TableName tableName) throws IOException, InterruptedException {
    LOG.info("Creating table: {} with multi-tenant configuration", tableName);

    // Build table descriptor with multi-tenant properties
    TableDescriptorBuilder tableBuilder = TableDescriptorBuilder.newBuilder(tableName);

    // Set multi-tenant properties
    tableBuilder.setValue(MultiTenantHFileWriter.TABLE_TENANT_PREFIX_LENGTH,
      String.valueOf(TENANT_PREFIX_LENGTH));
    tableBuilder.setValue(MultiTenantHFileWriter.TABLE_MULTI_TENANT_ENABLED, "true");

    // Configure column family with proper settings
    ColumnFamilyDescriptorBuilder cfBuilder = ColumnFamilyDescriptorBuilder.newBuilder(FAMILY);

    // Ensure HFile v4 format is used at column family level
    cfBuilder.setValue(HFile.FORMAT_VERSION_KEY,
      String.valueOf(HFile.MIN_FORMAT_VERSION_WITH_MULTI_TENANT));

    // Set smaller block size for easier testing
    cfBuilder.setBlocksize(8 * 1024); // 8KB blocks

    tableBuilder.setColumnFamily(cfBuilder.build());

    // Create the table
    testUtil.createTable(tableBuilder.build(), null);

    LOG.info("Created table {} with multi-tenant configuration", tableName);
  }

  /**
   * Write test data for all tenants in lexicographic order to avoid key ordering violations.
   */
  private void writeTestData(TableName tableName, String[] tenants, int[] rowsPerTenant)
    throws IOException {
    try (Connection connection = ConnectionFactory.createConnection(testUtil.getConfiguration());
      Table table = connection.getTable(tableName)) {

      List<Put> batchPuts = new ArrayList<>();

      // Generate all row keys first
      for (int tenantIndex = 0; tenantIndex < tenants.length; tenantIndex++) {
        String tenantId = tenants[tenantIndex];
        int rowsForThisTenant = rowsPerTenant[tenantIndex];

        for (int rowIndex = 0; rowIndex < rowsForThisTenant; rowIndex++) {
          String rowKey = String.format("%srow%05d", tenantId, rowIndex);
          Put putOperation = new Put(Bytes.toBytes(rowKey));

          String cellValue = String.format("value_tenant-%s_row-%05d", tenantId, rowIndex);
          putOperation.addColumn(FAMILY, QUALIFIER, Bytes.toBytes(cellValue));
          batchPuts.add(putOperation);
        }
      }

      // Sort puts by row key to ensure lexicographic ordering
      batchPuts.sort((p1, p2) -> Bytes.compareTo(p1.getRow(), p2.getRow()));

      LOG.info("Writing {} total rows to table in lexicographic order", batchPuts.size());
      table.put(batchPuts);
      LOG.info("Successfully wrote all test data to table {}", tableName);
    }
  }

  /**
   * Verify midkey calculation for the HFile.
   */
  private void verifyMidkeyCalculation(TableName tableName, String[] tenants, int[] rowsPerTenant)
    throws IOException {
    LOG.info("Verifying midkey calculation for table: {}", tableName);

    // Find the HFile path for the table
    List<Path> hfilePaths = findHFilePaths(tableName);
    assertTrue("Should have at least one HFile", hfilePaths.size() > 0);

    Path hfilePath = hfilePaths.get(0); // Use the first HFile
    LOG.info("Checking midkey for HFile: {}", hfilePath);

    FileSystem fs = testUtil.getTestFileSystem();
    CacheConfig cacheConf = new CacheConfig(testUtil.getConfiguration());

    try (HFile.Reader reader =
      HFile.createReader(fs, hfilePath, cacheConf, true, testUtil.getConfiguration())) {
      assertTrue("Reader should be AbstractMultiTenantReader",
        reader instanceof AbstractMultiTenantReader);

      // Get the midkey
      Optional<ExtendedCell> midkey = reader.midKey();
      assertTrue("Midkey should be present", midkey.isPresent());

      String midkeyString = Bytes.toString(CellUtil.cloneRow(midkey.get()));
      LOG.info("Midkey: {}", midkeyString);

      // Analyze the midkey
      int totalRows = 0;
      for (int rows : rowsPerTenant) {
        totalRows += rows;
      }

      // Log HFile properties
      LOG.info("HFile properties:");
      LOG.info("  - First key: {}",
        reader.getFirstRowKey().isPresent()
          ? Bytes.toString(reader.getFirstRowKey().get())
          : "N/A");
      LOG.info("  - Last key: {}",
        reader.getLastRowKey().isPresent() ? Bytes.toString(reader.getLastRowKey().get()) : "N/A");
      LOG.info("  - Entry count: {}", reader.getEntries());

      // Determine which tenant and position within that tenant
      String midkeyTenant = midkeyString.substring(0, TENANT_PREFIX_LENGTH);
      int midkeyPosition = 0;
      boolean foundTenant = false;

      for (int i = 0; i < tenants.length; i++) {
        if (tenants[i].equals(midkeyTenant)) {
          int rowNum = Integer.parseInt(midkeyString.substring(TENANT_PREFIX_LENGTH + 3));
          midkeyPosition += rowNum;
          foundTenant = true;
          LOG.info("Midkey analysis:");
          LOG.info("  - Located in tenant: {}", midkeyTenant);
          LOG.info("  - Row number within tenant: {}/{}", rowNum, rowsPerTenant[i]);
          LOG.info("  - Position in file: {}/{} ({}%)", midkeyPosition, totalRows,
            String.format("%.1f", (midkeyPosition * 100.0) / totalRows));
          LOG.info("  - Target midpoint: {}/{} (50.0%)", totalRows / 2, totalRows);
          LOG.info("  - Deviation from midpoint: {}%",
            String.format("%.1f", Math.abs(midkeyPosition - totalRows / 2) * 100.0 / totalRows));
          break;
        } else {
          midkeyPosition += rowsPerTenant[i];
        }
      }

      assertTrue("Midkey tenant should be found in tenant list", foundTenant);

      // First and last keys for comparison
      if (reader.getFirstRowKey().isPresent() && reader.getLastRowKey().isPresent()) {
        String firstKey = Bytes.toString(reader.getFirstRowKey().get());
        String lastKey = Bytes.toString(reader.getLastRowKey().get());
        LOG.info("First key: {}", firstKey);
        LOG.info("Last key: {}", lastKey);
        LOG.info("Midkey comparison - first: {}, midkey: {}, last: {}", firstKey, midkeyString,
          lastKey);
      }

      LOG.info("Total rows in dataset: {}", totalRows);
    }
  }

  /**
   * Comprehensive data integrity verification using scanning operations. This method tests various
   * scanning scenarios to ensure data integrity after split.
   */
  private void verifyDataIntegrityWithScanning(TableName tableName, String[] tenants,
    int[] rowsPerTenant) throws Exception {
    LOG.info("=== Comprehensive Scanning Verification After Split ===");

    try (Connection conn = ConnectionFactory.createConnection(testUtil.getConfiguration());
      Table table = conn.getTable(tableName)) {

      // Test 1: Full table scan verification
      LOG.info("Test 1: Full table scan verification");
      verifyFullTableScanAfterSplit(table, tenants, rowsPerTenant);

      // Test 2: Tenant-specific scan verification
      LOG.info("Test 2: Tenant-specific scan verification");
      verifyTenantSpecificScansAfterSplit(table, tenants, rowsPerTenant);

      // Test 3: Cross-region boundary scanning
      LOG.info("Test 3: Cross-region boundary scanning");
      verifyCrossRegionBoundaryScanning(table, tenants, rowsPerTenant);

      // Test 4: Edge cases and tenant isolation
      LOG.info("Test 4: Edge cases and tenant isolation verification");
      verifyEdgeCasesAfterSplit(table, tenants, rowsPerTenant);

      LOG.info("Comprehensive scanning verification completed successfully");
    }
  }

  /**
   * Verify full table scan returns all data correctly after split.
   */
  private void verifyFullTableScanAfterSplit(Table table, String[] tenants, int[] rowsPerTenant)
    throws IOException {
    LOG.info("Performing full table scan to verify all data after split");

    org.apache.hadoop.hbase.client.Scan fullScan = new org.apache.hadoop.hbase.client.Scan();
    fullScan.addColumn(FAMILY, QUALIFIER);

    try (org.apache.hadoop.hbase.client.ResultScanner scanner = table.getScanner(fullScan)) {
      int totalRowsScanned = 0;
      int[] tenantRowCounts = new int[tenants.length];
      // Track seen rows per tenant to identify gaps later
      @SuppressWarnings("unchecked")
      java.util.Set<String>[] seenRowsPerTenant = new java.util.HashSet[tenants.length];
      for (int i = 0; i < tenants.length; i++) {
        seenRowsPerTenant[i] = new java.util.HashSet<>();
      }

      String previousRowKey = null;

      for (org.apache.hadoop.hbase.client.Result result : scanner) {
        if (result.isEmpty()) {
          LOG.warn("Empty result encountered during scan");
          continue;
        }

        String rowKey = Bytes.toString(result.getRow());

        // Verify row ordering
        if (previousRowKey != null) {
          assertTrue(
            "Rows should be in lexicographic order: " + previousRowKey + " should be <= " + rowKey,
            Bytes.compareTo(Bytes.toBytes(previousRowKey), Bytes.toBytes(rowKey)) <= 0);
        }

        String tenantPrefix = rowKey.substring(0, TENANT_PREFIX_LENGTH);

        // Find which tenant this row belongs to
        int tenantIndex = -1;
        for (int i = 0; i < tenants.length; i++) {
          if (tenants[i].equals(tenantPrefix)) {
            tenantIndex = i;
            break;
          }
        }

        if (tenantIndex == -1) {
          fail("Found row with unknown tenant prefix: " + rowKey);
        }

        // Verify data integrity
        byte[] cellValue = result.getValue(FAMILY, QUALIFIER);
        if (cellValue == null) {
          fail("Missing value for row: " + rowKey);
        }

        String actualValue = Bytes.toString(cellValue);
        if (!actualValue.contains(tenantPrefix)) {
          fail("Tenant data mixing detected in full scan: Row " + rowKey + " expected tenant "
            + tenantPrefix + " but got value " + actualValue);
        }

        tenantRowCounts[tenantIndex]++;
        seenRowsPerTenant[tenantIndex].add(rowKey);

        // Log every 1000th row for progress tracking
        if (totalRowsScanned % 1000 == 0) {
          LOG.info("Scanned {} rows so far, current row: {}", totalRowsScanned, rowKey);
        }

        previousRowKey = rowKey;
        totalRowsScanned++;
      }

      // Detailed logging of per-tenant counts before assertions
      StringBuilder sb = new StringBuilder();
      sb.append("Per-tenant scan results: ");
      for (int i = 0; i < tenants.length; i++) {
        sb.append(tenants[i]).append("=").append(tenantRowCounts[i]).append("/")
          .append(rowsPerTenant[i]).append(", ");
      }
      sb.append("total=").append(totalRowsScanned);
      LOG.info(sb.toString());

      // Verify total row count
      int expectedTotal = Arrays.stream(rowsPerTenant).sum();
      if (totalRowsScanned != expectedTotal) {
        LOG.error("Row count mismatch in full scan:");
        LOG.error("  Expected: {}", expectedTotal);
        LOG.error("  Scanned: {}", totalRowsScanned);

        // Log missing rows per tenant
        for (int i = 0; i < tenants.length; i++) {
          if (tenantRowCounts[i] != rowsPerTenant[i]) {
            java.util.List<String> missing = new java.util.ArrayList<>();
            for (int r = 0; r < rowsPerTenant[i]; r++) {
              String expectedKey = String.format("%srow%05d", tenants[i], r);
              if (!seenRowsPerTenant[i].contains(expectedKey)) {
                missing.add(expectedKey);
              }
            }
            LOG.error("Missing rows for tenant {} ({} missing): {}", tenants[i], missing.size(),
              missing.size() <= 10 ? missing : missing.subList(0, 10) + "...");
          }
        }

        fail("Full scan should return all rows after split. Expected: " + expectedTotal + ", Got: "
          + totalRowsScanned);
      }

      // Verify per-tenant row counts
      for (int i = 0; i < tenants.length; i++) {
        if (tenantRowCounts[i] != rowsPerTenant[i]) {
          LOG.error("Row count mismatch for tenant {} in full scan: expected {}, got {}",
            tenants[i], rowsPerTenant[i], tenantRowCounts[i]);

          // Log some missing rows for debugging
          java.util.List<String> missing = new java.util.ArrayList<>();
          for (int r = 0; r < rowsPerTenant[i]; r++) {
            String expectedKey = String.format("%srow%05d", tenants[i], r);
            if (!seenRowsPerTenant[i].contains(expectedKey)) {
              missing.add(expectedKey);
              if (missing.size() >= 5) {
                break; // Show first 5 missing rows
              }
            }
          }
          LOG.error("Sample missing rows for tenant {}: {}", tenants[i], missing);

          fail("Row count mismatch for tenant " + tenants[i] + " in full scan: expected "
            + rowsPerTenant[i] + ", got " + tenantRowCounts[i]);
        }
      }

      LOG.info("Full table scan verified successfully: {}/{} rows scanned", totalRowsScanned,
        expectedTotal);
    }
  }

  /**
   * Verify tenant-specific scans work correctly after split.
   */
  private void verifyTenantSpecificScansAfterSplit(Table table, String[] tenants,
    int[] rowsPerTenant) throws IOException {
    LOG.info("Verifying tenant-specific scans after split");

    for (int tenantIndex = 0; tenantIndex < tenants.length; tenantIndex++) {
      String tenant = tenants[tenantIndex];
      int expectedRows = rowsPerTenant[tenantIndex];

      LOG.info("Testing tenant-specific scan for tenant {}: expecting {} rows", tenant,
        expectedRows);

      org.apache.hadoop.hbase.client.Scan tenantScan = new org.apache.hadoop.hbase.client.Scan();
      tenantScan.addColumn(FAMILY, QUALIFIER);
      tenantScan.withStartRow(Bytes.toBytes(tenant + "row"));
      tenantScan.withStopRow(Bytes.toBytes(tenant + "row" + "\uFFFF"));

      try (
        org.apache.hadoop.hbase.client.ResultScanner tenantScanner = table.getScanner(tenantScan)) {
        int tenantRowCount = 0;
        List<String> foundRows = new ArrayList<>();

        for (org.apache.hadoop.hbase.client.Result result : tenantScanner) {
          String rowKey = Bytes.toString(result.getRow());
          foundRows.add(rowKey);

          if (!rowKey.startsWith(tenant)) {
            fail("Tenant scan violation after split: Found row " + rowKey + " in scan for tenant "
              + tenant);
          }

          // Verify data integrity for this row
          byte[] cellValue = result.getValue(FAMILY, QUALIFIER);
          if (cellValue == null) {
            fail("Missing value for tenant row: " + rowKey);
          }

          String actualValue = Bytes.toString(cellValue);
          if (!actualValue.contains(tenant)) {
            fail("Tenant data corruption after split: Row " + rowKey + " expected tenant " + tenant
              + " but got value " + actualValue);
          }

          tenantRowCount++;
        }

        if (tenantRowCount != expectedRows) {
          LOG.error("Row count mismatch for tenant {} after split:", tenant);
          LOG.error("  Expected: {}", expectedRows);
          LOG.error("  Found: {}", tenantRowCount);
          LOG.error("  Found rows: {}", foundRows);
        }

        assertEquals("Row count mismatch for tenant " + tenant + " after split", expectedRows,
          tenantRowCount);

        LOG.info("Tenant {} scan successful after split: {}/{} rows verified", tenant,
          tenantRowCount, expectedRows);
      }
    }

    LOG.info("All tenant-specific scans verified successfully after split");
  }

  /**
   * Verify scanning across region boundaries works correctly.
   */
  private void verifyCrossRegionBoundaryScanning(Table table, String[] tenants, int[] rowsPerTenant)
    throws IOException {
    LOG.info("Verifying cross-region boundary scanning after split");

    // Test scanning across the split point
    // Find a range that likely spans both regions
    String firstTenant = tenants[0];
    String lastTenant = tenants[tenants.length - 1];

    org.apache.hadoop.hbase.client.Scan crossRegionScan = new org.apache.hadoop.hbase.client.Scan();
    crossRegionScan.addColumn(FAMILY, QUALIFIER);
    crossRegionScan.withStartRow(Bytes.toBytes(firstTenant + "row000"));
    crossRegionScan.withStopRow(Bytes.toBytes(lastTenant + "row999"));

    try (org.apache.hadoop.hbase.client.ResultScanner scanner = table.getScanner(crossRegionScan)) {
      int totalRowsScanned = 0;
      String previousRowKey = null;

      for (org.apache.hadoop.hbase.client.Result result : scanner) {
        String rowKey = Bytes.toString(result.getRow());

        // Verify row ordering is maintained across regions
        if (previousRowKey != null) {
          assertTrue("Row ordering should be maintained across regions: " + previousRowKey
            + " should be <= " + rowKey,
            Bytes.compareTo(Bytes.toBytes(previousRowKey), Bytes.toBytes(rowKey)) <= 0);
        }

        // Verify data integrity
        byte[] cellValue = result.getValue(FAMILY, QUALIFIER);
        if (cellValue == null) {
          fail("Missing value in cross-region scan for row: " + rowKey);
        }

        String tenantPrefix = rowKey.substring(0, TENANT_PREFIX_LENGTH);
        String actualValue = Bytes.toString(cellValue);
        if (!actualValue.contains(tenantPrefix)) {
          fail("Data corruption in cross-region scan: Row " + rowKey + " expected tenant "
            + tenantPrefix + " but got value " + actualValue);
        }

        previousRowKey = rowKey;
        totalRowsScanned++;
      }

      assertTrue("Cross-region scan should find data", totalRowsScanned > 0);
      LOG.info("Cross-region boundary scan verified: {} rows scanned with proper ordering",
        totalRowsScanned);
    }
  }

  /**
   * Verify edge cases and tenant isolation after split.
   */
  private void verifyEdgeCasesAfterSplit(Table table, String[] tenants, int[] rowsPerTenant)
    throws IOException {
    LOG.info("Verifying edge cases and tenant isolation after split");

    // Test 1: Non-existent tenant scan
    LOG.info("Testing scan with non-existent tenant prefix");
    String nonExistentTenant = "ZZZ";
    org.apache.hadoop.hbase.client.Scan nonExistentScan = new org.apache.hadoop.hbase.client.Scan();
    nonExistentScan.addColumn(FAMILY, QUALIFIER);
    nonExistentScan.withStartRow(Bytes.toBytes(nonExistentTenant + "row"));
    nonExistentScan.withStopRow(Bytes.toBytes(nonExistentTenant + "row" + "\uFFFF"));

    try (org.apache.hadoop.hbase.client.ResultScanner scanner = table.getScanner(nonExistentScan)) {
      int rowCount = 0;
      for (org.apache.hadoop.hbase.client.Result result : scanner) {
        rowCount++;
      }
      assertEquals("Non-existent tenant scan should return no results after split", 0, rowCount);
    }

    // Test 2: Tenant boundary isolation
    LOG.info("Testing tenant boundary isolation after split");
    for (int i = 0; i < tenants.length - 1; i++) {
      String tenant1 = tenants[i];
      String tenant2 = tenants[i + 1];

      // Scan from last row of tenant1 to first row of tenant2
      org.apache.hadoop.hbase.client.Scan boundaryScan = new org.apache.hadoop.hbase.client.Scan();
      boundaryScan.addColumn(FAMILY, QUALIFIER);
      boundaryScan
        .withStartRow(Bytes.toBytes(tenant1 + "row" + String.format("%05d", rowsPerTenant[i] - 1)));
      boundaryScan.withStopRow(Bytes.toBytes(tenant2 + "row001"));

      try (org.apache.hadoop.hbase.client.ResultScanner scanner = table.getScanner(boundaryScan)) {
        boolean foundTenant1 = false;
        boolean foundTenant2 = false;

        for (org.apache.hadoop.hbase.client.Result result : scanner) {
          String rowKey = Bytes.toString(result.getRow());

          if (rowKey.startsWith(tenant1)) {
            foundTenant1 = true;
          } else if (rowKey.startsWith(tenant2)) {
            foundTenant2 = true;
          } else {
            fail("Unexpected tenant in boundary scan after split: " + rowKey);
          }
        }

        // We should find data from both tenants at the boundary
        assertTrue("Should find tenant " + tenant1 + " data in boundary scan", foundTenant1);
        if (rowsPerTenant[i + 1] > 0) {
          assertTrue("Should find tenant " + tenant2 + " data in boundary scan", foundTenant2);
        }
      }
    }

    LOG.info("Edge cases and tenant isolation verification completed successfully");
  }

  /**
   * Verify data integrity after split using GET operations.
   */
  private void verifyDataIntegrityAfterSplit(TableName tableName, String[] tenants,
    int[] rowsPerTenant) throws Exception {
    LOG.info("Verifying data integrity with GET operations");

    try (Connection conn = ConnectionFactory.createConnection(testUtil.getConfiguration());
      Table table = conn.getTable(tableName)) {

      int totalRowsVerified = 0;

      for (int tenantIndex = 0; tenantIndex < tenants.length; tenantIndex++) {
        String tenant = tenants[tenantIndex];
        int rowsForThisTenant = rowsPerTenant[tenantIndex];

        for (int i = 0; i < rowsForThisTenant; i++) {
          String rowKey = String.format("%srow%05d", tenant, i);
          String expectedValue = String.format("value_tenant-%s_row-%05d", tenant, i);

          Get get = new Get(Bytes.toBytes(rowKey));
          get.addColumn(FAMILY, QUALIFIER);

          Result result = table.get(get);
          assertFalse("Result should not be empty for row: " + rowKey, result.isEmpty());

          byte[] actualValue = result.getValue(FAMILY, QUALIFIER);
          String actualValueStr = Bytes.toString(actualValue);
          assertEquals("Value mismatch for row " + rowKey, expectedValue, actualValueStr);
          totalRowsVerified++;
        }
      }

      int expectedTotal = Arrays.stream(rowsPerTenant).sum();
      assertEquals("All rows should be verified", expectedTotal, totalRowsVerified);
      LOG.info("Data integrity verified: {}/{} rows", totalRowsVerified, expectedTotal);
    }
  }

  /**
   * Find all HFiles created for the test table.
   */
  private List<Path> findHFilePaths(TableName tableName) throws IOException {
    List<Path> hfilePaths = new ArrayList<>();

    Path rootDir = testUtil.getDataTestDirOnTestFS();
    Path tableDir = new Path(rootDir, "data/default/" + tableName.getNameAsString());

    if (testUtil.getTestFileSystem().exists(tableDir)) {
      FileStatus[] regionDirs = testUtil.getTestFileSystem().listStatus(tableDir);

      for (FileStatus regionDir : regionDirs) {
        if (regionDir.isDirectory() && !regionDir.getPath().getName().startsWith(".")) {
          Path familyDir = new Path(regionDir.getPath(), Bytes.toString(FAMILY));

          if (testUtil.getTestFileSystem().exists(familyDir)) {
            FileStatus[] hfiles = testUtil.getTestFileSystem().listStatus(familyDir);

            for (FileStatus hfile : hfiles) {
              if (
                !hfile.getPath().getName().startsWith(".")
                  && !hfile.getPath().getName().endsWith(".tmp")
              ) {
                hfilePaths.add(hfile.getPath());
                LOG.debug("Found HFile: {} (size: {} bytes)", hfile.getPath().getName(),
                  hfile.getLen());
              }
            }
          }
        }
      }
    }

    LOG.info("Found {} HFiles total", hfilePaths.size());
    return hfilePaths;
  }

  /**
   * Trigger region split and wait for completion using HBaseTestingUtil methods.
   */
  private void triggerRegionSplit(String[] tenants, int[] rowsPerTenant, TableName tableName)
    throws Exception {
    LOG.info("Starting region split for table: {}", tableName);

    // First ensure cluster is healthy and responsive
    LOG.info("Checking cluster health before split");
    try {
      // Verify cluster is running
      assertTrue("Mini cluster should be running", testUtil.getMiniHBaseCluster() != null);
      LOG.info("Mini cluster is up and running");

      // Add more debug info about cluster state
      LOG.info("Master is active: {}", testUtil.getMiniHBaseCluster().getMaster().isActiveMaster());
      LOG.info("Number of region servers: {}",
        testUtil.getMiniHBaseCluster().getNumLiveRegionServers());
      LOG.info("Master address: {}", testUtil.getMiniHBaseCluster().getMaster().getServerName());

      // Ensure no regions are in transition before starting split
      testUtil.waitUntilNoRegionsInTransition(60000);

    } catch (Exception e) {
      LOG.warn("Cluster health check failed: {}", e.getMessage());
      throw new RuntimeException("Cluster is not healthy before split attempt", e);
    }

    // Get initial region count and submit split request
    LOG.info("Getting initial region count and submitting split");
    try (Connection connection = ConnectionFactory.createConnection(testUtil.getConfiguration());
      Admin admin = connection.getAdmin()) {

      // Ensure table exists and is available
      LOG.info("Verifying table exists: {}", tableName);
      boolean tableExists = admin.tableExists(tableName);
      if (!tableExists) {
        throw new RuntimeException("Table " + tableName + " does not exist before split");
      }

      // Ensure table is enabled
      if (!admin.isTableEnabled(tableName)) {
        LOG.info("Table {} is disabled, enabling it", tableName);
        admin.enableTable(tableName);
        testUtil.waitTableEnabled(tableName.getName(), 30000);
      }

      LOG.info("Table {} exists and is enabled", tableName);

      List<RegionInfo> regions = admin.getRegions(tableName);
      assertEquals("Should have exactly one region before split", 1, regions.size());
      LOG.info("Pre-split verification passed. Table {} has {} region(s)", tableName,
        regions.size());

      RegionInfo regionToSplit = regions.get(0);
      LOG.info("Region to split: {} [{} -> {}]", regionToSplit.getEncodedName(),
        Bytes.toStringBinary(regionToSplit.getStartKey()),
        Bytes.toStringBinary(regionToSplit.getEndKey()));

      // Trigger the split - let HBase choose the split point based on midkey calculation
      LOG.info("Submitting split request for table: {}", tableName);
      admin.split(tableName);
      LOG.info("Split request submitted successfully for table: {}", tableName);

      // Wait a moment for split request to be processed
      Thread.sleep(2000);
    }

    // Wait for split to complete using HBaseTestingUtil methods with extended timeouts
    LOG.info("Waiting for split processing to complete...");

    // First wait for no regions in transition
    boolean splitCompleted = false;
    int maxWaitCycles = 12; // 12 * 10 seconds = 2 minutes max
    int waitCycle = 0;

    while (!splitCompleted && waitCycle < maxWaitCycles) {
      waitCycle++;
      LOG.info("Split wait cycle {}/{}: Waiting for regions to stabilize...", waitCycle,
        maxWaitCycles);

      try {
        // Wait for no regions in transition (10 second timeout per cycle)
        testUtil.waitUntilNoRegionsInTransition(10000);

        // Check if split actually completed
        try (Connection conn = ConnectionFactory.createConnection(testUtil.getConfiguration());
          Admin checkAdmin = conn.getAdmin()) {

          List<RegionInfo> currentRegions = checkAdmin.getRegions(tableName);
          if (currentRegions.size() > 1) {
            splitCompleted = true;
            LOG.info("Split completed successfully! Regions after split: {}",
              currentRegions.size());
          } else {
            LOG.info("Split not yet complete, still {} region(s). Waiting...",
              currentRegions.size());
            Thread.sleep(5000); // Wait 5 seconds before next check
          }
        }

      } catch (Exception e) {
        LOG.warn("Error during split wait cycle {}: {}", waitCycle, e.getMessage());
        if (waitCycle == maxWaitCycles) {
          throw new RuntimeException("Split failed after maximum wait time", e);
        }
        Thread.sleep(5000); // Wait before retrying
      }
    }

    if (!splitCompleted) {
      throw new RuntimeException("Region split did not complete within timeout period");
    }

    // Give additional time for the split to fully stabilize
    LOG.info("Split completed, waiting for final stabilization...");
    Thread.sleep(3000);

    // Final verification of split completion
    LOG.info("Performing final verification of split completion...");
    try (Connection conn = ConnectionFactory.createConnection(testUtil.getConfiguration());
      Admin finalAdmin = conn.getAdmin()) {

      List<RegionInfo> regionsAfterSplit = finalAdmin.getRegions(tableName);
      if (regionsAfterSplit.size() <= 1) {
        fail("Region split did not complete successfully. Expected > 1 region, got: "
          + regionsAfterSplit.size());
      }
      LOG.info("Final verification passed. Regions after split: {}", regionsAfterSplit.size());

      // Log region details for debugging
      for (int i = 0; i < regionsAfterSplit.size(); i++) {
        RegionInfo region = regionsAfterSplit.get(i);
        LOG.info("Region {}: {} [{} -> {}]", i + 1, region.getEncodedName(),
          Bytes.toStringBinary(region.getStartKey()), Bytes.toStringBinary(region.getEndKey()));
      }

      LOG.info("Split operation completed successfully.");
    }
  }
}
