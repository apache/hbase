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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for multi-tenant HFile functionality.
 * <p>
 * This test validates the complete multi-tenant HFile workflow:
 * <ol>
 * <li><strong>Setup:</strong> Creates table with multi-tenant configuration</li>
 * <li><strong>Data Writing:</strong> Writes data for multiple tenants with distinct prefixes</li>
 * <li><strong>Flushing:</strong> Forces memstore flush to create multi-tenant HFile v4 files</li>
 * <li><strong>Verification:</strong> Tests various read patterns and tenant isolation</li>
 * <li><strong>Format Validation:</strong> Verifies HFile v4 structure and tenant sections</li>
 * </ol>
 * <p>
 * The test ensures tenant data isolation, format compliance, and data integrity across different
 * access patterns (GET, SCAN, tenant-specific scans).
 */
@Category(MediumTests.class)
public class MultiTenantHFileIntegrationTest {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(MultiTenantHFileIntegrationTest.class);

  private static final Logger LOG = LoggerFactory.getLogger(MultiTenantHFileIntegrationTest.class);

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  private static final TableName TABLE_NAME = TableName.valueOf("TestMultiTenantTable");
  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final byte[] QUALIFIER = Bytes.toBytes("q");

  private static final int TENANT_PREFIX_LENGTH = 3;
  private static final String[] TENANTS =
    { "T01", "T02", "T03", "T04", "T05", "T06", "T07", "T08", "T09", "T10" };
  private static final int[] ROWS_PER_TENANT = { 5, 8, 12, 3, 15, 7, 20, 6, 10, 14 };
  private static final Map<String, Integer> TENANT_DELETE_FAMILY_COUNTS = new HashMap<>();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    LOG.info("=== Setting up Multi-Tenant HFile Integration Test ===");
    Configuration conf = TEST_UTIL.getConfiguration();

    // Configure multi-tenant HFile settings
    conf.setInt(MultiTenantHFileWriter.TENANT_PREFIX_LENGTH, TENANT_PREFIX_LENGTH);
    conf.setInt(HFile.FORMAT_VERSION_KEY, HFile.MIN_FORMAT_VERSION_WITH_MULTI_TENANT);

    LOG.info("Starting mini cluster with multi-tenant HFile configuration");
    LOG.info("  - Tenant prefix length: {}", TENANT_PREFIX_LENGTH);
    LOG.info("  - HFile format version: {}", HFile.MIN_FORMAT_VERSION_WITH_MULTI_TENANT);

    TEST_UTIL.startMiniCluster(1);
    LOG.info("Mini cluster started successfully");
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    LOG.info("=== Shutting down Multi-Tenant HFile Integration Test ===");
    TEST_UTIL.shutdownMiniCluster();
    LOG.info("Mini cluster shutdown complete");
  }

  /**
   * End-to-end test of multi-tenant HFile functionality.
   * <p>
   * Test execution flow:
   * <ol>
   * <li>Create table with multi-tenant configuration</li>
   * <li>Write test data for {} tenants with varying row counts</li>
   * <li>Flush memstore to create multi-tenant HFiles</li>
   * <li>Verify data integrity using GET operations</li>
   * <li>Verify data using full table SCAN</li>
   * <li>Verify tenant isolation using tenant-specific scans</li>
   * <li>Test edge cases and cross-tenant isolation</li>
   * <li>Validate HFile format and tenant section structure</li>
   * </ol>
   */
  @Test(timeout = 180000)
  public void testMultiTenantHFileCreation() throws Exception {
    LOG.info("=== Starting Multi-Tenant HFile Integration Test ===");
    LOG.info("Test will process {} tenants with {} total expected rows", TENANTS.length,
      calculateTotalExpectedRows());

    // Phase 1: Setup
    LOG.info("Phase 1: Creating test table with multi-tenant configuration");
    createTestTable();

    // Phase 2: Data Writing
    LOG.info("Phase 2: Writing test data for {} tenants", TENANTS.length);
    writeTestData();

    // Phase 3: Pre-flush Verification
    LOG.info("Phase 3: Verifying memstore state before flush");
    assertTableMemStoreNotEmpty();

    // Phase 4: Flushing
    LOG.info("Phase 4: Flushing memstore to create multi-tenant HFiles");
    flushTable();

    // Phase 5: Post-flush Verification
    LOG.info("Phase 5: Verifying memstore state after flush");
    assertTableMemStoreEmpty();

    // Wait for HFiles to stabilize
    LOG.info("Waiting for HFiles to stabilize...");
    Thread.sleep(2000);

    // Phase 6: Data Verification
    LOG.info("Phase 6: Starting comprehensive data verification");
    verifyDataWithGet();
    verifyDataWithScan();
    verifyDataWithTenantSpecificScans();
    verifyEdgeCasesAndCrossTenantIsolation();

    // Phase 7: HFile Format Verification
    LOG.info("Phase 7: Verifying HFile format and structure");
    List<Path> hfilePaths = findHFilePaths();
    assertFalse("No HFiles found after flush", hfilePaths.isEmpty());
    LOG.info("Found {} HFiles for verification", hfilePaths.size());
    verifyHFileFormat(hfilePaths);

    LOG.info("=== Multi-tenant HFile integration test completed successfully ===");
  }

  /**
   * Calculate total expected rows across all tenants.
   * @return sum of rows across all tenants
   */
  private static int calculateTotalExpectedRows() {
    int total = 0;
    for (int rows : ROWS_PER_TENANT) {
      total += rows;
    }
    return total;
  }

  /**
   * Create test table with multi-tenant configuration. Sets up table properties required for
   * multi-tenant HFile functionality.
   */
  private void createTestTable() throws IOException {
    try (Admin admin = TEST_UTIL.getAdmin()) {
      TableDescriptorBuilder tableBuilder = TableDescriptorBuilder.newBuilder(TABLE_NAME);

      // Set multi-tenant properties
      tableBuilder.setValue(MultiTenantHFileWriter.TABLE_TENANT_PREFIX_LENGTH,
        String.valueOf(TENANT_PREFIX_LENGTH));
      tableBuilder.setValue(MultiTenantHFileWriter.TABLE_MULTI_TENANT_ENABLED, "true");

      tableBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(FAMILY).build());

      admin.createTable(tableBuilder.build());
      LOG.info("Created table {} with multi-tenant configuration", TABLE_NAME);

      try {
        TEST_UTIL.waitTableAvailable(TABLE_NAME);
        LOG.info("Table {} is now available", TABLE_NAME);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted waiting for table", e);
      }
    }
  }

  /**
   * Write test data for all tenants. Creates rows with format: {tenantId}row{paddedIndex} ->
   * value_tenant-{tenantId}_row-{paddedIndex}
   */
  private void writeTestData() throws IOException {
    try (Connection connection = TEST_UTIL.getConnection();
      Table table = connection.getTable(TABLE_NAME)) {

      List<Put> batchPuts = new ArrayList<>();
      List<Delete> batchDeletes = new ArrayList<>();
      TENANT_DELETE_FAMILY_COUNTS.clear();

      LOG.info("Generating test data for {} tenants:", TENANTS.length);
      for (int tenantIndex = 0; tenantIndex < TENANTS.length; tenantIndex++) {
        String tenantId = TENANTS[tenantIndex];
        TENANT_DELETE_FAMILY_COUNTS.put(tenantId, 0);
        int rowsForThisTenant = ROWS_PER_TENANT[tenantIndex];
        LOG.info("  - Tenant {}: {} rows", tenantId, rowsForThisTenant);

        for (int rowIndex = 0; rowIndex < rowsForThisTenant; rowIndex++) {
          String rowKey = String.format("%srow%03d", tenantId, rowIndex);
          Put putOperation = new Put(Bytes.toBytes(rowKey));

          String cellValue = String.format("value_tenant-%s_row-%03d", tenantId, rowIndex);
          putOperation.addColumn(FAMILY, QUALIFIER, Bytes.toBytes(cellValue));
          batchPuts.add(putOperation);

          if (rowIndex == 0 || rowIndex == rowsForThisTenant - 1) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            delete.addFamily(FAMILY, 0L);
            batchDeletes.add(delete);
            TENANT_DELETE_FAMILY_COUNTS.merge(tenantId, 1, Integer::sum);
          }
        }
      }

      LOG.info("Writing {} total rows to table in batch operation", batchPuts.size());
      table.put(batchPuts);
      if (!batchDeletes.isEmpty()) {
        LOG.info("Writing {} delete family markers with timestamp 0", batchDeletes.size());
        table.delete(batchDeletes);
      }
      LOG.info("Successfully wrote all test data to table {}", TABLE_NAME);
    }
  }

  /**
   * Verify that memstore contains data before flush.
   */
  private void assertTableMemStoreNotEmpty() {
    List<HRegion> regions = TEST_UTIL.getHBaseCluster().getRegions(TABLE_NAME);
    long totalSize = regions.stream().mapToLong(HRegion::getMemStoreDataSize).sum();
    assertTrue("Table memstore should not be empty", totalSize > 0);
    LOG.info("Memstore contains {} bytes of data before flush", totalSize);
  }

  /**
   * Verify that memstore is empty after flush.
   */
  private void assertTableMemStoreEmpty() {
    List<HRegion> regions = TEST_UTIL.getHBaseCluster().getRegions(TABLE_NAME);
    long totalSize = regions.stream().mapToLong(HRegion::getMemStoreDataSize).sum();
    assertEquals("Table memstore should be empty after flush", 0, totalSize);
    LOG.info("Memstore is empty after flush (size: {} bytes)", totalSize);
  }

  /**
   * Flush table to create HFiles on disk.
   */
  private void flushTable() throws IOException {
    LOG.info("Initiating flush operation for table {}", TABLE_NAME);
    TEST_UTIL.flush(TABLE_NAME);

    // Wait for flush to complete
    try {
      Thread.sleep(5000);
      TEST_UTIL.waitTableAvailable(TABLE_NAME, 30000);
      LOG.info("Flush operation completed successfully");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("Interrupted while waiting for flush", e);
    } catch (Exception e) {
      LOG.warn("Exception while waiting for table availability: {}", e.getMessage());
    }
  }

  /**
   * Verify data integrity using individual GET operations. Tests that each row can be retrieved
   * correctly with expected values.
   */
  private void verifyDataWithGet() throws Exception {
    LOG.info("=== Verification Phase 1: GET Operations ===");

    try (Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
      Table table = conn.getTable(TABLE_NAME)) {

      int totalRowsVerified = 0;

      for (int tenantIndex = 0; tenantIndex < TENANTS.length; tenantIndex++) {
        String tenant = TENANTS[tenantIndex];
        int rowsForThisTenant = ROWS_PER_TENANT[tenantIndex];

        LOG.info("Verifying GET operations for tenant {}: {} rows", tenant, rowsForThisTenant);

        for (int i = 0; i < rowsForThisTenant; i++) {
          String rowKey = tenant + "row" + String.format("%03d", i);
          String expectedValue = "value_tenant-" + tenant + "_row-" + String.format("%03d", i);

          Get get = new Get(Bytes.toBytes(rowKey));
          get.addColumn(FAMILY, QUALIFIER);

          Result result = table.get(get);
          if (result.isEmpty()) {
            fail("No result found for row: " + rowKey);
          }

          byte[] actualValue = result.getValue(FAMILY, QUALIFIER);
          String actualValueStr = Bytes.toString(actualValue);
          assertEquals("Value mismatch for row " + rowKey, expectedValue, actualValueStr);
          totalRowsVerified++;
        }

        LOG.info("Successfully verified {} GET operations for tenant {}", rowsForThisTenant,
          tenant);
      }

      LOG.info("GET verification completed: {}/{} rows verified successfully", totalRowsVerified,
        calculateTotalExpectedRows());
    }
  }

  /**
   * Verify data integrity using full table SCAN. Tests complete data retrieval and checks for
   * tenant data mixing.
   */
  private void verifyDataWithScan() throws IOException {
    LOG.info("=== Verification Phase 2: Full Table SCAN ===");

    try (Connection connection = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
      Table table = connection.getTable(TABLE_NAME)) {

      org.apache.hadoop.hbase.client.Scan tableScan = new org.apache.hadoop.hbase.client.Scan();
      tableScan.addColumn(FAMILY, QUALIFIER);

      try (
        org.apache.hadoop.hbase.client.ResultScanner resultScanner = table.getScanner(tableScan)) {
        int totalRowCount = 0;

        LOG.info("Starting full table scan to verify all data");

        for (org.apache.hadoop.hbase.client.Result scanResult : resultScanner) {
          String rowKey = Bytes.toString(scanResult.getRow());
          String extractedTenantId = rowKey.substring(0, TENANT_PREFIX_LENGTH);

          byte[] cellValue = scanResult.getValue(FAMILY, QUALIFIER);
          if (cellValue != null) {
            String actualValueString = Bytes.toString(cellValue);
            if (!actualValueString.contains(extractedTenantId)) {
              fail("Tenant data mixing detected: Row " + rowKey + " expected tenant "
                + extractedTenantId + " but got value " + actualValueString);
            }
          } else {
            fail("Missing value for row: " + rowKey);
          }

          totalRowCount++;
        }

        int expectedTotalRows = calculateTotalExpectedRows();
        assertEquals("Row count mismatch", expectedTotalRows, totalRowCount);

        LOG.info("Full table SCAN completed: {}/{} rows scanned successfully", totalRowCount,
          expectedTotalRows);
      }
    }
  }

  /**
   * Verify tenant isolation using tenant-specific SCAN operations. Tests that each tenant's data
   * can be accessed independently without cross-tenant leakage.
   */
  private void verifyDataWithTenantSpecificScans() throws IOException {
    LOG.info("=== Verification Phase 3: Tenant-Specific SCANs ===");

    try (Connection connection = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
      Table table = connection.getTable(TABLE_NAME)) {

      int totalTenantsVerified = 0;

      for (int tenantIndex = 0; tenantIndex < TENANTS.length; tenantIndex++) {
        String targetTenantId = TENANTS[tenantIndex];
        int expectedRowsForThisTenant = ROWS_PER_TENANT[tenantIndex];

        LOG.info("Verifying tenant-specific scan for tenant {}: expecting {} rows", targetTenantId,
          expectedRowsForThisTenant);

        org.apache.hadoop.hbase.client.Scan tenantScan = new org.apache.hadoop.hbase.client.Scan();
        tenantScan.addColumn(FAMILY, QUALIFIER);
        tenantScan.withStartRow(Bytes.toBytes(targetTenantId + "row"));
        tenantScan.withStopRow(Bytes.toBytes(targetTenantId + "row" + "\uFFFF"));

        try (org.apache.hadoop.hbase.client.ResultScanner tenantScanner =
          table.getScanner(tenantScan)) {
          int tenantRowCount = 0;
          List<String> foundRows = new ArrayList<>();

          for (org.apache.hadoop.hbase.client.Result scanResult : tenantScanner) {
            String rowKey = Bytes.toString(scanResult.getRow());
            foundRows.add(rowKey);

            if (!rowKey.startsWith(targetTenantId)) {
              fail("Tenant scan violation: Found row " + rowKey + " in scan for tenant "
                + targetTenantId);
            }

            tenantRowCount++;
          }

          // Debug logging to see which rows were found
          LOG.info("Tenant {} scan found {} rows: {}", targetTenantId, tenantRowCount, foundRows);

          if (tenantRowCount != expectedRowsForThisTenant) {
            // Generate expected rows for comparison
            List<String> expectedRows = new ArrayList<>();
            for (int j = 0; j < expectedRowsForThisTenant; j++) {
              expectedRows.add(targetTenantId + "row" + String.format("%03d", j));
            }
            LOG.error("Expected rows for {}: {}", targetTenantId, expectedRows);
            LOG.error("Found rows for {}: {}", targetTenantId, foundRows);

            // Find missing rows
            List<String> missingRows = new ArrayList<>(expectedRows);
            missingRows.removeAll(foundRows);
            LOG.error("Missing rows for {}: {}", targetTenantId, missingRows);
          }

          assertEquals("Row count mismatch for tenant " + targetTenantId, expectedRowsForThisTenant,
            tenantRowCount);

          LOG.info("Tenant {} scan successful: {}/{} rows verified", targetTenantId, tenantRowCount,
            expectedRowsForThisTenant);
        }

        totalTenantsVerified++;
      }

      LOG.info("Tenant-specific SCAN verification completed: {}/{} tenants verified successfully",
        totalTenantsVerified, TENANTS.length);
    }
  }

  /**
   * Verify edge cases and cross-tenant isolation boundaries. Tests non-existent tenant queries,
   * empty scan behavior, and tenant boundary conditions.
   */
  private void verifyEdgeCasesAndCrossTenantIsolation() throws IOException {
    LOG.info("=== Verification Phase 4: Edge Cases and Cross-Tenant Isolation ===");

    try (Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
      Table table = conn.getTable(TABLE_NAME)) {

      // Test 1: Non-existent tenant prefix
      LOG.info("Test 1: Scanning with non-existent tenant prefix");
      verifyNonExistentTenantScan(table);

      // Test 2: Tenant boundary isolation
      LOG.info("Test 2: Verifying tenant boundary isolation");
      verifyTenantBoundaries(table);

      // Test 3: Empty scan returns all rows
      LOG.info("Test 3: Verifying empty scan behavior");
      verifyEmptyScan(table);

      LOG.info("Edge cases and cross-tenant isolation verification completed successfully");
    }
  }

  /**
   * Verify that scanning with a non-existent tenant prefix returns no results.
   */
  private void verifyNonExistentTenantScan(Table table) throws IOException {
    String nonExistentPrefix = "ZZZ";
    LOG.info("Testing scan with non-existent tenant prefix: {}", nonExistentPrefix);

    org.apache.hadoop.hbase.client.Scan scan = new org.apache.hadoop.hbase.client.Scan();
    scan.addColumn(FAMILY, QUALIFIER);
    scan.withStartRow(Bytes.toBytes(nonExistentPrefix + "row"));
    scan.withStopRow(Bytes.toBytes(nonExistentPrefix + "row" + "\uFFFF"));

    try (org.apache.hadoop.hbase.client.ResultScanner scanner = table.getScanner(scan)) {
      int rowCount = 0;
      for (org.apache.hadoop.hbase.client.Result result : scanner) {
        LOG.error("Unexpected row found for non-existent tenant: {}",
          Bytes.toString(result.getRow()));
        rowCount++;
      }

      assertEquals("Scan with non-existent tenant prefix should return no results", 0, rowCount);
      LOG.info("Non-existent tenant scan test passed: {} rows returned", rowCount);
    }
  }

  /**
   * Verify tenant boundaries are properly enforced by scanning across adjacent tenant boundaries.
   * This test scans from the last row of one tenant to the first row of the next tenant to ensure
   * proper tenant isolation at boundaries.
   */
  private void verifyTenantBoundaries(Table table) throws IOException {
    LOG.info("Verifying tenant boundary isolation between adjacent tenants");

    int boundariesTested = 0;

    // Test boundaries between adjacent tenants
    for (int i = 0; i < TENANTS.length - 1; i++) {
      String tenant1 = TENANTS[i];
      String tenant2 = TENANTS[i + 1];
      int tenant1RowCount = ROWS_PER_TENANT[i];
      int tenant2RowCount = ROWS_PER_TENANT[i + 1];

      LOG.info("Testing boundary between tenant {} ({} rows) and tenant {} ({} rows)", tenant1,
        tenant1RowCount, tenant2, tenant2RowCount);

      // Create a scan that covers the boundary between tenant1 and tenant2
      org.apache.hadoop.hbase.client.Scan scan = new org.apache.hadoop.hbase.client.Scan();
      scan.addColumn(FAMILY, QUALIFIER);

      // Set start row to last row of tenant1
      String startRow = tenant1 + "row" + String.format("%03d", tenant1RowCount - 1);
      // Set stop row to second row of tenant2 (to ensure we get at least first row of tenant2)
      String stopRow = tenant2 + "row" + String.format("%03d", Math.min(1, tenant2RowCount - 1));

      LOG.info("  Boundary scan range: [{}] to [{}]", startRow, stopRow);

      scan.withStartRow(Bytes.toBytes(startRow));
      scan.withStopRow(Bytes.toBytes(stopRow));

      try (org.apache.hadoop.hbase.client.ResultScanner scanner = table.getScanner(scan)) {
        int tenant1Count = 0;
        int tenant2Count = 0;
        List<String> scannedRows = new ArrayList<>();

        for (org.apache.hadoop.hbase.client.Result result : scanner) {
          String rowKey = Bytes.toString(result.getRow());
          scannedRows.add(rowKey);

          if (rowKey.startsWith(tenant1)) {
            tenant1Count++;
          } else if (rowKey.startsWith(tenant2)) {
            tenant2Count++;
          } else {
            LOG.error("Unexpected tenant in boundary scan: {}", rowKey);
            fail("Unexpected tenant in boundary scan: " + rowKey);
          }
        }

        LOG.info("  Boundary scan results:");
        LOG.info("    - Rows from {}: {}", tenant1, tenant1Count);
        LOG.info("    - Rows from {}: {}", tenant2, tenant2Count);
        LOG.info("    - Total rows scanned: {}", scannedRows.size());

        // Log the actual rows found for debugging
        if (scannedRows.size() <= 5) {
          LOG.info("    - Scanned rows: {}", scannedRows);
        } else {
          LOG.info("    - Scanned rows (first 5): {}", scannedRows.subList(0, 5));
        }

        // We should find the last row from tenant1
        assertTrue("Should find at least one row from tenant " + tenant1, tenant1Count > 0);

        // We should find at least the first row from tenant2 (if tenant2 has any rows)
        if (tenant2RowCount > 0) {
          assertTrue("Should find at least one row from tenant " + tenant2, tenant2Count > 0);
        }

        // Ensure proper tenant separation - no unexpected tenants
        int totalFoundRows = tenant1Count + tenant2Count;
        assertEquals("All scanned rows should belong to expected tenants", scannedRows.size(),
          totalFoundRows);

        LOG.info("  Boundary test passed for tenants {} and {}", tenant1, tenant2);
      }

      boundariesTested++;
    }

    LOG.info("Tenant boundary verification completed: {}/{} boundaries tested successfully",
      boundariesTested, TENANTS.length - 1);
  }

  /**
   * Verify that an empty scan returns all rows across all tenants.
   */
  private void verifyEmptyScan(Table table) throws IOException {
    LOG.info("Testing empty scan to verify it returns all rows across all tenants");

    org.apache.hadoop.hbase.client.Scan emptyScan = new org.apache.hadoop.hbase.client.Scan();
    emptyScan.addColumn(FAMILY, QUALIFIER);

    try (org.apache.hadoop.hbase.client.ResultScanner emptyScanner = table.getScanner(emptyScan)) {
      int rowCount = 0;
      for (org.apache.hadoop.hbase.client.Result result : emptyScanner) {
        rowCount++;
      }

      int expectedTotal = calculateTotalExpectedRows();
      assertEquals("Empty scan should return all rows", expectedTotal, rowCount);
      LOG.info("Empty scan test passed: {}/{} rows returned", rowCount, expectedTotal);
    }
  }

  /**
   * Verify HFile format and multi-tenant structure. Validates that HFiles are properly formatted as
   * v4 with tenant sections.
   */
  private void verifyHFileFormat(List<Path> hfilePaths) throws IOException {
    LOG.info("=== HFile Format Verification ===");
    LOG.info("Verifying {} HFiles for multi-tenant format compliance", hfilePaths.size());

    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Configuration conf = TEST_UTIL.getConfiguration();
    CacheConfig cacheConf = new CacheConfig(conf);

    int totalHFilesVerified = 0;
    int totalCellsFoundAcrossAllFiles = 0;

    for (Path path : hfilePaths) {
      LOG.info("Verifying HFile: {}", path.getName());

      try (HFile.Reader reader = HFile.createReader(fs, path, cacheConf, true, conf)) {
        // Verify HFile version
        int version = reader.getTrailer().getMajorVersion();
        assertEquals("HFile should be version 4", HFile.MIN_FORMAT_VERSION_WITH_MULTI_TENANT,
          version);
        LOG.info("  HFile version: {} (correct)", version);

        // Verify reader type
        assertTrue("Reader should be an AbstractMultiTenantReader",
          reader instanceof AbstractMultiTenantReader);
        LOG.info("  Reader type: AbstractMultiTenantReader (correct)");

        AbstractMultiTenantReader mtReader = (AbstractMultiTenantReader) reader;
        byte[][] allTenantSectionIds = mtReader.getAllTenantSectionIds();

        assertTrue("Should have tenant sections", allTenantSectionIds.length > 0);
        LOG.info("  Found {} tenant sections in HFile", allTenantSectionIds.length);

        int totalCellsInThisFile = 0;
        int sectionsWithData = 0;

        for (byte[] tenantSectionId : allTenantSectionIds) {
          String tenantId = Bytes.toString(tenantSectionId);
          try {
            java.lang.reflect.Method getSectionReaderMethod =
              AbstractMultiTenantReader.class.getDeclaredMethod("getSectionReader", byte[].class);
            getSectionReaderMethod.setAccessible(true);
            Object sectionReader = getSectionReaderMethod.invoke(mtReader, tenantSectionId);

            if (sectionReader != null) {
              java.lang.reflect.Method getReaderMethod =
                sectionReader.getClass().getMethod("getReader");
              HFileReaderImpl sectionHFileReader =
                (HFileReaderImpl) getReaderMethod.invoke(sectionReader);

              HFileInfo sectionInfo = sectionHFileReader.getHFileInfo();
              byte[] deleteCountBytes =
                sectionInfo.get(org.apache.hadoop.hbase.regionserver.HStoreFile.DELETE_FAMILY_COUNT);
              if (deleteCountBytes != null) {
                long deleteCount = Bytes.toLong(deleteCountBytes);
                int expectedCount = TENANT_DELETE_FAMILY_COUNTS.getOrDefault(tenantId, 0);
                assertEquals("Delete family count mismatch for tenant " + tenantId, expectedCount,
                  deleteCount);
              }

              HFileScanner sectionScanner = sectionHFileReader.getScanner(conf, false, false);

              boolean hasData = sectionScanner.seekTo();
              if (hasData) {
                int sectionCellCount = 0;
                do {
                  Cell cell = sectionScanner.getCell();
                  if (cell != null) {
                    // Verify tenant prefix matches section ID for every entry
                    byte[] rowKeyBytes = CellUtil.cloneRow(cell);
                    byte[] rowTenantPrefix = new byte[TENANT_PREFIX_LENGTH];
                    System.arraycopy(rowKeyBytes, 0, rowTenantPrefix, 0, TENANT_PREFIX_LENGTH);

                    assertTrue("Row tenant prefix should match section ID",
                      Bytes.equals(tenantSectionId, rowTenantPrefix));

                    if (cell.getType() == Cell.Type.Put) {
                      sectionCellCount++;
                      totalCellsInThisFile++;
                    }
                  }
                } while (sectionScanner.next());

                assertTrue("Should have found data in tenant section", sectionCellCount > 0);
                sectionsWithData++;
                LOG.info("    Section {}: {} cells", tenantId, sectionCellCount);
              }
            }
          } catch (Exception e) {
            LOG.warn("Failed to access tenant section: " + tenantId, e);
          }
        }

        LOG.info("  Tenant sections with data: {}/{}", sectionsWithData,
          allTenantSectionIds.length);
        LOG.info("  Total cells in this HFile: {}", totalCellsInThisFile);
        totalCellsFoundAcrossAllFiles += totalCellsInThisFile;

        // Verify HFile metadata contains multi-tenant information
        LOG.info("  Verifying HFile metadata and structure");
        verifyHFileMetadata(reader, allTenantSectionIds, mtReader);

        LOG.info("  HFile verification completed for: {}", path.getName());
        totalHFilesVerified++;
      }
    }

    int expectedTotal = calculateTotalExpectedRows();
    assertEquals("Should have found all cells across all HFiles", expectedTotal,
      totalCellsFoundAcrossAllFiles);

    LOG.info("HFile format verification completed successfully:");
    LOG.info("  - HFiles verified: {}/{}", totalHFilesVerified, hfilePaths.size());
    LOG.info("  - Total cells verified: {}/{}", totalCellsFoundAcrossAllFiles, expectedTotal);
    LOG.info("  - All HFiles are properly formatted as multi-tenant v4");
  }

  /**
   * Verify HFile metadata contains expected multi-tenant information. Checks for section count,
   * tenant index levels, and other v4 metadata.
   */
  private void verifyHFileMetadata(HFile.Reader reader, byte[][] allTenantSectionIds,
    AbstractMultiTenantReader mtReader) throws IOException {
    HFileInfo fileInfo = reader.getHFileInfo();
    if (fileInfo == null) {
      LOG.warn("    - HFile info is null - cannot verify metadata");
      return;
    }

    FixedFileTrailer trailer = reader.getTrailer();
    assertEquals("Load-on-open offset should match section index offset for v4 container",
      trailer.getSectionIndexOffset(), trailer.getLoadOnOpenDataOffset());

    // Verify section count metadata
    byte[] sectionCountBytes =
      fileInfo.get(Bytes.toBytes(MultiTenantHFileWriter.FILEINFO_SECTION_COUNT));
    if (sectionCountBytes != null) {
      int sectionCount = Bytes.toInt(sectionCountBytes);
      LOG.info("    - HFile section count: {}", sectionCount);
      assertTrue("HFile should have tenant sections", sectionCount > 0);
      assertEquals("Section count should match found tenant sections", allTenantSectionIds.length,
        sectionCount);
    } else {
      LOG.warn("    - Missing SECTION_COUNT metadata in HFile info");
    }

    // Verify tenant index structure metadata
    byte[] tenantIndexLevelsBytes =
      fileInfo.get(Bytes.toBytes(MultiTenantHFileWriter.FILEINFO_TENANT_INDEX_LEVELS));
    if (tenantIndexLevelsBytes != null) {
      int tenantIndexLevels = Bytes.toInt(tenantIndexLevelsBytes);
      LOG.info("    - Tenant index levels: {}", tenantIndexLevels);
      assertTrue("HFile should have tenant index levels", tenantIndexLevels > 0);

      // Log index structure details
      if (tenantIndexLevels == 1) {
        LOG.info("    - Using single-level tenant index (suitable for {} sections)",
          allTenantSectionIds.length);
      } else {
        LOG.info("    - Using multi-level tenant index ({} levels for {} sections)",
          tenantIndexLevels, allTenantSectionIds.length);
      }
    } else {
      LOG.warn("    - Missing TENANT_INDEX_LEVELS metadata in HFile info");
    }

    // Verify reader provides multi-tenant specific information
    LOG.info("    - Multi-tenant reader statistics:");
    LOG.info("      * Total sections: {}", mtReader.getTotalSectionCount());
    LOG.info("      * Tenant index levels: {}", mtReader.getTenantIndexLevels());
    LOG.info("      * Tenant index max chunk size: {}", mtReader.getTenantIndexMaxChunkSize());

    // Verify consistency between metadata and reader state
    if (tenantIndexLevelsBytes != null) {
      int metadataTenantIndexLevels = Bytes.toInt(tenantIndexLevelsBytes);
      assertEquals("Tenant index levels should match between metadata and reader",
        metadataTenantIndexLevels, mtReader.getTenantIndexLevels());
    }

    assertEquals("Total section count should match found sections", allTenantSectionIds.length,
      mtReader.getTotalSectionCount());

    LOG.info("    - HFile metadata verification passed");
  }

  /**
   * Find all HFiles created for the test table. Scans the filesystem to locate HFiles in the
   * table's directory structure.
   */
  private List<Path> findHFilePaths() throws IOException {
    LOG.info("Searching for HFiles in table directory structure");

    List<Path> hfilePaths = new ArrayList<>();

    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path rootDir = TEST_UTIL.getDataTestDirOnTestFS();
    Path tableDir = new Path(rootDir, "data/default/" + TABLE_NAME.getNameAsString());

    if (fs.exists(tableDir)) {
      FileStatus[] regionDirs = fs.listStatus(tableDir);
      LOG.info("Found {} region directories to scan", regionDirs.length);

      for (FileStatus regionDir : regionDirs) {
        if (regionDir.isDirectory() && !regionDir.getPath().getName().startsWith(".")) {
          Path familyDir = new Path(regionDir.getPath(), Bytes.toString(FAMILY));

          if (fs.exists(familyDir)) {
            FileStatus[] hfiles = fs.listStatus(familyDir);

            for (FileStatus hfile : hfiles) {
              if (
                !hfile.getPath().getName().startsWith(".")
                  && !hfile.getPath().getName().endsWith(".tmp")
              ) {
                hfilePaths.add(hfile.getPath());
                LOG.info("Found HFile: {} (size: {} bytes)", hfile.getPath().getName(),
                  hfile.getLen());
              }
            }
          }
        }
      }
    }

    LOG.info("HFile discovery completed: {} HFiles found", hfilePaths.size());
    return hfilePaths;
  }
}
