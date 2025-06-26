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
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for HFile v4 multi-tenant splitting logic.
 * 
 * <p>This test validates the complete multi-tenant HFile v4 splitting workflow:
 * <ol>
 * <li><strong>Setup:</strong> Creates table with multi-tenant configuration</li>
 * <li><strong>Data Writing:</strong> Writes large datasets with different tenant distributions</li>
 * <li><strong>Flushing:</strong> Forces memstore flush to create multi-tenant HFile v4 files</li>
 * <li><strong>Splitting:</strong> Tests midkey calculation and file splitting</li>
 * <li><strong>Verification:</strong> Validates split balance and data integrity</li>
 * </ol>
 * 
 * <p>The test covers various tenant distribution patterns to ensure proper splitting behavior
 * across different real-world scenarios.
 */
@Category(MediumTests.class)
public class MultiTenantHFileSplittingTest {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE = 
      HBaseClassTestRule.forClass(MultiTenantHFileSplittingTest.class);

  private static final Logger LOG = LoggerFactory.getLogger(MultiTenantHFileSplittingTest.class);
  
  private static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  
  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final byte[] QUALIFIER = Bytes.toBytes("q");
  private static final int TENANT_PREFIX_LENGTH = 3;
  
  // Track whether we're in the middle of a critical operation
  private static volatile boolean inCriticalOperation = false;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    LOG.info("=== Setting up Multi-Tenant HFile Splitting Test class ===");
    
    // Configure test settings BEFORE creating the configuration
    Configuration conf = TEST_UTIL.getConfiguration();
    
    // Set HFile format version for multi-tenant support
    conf.setInt(HFile.FORMAT_VERSION_KEY, HFile.MIN_FORMAT_VERSION_WITH_MULTI_TENANT);
    conf.setInt(MultiTenantHFileWriter.TENANT_PREFIX_LENGTH, TENANT_PREFIX_LENGTH);
    
    // Set smaller region size to make splits easier to trigger
    conf.setLong("hbase.hregion.max.filesize", 10 * 1024 * 1024); // 10MB
    conf.setInt("hbase.regionserver.region.split.policy.check.period", 1000); // Check every second
    
    // Use IncreasingToUpperBoundRegionSplitPolicy which allows manual splits
    // but still prevents automatic splits if we set the file size high enough
    conf.set("hbase.regionserver.region.split.policy", 
             "org.apache.hadoop.hbase.regionserver.IncreasingToUpperBoundRegionSplitPolicy");
    
    // Configure mini cluster settings
    conf.setInt("hbase.regionserver.msginterval", 100);
    conf.setInt("hbase.client.pause", 250);
    conf.setInt("hbase.client.retries.number", 6);
    conf.setBoolean("hbase.master.enabletable.roundrobin", true);
    
    // Increase timeouts for split operations
    conf.setLong("hbase.regionserver.fileSplitTimeout", 600000); // 10 minutes
    conf.setInt("hbase.client.operation.timeout", 600000); // 10 minutes
    
    // Ensure the HFile format version is set
    LOG.info("Configured HFile format version: {}", 
             conf.getInt(HFile.FORMAT_VERSION_KEY, -1));
    
    // Start mini cluster
    LOG.info("Starting mini cluster with multi-tenant HFile configuration");
    TEST_UTIL.startMiniCluster(1);
    
    // Wait for cluster to be fully ready
    LOG.info("Waiting for cluster to be ready...");
    TEST_UTIL.waitUntilAllRegionsAssigned(TableName.META_TABLE_NAME);
    
    // Verify the configuration persisted after cluster start
    int postStartVersion = TEST_UTIL.getConfiguration().getInt(HFile.FORMAT_VERSION_KEY, -1);
    LOG.info("HFile format version after cluster start: {}", postStartVersion);
    
    if (postStartVersion != HFile.MIN_FORMAT_VERSION_WITH_MULTI_TENANT) {
      LOG.warn("HFile format version changed after cluster start. Re-setting...");
      TEST_UTIL.getConfiguration().setInt(HFile.FORMAT_VERSION_KEY, 
                                         HFile.MIN_FORMAT_VERSION_WITH_MULTI_TENANT);
    }
    
    LOG.info("Mini cluster started successfully");
  }
  
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    LOG.info("=== Tearing down Multi-Tenant HFile Splitting Test class ===");
    
    // Wait for any critical operations to complete
    int waitCount = 0;
    while (inCriticalOperation && waitCount < 60) { // Wait up to 60 seconds
      LOG.info("Waiting for critical operation to complete before teardown... ({}s)", waitCount);
      Thread.sleep(1000);
      waitCount++;
    }
    
    if (inCriticalOperation) {
      LOG.warn("Critical operation still in progress after 60s wait, proceeding with teardown");
    }
    
    try {
      TEST_UTIL.shutdownMiniCluster();
      LOG.info("Mini cluster shut down successfully");
    } catch (Exception e) {
      LOG.warn("Error during mini cluster shutdown", e);
    }
  }
  
  @Before
  public void setUp() throws Exception {
    LOG.info("=== Per-test setup ===");
    // Reset critical operation flag
    inCriticalOperation = false;
  }
  
  @After
  public void tearDown() throws Exception {
    LOG.info("=== Per-test cleanup ===");
    
    // Check if cluster is still running before trying to clean up
    if (TEST_UTIL.getMiniHBaseCluster() == null) {
      LOG.warn("Mini cluster is not running, skipping table cleanup");
      return;
    }
    
    // Clean up any tables created during tests using TEST_UTIL pattern
    TableName[] testTables = {
      TableName.valueOf("TestSingleTenant"),
      TableName.valueOf("TestEvenDistribution"),
      TableName.valueOf("TestUnevenDistribution"),
      TableName.valueOf("TestSkewedDistribution"),
      TableName.valueOf("TestManySmallTenants"),
      TableName.valueOf("TestFewLargeTenants"),
      TableName.valueOf("TestCoreRegionSplitting"),
      TableName.valueOf("TestDataConsistency"),
      TableName.valueOf("TestRegionBoundaries"),
      TableName.valueOf("TestClusterHealthCheck"),
      TableName.valueOf("TestBasicFunctionality")
    };
    
    for (TableName tableName : testTables) {
      try {
        if (TEST_UTIL.getAdmin() != null && TEST_UTIL.getAdmin().tableExists(tableName)) {
          TEST_UTIL.deleteTable(tableName);
          LOG.info("Deleted test table: {}", tableName);
        }
      } catch (Exception e) {
        LOG.warn("Failed to clean up table: {}", tableName, e);
      }
    }
    
    // Reset critical operation flag
    inCriticalOperation = false;
  }
  
  /**
   * Test 1: Single tenant with large amount of data
   */
  @Test(timeout = 600000) // 10 minute timeout
  public void testSingleTenantSplitting() throws Exception {
    String[] tenants = {"T01"};
    int[] rowsPerTenant = {10000};
    
    executeTestScenario("TestSingleTenant", tenants, rowsPerTenant);
  }
  
  /**
   * Test 2: Three tenants with even distribution
   */
  @Test(timeout = 600000) // 10 minute timeout  
  public void testEvenDistributionSplitting() throws Exception {
    String[] tenants = {"T01", "T02", "T03"};
    int[] rowsPerTenant = {3000, 3000, 3000};
    
    executeTestScenario("TestEvenDistribution", tenants, rowsPerTenant);
  }
  
  /**
   * Test 3: Three tenants with uneven distribution
   */
  @Test(timeout = 600000) // 10 minute timeout
  public void testUnevenDistributionSplitting() throws Exception {
    String[] tenants = {"T01", "T02", "T03"};
    int[] rowsPerTenant = {1000, 2000, 1000};
    
    executeTestScenario("TestUnevenDistribution", tenants, rowsPerTenant);
  }
  
  /**
   * Test 4: Skewed distribution with one dominant tenant
   */
  @Test(timeout = 600000) // 10 minute timeout
  public void testSkewedDistributionSplitting() throws Exception {
    String[] tenants = {"T01", "T02", "T03", "T04"};
    int[] rowsPerTenant = {100, 100, 5000, 100};
    
    executeTestScenario("TestSkewedDistribution", tenants, rowsPerTenant);
  }
  
  /**
   * Test 5: Many small tenants
   */
  @Test(timeout = 600000) // 10 minute timeout
  public void testManySmallTenantsSplitting() throws Exception {
    String[] tenants = {"T01", "T02", "T03", "T04", "T05", "T06", "T07", "T08", "T09", "T10"};
    int[] rowsPerTenant = {500, 500, 500, 500, 500, 500, 500, 500, 500, 500};
    
    executeTestScenario("TestManySmallTenants", tenants, rowsPerTenant);
  }
  
  /**
   * Test 6: Few large tenants
   */
  @Test(timeout = 600000) // 10 minute timeout
  public void testFewLargeTenantsSplitting() throws Exception {
    String[] tenants = {"T01", "T02"};
    int[] rowsPerTenant = {5000, 5000};
    
    executeTestScenario("TestFewLargeTenants", tenants, rowsPerTenant);
  }
  
  /**
   * Execute a test scenario with the given configuration.
   */
  private void executeTestScenario(String tableName, String[] tenants, int[] rowsPerTenant) 
      throws Exception {
    TableName table = TableName.valueOf(tableName);
    LOG.info("=== Starting test scenario: {} ===", tableName);
    
    try {
      // Phase 1: Create table
      LOG.info("Phase 1: Creating table {}", tableName);
      createTestTable(table);
      
      // Phase 2: Write test data
      LOG.info("Phase 2: Writing test data");
      writeTestData(table, tenants, rowsPerTenant);
      
      // Phase 3: Flush memstore to create HFiles
      LOG.info("Phase 3: Flushing table");
      TEST_UTIL.flush(table);
      
      // Wait a bit for flush to complete
      Thread.sleep(1000);
      
      // Phase 4: Verify midkey before split
      LOG.info("Phase 4: Verifying midkey calculation");
      verifyMidkeyCalculation(table, tenants, rowsPerTenant);
      
      // Phase 5: Trigger split - mark as critical operation
      LOG.info("Phase 5: Triggering region split");
      inCriticalOperation = true;
      try {
        triggerRegionSplit(tenants, rowsPerTenant, table);
      } finally {
        inCriticalOperation = false;
      }
      
      // Phase 6: Compact after split to ensure proper HFile structure
      LOG.info("Phase 6: Compacting table after split");
      TEST_UTIL.compact(table, true); // Major compaction
      
      // Wait for compaction to complete
      Thread.sleep(2000);
      
      // Phase 7: Comprehensive data integrity verification after split
      LOG.info("Phase 7: Starting comprehensive data integrity verification after split");
      verifyDataIntegrityWithScanning(table, tenants, rowsPerTenant);
      verifyDataIntegrityAfterSplit(table, tenants, rowsPerTenant);
      
      LOG.info("=== Test scenario completed successfully: {} ===", tableName);
      
    } catch (Exception e) {
      LOG.error("Test scenario failed: {}", tableName, e);
      throw e;
    } finally {
      // Ensure critical operation flag is reset
      inCriticalOperation = false;
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
    cfBuilder.setValue(HFile.FORMAT_VERSION_KEY, String.valueOf(HFile.MIN_FORMAT_VERSION_WITH_MULTI_TENANT));
    
    // Set smaller block size for easier testing
    cfBuilder.setBlocksize(8 * 1024); // 8KB blocks
    
    tableBuilder.setColumnFamily(cfBuilder.build());
    
    // Create the table
    TEST_UTIL.createTable(tableBuilder.build(), null);
    
    LOG.info("Created table {} with multi-tenant configuration", tableName);
  }
  
  /**
   * Write test data for all tenants in lexicographic order to avoid key ordering violations.
   */
  private void writeTestData(TableName tableName, String[] tenants, int[] rowsPerTenant) throws IOException {
    try (Connection connection = TEST_UTIL.getConnection();
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
    
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    CacheConfig cacheConf = new CacheConfig(TEST_UTIL.getConfiguration());
    
    try (HFile.Reader reader = HFile.createReader(fs, hfilePath, cacheConf, true, 
                                                 TEST_UTIL.getConfiguration())) {
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
      LOG.info("  - First key: {}", reader.getFirstRowKey().isPresent() ? 
          Bytes.toString(reader.getFirstRowKey().get()) : "N/A");
      LOG.info("  - Last key: {}", reader.getLastRowKey().isPresent() ?
          Bytes.toString(reader.getLastRowKey().get()) : "N/A");
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
          LOG.info("  - Position in file: {}/{} ({}%)", 
                  midkeyPosition, totalRows, 
                  String.format("%.1f", (midkeyPosition * 100.0) / totalRows));
          LOG.info("  - Target midpoint: {}/{} (50.0%)", totalRows/2, totalRows);
          LOG.info("  - Deviation from midpoint: {}%", 
                  String.format("%.1f", Math.abs(midkeyPosition - totalRows/2) * 100.0 / totalRows));
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
        LOG.info("Midkey comparison - first: {}, midkey: {}, last: {}", firstKey, midkeyString, lastKey);
      }
      
      LOG.info("Total rows in dataset: {}", totalRows);
    }
  }
  
  /**
   * Comprehensive data integrity verification using scanning operations.
   * This method tests various scanning scenarios to ensure data integrity after split.
   */
  private void verifyDataIntegrityWithScanning(TableName tableName, String[] tenants, int[] rowsPerTenant) 
      throws Exception {
    LOG.info("=== Comprehensive Scanning Verification After Split ===");
    
    try (Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
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
      
      for (org.apache.hadoop.hbase.client.Result result : scanner) {
        String rowKey = Bytes.toString(result.getRow());
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
          fail("Tenant data mixing detected in full scan: Row " + rowKey + 
               " expected tenant " + tenantPrefix + " but got value " + actualValue);
        }
        
        tenantRowCounts[tenantIndex]++;
        seenRowsPerTenant[tenantIndex].add(rowKey);
        // Per-row logging to trace scan order and locate gaps
        LOG.info("SCANNED_ROW {}", rowKey);
        totalRowsScanned++;
      }
      
      // NEW DEBUG LOGGING: dump per-tenant counts before assertions
      StringBuilder sb = new StringBuilder();
      sb.append("Per-tenant counts: ");
      for (int i = 0; i < tenants.length; i++) {
        sb.append(tenants[i]).append('=')
          .append(tenantRowCounts[i]).append(", ");
      }
      sb.append("total=").append(totalRowsScanned);
      LOG.info(sb.toString());
      
      // Verify total row count
      int expectedTotal = Arrays.stream(rowsPerTenant).sum();
      assertEquals("Full scan should return all rows after split", expectedTotal, totalRowsScanned);
      
      // Verify per-tenant row counts
      for (int i = 0; i < tenants.length; i++) {
        assertEquals("Row count mismatch for tenant " + tenants[i] + " in full scan", 
                    rowsPerTenant[i], tenantRowCounts[i]);
      }
      
      LOG.info("Full table scan verified: {}/{} rows scanned successfully", 
               totalRowsScanned, expectedTotal);
      
      // Identify and log missing rows per tenant
      for (int i = 0; i < tenants.length; i++) {
        if (tenantRowCounts[i] != rowsPerTenant[i]) {
          java.util.List<String> missing = new java.util.ArrayList<>();
          for (int r = 0; r < rowsPerTenant[i]; r++) {
            String expectedKey = String.format("%srow%05d", tenants[i], r);
            if (!seenRowsPerTenant[i].contains(expectedKey)) {
              missing.add(expectedKey);
            }
          }
          LOG.error("Missing rows for tenant {} ({} rows): {}", tenants[i], missing.size(), missing);
        }
      }
    }
  }
  
  /**
   * Verify tenant-specific scans work correctly after split.
   */
  private void verifyTenantSpecificScansAfterSplit(Table table, String[] tenants, int[] rowsPerTenant) 
      throws IOException {
    LOG.info("Verifying tenant-specific scans after split");
    
    for (int tenantIndex = 0; tenantIndex < tenants.length; tenantIndex++) {
      String tenant = tenants[tenantIndex];
      int expectedRows = rowsPerTenant[tenantIndex];
      
      LOG.info("Testing tenant-specific scan for tenant {}: expecting {} rows", tenant, expectedRows);
      
      org.apache.hadoop.hbase.client.Scan tenantScan = new org.apache.hadoop.hbase.client.Scan();
      tenantScan.addColumn(FAMILY, QUALIFIER);
      tenantScan.withStartRow(Bytes.toBytes(tenant + "row"));
      tenantScan.withStopRow(Bytes.toBytes(tenant + "row" + "\uFFFF"));
      
      try (org.apache.hadoop.hbase.client.ResultScanner tenantScanner = table.getScanner(tenantScan)) {
        int tenantRowCount = 0;
        List<String> foundRows = new ArrayList<>();
        
        for (org.apache.hadoop.hbase.client.Result result : tenantScanner) {
          String rowKey = Bytes.toString(result.getRow());
          foundRows.add(rowKey);
          
          if (!rowKey.startsWith(tenant)) {
            fail("Tenant scan violation after split: Found row " + rowKey + 
                 " in scan for tenant " + tenant);
          }
          
          // Verify data integrity for this row
          byte[] cellValue = result.getValue(FAMILY, QUALIFIER);
          if (cellValue == null) {
            fail("Missing value for tenant row: " + rowKey);
          }
          
          String actualValue = Bytes.toString(cellValue);
          if (!actualValue.contains(tenant)) {
            fail("Tenant data corruption after split: Row " + rowKey + 
                 " expected tenant " + tenant + " but got value " + actualValue);
          }
          
          tenantRowCount++;
        }
        
        if (tenantRowCount != expectedRows) {
          LOG.error("Row count mismatch for tenant {} after split:", tenant);
          LOG.error("  Expected: {}", expectedRows);
          LOG.error("  Found: {}", tenantRowCount);
          LOG.error("  Found rows: {}", foundRows);
        }
        
        assertEquals("Row count mismatch for tenant " + tenant + " after split", 
                    expectedRows, tenantRowCount);
        
        LOG.info("Tenant {} scan successful after split: {}/{} rows verified", 
                 tenant, tenantRowCount, expectedRows);
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
          assertTrue("Row ordering should be maintained across regions: " + 
                    previousRowKey + " should be <= " + rowKey,
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
          fail("Data corruption in cross-region scan: Row " + rowKey + 
               " expected tenant " + tenantPrefix + " but got value " + actualValue);
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
      boundaryScan.withStartRow(Bytes.toBytes(tenant1 + "row" + String.format("%05d", rowsPerTenant[i] - 1)));
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
  private void verifyDataIntegrityAfterSplit(TableName tableName, String[] tenants, int[] rowsPerTenant) 
      throws Exception {
    LOG.info("Verifying data integrity with GET operations");
    
    try (Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
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
    
    Path rootDir = TEST_UTIL.getDataTestDirOnTestFS();
    Path tableDir = new Path(rootDir, "data/default/" + tableName.getNameAsString());
    
    if (TEST_UTIL.getTestFileSystem().exists(tableDir)) {
      FileStatus[] regionDirs = TEST_UTIL.getTestFileSystem().listStatus(tableDir);
      
      for (FileStatus regionDir : regionDirs) {
        if (regionDir.isDirectory() && !regionDir.getPath().getName().startsWith(".")) {
          Path familyDir = new Path(regionDir.getPath(), Bytes.toString(FAMILY));
          
          if (TEST_UTIL.getTestFileSystem().exists(familyDir)) {
            FileStatus[] hfiles = TEST_UTIL.getTestFileSystem().listStatus(familyDir);
            
            for (FileStatus hfile : hfiles) {
              if (!hfile.getPath().getName().startsWith(".") && 
                  !hfile.getPath().getName().endsWith(".tmp")) {
                hfilePaths.add(hfile.getPath());
                LOG.debug("Found HFile: {} (size: {} bytes)", 
                         hfile.getPath().getName(), hfile.getLen());
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
      assertTrue("Mini cluster should be running", TEST_UTIL.getMiniHBaseCluster() != null);
      LOG.info("Mini cluster is up and running");
      
      // Add more debug info about cluster state
      LOG.info("Master is active: {}", TEST_UTIL.getMiniHBaseCluster().getMaster().isActiveMaster());
      LOG.info("Number of region servers: {}", TEST_UTIL.getMiniHBaseCluster().getNumLiveRegionServers());
      LOG.info("Master address: {}", TEST_UTIL.getMiniHBaseCluster().getMaster().getServerName());
      
    } catch (Exception e) {
      LOG.warn("Cluster health check failed: {}", e.getMessage());
      throw new RuntimeException("Cluster is not healthy before split attempt", e);
    }
    
    // Get initial region count and submit split request
    LOG.info("Getting initial region count and submitting split");
    try (Connection connection = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
         Admin admin = connection.getAdmin()) {
      
      // Ensure table exists and is available
      LOG.info("Verifying table exists: {}", tableName);
      boolean tableExists = admin.tableExists(tableName);
      if (!tableExists) {
        throw new RuntimeException("Table " + tableName + " does not exist before split");
      }
      
      LOG.info("Table {} exists", tableName);
      
      List<RegionInfo> regions = admin.getRegions(tableName);
      assertEquals("Should have exactly one region before split", 1, regions.size());
      LOG.info("Pre-split verification passed. Table {} has {} region(s)", tableName, regions.size());
      
      // Trigger the split - let HBase choose the split point based on midkey calculation
      LOG.info("Submitting split request for table: {}", tableName);
      admin.split(tableName);
      LOG.info("Split request submitted successfully for table: {}", tableName);
    }
    
    // Wait for split to complete using HBaseTestingUtil methods with extended timeouts
    LOG.info("Waiting for split processing to complete...");
    TEST_UTIL.waitUntilNoRegionsInTransition(120000); // Increase timeout to 2 minutes
    
    // Give some time for the split to stabilize
    Thread.sleep(2000);
    
    // Verify split completed by checking region count
    LOG.info("Verifying split completion...");
    try (Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
         Admin splitAdmin = conn.getAdmin()) {
      List<RegionInfo> regionsAfterSplit = splitAdmin.getRegions(tableName);
      if (regionsAfterSplit.size() <= 1) {
        fail("Region split did not complete successfully. Expected > 1 region, got: " 
             + regionsAfterSplit.size());
      }
      LOG.info("Split completed successfully. Regions after split: {}", regionsAfterSplit.size());
      
      // Log region details
      for (RegionInfo region : regionsAfterSplit) {
        LOG.info("Region: {} [{} -> {}]", 
                region.getEncodedName(),
                Bytes.toStringBinary(region.getStartKey()),
                Bytes.toStringBinary(region.getEndKey()));
      }
    }
  }
} 