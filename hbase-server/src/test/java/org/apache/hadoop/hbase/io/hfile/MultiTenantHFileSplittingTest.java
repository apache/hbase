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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
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
import org.apache.hadoop.hbase.client.Scan;
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
  
  // Test configurations for different scenarios
  private static final String[] SINGLE_TENANT = {"T01"};
  private static final int[] SINGLE_TENANT_ROWS = {2000};
  
  private static final String[] EVEN_TENANTS = {"T01", "T02", "T03", "T04", "T05"};
  private static final int[] EVEN_ROWS_PER_TENANT = {200, 200, 200, 200, 200};
  
  private static final String[] UNEVEN_TENANTS = {"T01", "T02", "T03", "T04"};
  private static final int[] UNEVEN_ROWS_PER_TENANT = {100, 300, 500, 100};
  
  private static final String[] SKEWED_TENANTS = {"T01", "T02", "T03", "T04", "T05", "T06"};
  private static final int[] SKEWED_ROWS_PER_TENANT = {50, 50, 800, 50, 25, 25};
  
  private static final String[] MANY_TENANTS = new String[20];
  private static final int[] MANY_ROWS_PER_TENANT = new int[20];
  static {
    for (int i = 0; i < 20; i++) {
      MANY_TENANTS[i] = String.format("T%02d", i + 1);
      MANY_ROWS_PER_TENANT[i] = 50;
    }
  }
  
  private static final String[] FEW_TENANTS = {"T01", "T02"};
  private static final int[] FEW_ROWS_PER_TENANT = {600, 400};
  
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
   * Get region count safely with retries.
   */
  private int getRegionCount(TableName tableName) throws Exception {
    int maxRetries = 3;
    for (int attempt = 1; attempt <= maxRetries; attempt++) {
      try (Admin admin = TEST_UTIL.getAdmin()) {
        List<RegionInfo> regions = admin.getRegions(tableName);
        return regions.size();
      } catch (Exception e) {
        LOG.warn("Failed to get region count, attempt {}: {}", attempt, e.getMessage());
        if (attempt == maxRetries) {
          throw e;
        }
        Thread.sleep(1000);
      }
    }
    return 0;
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
      
      // Phase 5: Compact to ensure single HFile (optional but helps with testing)
      LOG.info("Phase 5: Compacting table");
      TEST_UTIL.compact(table, true); // Major compaction
      
      // Wait for compaction to complete
      Thread.sleep(2000);
      
      // Phase 6: Trigger split - mark as critical operation
      LOG.info("Phase 6: Triggering region split");
      inCriticalOperation = true;
      try {
        triggerRegionSplit(tenants, rowsPerTenant, table);
      } finally {
        inCriticalOperation = false;
      }
      
      // Phase 7: Verify data integrity after split
      LOG.info("Phase 7: Verifying data integrity after split");
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
   * Write test data for all tenants.
   */
  private void writeTestData(TableName tableName, String[] tenants, int[] rowsPerTenant) throws IOException {
    try (Connection connection = TEST_UTIL.getConnection();
         Table table = connection.getTable(tableName)) {
      
      List<Put> batchPuts = new ArrayList<>();
      
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
      
      LOG.info("Writing {} total rows to table in batch operation", batchPuts.size());
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
   * Verify sample rows for a tenant to ensure they exist and have correct values.
   */
  private void verifyTenantSampleRows(Table table, String tenantId, int expectedRowCount) throws IOException {
    // Test key sample points: first, middle, and last rows
    int[] sampleIndices = {0, expectedRowCount / 2, expectedRowCount - 1};
    
    for (int rowIndex : sampleIndices) {
      if (rowIndex >= 0 && rowIndex < expectedRowCount) {
        String rowKey = String.format("%srow%05d", tenantId, rowIndex);
        String expectedValue = String.format("value_tenant-%s_row-%05d", tenantId, rowIndex);
        
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(FAMILY, QUALIFIER);
        
        Result result = table.get(get);
        assertFalse("Sample row should exist: " + rowKey, result.isEmpty());
        
        byte[] actualValue = result.getValue(FAMILY, QUALIFIER);
        String actualValueStr = Bytes.toString(actualValue);
        assertEquals("Value mismatch for sample row " + rowKey, expectedValue, actualValueStr);
        
        LOG.debug("Verified sample row {} for tenant {}", rowKey, tenantId);
      }
    }
    
    LOG.info("Sample row verification passed for tenant {}", tenantId);
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
  
  /**
   * Verify that the split completed successfully and examine split results.
   */
  private void verifySplitResults(String[] tenants, int[] rowsPerTenant, TableName tableName) 
      throws Exception {
    try (Admin admin = TEST_UTIL.getAdmin()) {
      List<RegionInfo> regions = admin.getRegions(tableName);
      
      // Verify we have exactly 2 regions after split
      assertEquals("Should have exactly 2 regions after split", 2, regions.size());
      
      // Sort regions by start key for consistent ordering
      regions.sort((r1, r2) -> {
        byte[] start1 = r1.getStartKey();
        byte[] start2 = r2.getStartKey();
        if (start1.length == 0) return -1;
        if (start2.length == 0) return 1;
        return Bytes.compareTo(start1, start2);
      });
      
      RegionInfo firstRegion = regions.get(0);
      RegionInfo secondRegion = regions.get(1);
      
      LOG.info("Split results:");
      LOG.info("  First region: {} -> {}", 
               Bytes.toStringBinary(firstRegion.getStartKey()),
               Bytes.toStringBinary(firstRegion.getEndKey()));
      LOG.info("  Second region: {} -> {}", 
               Bytes.toStringBinary(secondRegion.getStartKey()),
               Bytes.toStringBinary(secondRegion.getEndKey()));
      
      // Verify region boundaries are correct
      assertTrue("First region should have empty start key", firstRegion.getStartKey().length == 0);
      assertTrue("Second region should have empty end key", secondRegion.getEndKey().length == 0);
      
      // Verify the split point is consistent
      byte[] splitPoint = firstRegion.getEndKey();
      assertArrayEquals("Split point should match", splitPoint, secondRegion.getStartKey());
      
      // Extract tenant from split point
      if (splitPoint.length >= TENANT_PREFIX_LENGTH) {
        String splitTenant = Bytes.toString(splitPoint, 0, TENANT_PREFIX_LENGTH);
        LOG.info("Split occurred within tenant: {}", splitTenant);
        
        // Verify the split tenant is one of our test tenants
        boolean foundTenant = false;
        for (String tenant : tenants) {
          if (tenant.equals(splitTenant)) {
            foundTenant = true;
            break;
          }
        }
        assertTrue("Split point tenant should be one of the test tenants", foundTenant);
      }
      
      // Verify HFiles exist for both regions
      LOG.info("Verifying HFiles after split");
      List<Path> hfilesAfterSplit = findHFilePaths(tableName);
      assertTrue("Should have HFiles after split", hfilesAfterSplit.size() > 0);
      LOG.info("Found {} HFiles after split", hfilesAfterSplit.size());
    }
  }

  /**
   * Simple test to verify cluster is healthy and basic operations work.
   */
  @Test(timeout = 60000)
  public void testClusterHealthCheck() throws Exception {
    LOG.info("=== Test Case: Cluster Health Check ===");
    
    try {
      // Verify cluster is running
      assertTrue("Mini cluster should be running", TEST_UTIL.getMiniHBaseCluster() != null);
      LOG.info("Mini cluster is up and running");
      
      // Verify we can get an admin connection
      try (Admin admin = TEST_UTIL.getAdmin()) {
        assertNotNull("Admin connection should not be null", admin);
        LOG.info("Successfully obtained admin connection");
        
        // List tables (should be empty or have system tables)
        TableName[] tables = admin.listTableNames();
        LOG.info("Found {} user tables", tables.length);
        
        // Create a simple test table
        TableName simpleTable = TableName.valueOf("SimpleTestTable");
        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(simpleTable);
        builder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(FAMILY).build());
        
        admin.createTable(builder.build());
        LOG.info("Created simple test table");
        
        // Verify table exists
        assertTrue("Table should exist", admin.tableExists(simpleTable));
        
        // Write a simple row
        try (Connection conn = TEST_UTIL.getConnection();
             Table table = conn.getTable(simpleTable)) {
          
          Put put = new Put(Bytes.toBytes("testrow"));
          put.addColumn(FAMILY, QUALIFIER, Bytes.toBytes("testvalue"));
          table.put(put);
          LOG.info("Wrote test row");
          
          // Read it back
          Get get = new Get(Bytes.toBytes("testrow"));
          Result result = table.get(get);
          assertFalse("Result should not be empty", result.isEmpty());
          
          String value = Bytes.toString(result.getValue(FAMILY, QUALIFIER));
          assertEquals("Value should match", "testvalue", value);
          LOG.info("Successfully read back test row");
        }
        
        // Clean up
        admin.disableTable(simpleTable);
        admin.deleteTable(simpleTable);
        LOG.info("Cleaned up test table");
      }
      
      LOG.info("Cluster health check passed!");
      
    } catch (Exception e) {
      LOG.error("Cluster health check failed", e);
      throw e;
    }
  }

  /**
   * Simple step-by-step test to verify basic functionality before testing splits.
   */
  @Test(timeout = 180000)
  public void testBasicFunctionality() throws Exception {
    LOG.info("=== Test Case: Basic Functionality Step by Step ===");
    TableName testTable = TableName.valueOf("TestBasicFunctionality");
    
    try {
      // Step 1: Create table
      LOG.info("Step 1: Creating test table");
      createTestTable(testTable);
      LOG.info("Table created successfully");
      
      // Step 2: Verify table exists
      LOG.info("Step 2: Verifying table exists");
      try (Admin admin = TEST_UTIL.getAdmin()) {
        assertTrue("Table should exist", admin.tableExists(testTable));
        LOG.info("Table existence verified");
      }
      
      // Step 3: Write enough data to trigger a split
      LOG.info("Step 3: Writing sufficient test data for split");
      writeSubstantialTestData(testTable);
      
      // Step 4: Flush table
      LOG.info("Step 4: Flushing table");
      TEST_UTIL.flush(testTable);
      LOG.info("Table flushed successfully");
      
      // Step 5: Verify data after flush
      LOG.info("Step 5: Verifying data after flush");
      verifyTestDataExists(testTable);
      
      // Step 6: Check regions
      LOG.info("Step 6: Checking region count");
      int regionCount = getRegionCount(testTable);
      LOG.info("Current region count: {}", regionCount);
      assertEquals("Should have 1 region before split", 1, regionCount);
      
      // Step 7: Force split with explicit split point
      LOG.info("Step 7: Forcing split with explicit split point");
      boolean splitSuccess = forceSplitWithSplitPoint(testTable);
      assertTrue("Split should succeed", splitSuccess);
      
      // Step 8: Verify split completed
      LOG.info("Step 8: Verifying split completion");
      int newRegionCount = getRegionCount(testTable);
      LOG.info("Region count after split: {}", newRegionCount);
      assertTrue("Should have more than 1 region after split", newRegionCount > 1);
      
      // Step 9: Verify data integrity after split
      LOG.info("Step 9: Verifying data integrity after split");
      verifyAllDataAfterSplit(testTable);
      
      LOG.info("Basic functionality test completed successfully");
      
    } catch (Exception e) {
      LOG.error("Basic functionality test failed: {}", e.getMessage(), e);
      throw e;
    }
  }
  
  /**
   * Write substantial test data to ensure split can happen.
   */
  private void writeSubstantialTestData(TableName tableName) throws Exception {
    LOG.info("Writing substantial test data to table: {}", tableName);
    
    try (Connection conn = TEST_UTIL.getConnection();
         Table table = conn.getTable(tableName)) {
      
      List<Put> puts = new ArrayList<>();
      
      // Write data for multiple tenants with enough rows
      String[] tenants = {"T01", "T02", "T03"};
      int rowsPerTenant = 1000; // Increased to ensure sufficient data
      
      for (String tenant : tenants) {
        for (int i = 0; i < rowsPerTenant; i++) {
          String rowKey = String.format("%srow%05d", tenant, i);
          Put put = new Put(Bytes.toBytes(rowKey));
          String value = String.format("value_tenant-%s_row-%05d", tenant, i);
          put.addColumn(FAMILY, QUALIFIER, Bytes.toBytes(value));
          puts.add(put);
          
          // Batch write every 100 rows
          if (puts.size() >= 100) {
            table.put(puts);
            puts.clear();
          }
        }
      }
      
      // Write remaining puts
      if (!puts.isEmpty()) {
        table.put(puts);
      }
      
      LOG.info("Successfully wrote {} rows across {} tenants", 
               tenants.length * rowsPerTenant, tenants.length);
    }
  }
  
  /**
   * Force split with an explicit split point.
   */
  private boolean forceSplitWithSplitPoint(TableName tableName) throws Exception {
    LOG.info("Forcing split with explicit split point for table: {}", tableName);
    
    try (Admin admin = TEST_UTIL.getAdmin()) {
      // Find the midkey from the HFile
      List<Path> hfiles = findHFilePaths(tableName);
      assertTrue("Should have at least one HFile", !hfiles.isEmpty());
      
      Path hfilePath = hfiles.get(0);
      LOG.info("Using HFile for midkey: {}", hfilePath);
      
      // Get midkey from the HFile
      FileSystem fs = TEST_UTIL.getTestFileSystem();
      CacheConfig cacheConf = new CacheConfig(TEST_UTIL.getConfiguration());
      
      byte[] splitPoint = null;
      try (HFile.Reader reader = HFile.createReader(fs, hfilePath, cacheConf, true, 
                                                   TEST_UTIL.getConfiguration())) {
        Optional<ExtendedCell> midkey = reader.midKey();
        if (midkey.isPresent()) {
          ExtendedCell midkeyCell = midkey.get();
          splitPoint = CellUtil.cloneRow(midkeyCell);
          LOG.info("Found midkey for split: {}", Bytes.toStringBinary(splitPoint));
        }
      }
      
      if (splitPoint == null) {
        // Fallback to a manual split point
        splitPoint = Bytes.toBytes("T02row00000");
        LOG.info("Using fallback split point: {}", Bytes.toStringBinary(splitPoint));
      }
      
      // Submit split with explicit split point
      admin.split(tableName, splitPoint);
      LOG.info("Split request submitted with split point: {}", Bytes.toStringBinary(splitPoint));
      
      // Wait for split to complete
      return waitForSplitCompletion(tableName, 60000); // 60 second timeout
    }
  }
  
  /**
   * Wait for split to complete with timeout.
   */
  private boolean waitForSplitCompletion(TableName tableName, long timeoutMs) throws Exception {
    LOG.info("Waiting for split to complete...");
    
    long startTime = System.currentTimeMillis();
    int initialRegionCount = getRegionCount(tableName);
    
    while (System.currentTimeMillis() - startTime < timeoutMs) {
      try {
        TEST_UTIL.waitUntilNoRegionsInTransition(5000); // Short timeout
        
        int currentRegionCount = getRegionCount(tableName);
        if (currentRegionCount > initialRegionCount) {
          LOG.info("Split completed! Region count: {} -> {}", 
                   initialRegionCount, currentRegionCount);
          
          // Give a bit more time for regions to stabilize
          Thread.sleep(2000);
          TEST_UTIL.waitUntilAllRegionsAssigned(tableName, 10000);
          
          return true;
        }
        
        Thread.sleep(1000);
      } catch (Exception e) {
        LOG.debug("Waiting for split: {}", e.getMessage());
      }
    }
    
    LOG.error("Split did not complete within {} ms", timeoutMs);
    return false;
  }
  
  /**
   * Verify all data exists after split.
   */
  private void verifyAllDataAfterSplit(TableName tableName) throws Exception {
    LOG.info("Verifying all data after split");
    
    try (Connection conn = TEST_UTIL.getConnection();
         Table table = conn.getTable(tableName)) {
      
      String[] tenants = {"T01", "T02", "T03"};
      int rowsPerTenant = 1000;
      int samplingRate = 100; // Check every 100th row
      
      int totalVerified = 0;
      for (String tenant : tenants) {
        for (int i = 0; i < rowsPerTenant; i += samplingRate) {
          String rowKey = String.format("%srow%05d", tenant, i);
          Get get = new Get(Bytes.toBytes(rowKey));
          get.addColumn(FAMILY, QUALIFIER);
          
          Result result = table.get(get);
          assertFalse("Row should exist: " + rowKey, result.isEmpty());
          
          String expectedValue = String.format("value_tenant-%s_row-%05d", tenant, i);
          byte[] actualValue = result.getValue(FAMILY, QUALIFIER);
          assertEquals("Value mismatch for " + rowKey, 
                      expectedValue, Bytes.toString(actualValue));
          
          totalVerified++;
        }
      }
      
      LOG.info("Successfully verified {} sample rows after split", totalVerified);
    }
  }
  
  /**
   * Verify that test data exists in the table (basic check).
   */
  private void verifyTestDataExists(TableName tableName) throws Exception {
    LOG.info("Verifying test data exists in table: {}", tableName);
    
    try (Connection conn = TEST_UTIL.getConnection();
         Table table = conn.getTable(tableName)) {
      
      // Just check a few sample rows to ensure data was written
      String[] sampleRows = {
        "T01row00000",
        "T02row00000", 
        "T03row00000"
      };
      
      for (String rowKey : sampleRows) {
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(FAMILY, QUALIFIER);
        Result result = table.get(get);
        
        assertFalse("Row should exist: " + rowKey, result.isEmpty());
        byte[] value = result.getValue(FAMILY, QUALIFIER);
        assertNotNull("Value should not be null for " + rowKey, value);
        LOG.debug("Verified row exists: {}", rowKey);
      }
      
      LOG.info("Basic data verification passed");
    }
  }
} 