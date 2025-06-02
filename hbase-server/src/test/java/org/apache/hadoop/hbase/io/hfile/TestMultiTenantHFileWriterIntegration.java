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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
import org.apache.hadoop.hbase.client.Put;
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
 * Integration test for multi-tenant HFile writer. This test brings up a mini cluster,
 * creates a table with multi-tenant configuration, writes data, flushes, and verifies
 * that HFile v4 files are created with the proper format.
 */
@Category(MediumTests.class)
public class TestMultiTenantHFileWriterIntegration {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMultiTenantHFileWriterIntegration.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestMultiTenantHFileWriterIntegration.class);
  
  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  
  // Test constants
  private static final TableName TABLE_NAME = TableName.valueOf("TestMultiTenantTable");
  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final byte[] QUALIFIER = Bytes.toBytes("q");
  
  // Tenant configuration
  private static final int TENANT_PREFIX_LENGTH = 3;
  private static final String[] TENANTS = {"T01", "T02", "T03"};
  private static final int ROWS_PER_TENANT = 10;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Configure the cluster for multi-tenant HFiles
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setInt(MultiTenantHFileWriter.TENANT_PREFIX_LENGTH, TENANT_PREFIX_LENGTH);
    conf.setInt(HFile.FORMAT_VERSION_KEY, HFile.MIN_FORMAT_VERSION_WITH_MULTI_TENANT);
    
    // Start the mini cluster
    TEST_UTIL.startMiniCluster(1);
  }
  
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }
  
  /**
   * Test creating a table, writing data with tenant prefixes, flushing,
   * and verifying the resulting HFiles are multi-tenant v4 format.
   */
  @Test(timeout = 180000) // 3 minutes timeout
  public void testMultiTenantHFileCreation() throws Exception {
    // Create the test table with multi-tenant configuration
    createTestTable();
    
    // Write data for multiple tenants
    writeTestData();
    
    // Verify memstore has data before flush
    assertTableMemStoreNotEmpty();
    
    // Flush the table to create HFiles using TEST_UTIL.flush() 
    // which is more reliable than admin.flush()
    flushTable();
    
    // Verify memstore is empty after flush
    assertTableMemStoreEmpty();
    
    // Verify data using HBase APIs (GET and SCAN)
    verifyDataWithGet();
    verifyDataWithScan();
    verifyDataWithTenantSpecificScans();
    verifyEdgeCasesAndCrossTenantIsolation();
    
    // Verify that HFiles were created with the proper format
    List<Path> hfilePaths = findHFilePaths();
    LOG.info("Found {} HFiles after flush", hfilePaths.size());
    assertFalse("No HFiles found after flush", hfilePaths.isEmpty());
    
    // Verify each HFile's format and data
    verifyHFileFormat(hfilePaths);
    
    LOG.info("Multi-tenant HFile integration test completed successfully!");
  }
  
  /**
   * Create a test table with multi-tenant configuration.
   */
  private void createTestTable() throws IOException {
    try (Admin admin = TEST_UTIL.getAdmin()) {
      // Create table descriptor with multi-tenant configuration
      TableDescriptorBuilder tableBuilder = TableDescriptorBuilder.newBuilder(TABLE_NAME);
      
      // Set multi-tenant properties at the table level
      tableBuilder.setValue(MultiTenantHFileWriter.TABLE_TENANT_PREFIX_LENGTH, 
                         String.valueOf(TENANT_PREFIX_LENGTH));
      tableBuilder.setValue(MultiTenantHFileWriter.TABLE_MULTI_TENANT_ENABLED, "true");
      
      // Add column family
      tableBuilder.setColumnFamily(
          ColumnFamilyDescriptorBuilder.newBuilder(FAMILY).build());
      
      // Create the table
      admin.createTable(tableBuilder.build());
      LOG.info("Created table {} with multi-tenant configuration", TABLE_NAME);
      
      // Wait for the table to be available
      try {
        TEST_UTIL.waitTableAvailable(TABLE_NAME);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted while waiting for table to become available", e);
      }
    }
  }
  
  /**
   * Write test data with different tenant prefixes.
   */
  private void writeTestData() throws IOException {
    try (Connection conn = TEST_UTIL.getConnection();
         Table table = conn.getTable(TABLE_NAME)) {
      
      List<Put> puts = new ArrayList<>();
      
      // Generate data for each tenant
      for (String tenant : TENANTS) {
        for (int i = 0; i < ROWS_PER_TENANT; i++) {
          // Create row key with tenant prefix
          String rowKey = tenant + "_row_" + String.format("%03d", i);
          Put put = new Put(Bytes.toBytes(rowKey));
          put.addColumn(FAMILY, QUALIFIER, Bytes.toBytes("value_" + tenant + "_" + i));
          puts.add(put);
          LOG.debug("Created put for row: {}", rowKey);
        }
      }
      
      // Write all puts
      table.put(puts);
      LOG.info("Successfully wrote {} rows with tenant prefixes", puts.size());
      
      // Verify data was written by doing a quick scan
      try (org.apache.hadoop.hbase.client.ResultScanner scanner = table.getScanner(FAMILY)) {
        int scannedRows = 0;
        for (org.apache.hadoop.hbase.client.Result result : scanner) {
          scannedRows++;
          if (scannedRows <= 5) { // Log first 5 rows for debugging
            String rowKey = Bytes.toString(result.getRow());
            String value = Bytes.toString(result.getValue(FAMILY, QUALIFIER));
            LOG.info("Scanned row: {} = {}", rowKey, value);
          }
        }
        LOG.info("Total rows scanned after write: {}", scannedRows);
        
        if (scannedRows != puts.size()) {
          LOG.warn("Expected {} rows but scanned {} rows", puts.size(), scannedRows);
        }
      }
    }
  }
  
  /**
   * Verify that the table's regions have data in their memstores.
   */
  private void assertTableMemStoreNotEmpty() {
    List<HRegion> regions = TEST_UTIL.getHBaseCluster().getRegions(TABLE_NAME);
    long totalSize = regions.stream().mapToLong(HRegion::getMemStoreDataSize).sum();
    assertTrue("Table memstore should not be empty", totalSize > 0);
    LOG.info("Table memstore size before flush: {} bytes", totalSize);
  }
  
  /**
   * Verify that the table's regions have empty memstores after flush.
   */
  private void assertTableMemStoreEmpty() {
    List<HRegion> regions = TEST_UTIL.getHBaseCluster().getRegions(TABLE_NAME);
    long totalSize = regions.stream().mapToLong(HRegion::getMemStoreDataSize).sum();
    assertEquals("Table memstore should be empty after flush", 0, totalSize);
    LOG.info("Table memstore size after flush: {} bytes", totalSize);
  }
  
  /**
   * Flush the table using TEST_UTIL which has built-in retry logic.
   */
  private void flushTable() throws IOException {
    LOG.info("Flushing table {}", TABLE_NAME);
    
    // Log HFiles before flush
    List<Path> hfilesBeforeFlush = findHFilePaths();
    LOG.info("HFiles before flush: {}", hfilesBeforeFlush.size());
    
    TEST_UTIL.flush(TABLE_NAME);
    
    // Wait a bit for flush to complete
    try {
        Thread.sleep(2000);
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
    }
    
    // Log HFiles after flush
    List<Path> hfilesAfterFlush = findHFilePaths();
    LOG.info("HFiles after flush: {}", hfilesAfterFlush.size());
    for (Path hfile : hfilesAfterFlush) {
        LOG.info("HFile created: {}", hfile);
    }
    
    LOG.info("Successfully flushed table {}", TABLE_NAME);
  }
  
  /**
   * Verify data using HBase GET operations.
   * This tests individual row retrieval after data has been flushed to HFiles.
   */
  private void verifyDataWithGet() throws IOException {
    LOG.info("Verifying data using GET operations");
    
    try (Connection conn = TEST_UTIL.getConnection();
         Table table = conn.getTable(TABLE_NAME)) {
      
      int successfulGets = 0;
      int failedGets = 0;
      
      // Verify each row using GET
      for (String tenant : TENANTS) {
        for (int i = 0; i < ROWS_PER_TENANT; i++) {
          String rowKey = tenant + "_row_" + String.format("%03d", i);
          org.apache.hadoop.hbase.client.Get get = new org.apache.hadoop.hbase.client.Get(Bytes.toBytes(rowKey));
          get.addColumn(FAMILY, QUALIFIER);
          
          org.apache.hadoop.hbase.client.Result result = table.get(get);
          
          if (!result.isEmpty()) {
            byte[] value = result.getValue(FAMILY, QUALIFIER);
            String expectedValue = "value_" + tenant + "_" + i;
            String actualValue = Bytes.toString(value);
            
            assertEquals("Value mismatch for row " + rowKey, expectedValue, actualValue);
            successfulGets++;
            
            // Log first few successful gets
            if (successfulGets <= 5) {
              LOG.info("GET verified row: {} = {}", rowKey, actualValue);
            }
          } else {
            failedGets++;
            LOG.error("GET failed for row: {}", rowKey);
          }
        }
      }
      
      LOG.info("GET verification complete: {} successful, {} failed", successfulGets, failedGets);
      assertEquals("All GETs should succeed", TENANTS.length * ROWS_PER_TENANT, successfulGets);
      assertEquals("No GETs should fail", 0, failedGets);
    }
  }
  
  /**
   * Verify data using a full table SCAN.
   * This tests scanning across all tenant sections in the multi-tenant HFile.
   */
  private void verifyDataWithScan() throws IOException {
    LOG.info("Verifying data using full table SCAN");
    
    try (Connection conn = TEST_UTIL.getConnection();
         Table table = conn.getTable(TABLE_NAME)) {
      
      org.apache.hadoop.hbase.client.Scan scan = new org.apache.hadoop.hbase.client.Scan();
      scan.addColumn(FAMILY, QUALIFIER);
      
      int rowCount = 0;
      java.util.Map<String, Integer> tenantCounts = new java.util.HashMap<>();
      
      try (org.apache.hadoop.hbase.client.ResultScanner scanner = table.getScanner(scan)) {
        for (org.apache.hadoop.hbase.client.Result result : scanner) {
          rowCount++;
          
          String rowKey = Bytes.toString(result.getRow());
          String value = Bytes.toString(result.getValue(FAMILY, QUALIFIER));
          
          // Extract tenant from row key
          String tenant = rowKey.substring(0, TENANT_PREFIX_LENGTH);
          tenantCounts.put(tenant, tenantCounts.getOrDefault(tenant, 0) + 1);
          
          // Verify value format
          assertTrue("Value should start with 'value_'", value.startsWith("value_"));
          assertTrue("Value should contain tenant ID", value.contains(tenant));
          
          // Log first few rows
          if (rowCount <= 5) {
            LOG.info("SCAN found row: {} = {}", rowKey, value);
          }
        }
      }
      
      LOG.info("Full table SCAN complete: {} total rows", rowCount);
      assertEquals("Should scan all rows", TENANTS.length * ROWS_PER_TENANT, rowCount);
      
      // Verify each tenant has correct number of rows
      for (String tenant : TENANTS) {
        Integer count = tenantCounts.get(tenant);
        LOG.info("Tenant {} has {} rows", tenant, count);
        assertEquals("Tenant " + tenant + " should have " + ROWS_PER_TENANT + " rows", 
                    ROWS_PER_TENANT, count.intValue());
      }
    }
  }
  
  /**
   * Verify data using tenant-specific SCAN operations.
   * This tests scanning within specific tenant boundaries to ensure proper data isolation.
   */
  private void verifyDataWithTenantSpecificScans() throws IOException {
    LOG.info("Verifying data using tenant-specific SCAN operations");
    
    try (Connection conn = TEST_UTIL.getConnection();
         Table table = conn.getTable(TABLE_NAME)) {
      
      // Scan each tenant's data separately
      for (String tenant : TENANTS) {
        LOG.info("Scanning data for tenant: {}", tenant);
        
        // Create scan with start and stop rows for this tenant
        byte[] startRow = Bytes.toBytes(tenant);
        // Stop row is exclusive, so we use next tenant prefix or tenant + "~" for last tenant
        byte[] stopRow = Bytes.toBytes(tenant + "~"); // '~' is after '_' in ASCII
        
        org.apache.hadoop.hbase.client.Scan tenantScan = new org.apache.hadoop.hbase.client.Scan()
            .withStartRow(startRow)
            .withStopRow(stopRow)
            .addColumn(FAMILY, QUALIFIER);
        
        int tenantRowCount = 0;
        
        try (org.apache.hadoop.hbase.client.ResultScanner scanner = table.getScanner(tenantScan)) {
          for (org.apache.hadoop.hbase.client.Result result : scanner) {
            tenantRowCount++;
            
            String rowKey = Bytes.toString(result.getRow());
            String value = Bytes.toString(result.getValue(FAMILY, QUALIFIER));
            
            // Verify this row belongs to the current tenant
            assertTrue("Row should belong to tenant " + tenant, rowKey.startsWith(tenant));
            assertTrue("Value should contain tenant " + tenant, value.contains(tenant));
            
            // Log first few rows for this tenant
            if (tenantRowCount <= 3) {
              LOG.info("Tenant {} scan found: {} = {}", tenant, rowKey, value);
            }
          }
        }
        
        LOG.info("Tenant {} scan complete: {} rows found", tenant, tenantRowCount);
        assertEquals("Tenant " + tenant + " should have exactly " + ROWS_PER_TENANT + " rows",
                    ROWS_PER_TENANT, tenantRowCount);
      }
      
      // Test scan with partial row key (prefix scan)
      LOG.info("Testing partial row key scan");
      for (String tenant : TENANTS) {
        // Scan for specific row pattern within tenant
        String partialKey = tenant + "_row_00"; // Should match rows 000-009
        byte[] prefixStart = Bytes.toBytes(partialKey);
        byte[] prefixStop = Bytes.toBytes(tenant + "_row_01"); // Exclusive
        
        org.apache.hadoop.hbase.client.Scan prefixScan = new org.apache.hadoop.hbase.client.Scan()
            .withStartRow(prefixStart)
            .withStopRow(prefixStop)
            .addColumn(FAMILY, QUALIFIER);
        
        int prefixMatchCount = 0;
        try (org.apache.hadoop.hbase.client.ResultScanner scanner = table.getScanner(prefixScan)) {
          for (org.apache.hadoop.hbase.client.Result result : scanner) {
            prefixMatchCount++;
            String rowKey = Bytes.toString(result.getRow());
            assertTrue("Row should match prefix pattern", rowKey.startsWith(partialKey));
          }
        }
        
        LOG.info("Prefix scan for '{}' found {} rows", partialKey, prefixMatchCount);
        assertEquals("Should find exactly 10 rows matching prefix", 10, prefixMatchCount);
      }
    }
  }
  
  /**
   * Verify edge cases and cross-tenant isolation.
   * This tests that tenant boundaries are properly enforced and no data leakage occurs.
   */
  private void verifyEdgeCasesAndCrossTenantIsolation() throws IOException {
    LOG.info("Verifying edge cases and cross-tenant isolation");
    
    try (Connection conn = TEST_UTIL.getConnection();
         Table table = conn.getTable(TABLE_NAME)) {
      
      // Test 1: Verify GET with non-existent row returns empty result
      LOG.info("Test 1: Verifying GET with non-existent row");
      String nonExistentRow = "T99_row_999";
      org.apache.hadoop.hbase.client.Get get = new org.apache.hadoop.hbase.client.Get(Bytes.toBytes(nonExistentRow));
      org.apache.hadoop.hbase.client.Result result = table.get(get);
      assertTrue("GET for non-existent row should return empty result", result.isEmpty());
      
      // Test 2: Verify scan with row key between tenants returns correct data
      LOG.info("Test 2: Verifying scan between tenant boundaries");
      // Scan from middle of T01 to middle of T02
      byte[] startRow = Bytes.toBytes("T01_row_005");
      byte[] stopRow = Bytes.toBytes("T02_row_005");
      
      org.apache.hadoop.hbase.client.Scan boundaryScn = new org.apache.hadoop.hbase.client.Scan()
          .withStartRow(startRow)
          .withStopRow(stopRow)
          .addColumn(FAMILY, QUALIFIER);
      
      int boundaryRowCount = 0;
      int t01Count = 0;
      int t02Count = 0;
      
      try (org.apache.hadoop.hbase.client.ResultScanner scanner = table.getScanner(boundaryScn)) {
        for (org.apache.hadoop.hbase.client.Result res : scanner) {
          boundaryRowCount++;
          String rowKey = Bytes.toString(res.getRow());
          
          if (rowKey.startsWith("T01")) {
            t01Count++;
          } else if (rowKey.startsWith("T02")) {
            t02Count++;
          }
          
          // Log first few rows
          if (boundaryRowCount <= 3) {
            LOG.info("Boundary scan found: {}", rowKey);
          }
        }
      }
      
      LOG.info("Boundary scan found {} total rows: {} from T01, {} from T02", 
               boundaryRowCount, t01Count, t02Count);
      
      // Should get rows 005-009 from T01 (5 rows) and rows 000-004 from T02 (5 rows)
      assertEquals("Should find 5 rows from T01", 5, t01Count);
      assertEquals("Should find 5 rows from T02", 5, t02Count);
      assertEquals("Total boundary scan should find 10 rows", 10, boundaryRowCount);
      
      // Test 3: Verify reverse scan works correctly across tenant boundaries
      LOG.info("Test 3: Verifying reverse scan");
      org.apache.hadoop.hbase.client.Scan reverseScan = new org.apache.hadoop.hbase.client.Scan()
          .setReversed(true)
          .addColumn(FAMILY, QUALIFIER);
      
      int reverseRowCount = 0;
      String previousRowKey = null;
      
      try (org.apache.hadoop.hbase.client.ResultScanner scanner = table.getScanner(reverseScan)) {
        for (org.apache.hadoop.hbase.client.Result res : scanner) {
          reverseRowCount++;
          String rowKey = Bytes.toString(res.getRow());
          
          // Verify reverse order
          if (previousRowKey != null) {
            assertTrue("Rows should be in reverse order", 
                      rowKey.compareTo(previousRowKey) < 0);
          }
          previousRowKey = rowKey;
          
          // Log first few rows
          if (reverseRowCount <= 5) {
            LOG.info("Reverse scan found: {}", rowKey);
          }
        }
      }
      
      LOG.info("Reverse scan found {} total rows", reverseRowCount);
      assertEquals("Reverse scan should find all rows", 
                  TENANTS.length * ROWS_PER_TENANT, reverseRowCount);
      
      // Test 4: Verify batch GET operations work correctly
      LOG.info("Test 4: Verifying batch GET operations");
      List<org.apache.hadoop.hbase.client.Get> batchGets = new ArrayList<>();
      
      // Add GETs from different tenants
      batchGets.add(new org.apache.hadoop.hbase.client.Get(Bytes.toBytes("T01_row_000")));
      batchGets.add(new org.apache.hadoop.hbase.client.Get(Bytes.toBytes("T02_row_005")));
      batchGets.add(new org.apache.hadoop.hbase.client.Get(Bytes.toBytes("T03_row_009")));
      batchGets.add(new org.apache.hadoop.hbase.client.Get(Bytes.toBytes("T99_row_000"))); // Non-existent
      
      org.apache.hadoop.hbase.client.Result[] batchResults = table.get(batchGets);
      
      assertEquals("Batch GET should return 4 results", 4, batchResults.length);
      assertFalse("First result should not be empty", batchResults[0].isEmpty());
      assertFalse("Second result should not be empty", batchResults[1].isEmpty());
      assertFalse("Third result should not be empty", batchResults[2].isEmpty());
      assertTrue("Fourth result should be empty (non-existent row)", batchResults[3].isEmpty());
      
      // Verify the values
      assertEquals("value_T01_0", Bytes.toString(batchResults[0].getValue(FAMILY, QUALIFIER)));
      assertEquals("value_T02_5", Bytes.toString(batchResults[1].getValue(FAMILY, QUALIFIER)));
      assertEquals("value_T03_9", Bytes.toString(batchResults[2].getValue(FAMILY, QUALIFIER)));
      
      LOG.info("All edge cases and cross-tenant isolation tests passed!");
    }
  }
  
  /**
   * Verify that the HFiles are in v4 multi-tenant format.
   */
  private void verifyHFileFormat(List<Path> hfilePaths) throws IOException {
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Configuration conf = TEST_UTIL.getConfiguration();
    CacheConfig cacheConf = new CacheConfig(conf);
    
    for (Path path : hfilePaths) {
      LOG.info("Verifying HFile format for: {}", path);
      
      try (HFile.Reader reader = HFile.createReader(fs, path, cacheConf, true, conf)) {
        // Check file version
        int version = reader.getTrailer().getMajorVersion();
        assertEquals("HFile should be version 4", HFile.MIN_FORMAT_VERSION_WITH_MULTI_TENANT, version);
        
        // Verify reader type
        assertTrue("Reader should be an AbstractMultiTenantReader", 
                  reader instanceof AbstractMultiTenantReader);
        
        AbstractMultiTenantReader mtReader = (AbstractMultiTenantReader) reader;
        
        // Get all tenant section IDs available in the file
        byte[][] allTenantSectionIds = mtReader.getAllTenantSectionIds();
        LOG.info("Found {} tenant sections in HFile", allTenantSectionIds.length);
        
        int totalCellsFound = 0;
        
        // Verify each tenant section by iterating through available sections
        for (byte[] tenantSectionId : allTenantSectionIds) {
          String tenantId = Bytes.toString(tenantSectionId);
          LOG.info("Verifying data for tenant section: {}", tenantId);
          
          // Get section reader directly for this tenant section
          try {
            java.lang.reflect.Method getSectionReaderMethod = 
                AbstractMultiTenantReader.class.getDeclaredMethod("getSectionReader", byte[].class);
            getSectionReaderMethod.setAccessible(true);
            Object sectionReader = getSectionReaderMethod.invoke(mtReader, tenantSectionId);
            
            if (sectionReader != null) {
              // Get scanner for this section
              java.lang.reflect.Method getReaderMethod = 
                  sectionReader.getClass().getMethod("getReader");
              HFileReaderImpl sectionHFileReader = (HFileReaderImpl) getReaderMethod.invoke(sectionReader);
              
              HFileScanner sectionScanner = sectionHFileReader.getScanner(conf, false, false);
              
              // Scan through this section
              boolean hasData = sectionScanner.seekTo();
              if (hasData) {
                int sectionCellCount = 0;
                do {
                  Cell cell = sectionScanner.getCell();
                  if (cell != null) {
                    sectionCellCount++;
                    totalCellsFound++;
                    
                    String rowString = Bytes.toString(CellUtil.cloneRow(cell));
                    
                    // Log first few cells for verification
                    if (sectionCellCount <= 3) {
                      String value = Bytes.toString(CellUtil.cloneValue(cell));
                      LOG.info("Found cell in section {}: {} = {}", tenantId, rowString, value);
                    }
                  }
                } while (sectionScanner.next());
                
                LOG.info("Found {} cells in tenant section {}", sectionCellCount, tenantId);
                assertTrue("Should have found data in tenant section " + tenantId, sectionCellCount > 0);
              } else {
                LOG.warn("No data found in tenant section: {}", tenantId);
              }
            } else {
              LOG.warn("Could not get section reader for tenant section: {}", tenantId);
            }
          } catch (Exception e) {
            LOG.error("Failed to access tenant section: " + tenantId, e);
            // Continue with next section
          }
        }
        
        LOG.info("Total cells verified: {}", totalCellsFound);
        int expectedTotal = TENANTS.length * ROWS_PER_TENANT;
        assertEquals("Should have found all " + expectedTotal + " cells", 
                    expectedTotal, totalCellsFound);
        
        // Verify HFile info contains multi-tenant metadata
        HFileInfo fileInfo = reader.getHFileInfo();
        if (fileInfo != null) {
          byte[] sectionCountBytes = fileInfo.get(Bytes.toBytes("SECTION_COUNT"));
          if (sectionCountBytes != null) {
            int sectionCount = Bytes.toInt(sectionCountBytes);
            LOG.info("HFile contains {} tenant sections", sectionCount);
            assertTrue("HFile should have tenant sections", sectionCount > 0);
            assertEquals("Should have " + TENANTS.length + " tenant sections", 
                        TENANTS.length, sectionCount);
          }
          
          byte[] tenantIndexLevelsBytes = fileInfo.get(Bytes.toBytes("TENANT_INDEX_LEVELS"));
          if (tenantIndexLevelsBytes != null) {
            int tenantIndexLevels = Bytes.toInt(tenantIndexLevelsBytes);
            LOG.info("HFile tenant index has {} levels", tenantIndexLevels);
            assertTrue("HFile should have tenant index levels", tenantIndexLevels > 0);
          }
        }
      }
    }
  }
  
  /**
   * Find all HFiles created for our test table by directly scanning the filesystem.
   */
  private List<Path> findHFilePaths() throws IOException {
    List<Path> hfilePaths = new ArrayList<>();
    
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path rootDir = TEST_UTIL.getDataTestDirOnTestFS();
    
    // Use the same path calculation as HBase internals
    Path tableDir = new Path(rootDir, "data/default/" + TABLE_NAME.getNameAsString());
    LOG.info("Looking for HFiles in table directory: {}", tableDir);
    
    if (fs.exists(tableDir)) {
      // Look for region directories
      FileStatus[] regionDirs = fs.listStatus(tableDir);
      LOG.info("Found {} potential region directories", regionDirs.length);
      
      for (FileStatus regionDir : regionDirs) {
        LOG.info("Checking directory: {} (isDirectory: {})", 
                regionDir.getPath(), regionDir.isDirectory());
        
        if (regionDir.isDirectory() && !regionDir.getPath().getName().startsWith(".")) {
          Path familyDir = new Path(regionDir.getPath(), Bytes.toString(FAMILY));
          LOG.info("Looking for family directory: {}", familyDir);
          
          if (fs.exists(familyDir)) {
            FileStatus[] hfiles = fs.listStatus(familyDir);
            LOG.info("Found {} files in family directory", hfiles.length);
            
            for (FileStatus hfile : hfiles) {
              LOG.info("Checking file: {} (size: {} bytes)", 
                      hfile.getPath(), hfile.getLen());
              
              if (!hfile.getPath().getName().startsWith(".") && 
                  !hfile.getPath().getName().endsWith(".tmp")) {
                hfilePaths.add(hfile.getPath());
                LOG.info("Added HFile: {} (size: {} bytes)", 
                        hfile.getPath(), hfile.getLen());
              } else {
                LOG.info("Skipped file: {} (temp or hidden)", hfile.getPath());
              }
            }
          } else {
            LOG.warn("Family directory does not exist: {}", familyDir);
          }
        }
      }
    } else {
      LOG.warn("Table directory does not exist: {}", tableDir);
    }
    
    LOG.info("Total HFiles found: {}", hfilePaths.size());
    return hfilePaths;
  }
} 