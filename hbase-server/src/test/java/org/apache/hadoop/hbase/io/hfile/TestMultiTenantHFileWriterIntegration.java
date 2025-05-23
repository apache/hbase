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
    TEST_UTIL.flush(TABLE_NAME);
    LOG.info("Successfully flushed table {}", TABLE_NAME);
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
} 