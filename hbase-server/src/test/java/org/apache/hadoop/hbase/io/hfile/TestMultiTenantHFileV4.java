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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.ExtendedCell;

/**
 * Test tenant-aware reading capabilities with data from multiple tenants.
 * 
 * Note: This test focuses on the reading capabilities rather than writing with
 * the multi-tenant writer directly, to avoid multi-level index issues in test environments.
 * 
 * It tests:
 * 1. Writing data for 3 tenants in a sorted manner
 * 2. Reading that data back using tenant prefixes
 * 3. Verifying the integrity of each tenant's data set
 */
@Category({IOTests.class, MediumTests.class})
public class TestMultiTenantHFileV4 {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMultiTenantHFileV4.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestMultiTenantHFileV4.class);
  
  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  
  @Rule
  public TestName testName = new TestName();
  
  private Configuration conf;
  private FileSystem fs;
  private Path testDir;
  
  // Tenant configuration
  private static final int TENANT_PREFIX_LENGTH = 3;
  private static final String TENANT_1 = "T01";
  private static final String TENANT_2 = "T02";
  private static final String TENANT_3 = "T03";
  
  // Test data
  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final byte[] QUALIFIER = Bytes.toBytes("q");
  
  @Before
  public void setUp() throws IOException {
    conf = TEST_UTIL.getConfiguration();
    
    // Configure tenant prefix extraction
    conf.setInt(MultiTenantHFileWriter.TENANT_PREFIX_LENGTH, TENANT_PREFIX_LENGTH);
    conf.setInt(MultiTenantHFileWriter.TENANT_PREFIX_OFFSET, 0);
    
    // Explicitly configure HFile version 4
    conf.setInt(HFile.FORMAT_VERSION_KEY, HFile.MIN_FORMAT_VERSION_WITH_MULTI_TENANT);
    
    fs = FileSystem.get(conf);
    testDir = new Path(TEST_UTIL.getDataTestDir(), testName.getMethodName());
    if (fs.exists(testDir)) {
      fs.delete(testDir, true);
    }
    fs.mkdirs(testDir);
  }
  
  @After
  public void tearDown() throws IOException {
    fs.delete(testDir, true);
  }
  
  /**
   * Test writing data for multiple tenants and reading it back with tenant awareness.
   * 
   * This test:
   * 1. Creates data for 3 different tenants
   * 2. Writes all data to a single HFile (sorted by tenant)
   * 3. Reads back with tenant prefix awareness
   * 4. Verifies each tenant's data is correctly identified and retrieved
   */
  @Test
  public void testMultiTenantWriteRead() throws IOException {
    Path hfilePath = new Path(testDir, "test_v4.hfile");
    
    // Create test data for 3 different tenants
    Map<String, List<ExtendedCell>> tenantData = createTestData();
    
    // Write the data to a regular HFile (not using MultiTenantHFileWriter)
    writeHFile(hfilePath, tenantData);
    
    // Read back and verify using tenant extraction
    readAndVerifyHFile(hfilePath, tenantData);
  }
  
  /**
   * Create test data with different keys for each tenant
   */
  private Map<String, List<ExtendedCell>> createTestData() {
    Map<String, List<ExtendedCell>> tenantData = new HashMap<>();
    
    // Tenant 1 data
    List<ExtendedCell> tenant1Cells = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      // Pad numbers with leading zeros to ensure proper lexicographical ordering
      String paddedIndex = String.format("%02d", i);
      byte[] row = Bytes.toBytes(TENANT_1 + "_row_" + paddedIndex);
      byte[] value = Bytes.toBytes("value_" + i);
      tenant1Cells.add((ExtendedCell)new KeyValue(row, FAMILY, QUALIFIER, value));
    }
    tenantData.put(TENANT_1, tenant1Cells);
    
    // Tenant 2 data
    List<ExtendedCell> tenant2Cells = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      String paddedIndex = String.format("%02d", i);
      byte[] row = Bytes.toBytes(TENANT_2 + "_row_" + paddedIndex);
      byte[] value = Bytes.toBytes("value_" + (100 + i));
      tenant2Cells.add((ExtendedCell)new KeyValue(row, FAMILY, QUALIFIER, value));
    }
    tenantData.put(TENANT_2, tenant2Cells);
    
    // Tenant 3 data
    List<ExtendedCell> tenant3Cells = new ArrayList<>();
    for (int i = 0; i < 15; i++) {
      String paddedIndex = String.format("%02d", i);
      byte[] row = Bytes.toBytes(TENANT_3 + "_row_" + paddedIndex);
      byte[] value = Bytes.toBytes("value_" + (200 + i));
      tenant3Cells.add((ExtendedCell)new KeyValue(row, FAMILY, QUALIFIER, value));
    }
    tenantData.put(TENANT_3, tenant3Cells);
    
    return tenantData;
  }
  
  /**
   * Write all tenant data to an HFile v4
   */
  private void writeHFile(Path path, Map<String, List<ExtendedCell>> tenantData) throws IOException {
    // Setup HFile writing
    CacheConfig cacheConf = new CacheConfig(conf);
    CellComparator comparator = CellComparator.getInstance();
    
    // Create HFile context with table name (for tenant configuration)
    HFileContext hfileContext = new HFileContextBuilder()
        .withBlockSize(64 * 1024)
        .withCellComparator(comparator)
        .withTableName(TableName.valueOf("test_table").getName())
        .withHBaseCheckSum(true)
        .build();
    
    // Use the generic factory method which will return the appropriate writer factory based on configuration
    HFile.WriterFactory writerFactory = HFile.getWriterFactory(conf, cacheConf)
        .withFileContext(hfileContext)
        .withPath(fs, path);
    
    // Verify we got the correct writer factory type
    assertTrue("Expected MultiTenantHFileWriter.WriterFactory but got " + writerFactory.getClass().getName(),
               writerFactory instanceof MultiTenantHFileWriter.WriterFactory);
    LOG.info("Created writer factory instance: {}", writerFactory.getClass().getName());
    
    // Create writer
    try (HFile.Writer writer = writerFactory.create()) {
      // Verify we got a MultiTenantHFileWriter instance
      assertTrue("Expected MultiTenantHFileWriter but got " + writer.getClass().getName(),
                 writer instanceof MultiTenantHFileWriter);
      LOG.info("Created writer instance: {}", writer.getClass().getName());
      
      LOG.info("Writing HFile with multi-tenant data to {}", path);
      
      // Write data for each tenant - must be in proper sort order
      // First tenant 1 
      for (ExtendedCell cell : tenantData.get(TENANT_1)) {
        writer.append(cell);
      }
      
      // Then tenant 2
      for (ExtendedCell cell : tenantData.get(TENANT_2)) {
        writer.append(cell);
      }
      
      // Finally tenant 3
      for (ExtendedCell cell : tenantData.get(TENANT_3)) {
        writer.append(cell);
      }
      
      LOG.info("Finished writing {} cells to HFile", 
              tenantData.get(TENANT_1).size() + 
              tenantData.get(TENANT_2).size() + 
              tenantData.get(TENANT_3).size());
    }
  }
  
  /**
   * Read back the HFile and verify each tenant's data
   */
  private void readAndVerifyHFile(Path path, Map<String, List<ExtendedCell>> expectedData) throws IOException {
    // Create a CacheConfig
    CacheConfig cacheConf = new CacheConfig(conf);
    
    // Open the file directly using HFile class
    try (HFile.Reader reader = HFile.createReader(fs, path, cacheConf, true, conf)) {
      // Verify that we got a multi-tenant reader implementation
      assertTrue("Expected reader to be an AbstractMultiTenantReader but got " + reader.getClass().getName(),
                reader instanceof AbstractMultiTenantReader);
      LOG.info("Created reader instance: {}", reader.getClass().getName());
      
      LOG.info("Opened HFile reader for {}", path);
      
      // Create a scanner
      HFileScanner scanner = reader.getScanner(conf, false, true);
      
      // Verify that we got a multi-tenant scanner implementation
      assertTrue("Expected scanner to be a MultiTenantScanner but got " + scanner.getClass().getName(),
                scanner instanceof AbstractMultiTenantReader.MultiTenantScanner);
      LOG.info("Created scanner instance: {}", scanner.getClass().getName());
      
      // Verify data for each tenant
      verifyTenantData(scanner, TENANT_1, expectedData.get(TENANT_1));
      verifyTenantData(scanner, TENANT_2, expectedData.get(TENANT_2));
      verifyTenantData(scanner, TENANT_3, expectedData.get(TENANT_3));
    }
  }
  
  /**
   * Verify data for a specific tenant
   */
  private void verifyTenantData(HFileScanner scanner, String tenant, List<ExtendedCell> expectedCells) 
      throws IOException {
    LOG.info("Verifying data for tenant {}", tenant);
    
    // Seek to first row for this tenant
    ExtendedCell firstCell = expectedCells.get(0);
    int seekResult = scanner.seekTo(firstCell);
    assertTrue("Failed to seek to first key for tenant " + tenant, seekResult != -1);
    
    // Verify scanner properly initialized for this tenant
    HFile.Reader mainReader = scanner.getReader();
    if (mainReader instanceof AbstractMultiTenantReader) {
      // This part shows that the code flow is indeed going through AbstractMultiTenantReader
      AbstractMultiTenantReader mtReader = (AbstractMultiTenantReader)mainReader;
      LOG.info("Successfully verified scanner is using a multi-tenant reader for tenant {}", tenant);
    } else {
      fail("Expected AbstractMultiTenantReader to be used but got " + mainReader.getClass().getName());
    }
    
    // Verify all expected cells
    int cellCount = 0;
    do {
      Cell cell = scanner.getCell();
      assertNotNull("Cell should not be null", cell);
      
      // Get the row
      String row = Bytes.toString(CellUtil.cloneRow(cell));
      
      // Verify this is still the same tenant
      if (!row.startsWith(tenant)) {
        LOG.info("Reached end of tenant {}'s data", tenant);
        break;
      }
      
      // Verify against expected cell
      if (cellCount < expectedCells.size()) {
        Cell expectedCell = expectedCells.get(cellCount);
        
        assertEquals("Row mismatch", 
                    Bytes.toString(CellUtil.cloneRow(expectedCell)), 
                    Bytes.toString(CellUtil.cloneRow(cell)));
        
        assertEquals("Value mismatch", 
                    Bytes.toString(CellUtil.cloneValue(expectedCell)), 
                    Bytes.toString(CellUtil.cloneValue(cell)));
        
        cellCount++;
      }
    } while (scanner.next());
    
    // Verify we saw all expected cells
    assertEquals("Did not see expected number of cells for tenant " + tenant, 
                expectedCells.size(), cellCount);
    
    LOG.info("Successfully verified {} cells for tenant {}", cellCount, tenant);
  }
} 