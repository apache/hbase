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
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for HFilePrettyPrinter with HFile v4 multi-tenant features.
 * This test validates that the pretty printer correctly handles v4 HFiles
 * with multi-tenant capabilities including tenant information display,
 * tenant-aware block analysis, and comprehensive output formatting.
 */
@Category({ IOTests.class, MediumTests.class })
public class TestHFileV4PrettyPrinter {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestHFileV4PrettyPrinter.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestHFileV4PrettyPrinter.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();
  private static final int TENANT_PREFIX_LENGTH = 3;
  private static final String[] TENANTS = {"T01", "T02", "T03"};
  private static final int TEST_TIMEOUT_MS = 120000; // 2 minutes
  
  private static FileSystem fs;
  private static Configuration conf;
  private static final byte[] cf = Bytes.toBytes("cf");
  private static final byte[] fam = Bytes.toBytes("fam");
  private static PrintStream original;
  private static PrintStream ps;
  private static ByteArrayOutputStream stream;

  @Before
  public void setup() throws Exception {
    conf = UTIL.getConfiguration();
    
    // Configure HFile v4 multi-tenant settings
    conf.setInt(HFile.FORMAT_VERSION_KEY, HFile.MIN_FORMAT_VERSION_WITH_MULTI_TENANT);
    conf.setInt(MultiTenantHFileWriter.TENANT_PREFIX_LENGTH, TENANT_PREFIX_LENGTH);
    
    // Runs on local filesystem. Test does not need sync. Turn off checks.
    conf.setBoolean(CommonFSUtils.UNSAFE_STREAM_CAPABILITY_ENFORCE, false);
    
    // Start mini cluster for v4 HFile creation
    UTIL.startMiniCluster(1);
    
    fs = UTIL.getTestFileSystem();
    stream = new ByteArrayOutputStream();
    ps = new PrintStream(stream);
    original = System.out;
    
    LOG.info("Setup complete with HFile v4 configuration");
  }

  @After
  public void teardown() throws Exception {
    System.setOut(original);
    if (UTIL != null) {
      UTIL.shutdownMiniCluster();
    }
  }

  /**
   * Create a v4 multi-tenant HFile with test data.
   */
  private Path createV4HFile(String testName, int rowCount) throws Exception {
    TableName tableName = TableName.valueOf(testName + "_" + System.currentTimeMillis());
    
    try (Admin admin = UTIL.getAdmin()) {
      // Create table with multi-tenant configuration
      TableDescriptorBuilder tableBuilder = TableDescriptorBuilder.newBuilder(tableName);
      
      // Set multi-tenant properties
      tableBuilder.setValue(MultiTenantHFileWriter.TABLE_TENANT_PREFIX_LENGTH, 
                           String.valueOf(TENANT_PREFIX_LENGTH));
      tableBuilder.setValue(MultiTenantHFileWriter.TABLE_MULTI_TENANT_ENABLED, "true");
      
      // Configure column family for HFile v4
      ColumnFamilyDescriptorBuilder cfBuilder = ColumnFamilyDescriptorBuilder.newBuilder(cf);
      cfBuilder.setValue(HFile.FORMAT_VERSION_KEY, String.valueOf(HFile.MIN_FORMAT_VERSION_WITH_MULTI_TENANT));
      tableBuilder.setColumnFamily(cfBuilder.build());
      
      admin.createTable(tableBuilder.build());
      UTIL.waitTableAvailable(tableName);
      
      // Write test data with tenant prefixes
      try (Connection connection = ConnectionFactory.createConnection(conf);
           Table table = connection.getTable(tableName)) {
        
        List<Put> puts = new ArrayList<>();
        int rowsPerTenant = rowCount / TENANTS.length;
        
        for (String tenantId : TENANTS) {
          for (int i = 0; i < rowsPerTenant; i++) {
            String rowKey = String.format("%srow%03d", tenantId, i);
            Put put = new Put(Bytes.toBytes(rowKey));
            String cellValue = String.format("value_tenant-%s_row-%03d", tenantId, i);
            put.addColumn(cf, fam, Bytes.toBytes(cellValue));
            puts.add(put);
          }
        }
        
        table.put(puts);
        LOG.info("Wrote {} rows to v4 multi-tenant table {}", puts.size(), tableName);
      }
      
      // Flush to create HFile v4
      UTIL.flush(tableName);
      Thread.sleep(1000); // Wait for flush to complete
      
      // Find the created HFile
      List<Path> hfiles = UTIL.getHBaseCluster().getRegions(tableName).get(0)
        .getStore(cf).getStorefiles().stream()
        .map(sf -> sf.getPath())
        .collect(java.util.stream.Collectors.toList());
      
      assertTrue("Should have created at least one HFile", !hfiles.isEmpty());
      Path originalHfilePath = hfiles.get(0);
      
      LOG.info("Found original v4 HFile: {}", originalHfilePath);
      
      // Copy HFile to test data directory before table cleanup
      Path testDataDir = UTIL.getDataTestDir(testName);
      Path copiedHfilePath = new Path(testDataDir, "hfile_v4_" + System.currentTimeMillis());
      
      // Use FileUtil to copy the file
      org.apache.hadoop.fs.FileUtil.copy(fs, originalHfilePath, fs, copiedHfilePath, false, conf);
      
      LOG.info("Copied v4 HFile from {} to {}", originalHfilePath, copiedHfilePath);
      
      // Verify the copied file is actually v4
      try (HFile.Reader reader = HFile.createReader(fs, copiedHfilePath, CacheConfig.DISABLED, true, conf)) {
        int version = reader.getTrailer().getMajorVersion();
        assertEquals("Should be HFile v4", HFile.MIN_FORMAT_VERSION_WITH_MULTI_TENANT, version);
        LOG.info("Verified copied HFile v4 format: version {}", version);
      }
      
      // Clean up table (original HFiles will be deleted but our copy is safe)
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
      
      return copiedHfilePath;
    }
  }

  /**
   * Comprehensive test for HFilePrettyPrinter with HFile v4 multi-tenant features.
   * This test validates:
   * - HFile v4 format detection and verification
   * - All command-line options functionality (-m, -p, -v, -t, -b, -h, -s, -d)
   * - Multi-tenant specific output including tenant information
   * - Tenant boundary detection and display
   * - Block-level analysis with v4 multi-tenant structure
   * - Key/value pair display with tenant context
   */
  @Test(timeout = TEST_TIMEOUT_MS)
  public void testComprehensiveV4Output() throws Exception {
    Path testFile = createV4HFile("hfile_comprehensive_v4", 90);
    
    // First, verify the created file is actually v4 format (version detection)
    try (HFile.Reader reader = HFile.createReader(fs, testFile, CacheConfig.DISABLED, true, conf)) {
      int majorVersion = reader.getTrailer().getMajorVersion();
      LOG.info("Detected HFile version: {} (v4 threshold: {})", majorVersion, HFile.MIN_FORMAT_VERSION_WITH_MULTI_TENANT);
      assertTrue("Test file should be v4", 
                  majorVersion == HFile.MIN_FORMAT_VERSION_WITH_MULTI_TENANT);
    }
    
    System.setOut(ps);
    HFilePrettyPrinter printer = new HFilePrettyPrinter(conf);
    
    LOG.info("=== COMPREHENSIVE HFILE V4 OUTPUT TEST ===");
    LOG.info("Testing file: {}", testFile);
    
    // Run with ALL possible options for comprehensive output
    printer.run(new String[] { 
      "-m",    // metadata
      "-p",    // print key/value pairs  
      "-v",    // verbose
      "-t",    // tenant info (v4 specific)
      "-b",    // block index
      "-h",    // block headers
      "-s",    // statistics/histograms
      "-d",    // detailed output
      "-f", testFile.toString() 
    });
    
    String comprehensiveResult = stream.toString();
    
    LOG.info("=== FULL HFILE V4 COMPREHENSIVE OUTPUT START ===");
    LOG.info("\n{}", comprehensiveResult);
    LOG.info("=== FULL HFILE V4 COMPREHENSIVE OUTPUT END ===");
    
    // Verify all expected sections are present
    assertTrue("Should contain trailer information", comprehensiveResult.contains("Trailer:"));
    assertTrue("Should contain file info", comprehensiveResult.contains("Fileinfo:"));
    assertTrue("Should contain v4-specific information", 
                comprehensiveResult.contains("HFile v4 Specific Information:"));
    assertTrue("Should contain tenant information", 
                comprehensiveResult.contains("Tenant Information:"));
    assertTrue("Should contain block index", comprehensiveResult.contains("Block Index:"));
    assertTrue("Should contain block headers", comprehensiveResult.contains("Block Headers:"));
    assertTrue("Should contain key/value pairs", comprehensiveResult.contains("K: "));
    assertTrue("Should contain tenant boundaries", 
                comprehensiveResult.contains("--- Start of tenant section:") || 
                comprehensiveResult.contains("Scanning multi-tenant HFile v4"));
    
    // Verify tenant-specific data is present
    for (String tenant : TENANTS) {
      assertTrue("Should contain data for tenant " + tenant, 
                 comprehensiveResult.contains(tenant + "row"));
    }
    
    LOG.info("Comprehensive V4 test completed successfully");
  }
} 