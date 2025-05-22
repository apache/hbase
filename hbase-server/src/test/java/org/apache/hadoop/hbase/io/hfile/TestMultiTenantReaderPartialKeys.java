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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the multi-tenant HFile reader with partial row keys.
 */
@Category(SmallTests.class)
public class TestMultiTenantReaderPartialKeys {
  private static final Logger LOG = LoggerFactory.getLogger(TestMultiTenantReaderPartialKeys.class);
  
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMultiTenantReaderPartialKeys.class);
  
  private Configuration conf;
  private FileSystem fs;
  private Path testDir;
  private CacheConfig cacheConf;
  
  private static final String FAMILY = "f";
  private static final String QUALIFIER = "q";
  private static final Path TEST_DATA_DIR = new Path("target/test/data");
  
  @Before
  public void setUp() throws IOException {
    conf = new Configuration();
    // Set up the multi-tenant configuration
    conf.setBoolean("hbase.hfile.multi.tenant", true);
    fs = FileSystem.get(conf);
    testDir = new Path(TEST_DATA_DIR, "multitenant-partial-keys");
    fs.mkdirs(testDir);
    cacheConf = new CacheConfig(conf);
  }
  
  @After
  public void tearDown() throws IOException {
    fs.delete(testDir, true);
  }
  
  /**
   * Test reading with partial row keys across multiple tenant sections.
   * 
   * @throws IOException if an error occurs during testing
   */
  @Test
  public void testPartialRowKeyScanning() throws IOException {
    // Create test data with multiple tenant sections
    List<ExtendedCell> cells = new ArrayList<>();
    
    // Tenant 1: row keys start with "t1:"
    cells.add(createCell("t1:row1", FAMILY, QUALIFIER, 1, "value1"));
    cells.add(createCell("t1:row2", FAMILY, QUALIFIER, 2, "value2"));
    cells.add(createCell("t1:row3", FAMILY, QUALIFIER, 3, "value3"));
    
    // Tenant 2: row keys start with "t2:"
    cells.add(createCell("t2:row1", FAMILY, QUALIFIER, 4, "value4"));
    cells.add(createCell("t2:row2", FAMILY, QUALIFIER, 5, "value5"));
    
    // Tenant 3: row keys start with "t3:"
    cells.add(createCell("t3:row1", FAMILY, QUALIFIER, 6, "value6"));
    cells.add(createCell("t3:row2", FAMILY, QUALIFIER, 7, "value7"));
    cells.add(createCell("t3:row3", FAMILY, QUALIFIER, 8, "value8"));
    
    // Write cells to an HFile
    Path hfilePath = new Path(testDir, "testMultiTenantPartialRows.hfile");
    
    // Create tenant-specific HFile context
    HFileContext context = new HFileContextBuilder()
        .withBlockSize(4096)
        .build();
    
    // Configure for tenant separation with 4-byte prefix
    Configuration writerConf = new Configuration(conf);
    writerConf.setInt("hbase.hfile.tenant.prefix.length", 4); // "t1:r", "t2:r", "t3:r"
    
    // Write the multi-tenant HFile
    try (HFile.Writer writer = HFile.getWriterFactory(writerConf, cacheConf)
        .withPath(fs, hfilePath)
        .withFileContext(context)
        .create()) {
      
      // Write cells in order
      for (ExtendedCell cell : cells) {
        writer.append(cell);
      }
    }
    
    // Now read with various partial keys
    
    // Case 1: Complete tenant prefix (should match exact tenant)
    testPartialKeyScanWithPrefix(hfilePath, "t1:r", 3);
    testPartialKeyScanWithPrefix(hfilePath, "t2:r", 2);
    testPartialKeyScanWithPrefix(hfilePath, "t3:r", 3);
    
    // Case 2: Partial tenant prefix (should match multiple tenants)
    testPartialKeyScanWithPrefix(hfilePath, "t", 8); // All cells
    
    // Case 3: Complete row key (should match exact row)
    testPartialKeyScanWithPrefix(hfilePath, "t1:row1", 1);
    testPartialKeyScanWithPrefix(hfilePath, "t2:row2", 1);
    testPartialKeyScanWithPrefix(hfilePath, "t3:row3", 1);
    
    // Case 4: Partial row within tenant (should match rows within tenant)
    testPartialKeyScanWithPrefix(hfilePath, "t1:row", 3); // All t1 rows
    testPartialKeyScanWithPrefix(hfilePath, "t2:row", 2); // All t2 rows
    testPartialKeyScanWithPrefix(hfilePath, "t3:row", 3); // All t3 rows
  }
  
  /**
   * Helper method to test scanning with a partial key prefix
   * 
   * @param hfilePath The path to the HFile
   * @param prefix The row key prefix to scan for
   * @param expectedCount The expected number of matching cells
   * @throws IOException if an error occurs during testing
   */
  private void testPartialKeyScanWithPrefix(Path hfilePath, String prefix, int expectedCount) 
      throws IOException {
    LOG.info("Testing partial key scan with prefix: {}", prefix);
    
    // Open the reader
    try (HFile.Reader reader = HFile.createReader(fs, hfilePath, cacheConf, true, conf)) {
      // Verify it's a multi-tenant reader
      assertTrue("Reader should be an AbstractMultiTenantReader", 
                 reader instanceof AbstractMultiTenantReader);
      
      AbstractMultiTenantReader mtReader = (AbstractMultiTenantReader) reader;
      
      // Get scanner for partial key
      byte[] partialKey = Bytes.toBytes(prefix);
      HFileScanner scanner = mtReader.getScannerForPartialKey(conf, true, true, false, partialKey);
      
      // Scan through cells and count matches
      int count = 0;
      boolean hasEntry = scanner.seekTo();
      
      if (hasEntry) {
        // Determine if this is a prefix scan or exact match
        boolean isExactRowKeyMatch = shouldTreatAsExactMatch(scanner, prefix);
        
        do {
          Cell cell = scanner.getCell();
          assertNotNull("Cell should not be null", cell);
          
          String rowStr = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
          
          if (isExactRowKeyMatch) {
            // For exact match, we want the exact key
            if (rowStr.equals(prefix)) {
              count++;
              break; // We found our exact match
            } else if (rowStr.compareTo(prefix) > 0) {
              // We've gone past where our key would be
              break;
            }
          } else {
            // For prefix match, check if the row starts with our prefix
            if (rowStr.startsWith(prefix)) {
              count++;
            } else if (rowStr.compareTo(prefix) > 0 && 
                       !rowStr.startsWith(prefix.substring(0, Math.min(prefix.length(), rowStr.length())))) {
              // If we've moved past all possible matches, we can stop
              break;
            }
          }
        } while (scanner.next());
      }
      
      // Verify count matches expected
      assertEquals("Number of cells matching prefix '" + prefix + "'", expectedCount, count);
    }
  }
  
  /**
   * Determine if a key should be treated as an exact match or prefix scan
   * 
   * @param scanner The scanner positioned at the first cell
   * @param prefix The key pattern being searched for
   * @return true if the key should be treated as an exact match, false for prefix scan
   * @throws IOException If an error occurs
   */
  private boolean shouldTreatAsExactMatch(HFileScanner scanner, String prefix) throws IOException {
    // If the scanner isn't positioned, we can't determine
    if (!scanner.isSeeked()) {
      return false;
    }
    
    // Get the first row to examine
    Cell firstCell = scanner.getCell();
    String firstRow = Bytes.toString(firstCell.getRowArray(), 
                                 firstCell.getRowOffset(), 
                                 firstCell.getRowLength());
    
    // Case 1: Keys with separators
    if (prefix.contains(":") || prefix.contains("-")) {
      // If it ends with a separator, it's definitely a prefix
      if (prefix.endsWith(":") || prefix.endsWith("-")) {
        return false;
      }
      
      // If the prefix matches the beginning of the first row but isn't exactly equal,
      // it's likely a prefix scan
      if (firstRow.startsWith(prefix) && !firstRow.equals(prefix)) {
        return false;
      }
      
      // Otherwise treat as exact match
      return true;
    }
    
    // Case 2: Short keys without separators (like "t")
    // If the key is short and matches the beginning of the first row, treat as prefix
    if (prefix.length() < 3 && firstRow.startsWith(prefix) && !firstRow.equals(prefix)) {
      return false;
    }
    
    // Default to exact match for everything else
    return true;
  }
  
  /**
   * Create a KeyValue cell for testing
   * 
   * @param row Row key
   * @param family Column family
   * @param qualifier Column qualifier
   * @param timestamp Timestamp
   * @param value Cell value
   * @return A KeyValue cell
   */
  private ExtendedCell createCell(String row, String family, String qualifier, 
                         long timestamp, String value) {
    return new KeyValue(
        Bytes.toBytes(row),
        Bytes.toBytes(family),
        Bytes.toBytes(qualifier),
        timestamp,
        Bytes.toBytes(value));
  }
} 