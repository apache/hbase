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
package org.apache.hadoop.hbase.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for MultiTenantFSDataInputStreamWrapper position translation.
 */
@Category({IOTests.class, SmallTests.class})
public class MultiTenantFSDataInputStreamWrapperTest {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(MultiTenantFSDataInputStreamWrapperTest.class);

  private static final Logger LOG = LoggerFactory.getLogger(MultiTenantFSDataInputStreamWrapperTest.class);
  
  private HBaseTestingUtil testUtil;
  private FileSystem fs;
  private Path testFile;
  
  @Before
  public void setUp() throws IOException {
    testUtil = new HBaseTestingUtil();
    Configuration conf = testUtil.getConfiguration();
    fs = FileSystem.get(conf);
    testFile = new Path(testUtil.getDataTestDir(), "test-position-translation.dat");
  }
  
  @After 
  public void tearDown() throws IOException {
    if (fs != null && testFile != null) {
      fs.delete(testFile, false);
    }
  }
  
  /**
   * Test basic position translation functionality.
   */
  @Test
  public void testPositionTranslation() throws IOException {
    // Create a test file with known content
    String section1Data = "SECTION1DATA";
    String section2Data = "SECTION2DATA";
    
    try (FSDataOutputStream out = fs.create(testFile)) {
      out.writeBytes(section1Data);
      out.writeBytes(section2Data);
    }
    
    // Create wrapper for section 2 (starts at offset 12)
    long section2Offset = section1Data.length();
    FSDataInputStream baseStream = fs.open(testFile);
    FSDataInputStreamWrapper baseWrapper = new FSDataInputStreamWrapper(baseStream);
    MultiTenantFSDataInputStreamWrapper sectionWrapper = 
        new MultiTenantFSDataInputStreamWrapper(baseWrapper, section2Offset);
    
    // Test position translation
    assertEquals("Relative position 0 should map to section offset", 
                section2Offset, sectionWrapper.toAbsolutePosition(0));
    assertEquals("Relative position 5 should map to section offset + 5", 
                section2Offset + 5, sectionWrapper.toAbsolutePosition(5));
    
    assertEquals("Absolute position should map back to relative 0", 
                0, sectionWrapper.toRelativePosition(section2Offset));
    assertEquals("Absolute position should map back to relative 5", 
                5, sectionWrapper.toRelativePosition(section2Offset + 5));
    
    // Test stream operations
    FSDataInputStream sectionStream = sectionWrapper.getStream(false);
    assertNotNull("Section stream should not be null", sectionStream);
    
    // Seek to start of section (relative position 0)
    sectionStream.seek(0);
    assertEquals("Should be at relative position 0", 0, sectionStream.getPos());
    
    // Read some data
    byte[] buffer = new byte[8];
    int bytesRead = sectionStream.read(buffer);
    assertEquals("Should read 8 bytes", 8, bytesRead);
    assertEquals("Should read section 2 data", "SECTION2", new String(buffer));
    
    // Verify position after read
    assertEquals("Position should be at relative 8", 8, sectionStream.getPos());
    
    baseStream.close();
    LOG.info("Position translation test completed successfully");
  }
  
  /**
   * Test positional read functionality.
   */
  @Test
  public void testPositionalRead() throws IOException {
    // Create test data
    String testData = "0123456789ABCDEFGHIJ";
    try (FSDataOutputStream out = fs.create(testFile)) {
      out.writeBytes(testData);
    }
    
    // Create wrapper for section starting at offset 10
    long sectionOffset = 10;
    FSDataInputStream baseStream = fs.open(testFile);
    FSDataInputStreamWrapper baseWrapper = new FSDataInputStreamWrapper(baseStream);
    MultiTenantFSDataInputStreamWrapper sectionWrapper = 
        new MultiTenantFSDataInputStreamWrapper(baseWrapper, sectionOffset);
    
    FSDataInputStream sectionStream = sectionWrapper.getStream(false);
    
    // Test positional read at relative position 0 (should read 'A')
    byte[] buffer = new byte[1];
    int bytesRead = sectionStream.read(0, buffer, 0, 1);
    assertEquals("Should read 1 byte", 1, bytesRead);
    assertEquals("Should read 'A' (char at absolute position 10)", 'A', (char)buffer[0]);
    
    // Test positional read at relative position 5 (should read 'F')
    bytesRead = sectionStream.read(5, buffer, 0, 1);
    assertEquals("Should read 1 byte", 1, bytesRead);
    assertEquals("Should read 'F' (char at absolute position 15)", 'F', (char)buffer[0]);
    
    baseStream.close();
    LOG.info("Positional read test completed successfully");
  }
} 