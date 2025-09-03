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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
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
 * Validates that multi-tenant HFile v4 builds and reads a multi-level section index.
 * This test forces a multi-level index by setting small chunk sizes and writing many tenants.
 */
@Category(MediumTests.class)
public class MultiTenantHFileMultiLevelIndexTest {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(MultiTenantHFileMultiLevelIndexTest.class);

  private static final Logger LOG = LoggerFactory.getLogger(MultiTenantHFileMultiLevelIndexTest.class);

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  private static final TableName TABLE_NAME = TableName.valueOf("TestMultiLevelIndex");
  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final byte[] QUALIFIER = Bytes.toBytes("q");

  private static final int TENANT_PREFIX_LENGTH = 3;
  // Force small chunking so we create multiple leaf blocks and an intermediate level.
  private static final int FORCED_MAX_CHUNK_SIZE = 3;  // entries per index block
  private static final int FORCED_MIN_INDEX_ENTRIES = 4; // root fanout threshold

  // Create enough tenants to exceed FORCED_MAX_CHUNK_SIZE and FORCED_MIN_INDEX_ENTRIES
  private static final int NUM_TENANTS = 20;
  private static final int ROWS_PER_TENANT = 2;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setInt(MultiTenantHFileWriter.TENANT_PREFIX_LENGTH, TENANT_PREFIX_LENGTH);
    conf.setInt(HFile.FORMAT_VERSION_KEY, HFile.MIN_FORMAT_VERSION_WITH_MULTI_TENANT);
    // Force multi-level section index with small blocks
    conf.setInt(SectionIndexManager.SECTION_INDEX_MAX_CHUNK_SIZE, FORCED_MAX_CHUNK_SIZE);
    conf.setInt(SectionIndexManager.SECTION_INDEX_MIN_NUM_ENTRIES, FORCED_MIN_INDEX_ENTRIES);
    TEST_UTIL.startMiniCluster(1);
    try (Admin admin = TEST_UTIL.getAdmin()) {
      TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(TABLE_NAME);
      tdb.setValue(MultiTenantHFileWriter.TABLE_TENANT_PREFIX_LENGTH, String.valueOf(TENANT_PREFIX_LENGTH));
      tdb.setValue(MultiTenantHFileWriter.TABLE_MULTI_TENANT_ENABLED, "true");
      tdb.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(FAMILY).build());
      admin.createTable(tdb.build());
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test(timeout = 120000)
  public void testMultiLevelSectionIndexTraversal() throws Exception {
    writeManyTenants();
    TEST_UTIL.flush(TABLE_NAME);

    // Wait a bit for files to land
    Thread.sleep(2000);

    List<Path> hfiles = findHFiles();
    assertTrue("Expected at least one HFile", !hfiles.isEmpty());

    int totalRows = 0;
    java.util.Set<String> uniqueSectionIds = new java.util.HashSet<>();
    for (Path p : hfiles) {
      try (HFile.Reader r = HFile.createReader(TEST_UTIL.getTestFileSystem(), p,
          new CacheConfig(TEST_UTIL.getConfiguration()), true, TEST_UTIL.getConfiguration())) {
        assertEquals("HFile should be version 4",
            HFile.MIN_FORMAT_VERSION_WITH_MULTI_TENANT,
            r.getTrailer().getMajorVersion());
        assertTrue("Reader should be multi-tenant", r instanceof AbstractMultiTenantReader);

        // Validate multi-level index per trailer
        int levels = r.getTrailer().getNumDataIndexLevels();
        LOG.info("HFile {} trailer reports section index levels: {}", p.getName(), levels);
        assertTrue("Expected multi-level section index (>=2 levels)", levels >= 2);

        byte[][] tenantSections = ((AbstractMultiTenantReader) r).getAllTenantSectionIds();
        for (byte[] id : tenantSections) {
          uniqueSectionIds.add(Bytes.toStringBinary(id));
        }

        // Scan all data via the multi-tenant reader to ensure traversal works across levels
        HFileScanner scanner = ((AbstractMultiTenantReader) r)
            .getScanner(TEST_UTIL.getConfiguration(), false, false);
        int rowsInThisFile = 0;
        if (scanner.seekTo()) {
          do {
            rowsInThisFile++;
          } while (scanner.next());
        }
        LOG.info("HFile {} contains {} cells", p.getName(), rowsInThisFile);
        totalRows += rowsInThisFile;
      }
    }

    assertEquals("Unique tenant sections across all files should equal tenants",
        NUM_TENANTS, uniqueSectionIds.size());

    assertEquals("Total cells should match expected write count",
        NUM_TENANTS * ROWS_PER_TENANT, totalRows);
  }

  private static void writeManyTenants() throws IOException {
    try (Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
         Table table = conn.getTable(TABLE_NAME)) {
      List<Put> puts = new ArrayList<>();
      for (int t = 0; t < NUM_TENANTS; t++) {
        String tenant = String.format("T%02d", t); // e.g., T00, T01 ... T19
        String tenantPrefix = tenant; // length 3 when combined with another char? Ensure 3 chars
        // Ensure prefix is exactly TENANT_PREFIX_LENGTH by padding if needed
        if (tenantPrefix.length() < TENANT_PREFIX_LENGTH) {
          tenantPrefix = String.format("%-3s", tenantPrefix).replace(' ', 'X');
        } else if (tenantPrefix.length() > TENANT_PREFIX_LENGTH) {
          tenantPrefix = tenantPrefix.substring(0, TENANT_PREFIX_LENGTH);
        }
        for (int i = 0; i < ROWS_PER_TENANT; i++) {
          String row = tenantPrefix + "row" + String.format("%03d", i);
          Put p = new Put(Bytes.toBytes(row));
          p.addColumn(FAMILY, QUALIFIER, Bytes.toBytes("v-" + tenantPrefix + "-" + i));
          puts.add(p);
        }
      }
      table.put(puts);
    }
  }

  private static List<Path> findHFiles() throws IOException {
    List<Path> hfiles = new ArrayList<>();
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path rootDir = TEST_UTIL.getDataTestDirOnTestFS();
    Path tableDir = new Path(rootDir, "data/default/" + TABLE_NAME.getNameAsString());
    if (!fs.exists(tableDir)) return hfiles;
    for (FileStatus regionDir : fs.listStatus(tableDir)) {
      if (!regionDir.isDirectory() || regionDir.getPath().getName().startsWith(".")) continue;
      Path familyDir = new Path(regionDir.getPath(), Bytes.toString(FAMILY));
      if (!fs.exists(familyDir)) continue;
      for (FileStatus hfile : fs.listStatus(familyDir)) {
        if (!hfile.getPath().getName().startsWith(".") && !hfile.getPath().getName().endsWith(".tmp")) {
          hfiles.add(hfile.getPath());
        }
      }
    }
    return hfiles;
  }
}


