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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates multi-tenant configuration precedence and enforces that column-family settings are
 * ignored in favor of table/cluster scope.
 */
@Category(MediumTests.class)
public class TestMultiTenantConfigPrecedence {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMultiTenantConfigPrecedence.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestMultiTenantConfigPrecedence.class);

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final byte[] QUALIFIER = Bytes.toBytes("q");

  private static final int CLUSTER_PREFIX_LENGTH = 2;
  private static final int TABLE_PREFIX_LENGTH = 3;

  private static final TableName TABLE_DISABLE_OVERRIDE =
    TableName.valueOf("TestMultiTenantDisableOverride");
  private static final TableName TABLE_PREFIX_OVERRIDE =
    TableName.valueOf("TestMultiTenantPrefixOverride");
  private static final TableName TABLE_CLUSTER_FALLBACK =
    TableName.valueOf("TestMultiTenantClusterFallback");
  private static final TableName TABLE_INVALID_PREFIX =
    TableName.valueOf("TestMultiTenantInvalidPrefix");

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setInt(HFile.FORMAT_VERSION_KEY, HFile.MIN_FORMAT_VERSION_WITH_MULTI_TENANT);
    conf.setInt(MultiTenantHFileWriter.TENANT_PREFIX_LENGTH, CLUSTER_PREFIX_LENGTH);
    conf.set(MultiTenantHFileWriter.TABLE_MULTI_TENANT_ENABLED, "true");
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test(timeout = 120000)
  public void testTablePropertyDisablesMultiTenant() throws Exception {
    createTable(TABLE_DISABLE_OVERRIDE, "false", String.valueOf(TABLE_PREFIX_LENGTH),
      cfConfig(MultiTenantHFileWriter.TABLE_MULTI_TENANT_ENABLED, "true",
        MultiTenantHFileWriter.TABLE_TENANT_PREFIX_LENGTH, "5",
        MultiTenantHFileWriter.TENANT_PREFIX_LENGTH, "7"));

    writeTenantRows(TABLE_DISABLE_OVERRIDE, new String[] { "AAA", "BBB" }, 3);
    assertTenantConfiguration(TABLE_DISABLE_OVERRIDE, 0, 1);
  }

  @Test(timeout = 120000)
  public void testTablePropertyOverridesClusterPrefixLength() throws Exception {
    createTable(TABLE_PREFIX_OVERRIDE, "true", String.valueOf(TABLE_PREFIX_LENGTH),
      cfConfig(MultiTenantHFileWriter.TABLE_TENANT_PREFIX_LENGTH, "1",
        MultiTenantHFileWriter.TENANT_PREFIX_LENGTH, "1"));

    writeTenantRows(TABLE_PREFIX_OVERRIDE, new String[] { "T01", "T02", "T03" }, 2);
    assertTenantConfiguration(TABLE_PREFIX_OVERRIDE, TABLE_PREFIX_LENGTH, 3);
  }

  @Test(timeout = 120000)
  public void testClusterConfigUsedWhenTablePropertiesMissing() throws Exception {
    createTable(TABLE_CLUSTER_FALLBACK, null, null,
      cfConfig(MultiTenantHFileWriter.TABLE_MULTI_TENANT_ENABLED, "false",
        MultiTenantHFileWriter.TABLE_TENANT_PREFIX_LENGTH, "9",
        MultiTenantHFileWriter.TENANT_PREFIX_LENGTH, "9"));

    writeTenantRows(TABLE_CLUSTER_FALLBACK, new String[] { "AA", "BB" }, 3);
    assertTenantConfiguration(TABLE_CLUSTER_FALLBACK, CLUSTER_PREFIX_LENGTH, 2);
  }

  @Test(timeout = 120000)
  public void testInvalidTablePrefixLengthFallsBackToCluster() throws Exception {
    createTable(TABLE_INVALID_PREFIX, "true", "not-a-number",
      cfConfig(MultiTenantHFileWriter.TABLE_TENANT_PREFIX_LENGTH, "6",
        MultiTenantHFileWriter.TENANT_PREFIX_LENGTH, "6"));

    writeTenantRows(TABLE_INVALID_PREFIX, new String[] { "CC", "DD" }, 2);
    assertTenantConfiguration(TABLE_INVALID_PREFIX, CLUSTER_PREFIX_LENGTH, 2);
  }

  private static void createTable(TableName tableName, String tableMultiTenantEnabled,
    String tablePrefixLength, Map<String, String> cfConfiguration) throws IOException {
    try (Admin admin = TEST_UTIL.getAdmin()) {
      TableDescriptorBuilder tableBuilder = TableDescriptorBuilder.newBuilder(tableName);
      ColumnFamilyDescriptorBuilder familyBuilder =
        ColumnFamilyDescriptorBuilder.newBuilder(FAMILY);
      if (cfConfiguration != null) {
        for (Map.Entry<String, String> entry : cfConfiguration.entrySet()) {
          familyBuilder.setConfiguration(entry.getKey(), entry.getValue());
        }
      }
      tableBuilder.setColumnFamily(familyBuilder.build());
      if (tableMultiTenantEnabled != null) {
        tableBuilder.setValue(MultiTenantHFileWriter.TABLE_MULTI_TENANT_ENABLED,
          tableMultiTenantEnabled);
      }
      if (tablePrefixLength != null) {
        tableBuilder.setValue(MultiTenantHFileWriter.TABLE_TENANT_PREFIX_LENGTH, tablePrefixLength);
      }
      admin.createTable(tableBuilder.build());
    }
    try {
      TEST_UTIL.waitTableAvailable(tableName);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted waiting for table " + tableName, e);
    }
  }

  private static Map<String, String> cfConfig(String... entries) {
    Map<String, String> config = new HashMap<>();
    if (entries == null) {
      return config;
    }
    if (entries.length % 2 != 0) {
      throw new IllegalArgumentException("cfConfig requires key/value pairs");
    }
    for (int i = 0; i < entries.length; i += 2) {
      config.put(entries[i], entries[i + 1]);
    }
    return config;
  }

  private static void writeTenantRows(TableName tableName, String[] tenantPrefixes,
    int rowsPerTenant) throws IOException {
    List<Put> puts = new ArrayList<>();
    for (String tenantPrefix : tenantPrefixes) {
      for (int i = 0; i < rowsPerTenant; i++) {
        String rowKey = tenantPrefix + "_row_" + i;
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(FAMILY, QUALIFIER, Bytes.toBytes("v"));
        puts.add(put);
      }
    }
    try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
      table.put(puts);
    }
    TEST_UTIL.flush(tableName);
  }

  private static void assertTenantConfiguration(TableName tableName, int expectedPrefixLength,
    int expectedUniqueSections) throws Exception {
    List<Path> hfiles = waitForHFiles(tableName);
    assertFalse("No HFiles found for " + tableName, hfiles.isEmpty());

    Set<String> sectionIds = new HashSet<>();
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Configuration conf = TEST_UTIL.getConfiguration();
    CacheConfig cacheConfig = new CacheConfig(conf);
    for (Path hfile : hfiles) {
      try (HFile.Reader reader = HFile.createReader(fs, hfile, cacheConfig, true, conf)) {
        assertEquals("HFile should be version 4", HFile.MIN_FORMAT_VERSION_WITH_MULTI_TENANT,
          reader.getTrailer().getMajorVersion());
        assertTrue("Reader should be multi-tenant", reader instanceof AbstractMultiTenantReader);
        assertEquals("Tenant prefix length mismatch for " + hfile, expectedPrefixLength,
          reader.getTrailer().getTenantPrefixLength());

        byte[][] tenantSections = ((AbstractMultiTenantReader) reader).getAllTenantSectionIds();
        for (byte[] sectionId : tenantSections) {
          sectionIds.add(Bytes.toStringBinary(sectionId));
        }
      }
    }

    assertEquals("Unexpected unique tenant section count for " + tableName, expectedUniqueSections,
      sectionIds.size());
  }

  private static List<Path> waitForHFiles(TableName tableName) throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    Waiter.waitFor(conf, 30000, () -> !findHFiles(tableName).isEmpty());
    return findHFiles(tableName);
  }

  private static List<Path> findHFiles(TableName tableName) throws IOException {
    List<Path> hfiles = new ArrayList<>();
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path rootDir = CommonFSUtils.getRootDir(TEST_UTIL.getConfiguration());
    Path tableDir = CommonFSUtils.getTableDir(rootDir, tableName);
    if (!fs.exists(tableDir)) {
      return hfiles;
    }

    FileStatus[] regionDirs = fs.listStatus(tableDir);
    for (FileStatus regionDir : regionDirs) {
      if (!regionDir.isDirectory()) {
        continue;
      }
      Path familyDir = new Path(regionDir.getPath(), Bytes.toString(FAMILY));
      if (!fs.exists(familyDir)) {
        continue;
      }
      for (FileStatus hfile : fs.listStatus(familyDir)) {
        String name = hfile.getPath().getName();
        if (name.startsWith(".") || name.endsWith(".tmp")) {
          continue;
        }
        hfiles.add(hfile.getPath());
      }
    }
    LOG.info("Found {} HFiles for {}", hfiles.size(), tableName);
    return hfiles;
  }
}
