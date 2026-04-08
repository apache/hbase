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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.BlockType;
import org.apache.hadoop.hbase.io.hfile.BloomFilterMetrics;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.io.hfile.MultiTenantBloomSupport;
import org.apache.hadoop.hbase.io.hfile.MultiTenantHFileWriter;
import org.apache.hadoop.hbase.io.hfile.ReaderContextBuilder;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.BloomFilterUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests bloom filter correctness for HFile v4 multi-tenant files with multiple tenant sections.
 * Covers all bloom filter types: ROW, ROWCOL, and ROWPREFIX_FIXED_LENGTH. Validates that:
 * <ul>
 * <li>Present keys in each tenant section pass the bloom filter</li>
 * <li>Absent keys in each tenant section are filtered out</li>
 * <li>Keys belonging to one tenant do not pass bloom checks routed through another tenant's
 * section</li>
 * <li>Both the per-section (MultiTenantBloomSupport) and StoreFileReader paths agree</li>
 * </ul>
 */
@Category(MediumTests.class)
public class TestMultiTenantBloomFilterMultiSection {
  private static final Logger LOG =
    LoggerFactory.getLogger(TestMultiTenantBloomFilterMultiSection.class);

  private static final int NUM_TENANTS = 10;
  private static final int TENANT_PREFIX_LENGTH = 4;
  private static final int ROWS_PER_TENANT = 50;
  private static final byte[] CF = Bytes.toBytes("cf");
  private static final byte[] QUALIFIER = Bytes.toBytes("q");

  private Configuration conf;
  private FileSystem fs;
  private Path baseDir;
  private CacheConfig cacheConfig;

  @Before
  public void setUp() throws Exception {
    conf = HBaseConfiguration.create();
    conf.setInt(HFile.FORMAT_VERSION_KEY, HFile.MIN_FORMAT_VERSION_WITH_MULTI_TENANT);
    conf.setBoolean(MultiTenantHFileWriter.TABLE_MULTI_TENANT_ENABLED, true);
    fs = FileSystem.getLocal(conf);
    baseDir = new Path(Files.createTempDirectory("mt-bloom-multi-section").toUri());
    cacheConfig = new CacheConfig(conf);
  }

  @After
  public void tearDown() throws Exception {
    if (fs != null && baseDir != null) {
      fs.delete(baseDir, true);
    }
  }

  /** Generate a 4-character tenant prefix like "t00_", "t01_", ... "t09_". */
  private static String tenantPrefix(int tenantIndex) {
    return String.format("t%02d_", tenantIndex);
  }

  /** Generate a row key: {tenantPrefix}{rowSuffix} */
  private static byte[] rowKey(int tenantIndex, int rowIndex) {
    return Bytes.toBytes(tenantPrefix(tenantIndex) + String.format("row%04d", rowIndex));
  }

  /** Generate a row key guaranteed absent from any tenant section. */
  private static byte[] absentRowKey(int tenantIndex) {
    return Bytes.toBytes(tenantPrefix(tenantIndex) + "zzz_absent");
  }

  /** Generate a row key with a prefix that matches no tenant. */
  private static byte[] unknownTenantRowKey() {
    return Bytes.toBytes("zzzz_no_such_tenant");
  }

  private Map<String, String> createTableProps(String bloomType) {
    Map<String, String> props = new HashMap<>();
    props.put(MultiTenantHFileWriter.TABLE_MULTI_TENANT_ENABLED, "true");
    props.put(MultiTenantHFileWriter.TABLE_TENANT_PREFIX_LENGTH,
      String.valueOf(TENANT_PREFIX_LENGTH));
    props.put("BLOOMFILTER", bloomType);
    return props;
  }

  private Path writeMultiTenantHFile(BloomType bloomType, Map<String, String> tableProps)
    throws IOException {
    Path file = StoreFileWriter.getUniqueFile(fs, baseDir);
    HFileContext context = new HFileContextBuilder().withBlockSize(4096).withColumnFamily(CF)
      .withTableName(Bytes.toBytes("test_table")).build();
    MultiTenantHFileWriter writer = MultiTenantHFileWriter.create(fs, file, conf, cacheConfig,
      tableProps, context, bloomType, bloomType, null, true);
    long ts = EnvironmentEdgeManager.currentTime();
    for (int t = 0; t < NUM_TENANTS; t++) {
      for (int r = 0; r < ROWS_PER_TENANT; r++) {
        byte[] row = rowKey(t, r);
        KeyValue kv = new KeyValue(row, CF, QUALIFIER, ts, Bytes.toBytes("val"));
        writer.append(kv);
      }
    }
    writer.close();
    return file;
  }

  private StoreFileReader openReader(Path file) throws IOException {
    var readerContext = new ReaderContextBuilder().withFileSystemAndPath(fs, file).build();
    StoreFileInfo storeFileInfo = StoreFileInfo.createStoreFileInfoForHFile(conf, fs, file, true);
    storeFileInfo.initHFileInfo(readerContext);
    StoreFileReader reader = storeFileInfo.createReader(readerContext, cacheConfig);
    storeFileInfo.getHFileInfo().initMetaAndIndex(reader.getHFileReader());
    reader.loadFileInfo();
    reader.loadBloomfilter(BlockType.GENERAL_BLOOM_META, new BloomFilterMetrics());
    reader.loadBloomfilter(BlockType.DELETE_FAMILY_BLOOM_META, new BloomFilterMetrics());
    return reader;
  }

  @Test
  public void testRowBloomWithMultipleTenants() throws Exception {
    Map<String, String> tableProps = createTableProps("ROW");
    Path file = writeMultiTenantHFile(BloomType.ROW, tableProps);
    StoreFileReader reader = openReader(file);
    try {
      HFile.Reader hfileReader = reader.getHFileReader();
      assertTrue("v4 reader must implement MultiTenantBloomSupport",
        hfileReader instanceof MultiTenantBloomSupport);
      MultiTenantBloomSupport bloomSupport = (MultiTenantBloomSupport) hfileReader;

      LOG.info("ROW bloom: verifying {} tenants x {} rows", NUM_TENANTS, ROWS_PER_TENANT);
      for (int t = 0; t < NUM_TENANTS; t++) {
        for (int r = 0; r < ROWS_PER_TENANT; r++) {
          byte[] row = rowKey(t, r);
          assertTrue("ROW bloom must pass for present key tenant=" + t + " row=" + r,
            bloomSupport.passesGeneralRowBloomFilter(row, 0, row.length));

          Scan getScan = new Scan(new Get(row));
          assertTrue("StoreFileReader.passesBloomFilter must pass for present key",
            reader.passesBloomFilter(getScan, null));
        }

        byte[] absent = absentRowKey(t);
        boolean directResult = bloomSupport.passesGeneralRowBloomFilter(absent, 0, absent.length);
        Scan absentScan = new Scan(new Get(absent));
        boolean storeReaderResult = reader.passesBloomFilter(absentScan, null);
        LOG.info("ROW bloom absent key tenant={}: direct={} storeReader={}", t, directResult,
          storeReaderResult);
        // Both paths must agree (both should filter the absent key)
        assertFalse("ROW bloom should filter absent key in tenant " + t, directResult);
        assertFalse("StoreFileReader should filter absent key in tenant " + t, storeReaderResult);
      }

      byte[] unknownRow = unknownTenantRowKey();
      assertFalse("ROW bloom should filter key with unknown tenant prefix",
        bloomSupport.passesGeneralRowBloomFilter(unknownRow, 0, unknownRow.length));
    } finally {
      reader.close(false);
    }
  }

  @Test
  public void testRowColBloomWithMultipleTenants() throws Exception {
    Map<String, String> tableProps = createTableProps("ROWCOL");
    Path file = writeMultiTenantHFile(BloomType.ROWCOL, tableProps);
    StoreFileReader reader = openReader(file);
    try {
      HFile.Reader hfileReader = reader.getHFileReader();
      assertTrue(hfileReader instanceof MultiTenantBloomSupport);
      MultiTenantBloomSupport bloomSupport = (MultiTenantBloomSupport) hfileReader;

      LOG.info("ROWCOL bloom: verifying {} tenants x {} rows", NUM_TENANTS, ROWS_PER_TENANT);
      for (int t = 0; t < NUM_TENANTS; t++) {
        for (int r = 0; r < ROWS_PER_TENANT; r++) {
          byte[] row = rowKey(t, r);
          ExtendedCell presentCell =
            PrivateCellUtil.createFirstOnRow(row, HConstants.EMPTY_BYTE_ARRAY, QUALIFIER);
          assertTrue("ROWCOL bloom must pass for present cell tenant=" + t + " row=" + r,
            bloomSupport.passesGeneralRowColBloomFilter(presentCell));

          TreeSet<byte[]> columns = new TreeSet<>(Bytes.BYTES_COMPARATOR);
          columns.add(QUALIFIER);
          Scan getScan = new Scan(new Get(row));
          assertTrue("StoreFileReader ROWCOL must pass for present key",
            reader.passesBloomFilter(getScan, columns));
        }

        byte[] absent = absentRowKey(t);
        ExtendedCell absentCell =
          PrivateCellUtil.createFirstOnRow(absent, HConstants.EMPTY_BYTE_ARRAY, QUALIFIER);
        boolean directResult = bloomSupport.passesGeneralRowColBloomFilter(absentCell);
        TreeSet<byte[]> columns = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        columns.add(QUALIFIER);
        Scan absentScan = new Scan(new Get(absent));
        boolean storeReaderResult = reader.passesBloomFilter(absentScan, columns);
        LOG.info("ROWCOL bloom absent key tenant={}: direct={} storeReader={}", t, directResult,
          storeReaderResult);
        assertFalse("ROWCOL bloom should filter absent cell in tenant " + t, directResult);
        assertFalse("StoreFileReader ROWCOL should filter absent cell in tenant " + t,
          storeReaderResult);
      }

      byte[] unknownRow = unknownTenantRowKey();
      ExtendedCell unknownCell =
        PrivateCellUtil.createFirstOnRow(unknownRow, HConstants.EMPTY_BYTE_ARRAY, QUALIFIER);
      assertFalse("ROWCOL bloom should filter cell with unknown tenant prefix",
        bloomSupport.passesGeneralRowColBloomFilter(unknownCell));
    } finally {
      reader.close(false);
    }
  }

  @Test
  public void testRowPrefixBloomWithMultipleTenants() throws Exception {
    int bloomPrefixLength = TENANT_PREFIX_LENGTH + 3; // "t00_row" = 7 chars
    conf.set(BloomFilterUtil.PREFIX_LENGTH_KEY, String.valueOf(bloomPrefixLength));
    Map<String, String> tableProps = createTableProps("ROWPREFIX_FIXED_LENGTH");
    Path file = writeMultiTenantHFile(BloomType.ROWPREFIX_FIXED_LENGTH, tableProps);
    StoreFileReader reader = openReader(file);
    try {
      HFile.Reader hfileReader = reader.getHFileReader();
      assertTrue(hfileReader instanceof MultiTenantBloomSupport);
      MultiTenantBloomSupport bloomSupport = (MultiTenantBloomSupport) hfileReader;

      LOG.info("ROWPREFIX bloom (prefixLen={}): verifying {} tenants", bloomPrefixLength,
        NUM_TENANTS);
      for (int t = 0; t < NUM_TENANTS; t++) {
        byte[] presentRow = rowKey(t, 0);
        byte[] prefix = Bytes.copy(presentRow, 0, Math.min(bloomPrefixLength, presentRow.length));
        assertTrue("ROWPREFIX bloom must pass for present prefix tenant=" + t,
          bloomSupport.passesGeneralRowPrefixBloomFilter(prefix, 0, prefix.length));

        Scan getScan = new Scan(new Get(presentRow));
        assertTrue("StoreFileReader ROWPREFIX must pass for present key",
          reader.passesBloomFilter(getScan, null));
      }

      for (int t = 0; t < NUM_TENANTS; t++) {
        byte[] absentRow = Bytes.toBytes(tenantPrefix(t) + "zzz_absent");
        byte[] absentPrefix =
          Bytes.copy(absentRow, 0, Math.min(bloomPrefixLength, absentRow.length));
        boolean directResult =
          bloomSupport.passesGeneralRowPrefixBloomFilter(absentPrefix, 0, absentPrefix.length);
        Scan absentScan = new Scan(new Get(absentRow));
        boolean storeReaderResult = reader.passesBloomFilter(absentScan, null);
        LOG.info("ROWPREFIX bloom absent key tenant={}: direct={} storeReader={}", t, directResult,
          storeReaderResult);
        assertFalse("ROWPREFIX bloom should filter absent prefix in tenant " + t, directResult);
        assertFalse("StoreFileReader ROWPREFIX should filter absent prefix in tenant " + t,
          storeReaderResult);
      }

      byte[] unknownRow = unknownTenantRowKey();
      byte[] unknownPrefix =
        Bytes.copy(unknownRow, 0, Math.min(bloomPrefixLength, unknownRow.length));
      assertFalse("ROWPREFIX bloom should filter prefix with unknown tenant",
        bloomSupport.passesGeneralRowPrefixBloomFilter(unknownPrefix, 0, unknownPrefix.length));
    } finally {
      reader.close(false);
    }
  }

  @Test
  public void testCrossTenantIsolation() throws Exception {
    Map<String, String> tableProps = createTableProps("ROW");
    Path file = writeMultiTenantHFile(BloomType.ROW, tableProps);
    StoreFileReader reader = openReader(file);
    try {
      MultiTenantBloomSupport bloomSupport = (MultiTenantBloomSupport) reader.getHFileReader();

      LOG.info("Cross-tenant isolation: verifying keys unique to one tenant "
        + "are not found via another tenant's bloom section");
      for (int t = 0; t < NUM_TENANTS; t++) {
        byte[] presentInT = rowKey(t, 0);
        assertTrue("Key must pass bloom for its own tenant section",
          bloomSupport.passesGeneralRowBloomFilter(presentInT, 0, presentInT.length));
      }

      // A row prefix from tenant 0 with a suffix that only tenant 5 has
      byte[] crossRow = Bytes.toBytes(tenantPrefix(0) + "row_only_in_t5");
      assertFalse("Cross-tenant key must be filtered by bloom",
        bloomSupport.passesGeneralRowBloomFilter(crossRow, 0, crossRow.length));
    } finally {
      reader.close(false);
    }
  }

  @Test
  public void testMultiSectionBloomNotCachedLocally() throws Exception {
    Map<String, String> tableProps = createTableProps("ROW");
    Path file = writeMultiTenantHFile(BloomType.ROW, tableProps);
    StoreFileReader reader = openReader(file);
    try {
      assertNull("Multi-section v4 file must NOT have a locally cached generalBloomFilter "
        + "(per-section routing required for correctness)", reader.getGeneralBloomFilter());
    } finally {
      reader.close(false);
    }
  }

  /**
   * Validates the StoreFileReader fast path: for a single-section v4 file the bloom filter is
   * cached locally in {@code generalBloomFilter}, avoiding the per-call multi-tenant overhead.
   */
  @Test
  public void testSingleSectionBloomCachedLocally() throws Exception {
    Configuration singleConf = HBaseConfiguration.create(conf);
    singleConf.setBoolean(MultiTenantHFileWriter.TABLE_MULTI_TENANT_ENABLED, false);

    Path file = StoreFileWriter.getUniqueFile(fs, baseDir);
    HFileContext context = new HFileContextBuilder().withBlockSize(4096).withColumnFamily(CF)
      .withTableName(Bytes.toBytes("single_tenant")).build();

    Map<String, String> singleProps = new HashMap<>();
    singleProps.put(MultiTenantHFileWriter.TABLE_MULTI_TENANT_ENABLED, "false");
    singleProps.put("BLOOMFILTER", "ROW");

    MultiTenantHFileWriter writer = MultiTenantHFileWriter.create(fs, file, singleConf, cacheConfig,
      singleProps, context, BloomType.ROW, BloomType.ROW, null, true);
    long ts = EnvironmentEdgeManager.currentTime();
    for (int r = 0; r < 100; r++) {
      byte[] row = Bytes.toBytes(String.format("row%04d", r));
      writer.append(new KeyValue(row, CF, QUALIFIER, ts, Bytes.toBytes("val")));
    }
    writer.close();

    StoreFileReader reader = openReader(file);
    try {
      assertTrue(reader.getHFileReader() instanceof MultiTenantBloomSupport);
      assertNotNull("Single-section v4 file MUST have bloom cached locally for fast path",
        reader.getGeneralBloomFilter());

      byte[] present = Bytes.toBytes("row0050");
      Scan getScan = new Scan(new Get(present));
      assertTrue("Local bloom must pass for present key", reader.passesBloomFilter(getScan, null));

      byte[] absent = Bytes.toBytes("row9999");
      Scan absentScan = new Scan(new Get(absent));
      assertFalse("Local bloom must filter absent key", reader.passesBloomFilter(absentScan, null));

      byte[] afterLast = Bytes.toBytes("zzz_after_all_keys");
      Scan afterLastScan = new Scan(new Get(afterLast));
      assertFalse("Local bloom must filter key beyond lastBloomKey",
        reader.passesBloomFilter(afterLastScan, null));
    } finally {
      reader.close(false);
    }
  }

  /** ROW and ROWCOL blooms must bypass (return true) for non-get scans. */
  @Test
  public void testNonGetScanBypassesRowAndRowColBloom() throws Exception {
    Map<String, String> tableProps = createTableProps("ROW");
    Path rowFile = writeMultiTenantHFile(BloomType.ROW, tableProps);
    StoreFileReader rowReader = openReader(rowFile);
    try {
      byte[] absent = absentRowKey(0);
      Scan rangeScan = new Scan().withStartRow(absent).withStopRow(Bytes.toBytes("t01_"));
      assertTrue("ROW bloom must be bypassed for non-get scans",
        rowReader.passesBloomFilter(rangeScan, null));
    } finally {
      rowReader.close(false);
    }

    Map<String, String> rowColProps = createTableProps("ROWCOL");
    Path rowColFile = writeMultiTenantHFile(BloomType.ROWCOL, rowColProps);
    StoreFileReader rowColReader = openReader(rowColFile);
    try {
      byte[] absent = absentRowKey(0);
      Scan rangeScan = new Scan().withStartRow(absent).withStopRow(Bytes.toBytes("t01_"));
      TreeSet<byte[]> columns = new TreeSet<>(Bytes.BYTES_COMPARATOR);
      columns.add(QUALIFIER);
      assertTrue("ROWCOL bloom must be bypassed for non-get scans",
        rowColReader.passesBloomFilter(rangeScan, columns));
    } finally {
      rowColReader.close(false);
    }
  }

  /** ROWCOL bloom should filter when the row exists but the qualifier does not. */
  @Test
  public void testRowColBloomFiltersAbsentQualifier() throws Exception {
    Map<String, String> tableProps = createTableProps("ROWCOL");
    Path file = writeMultiTenantHFile(BloomType.ROWCOL, tableProps);
    StoreFileReader reader = openReader(file);
    try {
      MultiTenantBloomSupport bloomSupport = (MultiTenantBloomSupport) reader.getHFileReader();
      byte[] presentRow = rowKey(3, 10);
      byte[] absentQualifier = Bytes.toBytes("no_such_qualifier");

      ExtendedCell absentCell =
        PrivateCellUtil.createFirstOnRow(presentRow, HConstants.EMPTY_BYTE_ARRAY, absentQualifier);
      assertFalse("ROWCOL bloom must filter present row with absent qualifier",
        bloomSupport.passesGeneralRowColBloomFilter(absentCell));

      TreeSet<byte[]> columns = new TreeSet<>(Bytes.BYTES_COMPARATOR);
      columns.add(absentQualifier);
      Scan getScan = new Scan(new Get(presentRow));
      assertFalse("StoreFileReader ROWCOL must filter present row with absent qualifier",
        reader.passesBloomFilter(getScan, columns));
    } finally {
      reader.close(false);
    }
  }

  /** ROWCOL passesBloomFilter returns true when columns is null or has more than 1 column. */
  @Test
  public void testRowColBloomBypassesMultiColumnAndNullColumns() throws Exception {
    Map<String, String> tableProps = createTableProps("ROWCOL");
    Path file = writeMultiTenantHFile(BloomType.ROWCOL, tableProps);
    StoreFileReader reader = openReader(file);
    try {
      byte[] absent = absentRowKey(0);
      Scan getScan = new Scan(new Get(absent));

      assertTrue("ROWCOL bloom must bypass when columns is null",
        reader.passesBloomFilter(getScan, null));

      TreeSet<byte[]> multiCols = new TreeSet<>(Bytes.BYTES_COMPARATOR);
      multiCols.add(Bytes.toBytes("col1"));
      multiCols.add(Bytes.toBytes("col2"));
      assertTrue("ROWCOL bloom must bypass when columns.size() > 1",
        reader.passesBloomFilter(getScan, multiCols));
    } finally {
      reader.close(false);
    }
  }

  /**
   * Row keys shorter than the tenant prefix length cannot be routed to a section.
   * {@code extractTenantSectionId} returns null, so bloom should pass through (return true).
   */
  @Test
  public void testShortRowKeyPassesThroughBloom() throws Exception {
    Map<String, String> tableProps = createTableProps("ROW");
    Path file = writeMultiTenantHFile(BloomType.ROW, tableProps);
    StoreFileReader reader = openReader(file);
    try {
      MultiTenantBloomSupport bloomSupport = (MultiTenantBloomSupport) reader.getHFileReader();

      byte[] shortRow = Bytes.toBytes("ab");
      assertTrue("Row shorter than prefix length must pass through (ROW)",
        bloomSupport.passesGeneralRowBloomFilter(shortRow, 0, shortRow.length));

      byte[] emptyRow = new byte[0];
      assertTrue("Empty row must pass through (ROW)",
        bloomSupport.passesGeneralRowBloomFilter(emptyRow, 0, emptyRow.length));
    } finally {
      reader.close(false);
    }
  }

  /** ROWPREFIX bloom does NOT bypass for range scans — it checks prefix overlap. */
  @Test
  public void testRowPrefixBloomWithRangeScan() throws Exception {
    int bloomPrefixLength = TENANT_PREFIX_LENGTH + 3;
    conf.set(BloomFilterUtil.PREFIX_LENGTH_KEY, String.valueOf(bloomPrefixLength));
    Map<String, String> tableProps = createTableProps("ROWPREFIX_FIXED_LENGTH");
    Path file = writeMultiTenantHFile(BloomType.ROWPREFIX_FIXED_LENGTH, tableProps);
    StoreFileReader reader = openReader(file);
    try {
      byte[] startRow = rowKey(2, 0);
      byte[] stopRow = rowKey(2, 10);
      Scan rangeScan = new Scan().withStartRow(startRow).withStopRow(stopRow);
      assertTrue("ROWPREFIX bloom must pass for range scan within existing prefix",
        reader.passesBloomFilter(rangeScan, null));

      byte[] absentStart = Bytes.toBytes(tenantPrefix(2) + "zzz0000");
      byte[] absentStop = Bytes.toBytes(tenantPrefix(2) + "zzz9999");
      Scan absentRange = new Scan().withStartRow(absentStart).withStopRow(absentStop);
      assertFalse("ROWPREFIX bloom must filter range scan for absent prefix",
        reader.passesBloomFilter(absentRange, null));

      byte[] t0Start = Bytes.toBytes(tenantPrefix(0) + "row0000");
      byte[] t5Stop = Bytes.toBytes(tenantPrefix(5) + "row0000");
      Scan crossTenantScan = new Scan().withStartRow(t0Start).withStopRow(t5Stop);
      assertTrue(
        "ROWPREFIX bloom must bypass when start/stop prefix diverges before " + "bloomPrefixLength",
        reader.passesBloomFilter(crossTenantScan, null));
    } finally {
      reader.close(false);
    }
  }

  /** Validates delete-family bloom filter with multi-section routing. */
  @Test
  public void testDeleteFamilyBloomWithMultipleTenants() throws Exception {
    Map<String, String> tableProps = createTableProps("ROW");
    Path file = StoreFileWriter.getUniqueFile(fs, baseDir);
    HFileContext context = new HFileContextBuilder().withBlockSize(4096).withColumnFamily(CF)
      .withTableName(Bytes.toBytes("test_delete_bloom")).build();
    MultiTenantHFileWriter writer = MultiTenantHFileWriter.create(fs, file, conf, cacheConfig,
      tableProps, context, BloomType.ROW, BloomType.ROW, null, true);

    long ts = EnvironmentEdgeManager.currentTime();
    for (int t = 0; t < NUM_TENANTS; t++) {
      for (int r = 0; r < 10; r++) {
        byte[] row = rowKey(t, r);
        if (r == 5) {
          writer.append(new KeyValue(row, CF, null, ts, KeyValue.Type.DeleteFamily));
        }
        writer.append(new KeyValue(row, CF, QUALIFIER, ts, Bytes.toBytes("val")));
      }
    }
    writer.close();

    StoreFileReader reader = openReader(file);
    try {
      MultiTenantBloomSupport bloomSupport = (MultiTenantBloomSupport) reader.getHFileReader();
      for (int t = 0; t < NUM_TENANTS; t++) {
        byte[] deletedRow = rowKey(t, 5);
        assertTrue("Delete family bloom must pass for row with delete marker in tenant " + t,
          bloomSupport.passesDeleteFamilyBloomFilter(deletedRow, 0, deletedRow.length));

        assertTrue("StoreFileReader delete bloom must pass for row with delete marker",
          reader.passesDeleteFamilyBloomFilter(deletedRow, 0, deletedRow.length));
      }
    } finally {
      reader.close(false);
    }
  }
}
