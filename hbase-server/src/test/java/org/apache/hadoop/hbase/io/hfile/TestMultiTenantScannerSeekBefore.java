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
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests for {@link AbstractMultiTenantReader.MultiTenantScanner#seekBefore} cross-section fallback.
 * <p>
 * When seekBefore targets a key at or before the first key of section N, the scanner must fall back
 * to the last key of section N-1. Without this fallback, reverse scans and seekToPreviousRow miss
 * data at tenant section boundaries.
 */
@Category(SmallTests.class)
public class TestMultiTenantScannerSeekBefore {

  private static final byte[] CF = Bytes.toBytes("cf");
  private static final byte[] QUAL = Bytes.toBytes("q");

  /**
   * Writes a multi-tenant V4 HFile with two tenant sections ("aa" and "bb"), each containing
   * several rows. Returns the file path.
   */
  private Path writeMultiTenantFile(Configuration conf, FileSystem fs, Path baseDir)
    throws IOException {
    Path file = StoreFileWriter.getUniqueFile(fs, baseDir);

    CacheConfig cacheConfig = new CacheConfig(conf);
    Map<String, String> tableProps = new HashMap<>();
    tableProps.put(MultiTenantHFileWriter.TABLE_MULTI_TENANT_ENABLED, "true");
    tableProps.put(MultiTenantHFileWriter.TABLE_TENANT_PREFIX_LENGTH, "2");

    HFileContext context = new HFileContextBuilder().withBlockSize(4096).withColumnFamily(CF)
      .withTableName(Bytes.toBytes("tbl")).build();

    MultiTenantHFileWriter writer = MultiTenantHFileWriter.create(fs, file, conf, cacheConfig,
      tableProps, context, BloomType.ROW, BloomType.ROW, null, true);

    long ts = EnvironmentEdgeManager.currentTime();

    // Section "aa": 3 rows
    writer.append(new KeyValue(Bytes.toBytes("aa-0001"), CF, QUAL, ts, Bytes.toBytes("v1")));
    writer.append(new KeyValue(Bytes.toBytes("aa-0002"), CF, QUAL, ts, Bytes.toBytes("v2")));
    writer.append(new KeyValue(Bytes.toBytes("aa-0003"), CF, QUAL, ts, Bytes.toBytes("v3")));

    // Section "bb": 3 rows
    writer.append(new KeyValue(Bytes.toBytes("bb-0001"), CF, QUAL, ts, Bytes.toBytes("v4")));
    writer.append(new KeyValue(Bytes.toBytes("bb-0002"), CF, QUAL, ts, Bytes.toBytes("v5")));
    writer.append(new KeyValue(Bytes.toBytes("bb-0003"), CF, QUAL, ts, Bytes.toBytes("v6")));

    writer.close();
    return file;
  }

  /**
   * Opens a multi-tenant reader and its scanner for the given HFile.
   */
  private HFile.Reader openReader(Configuration conf, FileSystem fs, Path file) throws IOException {
    return HFile.createReader(fs, file, new CacheConfig(conf), true, conf);
  }

  /**
   * seekBefore at the first key of the second section must fall back to the last key of the first
   * section. This is the core C5 bug — without cross-section fallback, seekBefore returns false and
   * reverse scans miss data at section boundaries.
   */
  @Test
  public void testSeekBeforeFallsToPreviousSection() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.setInt(HFile.FORMAT_VERSION_KEY, HFile.MIN_FORMAT_VERSION_WITH_MULTI_TENANT);
    FileSystem fs = FileSystem.getLocal(conf);
    Path baseDir = new Path(Files.createTempDirectory("seekbefore-test").toUri());

    try {
      Path file = writeMultiTenantFile(conf, fs, baseDir);

      try (HFile.Reader reader = openReader(conf, fs, file)) {
        HFileScanner scanner = reader.getScanner(conf, false, true);

        // seekBefore the first key of section "bb" → should land on last key of section "aa"
        KeyValue target = new KeyValue(Bytes.toBytes("bb-0001"), CF, QUAL,
          EnvironmentEdgeManager.currentTime(), KeyValue.Type.Maximum);
        boolean found = scanner.seekBefore(target);

        assertTrue("seekBefore(bb-0001) should succeed by falling back to section 'aa'", found);

        String row = Bytes.toString(CellUtil.cloneRow(scanner.getCell()));
        assertEquals("Should be positioned at last key of section 'aa'", "aa-0003", row);
      }
    } finally {
      fs.delete(baseDir, true);
    }
  }

  /**
   * seekBefore within the same section (not at the boundary) should work as usual — no
   * cross-section fallback needed.
   */
  @Test
  public void testSeekBeforeWithinSameSection() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.setInt(HFile.FORMAT_VERSION_KEY, HFile.MIN_FORMAT_VERSION_WITH_MULTI_TENANT);
    FileSystem fs = FileSystem.getLocal(conf);
    Path baseDir = new Path(Files.createTempDirectory("seekbefore-same").toUri());

    try {
      Path file = writeMultiTenantFile(conf, fs, baseDir);

      try (HFile.Reader reader = openReader(conf, fs, file)) {
        HFileScanner scanner = reader.getScanner(conf, false, true);

        // seekBefore(bb-0002) should position to bb-0001 (within same section)
        KeyValue target = new KeyValue(Bytes.toBytes("bb-0002"), CF, QUAL,
          EnvironmentEdgeManager.currentTime(), KeyValue.Type.Maximum);
        boolean found = scanner.seekBefore(target);

        assertTrue("seekBefore(bb-0002) should succeed within section 'bb'", found);

        String row = Bytes.toString(CellUtil.cloneRow(scanner.getCell()));
        assertEquals("Should be positioned at bb-0001", "bb-0001", row);
      }
    } finally {
      fs.delete(baseDir, true);
    }
  }

  /**
   * seekBefore the first key of the first section (and of the entire file) should return false —
   * there is no earlier key to position to.
   */
  @Test
  public void testSeekBeforeFirstKeyReturnsFalse() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.setInt(HFile.FORMAT_VERSION_KEY, HFile.MIN_FORMAT_VERSION_WITH_MULTI_TENANT);
    FileSystem fs = FileSystem.getLocal(conf);
    Path baseDir = new Path(Files.createTempDirectory("seekbefore-first").toUri());

    try {
      Path file = writeMultiTenantFile(conf, fs, baseDir);

      try (HFile.Reader reader = openReader(conf, fs, file)) {
        HFileScanner scanner = reader.getScanner(conf, false, true);

        // seekBefore the very first key → no earlier key exists
        KeyValue target = new KeyValue(Bytes.toBytes("aa-0001"), CF, QUAL,
          EnvironmentEdgeManager.currentTime(), KeyValue.Type.Maximum);
        boolean found = scanner.seekBefore(target);

        assertFalse("seekBefore first key in entire file should return false", found);
      }
    } finally {
      fs.delete(baseDir, true);
    }
  }
}
