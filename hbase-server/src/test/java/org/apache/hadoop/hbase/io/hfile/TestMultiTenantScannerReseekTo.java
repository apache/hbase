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
 * Tests for {@link AbstractMultiTenantReader.MultiTenantScanner#reseekTo} forward-only contract.
 * <p>
 * The reseekTo contract requires forward-only movement. When the scanner is positioned in section N
 * and reseekTo targets a key in an earlier section, the scanner must NOT rewind — it should report
 * "already past" and keep its current position.
 */
@Category(SmallTests.class)
public class TestMultiTenantScannerReseekTo {

  private static final byte[] CF = Bytes.toBytes("cf");
  private static final byte[] QUAL = Bytes.toBytes("q");

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
   * reseekTo with a key in an earlier section must not rewind. The scanner should stay in its
   * current section and not move backward.
   */
  @Test
  public void testReseekToDoesNotRewindToPreviousSection() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.setInt(HFile.FORMAT_VERSION_KEY, HFile.MIN_FORMAT_VERSION_WITH_MULTI_TENANT);
    FileSystem fs = FileSystem.getLocal(conf);
    Path baseDir = new Path(Files.createTempDirectory("reseekto-test").toUri());

    try {
      Path file = writeMultiTenantFile(conf, fs, baseDir);

      try (HFile.Reader reader = HFile.createReader(fs, file, new CacheConfig(conf), true, conf)) {
        HFileScanner scanner = reader.getScanner(conf, false, true);

        // Position at start, then advance into section "bb"
        assertTrue("seekTo start should succeed", scanner.seekTo());
        while (Bytes.toString(CellUtil.cloneRow(scanner.getCell())).startsWith("aa")) {
          assertTrue("next should succeed while in section aa", scanner.next());
        }

        String posBeforeReseek = Bytes.toString(CellUtil.cloneRow(scanner.getCell()));
        assertTrue("Should be in section bb now", posBeforeReseek.startsWith("bb"));

        // reseekTo a key in section "aa" — BEFORE current position
        KeyValue aaKey = new KeyValue(Bytes.toBytes("aa-0001"), CF, QUAL,
          EnvironmentEdgeManager.currentTime(), KeyValue.Type.Maximum);
        int reseekResult = scanner.reseekTo(aaKey);

        // Must return 1 (already past) and keep a valid position in section "bb"
        assertEquals("reseekTo backward key should return 1 (already past)", 1, reseekResult);
        assertTrue("Scanner should still be seeked after backward reseekTo",
          scanner.getCell() != null);
        String posAfterReseek = Bytes.toString(CellUtil.cloneRow(scanner.getCell()));
        assertTrue("Scanner must not rewind to section aa, but was at: " + posAfterReseek,
          posAfterReseek.startsWith("bb"));
        assertEquals("Scanner should stay at original position", posBeforeReseek, posAfterReseek);
      }
    } finally {
      fs.delete(baseDir, true);
    }
  }
}
