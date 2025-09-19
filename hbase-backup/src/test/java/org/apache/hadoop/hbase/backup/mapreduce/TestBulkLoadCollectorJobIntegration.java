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
package org.apache.hadoop.hbase.backup.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.util.BulkLoadProcessor;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.io.asyncfs.monitor.StreamSlowMonitor;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.regionserver.wal.ProtobufLogWriter;
import org.apache.hadoop.hbase.regionserver.wal.WALUtil;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MapReduceTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.wal.FSHLogProvider;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;

/**
 * Integration-like unit test for BulkLoadCollectorJob.
 * <p>
 * - Creates a WAL with a BULK_LOAD descriptor (ProtobufLogWriter).
 * <p>
 * - Runs BulkLoadCollectorJob.
 * <p>
 * - Verifies the job emits the expected store-file path.
 */
@Category({ MapReduceTests.class, LargeTests.class })
public class TestBulkLoadCollectorJobIntegration {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBulkLoadCollectorJobIntegration.class);

  private static final Logger LOG =
    LoggerFactory.getLogger(TestBulkLoadCollectorJobIntegration.class);
  private final static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static Configuration conf;
  private static FileSystem fs;
  private static Path hbaseDir;
  static final TableName tableName = TableName.valueOf(getName());
  static final RegionInfo info = RegionInfoBuilder.newBuilder(tableName).build();
  private static final byte[] family = Bytes.toBytes("column");
  private static Path logDir;
  protected MultiVersionConcurrencyControl mvcc;
  protected static NavigableMap<byte[], Integer> scopes = new TreeMap<>(Bytes.BYTES_COMPARATOR);

  private static String getName() {
    return "TestBulkLoadCollectorJobIntegration";
  }

  @Before
  public void setUp() throws Exception {
    if (hbaseDir != null && fs != null) fs.delete(hbaseDir, true);
    mvcc = new MultiVersionConcurrencyControl();
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    conf.setInt("dfs.blocksize", 1024 * 1024);
    conf.setInt("dfs.replication", 1);

    // Start a mini DFS cluster
    TEST_UTIL.startMiniDFSCluster(3);

    conf = TEST_UTIL.getConfiguration();
    fs = TEST_UTIL.getDFSCluster().getFileSystem();

    hbaseDir = TEST_UTIL.createRootDir();

    // Use a deterministic test WAL directory under the test filesystem
    logDir = new Path(TEST_UTIL.getDataTestDirOnTestFS(), "WALs/23-11-2024");
    fs.mkdirs(logDir);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (fs != null && hbaseDir != null) fs.delete(hbaseDir, true);
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Test that BulkLoadCollectorJob discovers and emits store-file paths from WAL files created
   * using WALFactory (no RegionServer needed).
   */
  @Test
  public void testBulkLoadCollectorEmitsStoreFilesFromWAL() throws Exception {
    // Create WAL entry with BULK_LOAD descriptor
    final String storeFileName = "storefile-abc.hfile";
    WAL.Entry entry =
      createBulkLoadWalEntry(info.getEncodedName(), Bytes.toString(family), storeFileName);

    // Verify the processor would extract relative paths
    List<Path> relativePaths =
      BulkLoadProcessor.processBulkLoadFiles(entry.getKey(), entry.getEdit());
    LOG.debug("BulkLoadProcessor returned {} relative path(s): {}", relativePaths.size(),
      relativePaths);
    assertEquals("Expected exactly one relative path from BulkLoadProcessor", 1,
      relativePaths.size());

    // Build WAL file path and write WAL using ProtobufLogWriter into logDir
    String walFileName = "wal-" + EnvironmentEdgeManager.currentTime();
    Path walFilePath = new Path(logDir, walFileName);
    fs.mkdirs(logDir);

    FSHLogProvider.Writer writer = null;
    try {
      writer = new ProtobufLogWriter();
      long blockSize = WALUtil.getWALBlockSize(conf, fs, walFilePath);
      writer.init(fs, walFilePath, conf, true, blockSize,
        StreamSlowMonitor.create(conf, walFileName));
      writer.append(entry);
      writer.sync(true);
      writer.close();
    } catch (Exception e) {
      throw new IOException("Failed to write WAL via ProtobufLogWriter", e);
    } finally {
      try {
        if (writer != null) writer.close();
      } catch (Exception ignore) {
      }
    }

    // Assert WAL file exists and has content
    boolean exists = fs.exists(walFilePath);
    long len = exists ? fs.getFileStatus(walFilePath).getLen() : -1L;
    assertTrue("WAL file should exist at " + walFilePath, exists);
    assertTrue("WAL file should have non-zero length, actual=" + len, len > 0);

    // Run the MR job
    Path walInputDir = logDir;
    Path outDir = new Path("/tmp/test-bulk-files-output-" + System.currentTimeMillis());

    int res = ToolRunner.run(TEST_UTIL.getConfiguration(),
      new BulkLoadCollectorJob(TEST_UTIL.getConfiguration()),
      new String[] { walInputDir.toString(), outDir.toString() });
    assertEquals("BulkLoadCollectorJob should exit with code 0", 0, res);

    // Inspect job output
    FileSystem dfs = TEST_UTIL.getDFSCluster().getFileSystem();
    assertTrue("Output directory should exist", dfs.exists(outDir));

    List<Path> partFiles = Arrays.stream(dfs.listStatus(outDir)).map(FileStatus::getPath)
      .filter(p -> p.getName().startsWith("part-")).toList();

    assertFalse("Expect at least one part file in output", partFiles.isEmpty());

    // Read all lines (collect while stream is open)
    List<String> lines = partFiles.stream().flatMap(p -> {
      try (FSDataInputStream in = dfs.open(p);
        BufferedReader r = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
        List<String> fileLines = r.lines().toList();
        return fileLines.stream();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }).toList();

    assertFalse("Job should have emitted at least one storefile path", lines.isEmpty());

    boolean found = lines.stream().anyMatch(l -> l.contains(storeFileName));
    assertTrue(
      "Expected emitted path to contain store file name: " + storeFileName + " ; got: " + lines,
      found);

    // cleanup
    dfs.delete(outDir, true);
  }

  private WAL.Entry createBulkLoadWalEntry(String regionName, String family, String... storeFiles) {

    WALProtos.StoreDescriptor.Builder storeDescBuilder =
      WALProtos.StoreDescriptor.newBuilder().setFamilyName(ByteString.copyFromUtf8(family))
        .setStoreHomeDir(family).addAllStoreFile(Arrays.asList(storeFiles));

    WALProtos.BulkLoadDescriptor.Builder bulkDescBuilder = WALProtos.BulkLoadDescriptor.newBuilder()
      .setReplicate(true).setEncodedRegionName(ByteString.copyFromUtf8(regionName))
      .setTableName(ProtobufUtil.toProtoTableName(tableName)).setBulkloadSeqNum(1000)
      .addStores(storeDescBuilder);

    byte[] valueBytes = bulkDescBuilder.build().toByteArray();

    WALEdit edit = new WALEdit();
    Cell cell = CellBuilderFactory.create(CellBuilderType.DEEP_COPY).setType(Cell.Type.Put)
      .setRow(new byte[] { 1 }).setFamily(WALEdit.METAFAMILY).setQualifier(WALEdit.BULK_LOAD)
      .setValue(valueBytes).build();
    edit.add(cell);

    long ts = EnvironmentEdgeManager.currentTime();
    WALKeyImpl key = getWalKeyImpl(ts, scopes);
    return new WAL.Entry(key, edit);
  }

  protected WALKeyImpl getWalKeyImpl(final long time, NavigableMap<byte[], Integer> scopes) {
    return new WALKeyImpl(info.getEncodedNameAsBytes(), tableName, time, mvcc, scopes);
  }
}
