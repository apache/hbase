/**
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
package org.apache.hadoop.hbase.snapshot;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.*;
import org.junit.experimental.categories.Category;

/**
 * Test snapshot log splitter
 */
@Category(SmallTests.class)
public class TestSnapshotLogSplitter {
  final Log LOG = LogFactory.getLog(getClass());

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private byte[] TEST_QUALIFIER = Bytes.toBytes("q");
  private byte[] TEST_FAMILY = Bytes.toBytes("f");

  private Configuration conf;
  private FileSystem fs;
  private Path logFile;

  @Before
  public void setup() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    fs = FileSystem.get(conf);
    logFile = new Path(TEST_UTIL.getDataTestDir(), "test.log");
    writeTestLog(logFile);
  }

  @After
  public void tearDown() throws Exception {
    fs.delete(logFile, false);
  }

  @Test
  public void testSplitLogs() throws IOException {
    Map<byte[], byte[]> regionsMap = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
    splitTestLogs(getTableName(5), regionsMap);
  }

  @Test
  public void testSplitLogsOnDifferentTable() throws IOException {
    byte[] tableName = getTableName(1);
    Map<byte[], byte[]> regionsMap = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
    for (int j = 0; j < 10; ++j) {
      byte[] regionName = getRegionName(tableName, j);
      byte[] newRegionName = getNewRegionName(tableName, j);
      regionsMap.put(regionName, newRegionName);
    }
    splitTestLogs(tableName, regionsMap);
  }

  /*
   * Split and verify test logs for the specified table
   */
  private void splitTestLogs(final byte[] tableName, final Map<byte[], byte[]> regionsMap)
      throws IOException {
    Path tableDir = new Path(TEST_UTIL.getDataTestDir(), Bytes.toString(tableName));
    SnapshotLogSplitter logSplitter = new SnapshotLogSplitter(conf, fs, tableDir,
      tableName, regionsMap);
    try {
      logSplitter.splitLog(logFile);
    } finally {
      logSplitter.close();
    }
    verifyRecoverEdits(tableDir, tableName, regionsMap);
  }

  /*
   * Verify that every logs in the table directory has just the specified table and regions.
   */
  private void verifyRecoverEdits(final Path tableDir, final byte[] tableName,
      final Map<byte[], byte[]> regionsMap) throws IOException {
    for (FileStatus regionStatus: FSUtils.listStatus(fs, tableDir)) {
      assertTrue(regionStatus.getPath().getName().startsWith(Bytes.toString(tableName)));
      Path regionEdits = HLog.getRegionDirRecoveredEditsDir(regionStatus.getPath());
      byte[] regionName = Bytes.toBytes(regionStatus.getPath().getName());
      assertFalse(regionsMap.containsKey(regionName));
      for (FileStatus logStatus: FSUtils.listStatus(fs, regionEdits)) {
        HLog.Reader reader = HLog.getReader(fs, logStatus.getPath(), conf);
        try {
          HLog.Entry entry;
          while ((entry = reader.next()) != null) {
            HLogKey key = entry.getKey();
            assertArrayEquals(tableName, key.getTablename());
            assertArrayEquals(regionName, key.getEncodedRegionName());
          }
        } finally {
          reader.close();
        }
      }
    }
  }

  /*
   * Write some entries in the log file.
   * 7 different tables with name "testtb-%d"
   * 10 region per table with name "tableName-region-%d"
   * 50 entry with row key "row-%d"
   */
  private void writeTestLog(final Path logFile) throws IOException {
    fs.mkdirs(logFile.getParent());
    HLog.Writer writer = HLog.createWriter(fs, logFile, conf);
    try {
      for (int i = 0; i < 7; ++i) {
        byte[] tableName = getTableName(i);
        for (int j = 0; j < 10; ++j) {
          byte[] regionName = getRegionName(tableName, j);
          for (int k = 0; k < 50; ++k) {
            byte[] rowkey = Bytes.toBytes("row-" + k);
            HLogKey key = new HLogKey(regionName, tableName, (long)k,
              System.currentTimeMillis(), HConstants.DEFAULT_CLUSTER_ID);
            WALEdit edit = new WALEdit();
            edit.add(new KeyValue(rowkey, TEST_FAMILY, TEST_QUALIFIER, rowkey));
            writer.append(new HLog.Entry(key, edit));
          }
        }
      }
    } finally {
      writer.close();
    }
  }

  private byte[] getTableName(int tableId) {
    return Bytes.toBytes("testtb-" + tableId);
  }

  private byte[] getRegionName(final byte[] tableName, int regionId) {
    return Bytes.toBytes(Bytes.toString(tableName) + "-region-" + regionId);
  }

  private byte[] getNewRegionName(final byte[] tableName, int regionId) {
    return Bytes.toBytes(Bytes.toString(tableName) + "-new-region-" + regionId);
  }
}
