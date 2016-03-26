/**
 *
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
package org.apache.hadoop.hbase.regionserver.wal;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.SampleRegionWALObserver;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKey;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * WAL tests that can be reused across providers.
 */
public abstract class AbstractTestProtobufLog<W extends Closeable> {
  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  protected FileSystem fs;
  protected Path dir;
  protected WALFactory wals;

  @Rule
  public final TestName currentTest = new TestName();

  @Before
  public void setUp() throws Exception {
    fs = TEST_UTIL.getDFSCluster().getFileSystem();
    dir = new Path(TEST_UTIL.createRootDir(), currentTest.getMethodName());
    wals = new WALFactory(TEST_UTIL.getConfiguration(), null, currentTest.getMethodName());
  }

  @After
  public void tearDown() throws Exception {
    wals.close();
    FileStatus[] entries = fs.listStatus(new Path("/"));
    for (FileStatus dir : entries) {
      fs.delete(dir.getPath(), true);
    }
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Make block sizes small.
    TEST_UTIL.getConfiguration().setInt("dfs.blocksize", 1024 * 1024);
    // needed for testAppendClose()
    TEST_UTIL.getConfiguration().setBoolean("dfs.support.broken.append", true);
    TEST_UTIL.getConfiguration().setBoolean("dfs.support.append", true);
    // quicker heartbeat interval for faster DN death notification
    TEST_UTIL.getConfiguration().setInt("dfs.namenode.heartbeat.recheck-interval", 5000);
    TEST_UTIL.getConfiguration().setInt("dfs.heartbeat.interval", 1);
    TEST_UTIL.getConfiguration().setInt("dfs.client.socket-timeout", 5000);

    // faster failover with cluster.shutdown();fs.close() idiom
    TEST_UTIL.getConfiguration()
        .setInt("hbase.ipc.client.connect.max.retries", 1);
    TEST_UTIL.getConfiguration().setInt(
        "dfs.client.block.recovery.retries", 1);
    TEST_UTIL.getConfiguration().setInt(
      "hbase.ipc.client.connection.maxidletime", 500);
    TEST_UTIL.getConfiguration().set(CoprocessorHost.WAL_COPROCESSOR_CONF_KEY,
        SampleRegionWALObserver.class.getName());
    TEST_UTIL.startMiniDFSCluster(3);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Reads the WAL with and without WALTrailer.
   * @throws IOException
   */
  @Test
  public void testWALTrailer() throws IOException {
    // read With trailer.
    doRead(true);
    // read without trailer
    doRead(false);
  }

  /**
   * Appends entries in the WAL and reads it.
   * @param withTrailer If 'withTrailer' is true, it calls a close on the WALwriter before reading
   *          so that a trailer is appended to the WAL. Otherwise, it starts reading after the sync
   *          call. This means that reader is not aware of the trailer. In this scenario, if the
   *          reader tries to read the trailer in its next() call, it returns false from
   *          ProtoBufLogReader.
   * @throws IOException
   */
  private void doRead(boolean withTrailer) throws IOException {
    final int columnCount = 5;
    final int recordCount = 5;
    final TableName tableName =
        TableName.valueOf("tablename");
    final byte[] row = Bytes.toBytes("row");
    long timestamp = System.currentTimeMillis();
    Path path = new Path(dir, "tempwal");
    // delete the log if already exists, for test only
    fs.delete(path, true);
    W writer = null;
    ProtobufLogReader reader = null;
    try {
      HRegionInfo hri = new HRegionInfo(tableName,
          HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
      HTableDescriptor htd = new HTableDescriptor(tableName);
      fs.mkdirs(dir);
      // Write log in pb format.
      writer = createWriter(path);
      for (int i = 0; i < recordCount; ++i) {
        WALKey key = new WALKey(
            hri.getEncodedNameAsBytes(), tableName, i, timestamp, HConstants.DEFAULT_CLUSTER_ID);
        WALEdit edit = new WALEdit();
        for (int j = 0; j < columnCount; ++j) {
          if (i == 0) {
            htd.addFamily(new HColumnDescriptor("column" + j));
          }
          String value = i + "" + j;
          edit.add(new KeyValue(row, row, row, timestamp, Bytes.toBytes(value)));
        }
        append(writer, new WAL.Entry(key, edit));
      }
      sync(writer);
      if (withTrailer) writer.close();

      // Now read the log using standard means.
      reader = (ProtobufLogReader) wals.createReader(fs, path);
      if (withTrailer) {
        assertNotNull(reader.trailer);
      } else {
        assertNull(reader.trailer);
      }
      for (int i = 0; i < recordCount; ++i) {
        WAL.Entry entry = reader.next();
        assertNotNull(entry);
        assertEquals(columnCount, entry.getEdit().size());
        assertArrayEquals(hri.getEncodedNameAsBytes(), entry.getKey().getEncodedRegionName());
        assertEquals(tableName, entry.getKey().getTablename());
        int idx = 0;
        for (Cell val : entry.getEdit().getCells()) {
          assertTrue(Bytes.equals(row, 0, row.length, val.getRowArray(), val.getRowOffset(),
            val.getRowLength()));
          String value = i + "" + idx;
          assertArrayEquals(Bytes.toBytes(value), CellUtil.cloneValue(val));
          idx++;
        }
      }
      WAL.Entry entry = reader.next();
      assertNull(entry);
    } finally {
      if (writer != null) {
        writer.close();
      }
      if (reader != null) {
        reader.close();
      }
    }
  }

  protected abstract W createWriter(Path path) throws IOException;

  protected abstract void append(W writer, WAL.Entry entry) throws IOException;

  protected abstract void sync(W writer) throws IOException;
}
