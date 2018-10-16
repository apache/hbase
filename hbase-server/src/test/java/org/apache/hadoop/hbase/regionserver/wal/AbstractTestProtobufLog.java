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

import java.io.IOException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALProvider;
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
public abstract class AbstractTestProtobufLog {
  protected static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  protected FileSystem fs;
  protected Path dir;
  protected WALFactory wals;

  @Rule
  public final TestName currentTest = new TestName();

  @Before
  public void setUp() throws Exception {
    fs = TEST_UTIL.getDFSCluster().getFileSystem();
    dir = new Path(TEST_UTIL.createRootDir(), currentTest.getMethodName());
    wals = new WALFactory(TEST_UTIL.getConfiguration(), currentTest.getMethodName());
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
    // quicker heartbeat interval for faster DN death notification
    TEST_UTIL.getConfiguration().setInt("dfs.namenode.heartbeat.recheck-interval", 5000);
    TEST_UTIL.getConfiguration().setInt("dfs.heartbeat.interval", 1);
    TEST_UTIL.getConfiguration().setInt("dfs.client.socket-timeout", 5000);

    // faster failover with cluster.shutdown();fs.close() idiom
    TEST_UTIL.getConfiguration().setInt("dfs.client.block.recovery.retries", 1);
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
    int columnCount = 5;
    int recordCount = 5;
    TableName tableName = TableName.valueOf("tablename");
    byte[] row = Bytes.toBytes("row");
    long timestamp = System.currentTimeMillis();
    Path path = new Path(dir, "tempwal");
    // delete the log if already exists, for test only
    fs.delete(path, true);
    fs.mkdirs(dir);
    try (WALProvider.Writer writer = createWriter(path)) {
      ProtobufLogTestHelper.doWrite(writer, withTrailer, tableName, columnCount, recordCount, row,
        timestamp);
      try (ProtobufLogReader reader = (ProtobufLogReader) wals.createReader(fs, path)) {
        ProtobufLogTestHelper.doRead(reader, withTrailer, tableName, columnCount, recordCount, row,
          timestamp);
      }
    }
  }

  protected abstract WALProvider.Writer createWriter(Path path) throws IOException;
}
