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
package org.apache.hadoop.hbase.replication.regionserver;

import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNameTestRule;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.rules.TestName;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

/**
 * Base class for WALEntryStream tests.
 */
public abstract class WALEntryStreamTestBase {

  protected static final long TEST_TIMEOUT_MS = 5000;
  protected static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();;
  protected static Configuration CONF;
  protected static FileSystem fs;
  protected static MiniDFSCluster cluster;
  protected static final TableName tableName = TableName.valueOf("tablename");
  protected static final byte[] family = Bytes.toBytes("column");
  protected static final byte[] qualifier = Bytes.toBytes("qualifier");
  protected static final RegionInfo info = RegionInfoBuilder.newBuilder(tableName)
    .setStartKey(HConstants.EMPTY_START_ROW).setEndKey(HConstants.LAST_ROW).build();
  protected static final NavigableMap<byte[], Integer> scopes = getScopes();
  protected final String fakeWalGroupId = "fake-wal-group-id";

  /**
   * Test helper that waits until a non-null entry is available in the stream next or times out. A
   * {@link WALEntryStream} provides a streaming access to a queue of log files. Since the stream
   * can be consumed as the file is being written, callers relying on {@link WALEntryStream#next()}
   * may need to retry multiple times before an entry appended to the WAL is visible to the stream
   * consumers. One such cause of delay is the close() of writer writing these log files. While the
   * closure is in progress, the stream does not switch to the next log in the queue and next() may
   * return null entries. This utility wraps these retries into a single next call and that makes
   * the test code simpler.
   */
  protected static class WALEntryStreamWithRetries extends WALEntryStream {
    // Class member to be able to set a non-final from within a lambda.
    private Entry result;

    public WALEntryStreamWithRetries(ReplicationSourceLogQueue logQueue, Configuration conf,
      long startPosition, WALFileLengthProvider walFileLengthProvider, ServerName serverName,
      MetricsSource metrics, String walGroupId) throws IOException {
      super(logQueue, conf, startPosition, walFileLengthProvider, serverName, metrics, walGroupId);
    }

    @Override
    public Entry next() {
      Waiter.waitFor(CONF, TEST_TIMEOUT_MS, () -> {
        result = WALEntryStreamWithRetries.super.next();
        return result != null;
      });
      return result;
    }
  }

  private static NavigableMap<byte[], Integer> getScopes() {
    NavigableMap<byte[], Integer> scopes = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    scopes.put(family, 1);
    return scopes;
  }

  class PathWatcher implements WALActionsListener {

    Path currentPath;

    @Override
    public void preLogRoll(Path oldPath, Path newPath) {
      logQueue.enqueueLog(newPath, fakeWalGroupId);
      currentPath = newPath;
    }
  }

  protected WAL log;
  protected ReplicationSourceLogQueue logQueue;
  protected PathWatcher pathWatcher;

  @Rule
  public TestName tn = new TestName();
  protected final MultiVersionConcurrencyControl mvcc = new MultiVersionConcurrencyControl();

  protected static void startCluster() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    CONF = TEST_UTIL.getConfiguration();
    CONF.setLong("replication.source.sleepforretries", 10);
    TEST_UTIL.startMiniDFSCluster(3);

    cluster = TEST_UTIL.getDFSCluster();
    fs = cluster.getFileSystem();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  protected void initWAL() throws IOException {
    ReplicationSource source = mock(ReplicationSource.class);
    MetricsSource metricsSource = new MetricsSource("2");
    // Source with the same id is shared and carries values from the last run
    metricsSource.clear();
    logQueue = new ReplicationSourceLogQueue(CONF, metricsSource, source);
    pathWatcher = new PathWatcher();
    final WALFactory wals =
      new WALFactory(CONF, TableNameTestRule.cleanUpTestName(tn.getMethodName()));
    wals.getWALProvider().addWALActionsListener(pathWatcher);
    log = wals.getWAL(info);
  }

  @After
  public void tearDown() throws Exception {
    Closeables.close(log, true);
  }

  protected void appendToLogAndSync() throws IOException {
    appendToLogAndSync(1);
  }

  protected void appendToLogAndSync(int count) throws IOException {
    long txid = appendToLog(count);
    log.sync(txid);
  }

  protected long appendToLog(int count) throws IOException {
    return log.appendData(info, new WALKeyImpl(info.getEncodedNameAsBytes(), tableName,
      EnvironmentEdgeManager.currentTime(), mvcc, scopes), getWALEdits(count));
  }

  protected WALEdit getWALEdits(int count) {
    WALEdit edit = new WALEdit();
    for (int i = 0; i < count; i++) {
      edit.add(new KeyValue(Bytes.toBytes(EnvironmentEdgeManager.currentTime()), family, qualifier,
        EnvironmentEdgeManager.currentTime(), qualifier));
    }
    return edit;
  }
}
