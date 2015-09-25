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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Category(LargeTests.class)
@RunWith(Parameterized.class)
public class TestReplicationWALReaderManager {

  private static HBaseTestingUtility TEST_UTIL;
  private static Configuration conf;
  private static Path hbaseDir;
  private static FileSystem fs;
  private static MiniDFSCluster cluster;
  private static final TableName tableName = TableName.valueOf("tablename");
  private static final byte [] family = Bytes.toBytes("column");
  private static final byte [] qualifier = Bytes.toBytes("qualifier");
  private static final HRegionInfo info = new HRegionInfo(tableName,
      HConstants.EMPTY_START_ROW, HConstants.LAST_ROW, false);
  private static final HTableDescriptor htd = new HTableDescriptor(tableName);

  private WAL log;
  private ReplicationWALReaderManager logManager;
  private PathWatcher pathWatcher;
  private int nbRows;
  private int walEditKVs;
  private final AtomicLong sequenceId = new AtomicLong(1);
  @Rule public TestName tn = new TestName();

  @Parameters
  public static Collection<Object[]> parameters() {
    // Try out different combinations of row count and KeyValue count
    int[] NB_ROWS = { 1500, 60000 };
    int[] NB_KVS = { 1, 100 };
    // whether compression is used
    Boolean[] BOOL_VALS = { false, true };
    List<Object[]> parameters = new ArrayList<Object[]>();
    for (int nbRows : NB_ROWS) {
      for (int walEditKVs : NB_KVS) {
        for (boolean b : BOOL_VALS) {
          Object[] arr = new Object[3];
          arr[0] = nbRows;
          arr[1] = walEditKVs;
          arr[2] = b;
          parameters.add(arr);
        }
      }
    }
    return parameters;
  }

  public TestReplicationWALReaderManager(int nbRows, int walEditKVs, boolean enableCompression) {
    this.nbRows = nbRows;
    this.walEditKVs = walEditKVs;
    TEST_UTIL.getConfiguration().setBoolean(HConstants.ENABLE_WAL_COMPRESSION,
      enableCompression);
  }
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    conf = TEST_UTIL.getConfiguration();
    TEST_UTIL.startMiniDFSCluster(3);

    hbaseDir = TEST_UTIL.createRootDir();
    cluster = TEST_UTIL.getDFSCluster();
    fs = cluster.getFileSystem();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    logManager = new ReplicationWALReaderManager(fs, conf);
    List<WALActionsListener> listeners = new ArrayList<WALActionsListener>();
    pathWatcher = new PathWatcher();
    listeners.add(pathWatcher);
    final WALFactory wals = new WALFactory(conf, listeners, tn.getMethodName());
    log = wals.getWAL(info.getEncodedNameAsBytes(), info.getTable().getNamespace());
  }

  @After
  public void tearDown() throws Exception {
    log.close();
  }

  @Test
  public void test() throws Exception {
    // Grab the path that was generated when the log rolled as part of its creation
    Path path = pathWatcher.currentPath;

    assertEquals(0, logManager.getPosition());

    appendToLog();

    // There's one edit in the log, read it. Reading past it needs to return nulls
    assertNotNull(logManager.openReader(path));
    logManager.seek();
    WAL.Entry entry = logManager.readNextAndSetPosition();
    assertNotNull(entry);
    entry = logManager.readNextAndSetPosition();
    assertNull(entry);
    logManager.closeReader();
    long oldPos = logManager.getPosition();

    appendToLog();

    // Read the newly added entry, make sure we made progress
    assertNotNull(logManager.openReader(path));
    logManager.seek();
    entry = logManager.readNextAndSetPosition();
    assertNotEquals(oldPos, logManager.getPosition());
    assertNotNull(entry);
    logManager.closeReader();
    oldPos = logManager.getPosition();

    log.rollWriter();

    // We rolled but we still should see the end of the first log and not get data
    assertNotNull(logManager.openReader(path));
    logManager.seek();
    entry = logManager.readNextAndSetPosition();
    assertEquals(oldPos, logManager.getPosition());
    assertNull(entry);
    logManager.finishCurrentFile();

    path = pathWatcher.currentPath;

    for (int i = 0; i < nbRows; i++) { appendToLogPlus(walEditKVs); }
    log.rollWriter();
    logManager.openReader(path);
    logManager.seek();
    for (int i = 0; i < nbRows; i++) {
      WAL.Entry e = logManager.readNextAndSetPosition();
      if (e == null) {
        fail("Should have enough entries");
      }
    }
  }

  private void appendToLog() throws IOException {
    appendToLogPlus(1);
  }

  private void appendToLogPlus(int count) throws IOException {
    final long txid = log.append(htd, info, new WALKey(info.getEncodedNameAsBytes(), tableName,
        System.currentTimeMillis()), getWALEdits(count), sequenceId, true, null);
    log.sync(txid);
  }

  private WALEdit getWALEdits(int count) {
    WALEdit edit = new WALEdit();
    for (int i = 0; i < count; i++) {
      edit.add(new KeyValue(Bytes.toBytes(System.currentTimeMillis()), family, qualifier,
        System.currentTimeMillis(), qualifier));
    }
    return edit;
  }

  class PathWatcher extends WALActionsListener.Base {

    Path currentPath;

    @Override
    public void preLogRoll(Path oldPath, Path newPath) throws IOException {
      currentPath = newPath;
    }
  }
}
