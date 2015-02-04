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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALKey;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

/**
 * Test that the actions are called while playing with an WAL
 */
@Category({RegionServerTests.class, SmallTests.class})
public class TestWALActionsListener {
  protected static final Log LOG = LogFactory.getLog(TestWALActionsListener.class);

  private final static HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();

  private final static byte[] SOME_BYTES =  Bytes.toBytes("t");
  private static FileSystem fs;
  private static Configuration conf;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    conf.setInt("hbase.regionserver.maxlogs", 5);
    fs = FileSystem.get(conf);
    FSUtils.setRootDir(conf, TEST_UTIL.getDataTestDir());
  }

  @Before
  public void setUp() throws Exception {
    fs.delete(new Path(TEST_UTIL.getDataTestDir(), HConstants.HREGION_LOGDIR_NAME), true);
    fs.delete(new Path(TEST_UTIL.getDataTestDir(), HConstants.HREGION_OLDLOGDIR_NAME), true);
  }

  @After
  public void tearDown() throws Exception {
    setUp();
  }

  /**
   * Add a bunch of dummy data and roll the logs every two insert. We
   * should end up with 10 rolled files (plus the roll called in
   * the constructor). Also test adding a listener while it's running.
   */
  @Test
  public void testActionListener() throws Exception {
    DummyWALActionsListener observer = new DummyWALActionsListener();
    List<WALActionsListener> list = new ArrayList<WALActionsListener>();
    list.add(observer);
    final WALFactory wals = new WALFactory(conf, list, "testActionListener");
    DummyWALActionsListener laterobserver = new DummyWALActionsListener();
    final AtomicLong sequenceId = new AtomicLong(1);
    HRegionInfo hri = new HRegionInfo(TableName.valueOf(SOME_BYTES),
             SOME_BYTES, SOME_BYTES, false);
    final WAL wal = wals.getWAL(hri.getEncodedNameAsBytes());

    for (int i = 0; i < 20; i++) {
      byte[] b = Bytes.toBytes(i+"");
      KeyValue kv = new KeyValue(b,b,b);
      WALEdit edit = new WALEdit();
      edit.add(kv);
      HTableDescriptor htd = new HTableDescriptor();
      htd.addFamily(new HColumnDescriptor(b));

      final long txid = wal.append(htd, hri, new WALKey(hri.getEncodedNameAsBytes(),
          TableName.valueOf(b), 0), edit, sequenceId, true, null);
      wal.sync(txid);
      if (i == 10) {
        wal.registerWALActionsListener(laterobserver);
      }
      if (i % 2 == 0) {
        wal.rollWriter();
      }
    }

    wal.close();

    assertEquals(11, observer.preLogRollCounter);
    assertEquals(11, observer.postLogRollCounter);
    assertEquals(5, laterobserver.preLogRollCounter);
    assertEquals(5, laterobserver.postLogRollCounter);
    assertEquals(1, observer.closedCount);
  }


  /**
   * Just counts when methods are called
   */
  public static class DummyWALActionsListener extends WALActionsListener.Base {
    public int preLogRollCounter = 0;
    public int postLogRollCounter = 0;
    public int closedCount = 0;

    @Override
    public void preLogRoll(Path oldFile, Path newFile) {
      preLogRollCounter++;
    }

    @Override
    public void postLogRoll(Path oldFile, Path newFile) {
      postLogRollCounter++;
    }

    @Override
    public void logCloseRequested() {
      closedCount++;
    }
  }

}

