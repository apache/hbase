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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

/**
 * Test that the actions are called while playing with an HLog
 */
@Category(SmallTests.class)
public class TestWALActionsListener {
  protected static final Log LOG = LogFactory.getLog(TestWALActionsListener.class);

  private final static HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();

  private final static byte[] SOME_BYTES =  Bytes.toBytes("t");
  private static FileSystem fs;
  private static Path oldLogDir;
  private static Path logDir;
  private static Configuration conf;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    conf.setInt("hbase.regionserver.maxlogs", 5);
    fs = FileSystem.get(conf);
    oldLogDir = new Path(TEST_UTIL.getDataTestDir(),
        HConstants.HREGION_OLDLOGDIR_NAME);
    logDir = new Path(TEST_UTIL.getDataTestDir(),
        HConstants.HREGION_LOGDIR_NAME);
  }

  @Before
  public void setUp() throws Exception {
    fs.delete(logDir, true);
    fs.delete(oldLogDir, true);
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
    DummyWALActionsListener laterobserver = new DummyWALActionsListener();
    HLog hlog = new HLog(fs, logDir, oldLogDir, conf, list, null);
    HRegionInfo hri = new HRegionInfo(SOME_BYTES,
             SOME_BYTES, SOME_BYTES, false);

    for (int i = 0; i < 20; i++) {
      byte[] b = Bytes.toBytes(i+"");
      KeyValue kv = new KeyValue(b,b,b);
      WALEdit edit = new WALEdit();
      edit.add(kv);
      HTableDescriptor htd = new HTableDescriptor();
      htd.addFamily(new HColumnDescriptor(b));

      HLogKey key = new HLogKey(b,b, 0, 0, HConstants.DEFAULT_CLUSTER_ID);
      hlog.append(hri, key, edit, htd, true);
      if (i == 10) {
        hlog.registerWALActionsListener(laterobserver);
      }
      if (i % 2 == 0) {
        hlog.rollWriter();
      }
    }

    hlog.close();
    hlog.closeAndDelete();

    assertEquals(11, observer.preLogRollCounter);
    assertEquals(11, observer.postLogRollCounter);
    assertEquals(5, laterobserver.preLogRollCounter);
    assertEquals(5, laterobserver.postLogRollCounter);
    assertEquals(2, observer.closedCount);
  }


  /**
   * Just counts when methods are called
   */
  static class DummyWALActionsListener implements WALActionsListener {
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
    public void preLogArchive(Path oldFile, Path newFile) {
      // Not interested
    }

    @Override
    public void postLogArchive(Path oldFile, Path newFile) {
      // Not interested
    }

    @Override
    public void logRollRequested() {
      // Not interested
    }

    @Override
    public void visitLogEntryBeforeWrite(HRegionInfo info, HLogKey logKey,
        WALEdit logEdit) {
      // Not interested

    }

    @Override
    public void logCloseRequested() {
      closedCount++;
    }

    public void visitLogEntryBeforeWrite(HTableDescriptor htd, HLogKey logKey, WALEdit logEdit) {
      //To change body of implemented methods use File | Settings | File Templates.
    }

  }

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}

