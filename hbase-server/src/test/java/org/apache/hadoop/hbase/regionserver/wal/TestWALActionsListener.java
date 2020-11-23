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
package org.apache.hadoop.hbase.regionserver.wal;

import static org.junit.Assert.assertEquals;

import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test that the actions are called while playing with an WAL
 */
@Category({RegionServerTests.class, SmallTests.class})
public class TestWALActionsListener {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestWALActionsListener.class);

  private final static HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();

  private final static byte[] SOME_BYTES =  Bytes.toBytes("t");
  private static Configuration conf;
  private static Path rootDir;
  private static Path walRootDir;
  private static FileSystem fs;
  private static FileSystem logFs;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    conf.setInt("hbase.regionserver.maxlogs", 5);
    rootDir = TEST_UTIL.createRootDir();
    walRootDir = TEST_UTIL.createWALRootDir();
    fs = CommonFSUtils.getRootDirFileSystem(conf);
    logFs = CommonFSUtils.getWALFileSystem(conf);
  }

  @Before
  public void setUp() throws Exception {
    fs.delete(rootDir, true);
    logFs.delete(new Path(walRootDir, HConstants.HREGION_LOGDIR_NAME), true);
    logFs.delete(new Path(walRootDir, HConstants.HREGION_OLDLOGDIR_NAME), true);
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
    final WALFactory wals = new WALFactory(conf, "testActionListener");
    wals.getWALProvider().addWALActionsListener(observer);
    DummyWALActionsListener laterobserver = new DummyWALActionsListener();
    RegionInfo hri = RegionInfoBuilder.newBuilder(TableName.valueOf(SOME_BYTES))
        .setStartKey(SOME_BYTES).setEndKey(SOME_BYTES).build();
    final WAL wal = wals.getWAL(hri);
    MultiVersionConcurrencyControl mvcc = new MultiVersionConcurrencyControl();
    for (int i = 0; i < 20; i++) {
      byte[] b = Bytes.toBytes(i + "");
      KeyValue kv = new KeyValue(b, b, b);
      WALEdit edit = new WALEdit();
      edit.add(kv);
      NavigableMap<byte[], Integer> scopes = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      scopes.put(b, 0);
      long txid = wal.appendData(hri,
        new WALKeyImpl(hri.getEncodedNameAsBytes(), TableName.valueOf(b), 0, mvcc, scopes), edit);
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
  public static class DummyWALActionsListener implements WALActionsListener {
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

