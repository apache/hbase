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
package org.apache.hadoop.hbase.wal;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
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
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(MediumTests.class)
public class TestWALRootDir {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestWALRootDir.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestWALRootDir.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Configuration conf;
  private static FileSystem fs;
  private static FileSystem walFs;
  private static final TableName tableName = TableName.valueOf("TestWALWALDir");
  private static final byte [] rowName = Bytes.toBytes("row");
  private static final byte [] family = Bytes.toBytes("column");
  private static Path walRootDir;
  private static Path rootDir;
  private static WALFactory wals;

  @Before
  public void setUp() throws Exception {
    cleanup();
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    TEST_UTIL.startMiniDFSCluster(1);
    rootDir = TEST_UTIL.createRootDir();
    walRootDir = TEST_UTIL.createWALRootDir();
    fs = CommonFSUtils.getRootDirFileSystem(conf);
    walFs = CommonFSUtils.getWALFileSystem(conf);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    cleanup();
    TEST_UTIL.shutdownMiniDFSCluster();
  }

  @Test
  public void testWALRootDir() throws Exception {
    RegionInfo regionInfo = RegionInfoBuilder.newBuilder(tableName).build();
    wals = new WALFactory(conf, "testWALRootDir");
    WAL log = wals.getWAL(regionInfo);

    assertEquals(1, getWALFiles(walFs, walRootDir).size());
    byte [] value = Bytes.toBytes("value");
    WALEdit edit = new WALEdit();
    edit.add(new KeyValue(rowName, family, Bytes.toBytes("1"),
        System.currentTimeMillis(), value));
    long txid =
      log.appendData(regionInfo, getWalKey(System.currentTimeMillis(), regionInfo, 0), edit);
    log.sync(txid);
    assertEquals("Expect 1 log have been created", 1,
        getWALFiles(walFs, walRootDir).size());
    log.rollWriter();
    //Create 1 more WAL
    assertEquals(2, getWALFiles(walFs, new Path(walRootDir,
        HConstants.HREGION_LOGDIR_NAME)).size());
    edit.add(new KeyValue(rowName, family, Bytes.toBytes("2"),
        System.currentTimeMillis(), value));
    txid = log.appendData(regionInfo, getWalKey(System.currentTimeMillis(), regionInfo, 1), edit);
    log.sync(txid);
    log.rollWriter();
    log.shutdown();

    assertEquals("Expect 3 logs in WALs dir", 3, getWALFiles(walFs,
        new Path(walRootDir, HConstants.HREGION_LOGDIR_NAME)).size());
  }

  private WALKeyImpl getWalKey(final long time, RegionInfo hri, final long startPoint) {
    return new WALKeyImpl(hri.getEncodedNameAsBytes(), tableName, time,
        new MultiVersionConcurrencyControl(startPoint));
  }

  private List<FileStatus> getWALFiles(FileSystem fs, Path dir)
      throws IOException {
    List<FileStatus> result = new ArrayList<FileStatus>();
    LOG.debug("Scanning " + dir.toString() + " for WAL files");

    FileStatus[] files = fs.listStatus(dir);
    if (files == null) return Collections.emptyList();
    for (FileStatus file : files) {
      if (file.isDirectory()) {
        // recurse into sub directories
        result.addAll(getWALFiles(fs, file.getPath()));
      } else {
        String name = file.getPath().toString();
        if (!name.startsWith(".")) {
          result.add(file);
        }
      }
    }
    return result;
  }

  private static void cleanup() throws Exception{
    walFs.delete(walRootDir, true);
    fs.delete(rootDir, true);
  }

}

