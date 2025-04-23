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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MediumTests.class, RegionServerTests.class })
public class TestCompactionToolNpeFix {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCompactionToolNpeFix.class);

  private static final HBaseTestingUtil TESTUTIL = new HBaseTestingUtil();

  private HRegion region;
  private final static byte[] qualifier = Bytes.toBytes("qf");
  private static Path rootDir;
  private final TableName tableName = TableName.valueOf(getClass().getSimpleName());

  @BeforeClass
  public static void setUpAfterClass() throws Exception {
    TESTUTIL.getConfiguration().setBoolean(MemStoreLAB.USEMSLAB_KEY, false);
    TESTUTIL.startMiniCluster();
    rootDir = TESTUTIL.getDefaultRootDirPath();
    TESTUTIL.startMiniMapReduceCluster();

  }

  @AfterClass
  public static void tearDown() throws Exception {
    TESTUTIL.shutdownMiniMapReduceCluster();
    TESTUTIL.shutdownMiniCluster();
    TESTUTIL.cleanupTestDir();

  }

  @Before
  public void setUp() throws IOException {
    TESTUTIL.createTable(tableName, HBaseTestingUtil.fam1);
    this.region = TESTUTIL.getMiniHBaseCluster().getRegions(tableName).get(0);
  }

  @After
  public void after() throws IOException {
    TESTUTIL.deleteTable(tableName);
  }

  private void putAndFlush(int key) throws Exception {
    Put put = new Put(Bytes.toBytes(key));
    put.addColumn(HBaseTestingUtil.fam1, qualifier, Bytes.toBytes("val" + key));
    region.put(put);
    TESTUTIL.flush(tableName);
  }

  private HStore prepareStoreWithMultiFiles() throws Exception {
    for (int i = 0; i < 5; i++) {
      this.putAndFlush(i);
    }
    HStore store = region.getStore(HBaseTestingUtil.fam1);
    assertEquals(5, store.getStorefilesCount());
    return store;
  }

  @Test
  public void testCompactedFilesArchived() throws Exception {
    HStore store = prepareStoreWithMultiFiles();
    Path tableDir = CommonFSUtils.getTableDir(rootDir, region.getRegionInfo().getTable());
    FileSystem fs = store.getFileSystem();
    String storePath = tableDir + "/" + region.getRegionInfo().getEncodedName() + "/"
      + Bytes.toString(HBaseTestingUtil.fam1);
    FileStatus[] regionDirFiles = fs.listStatus(new Path(storePath));
    assertEquals(5, regionDirFiles.length);
    String defaultFS = TESTUTIL.getMiniHBaseCluster().getConfiguration().get("fs.defaultFS");
    Configuration config = HBaseConfiguration.create();
    config.set("fs.defaultFS", defaultFS);
    int result = ToolRunner.run(config, new CompactionTool(),
      new String[] { "-compactOnce", "-major", storePath });
    assertEquals(0, result);
    regionDirFiles = fs.listStatus(new Path(storePath));
    assertEquals(1, regionDirFiles.length);
  }

  @Test
  public void testCompactedFilesArchivedMapRed() throws Exception {
    HStore store = prepareStoreWithMultiFiles();
    Path tableDir = CommonFSUtils.getTableDir(rootDir, region.getRegionInfo().getTable());
    FileSystem fs = store.getFileSystem();
    String storePath = tableDir + "/" + region.getRegionInfo().getEncodedName() + "/"
      + Bytes.toString(HBaseTestingUtil.fam1);
    FileStatus[] regionDirFiles = fs.listStatus(new Path(storePath));
    assertEquals(5, regionDirFiles.length);
    String defaultFS = TESTUTIL.getMiniHBaseCluster().getConfiguration().get("fs.defaultFS");
    Configuration config = HBaseConfiguration.create(TESTUTIL.getConfiguration());
    config.setBoolean(MemStoreLAB.USEMSLAB_KEY, true);
    config.set("fs.defaultFS", defaultFS);
    int result = ToolRunner.run(config, new CompactionTool(),
      new String[] { "-compactOnce", "-mapred", "-major", storePath });
    assertEquals(0, result);
    regionDirFiles = fs.listStatus(new Path(storePath));
    assertEquals(1, regionDirFiles.length);
  }
}
