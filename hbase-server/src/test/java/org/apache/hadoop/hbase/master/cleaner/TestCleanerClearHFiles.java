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
package org.apache.hadoop.hbase.master.cleaner;

import static org.apache.hadoop.hbase.master.HMaster.HBASE_MASTER_CLEANER_INTERVAL;
import static org.apache.hadoop.hbase.master.cleaner.HFileCleaner.HFILE_CLEANER_CUSTOM_PATHS_PLUGINS;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category(LargeTests.class)
public class TestCleanerClearHFiles {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCleanerClearHFiles.class);

  @Rule
  public TestName name = new TestName();

  private static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Configuration conf = TEST_UTIL.getConfiguration();
  private static Admin admin = null;

  private static final byte[] COLUMN_FAMILY = Bytes.toBytes("CF");

  private static final String TABLE1 = "table1";
  private static final String TABLE2 = "table2";
  private static final String DEFAULT_ARCHIVE_SUBDIRS_PREFIX = "data/default/";

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    conf.setStrings(HFileCleaner.HFILE_CLEANER_CUSTOM_PATHS,
      DEFAULT_ARCHIVE_SUBDIRS_PREFIX + TABLE1);
    conf.setStrings(HFILE_CLEANER_CUSTOM_PATHS_PLUGINS, HFileLinkCleaner.class.getName());

    conf.setInt(TimeToLiveHFileCleaner.TTL_CONF_KEY, 10);
    conf.setInt(HBASE_MASTER_CLEANER_INTERVAL, 20000);

    TEST_UTIL.startMiniCluster();
    admin = TEST_UTIL.getAdmin();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testClearArchive() throws Exception {
    DistributedFileSystem fs = TEST_UTIL.getDFSCluster().getFileSystem();
    Table table1 = createTable(TEST_UTIL, TableName.valueOf(TABLE1));
    Table table2 = createTable(TEST_UTIL, TableName.valueOf(TABLE2));

    admin.disableTable(table1.getName());
    admin.deleteTable(table1.getName());
    admin.disableTable(table2.getName());
    admin.deleteTable(table2.getName());

    Path archiveDir = HFileArchiveUtil.getArchivePath(conf);
    Path archiveTable1Path = new Path(archiveDir, DEFAULT_ARCHIVE_SUBDIRS_PREFIX + TABLE1);
    Path archiveTable2Path = new Path(archiveDir, DEFAULT_ARCHIVE_SUBDIRS_PREFIX + TABLE2);

    TEST_UTIL.waitFor(10000, () -> !notExistOrEmptyDir(archiveTable1Path, fs)
      && !notExistOrEmptyDir(archiveTable2Path, fs));

    TEST_UTIL.waitFor(30000,
      () -> notExistOrEmptyDir(archiveTable1Path, fs) && notExistOrEmptyDir(archiveTable2Path, fs));
  }

  private boolean notExistOrEmptyDir(Path dir, DistributedFileSystem fs) {
    try {
      return fs.listStatus(dir).length == 0;
    } catch (Exception e) {
      return e instanceof FileNotFoundException;
    }
  }

  private Table createTable(HBaseTestingUtility util, TableName tableName) throws IOException {
    TableDescriptor td = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(COLUMN_FAMILY).build()).build();
    return util.createTable(td, null);
  }
}
