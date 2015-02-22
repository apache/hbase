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
package org.apache.hadoop.hbase.mob.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.master.TableLockManager;
import org.apache.hadoop.hbase.master.TableLockManager.TableLock;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.mob.mapreduce.SweepJob.DummyMobAbortable;
import org.apache.hadoop.hbase.mob.mapreduce.SweepJob.SweepCounter;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.serializer.JavaSerialization;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.counters.GenericCounter;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Matchers;

@Category(MediumTests.class)
public class TestMobSweepReducer {

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static String tableName = "testSweepReducer";
  private final static String row = "row";
  private final static String family = "family";
  private final static String qf = "qf";
  private static HTable table;
  private static Admin admin;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt("hbase.master.info.port", 0);
    TEST_UTIL.getConfiguration().setBoolean("hbase.regionserver.info.port.auto", true);

    TEST_UTIL.getConfiguration().setInt("hfile.format.version", 3);

    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @SuppressWarnings("deprecation")
  @Before
  public void setUp() throws Exception {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    HColumnDescriptor hcd = new HColumnDescriptor(family);
    hcd.setMobEnabled(true);
    hcd.setMobThreshold(3L);
    hcd.setMaxVersions(4);
    desc.addFamily(hcd);

    admin = TEST_UTIL.getHBaseAdmin();
    admin.createTable(desc);
    table = new HTable(TEST_UTIL.getConfiguration(), tableName);
  }

  @After
  public void tearDown() throws Exception {
    admin.disableTable(TableName.valueOf(tableName));
    admin.deleteTable(TableName.valueOf(tableName));
    admin.close();
  }

  private List<String> getKeyFromSequenceFile(FileSystem fs, Path path,
                                              Configuration conf) throws Exception {
    List<String> list = new ArrayList<String>();
    SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));

    String next = (String) reader.next((String) null);
    while (next != null) {
      list.add(next);
      next = (String) reader.next((String) null);
    }
    reader.close();
    return list;
  }

  @Test
  public void testRun() throws Exception {

    TableName tn = TableName.valueOf(tableName);
    byte[] mobValueBytes = new byte[100];

    //get the path where mob files lie in
    Path mobFamilyPath = MobUtils.getMobFamilyPath(TEST_UTIL.getConfiguration(), tn, family);

    Put put = new Put(Bytes.toBytes(row));
    put.add(Bytes.toBytes(family), Bytes.toBytes(qf), 1, mobValueBytes);
    Put put2 = new Put(Bytes.toBytes(row + "ignore"));
    put2.add(Bytes.toBytes(family), Bytes.toBytes(qf), 1, mobValueBytes);
    table.put(put);
    table.put(put2);
    table.flushCommits();
    admin.flush(tn);

    FileStatus[] fileStatuses = TEST_UTIL.getTestFileSystem().listStatus(mobFamilyPath);
    //check the generation of a mob file
    assertEquals(1, fileStatuses.length);

    String mobFile1 = fileStatuses[0].getPath().getName();

    Configuration configuration = new Configuration(TEST_UTIL.getConfiguration());
    configuration.setFloat(MobConstants.MOB_SWEEP_TOOL_COMPACTION_RATIO, 0.6f);
    configuration.setStrings(TableInputFormat.INPUT_TABLE, tableName);
    configuration.setStrings(TableInputFormat.SCAN_COLUMN_FAMILY, family);
    configuration.setStrings(SweepJob.WORKING_VISITED_DIR_KEY, "jobWorkingNamesDir");
    configuration.setStrings(SweepJob.WORKING_FILES_DIR_KEY, "compactionFileDir");
    configuration.setStrings(CommonConfigurationKeys.IO_SERIALIZATIONS_KEY,
            JavaSerialization.class.getName());
    configuration.set(SweepJob.WORKING_VISITED_DIR_KEY, "compactionVisitedDir");
    configuration.setLong(MobConstants.MOB_SWEEP_TOOL_COMPACTION_START_DATE,
        System.currentTimeMillis() + 24 * 3600 * 1000);

    ZooKeeperWatcher zkw = new ZooKeeperWatcher(configuration, "1", new DummyMobAbortable());
    TableName lockName = MobUtils.getTableLockName(tn);
    String znode = ZKUtil.joinZNode(zkw.tableLockZNode, lockName.getNameAsString());
    configuration.set(SweepJob.SWEEP_JOB_ID, "1");
    configuration.set(SweepJob.SWEEP_JOB_TABLE_NODE, znode);
    ServerName serverName = SweepJob.getCurrentServerName(configuration);
    configuration.set(SweepJob.SWEEP_JOB_SERVERNAME, serverName.toString());

    TableLockManager tableLockManager = TableLockManager.createTableLockManager(configuration, zkw,
        serverName);
    TableLock lock = tableLockManager.writeLock(lockName, "Run sweep tool");
    lock.acquire();
    try {
      // use the same counter when mocking
      Counter counter = new GenericCounter();
      Reducer<Text, KeyValue, Writable, Writable>.Context ctx = mock(Reducer.Context.class);
      when(ctx.getConfiguration()).thenReturn(configuration);
      when(ctx.getCounter(Matchers.any(SweepCounter.class))).thenReturn(counter);
      when(ctx.nextKey()).thenReturn(true).thenReturn(false);
      when(ctx.getCurrentKey()).thenReturn(new Text(mobFile1));

      byte[] refBytes = Bytes.toBytes(mobFile1);
      long valueLength = refBytes.length;
      byte[] newValue = Bytes.add(Bytes.toBytes(valueLength), refBytes);
      KeyValue kv2 = new KeyValue(Bytes.toBytes(row), Bytes.toBytes(family), Bytes.toBytes(qf), 1,
        KeyValue.Type.Put, newValue);
      List<KeyValue> list = new ArrayList<KeyValue>();
      list.add(kv2);

      when(ctx.getValues()).thenReturn(list);

      SweepReducer reducer = new SweepReducer();
      reducer.run(ctx);
    } finally {
      lock.release();
    }
    FileStatus[] filsStatuses2 = TEST_UTIL.getTestFileSystem().listStatus(mobFamilyPath);
    String mobFile2 = filsStatuses2[0].getPath().getName();
    //new mob file is generated, old one has been archived
    assertEquals(1, filsStatuses2.length);
    assertEquals(false, mobFile2.equalsIgnoreCase(mobFile1));

    //test sequence file
    String workingPath = configuration.get(SweepJob.WORKING_VISITED_DIR_KEY);
    FileStatus[] statuses = TEST_UTIL.getTestFileSystem().listStatus(new Path(workingPath));
    Set<String> files = new TreeSet<String>();
    for (FileStatus st : statuses) {
      files.addAll(getKeyFromSequenceFile(TEST_UTIL.getTestFileSystem(),
              st.getPath(), configuration));
    }
    assertEquals(1, files.size());
    assertEquals(true, files.contains(mobFile1));
  }
}
