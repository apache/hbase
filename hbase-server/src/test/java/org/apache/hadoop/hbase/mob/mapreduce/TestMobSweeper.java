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

import java.io.IOException;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestMobSweeper {
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private String tableName;
  private final static String row = "row_";
  private final static String family = "family";
  private final static String column = "column";
  private static Table table;
  private static BufferedMutator bufMut;
  private static Admin admin;

  private Random random = new Random();
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt("hbase.master.info.port", 0);
    TEST_UTIL.getConfiguration().setBoolean("hbase.regionserver.info.port.auto", true);
    TEST_UTIL.getConfiguration().setInt("hbase.hstore.compaction.min", 15); // avoid major compactions
    TEST_UTIL.getConfiguration().setInt("hbase.hstore.compaction.max", 30); // avoid major compactions
    TEST_UTIL.getConfiguration().setInt("hfile.format.version", 3);

    TEST_UTIL.startMiniCluster();

    TEST_UTIL.startMiniMapReduceCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    TEST_UTIL.shutdownMiniMapReduceCluster();
  }

  @SuppressWarnings("deprecation")
  @Before
  public void setUp() throws Exception {
    long tid = System.currentTimeMillis();
    tableName = "testSweeper" + tid;
    HTableDescriptor desc = new HTableDescriptor(tableName);
    HColumnDescriptor hcd = new HColumnDescriptor(family);
    hcd.setMobEnabled(true);
    hcd.setMobThreshold(3L);
    hcd.setMaxVersions(4);
    desc.addFamily(hcd);

    admin = TEST_UTIL.getHBaseAdmin();
    admin.createTable(desc);
    Connection c = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
    TableName tn = TableName.valueOf(tableName);
    table = c.getTable(tn);
    bufMut = c.getBufferedMutator(tn);
  }

  @After
  public void tearDown() throws Exception {
    admin.disableTable(TableName.valueOf(tableName));
    admin.deleteTable(TableName.valueOf(tableName));
    admin.close();
  }

  private Path getMobFamilyPath(Configuration conf, String tableNameStr,
                                String familyName) {
    Path p = new Path(MobUtils.getMobRegionPath(conf, TableName.valueOf(tableNameStr)),
            familyName);
    return p;
  }

  private String mergeString(Set<String> set) {
    StringBuilder sb = new StringBuilder();
    for (String s : set)
      sb.append(s);
    return sb.toString();
  }

  private void generateMobTable(Admin admin, BufferedMutator table, String tableName, int count,
    int flushStep) throws IOException, InterruptedException {
    if (count <= 0 || flushStep <= 0)
      return;
    int index = 0;
    for (int i = 0; i < count; i++) {
      byte[] mobVal = new byte[101*1024];
      random.nextBytes(mobVal);

      Put put = new Put(Bytes.toBytes(row + i));
      put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), mobVal);
      table.mutate(put);
      if (index++ % flushStep == 0) {
        table.flush();
        admin.flush(TableName.valueOf(tableName));
      }
    }
    table.flush();
    admin.flush(TableName.valueOf(tableName));
  }

  @Test
  public void testSweeper() throws Exception {
    int count = 10;
    //create table and generate 10 mob files
    generateMobTable(admin, bufMut, tableName, count, 1);
    //get mob files
    Path mobFamilyPath = getMobFamilyPath(TEST_UTIL.getConfiguration(), tableName, family);
    FileStatus[] fileStatuses = TEST_UTIL.getTestFileSystem().listStatus(mobFamilyPath);
    // mobFileSet0 stores the original mob files
    TreeSet<String> mobFilesSet = new TreeSet<String>();
    for (FileStatus status : fileStatuses) {
      mobFilesSet.add(status.getPath().getName());
    }

    //scan the table, retreive the references
    Scan scan = new Scan();
    scan.setAttribute(MobConstants.MOB_SCAN_RAW, Bytes.toBytes(Boolean.TRUE));
    scan.setAttribute(MobConstants.MOB_SCAN_REF_ONLY, Bytes.toBytes(Boolean.TRUE));
    ResultScanner rs = table.getScanner(scan);
    TreeSet<String> mobFilesScanned = new TreeSet<String>();
    for (Result res : rs) {
      byte[] valueBytes = res.getValue(Bytes.toBytes(family),
          Bytes.toBytes(column));
      mobFilesScanned.add(Bytes.toString(valueBytes, Bytes.SIZEOF_INT,
          valueBytes.length - Bytes.SIZEOF_INT));
    }
    //there should be 10 mob files
    assertEquals(10, mobFilesScanned.size());
    //check if we store the correct reference of mob files
    assertEquals(mergeString(mobFilesSet), mergeString(mobFilesScanned));

    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setLong(SweepJob.MOB_SWEEP_JOB_DELAY, 24 * 60 * 60 * 1000);

    String[] args = new String[2];
    args[0] = tableName;
    args[1] = family;
    assertEquals(0, ToolRunner.run(conf, new Sweeper(), args));

    mobFamilyPath = getMobFamilyPath(TEST_UTIL.getConfiguration(), tableName, family);
    fileStatuses = TEST_UTIL.getTestFileSystem().listStatus(mobFamilyPath);
    mobFilesSet = new TreeSet<String>();
    for (FileStatus status : fileStatuses) {
      mobFilesSet.add(status.getPath().getName());
    }
    assertEquals(10, mobFilesSet.size());

    scan = new Scan();
    scan.setAttribute(MobConstants.MOB_SCAN_RAW, Bytes.toBytes(Boolean.TRUE));
    scan.setAttribute(MobConstants.MOB_SCAN_REF_ONLY, Bytes.toBytes(Boolean.TRUE));
    rs = table.getScanner(scan);
    TreeSet<String> mobFilesScannedAfterJob = new TreeSet<String>();
    for (Result res : rs) {
      byte[] valueBytes = res.getValue(Bytes.toBytes(family), Bytes.toBytes(
          column));
      mobFilesScannedAfterJob.add(Bytes.toString(valueBytes, Bytes.SIZEOF_INT,
          valueBytes.length - Bytes.SIZEOF_INT));
    }
    assertEquals(10, mobFilesScannedAfterJob.size());

    fileStatuses = TEST_UTIL.getTestFileSystem().listStatus(mobFamilyPath);
    mobFilesSet = new TreeSet<String>();
    for (FileStatus status : fileStatuses) {
      mobFilesSet.add(status.getPath().getName());
    }
    assertEquals(10, mobFilesSet.size());
    assertEquals(true, mobFilesScannedAfterJob.iterator().next()
            .equalsIgnoreCase(mobFilesSet.iterator().next()));
  }

  private void testCompactionDelaySweeperInternal(Table table, BufferedMutator bufMut, String tableName)
    throws Exception {
    int count = 10;
    //create table and generate 10 mob files
    generateMobTable(admin, bufMut, tableName, count, 1);
    //get mob files
    Path mobFamilyPath = getMobFamilyPath(TEST_UTIL.getConfiguration(), tableName, family);
    FileStatus[] fileStatuses = TEST_UTIL.getTestFileSystem().listStatus(mobFamilyPath);
    // mobFileSet0 stores the orignal mob files
    TreeSet<String> mobFilesSet = new TreeSet<String>();
    for (FileStatus status : fileStatuses) {
      mobFilesSet.add(status.getPath().getName());
    }

    //scan the table, retreive the references
    Scan scan = new Scan();
    scan.setAttribute(MobConstants.MOB_SCAN_RAW, Bytes.toBytes(Boolean.TRUE));
    scan.setAttribute(MobConstants.MOB_SCAN_REF_ONLY, Bytes.toBytes(Boolean.TRUE));
    ResultScanner rs = table.getScanner(scan);
    TreeSet<String> mobFilesScanned = new TreeSet<String>();
    for (Result res : rs) {
      byte[] valueBytes = res.getValue(Bytes.toBytes(family),
              Bytes.toBytes(column));
      mobFilesScanned.add(Bytes.toString(valueBytes, Bytes.SIZEOF_INT,
          valueBytes.length - Bytes.SIZEOF_INT));
    }
    //there should be 10 mob files
    assertEquals(10, mobFilesScanned.size());
    //check if we store the correct reference of mob files
    assertEquals(mergeString(mobFilesSet), mergeString(mobFilesScanned));

    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setLong(SweepJob.MOB_SWEEP_JOB_DELAY, 0);
    String[] args = new String[2];
    args[0] = tableName;
    args[1] = family;
    assertEquals(0, ToolRunner.run(conf, new Sweeper(), args));

    mobFamilyPath = getMobFamilyPath(TEST_UTIL.getConfiguration(), tableName, family);
    fileStatuses = TEST_UTIL.getTestFileSystem().listStatus(mobFamilyPath);
    mobFilesSet = new TreeSet<String>();
    for (FileStatus status : fileStatuses) {
      mobFilesSet.add(status.getPath().getName());
    }
    assertEquals(1, mobFilesSet.size());

    scan = new Scan();
    scan.setAttribute(MobConstants.MOB_SCAN_RAW, Bytes.toBytes(Boolean.TRUE));
    scan.setAttribute(MobConstants.MOB_SCAN_REF_ONLY, Bytes.toBytes(Boolean.TRUE));
    rs = table.getScanner(scan);
    TreeSet<String> mobFilesScannedAfterJob = new TreeSet<String>();
    for (Result res : rs) {
      byte[] valueBytes = res.getValue(Bytes.toBytes(family), Bytes.toBytes(
              column));
      mobFilesScannedAfterJob.add(Bytes.toString(valueBytes, Bytes.SIZEOF_INT,
          valueBytes.length - Bytes.SIZEOF_INT));
    }
    assertEquals(1, mobFilesScannedAfterJob.size());

    fileStatuses = TEST_UTIL.getTestFileSystem().listStatus(mobFamilyPath);
    mobFilesSet = new TreeSet<String>();
    for (FileStatus status : fileStatuses) {
      mobFilesSet.add(status.getPath().getName());
    }
    assertEquals(1, mobFilesSet.size());
    assertEquals(true, mobFilesScannedAfterJob.iterator().next()
            .equalsIgnoreCase(mobFilesSet.iterator().next()));
  }

  @Test
  public void testCompactionDelaySweeper() throws Exception {
    testCompactionDelaySweeperInternal(table, bufMut, tableName);
  }

  @Test
  public void testCompactionDelaySweeperWithNamespace() throws Exception {
    // create a table with namespace
    NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create("ns").build();
    admin.createNamespace(namespaceDescriptor);
    String tableNameAsString = "ns:testSweeperWithNamespace";
    TableName tableName = TableName.valueOf(tableNameAsString);
    HTableDescriptor desc = new HTableDescriptor(tableName);
    HColumnDescriptor hcd = new HColumnDescriptor(family);
    hcd.setMobEnabled(true);
    hcd.setMobThreshold(3L);
    hcd.setMaxVersions(4);
    desc.addFamily(hcd);
    admin.createTable(desc);
    Connection c = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
    BufferedMutator bufMut = c.getBufferedMutator(tableName);
    Table table = c.getTable(tableName);
    testCompactionDelaySweeperInternal(table, bufMut, tableNameAsString);
    table.close();
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
    admin.deleteNamespace("ns");
  }
}
