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
package org.apache.hadoop.hbase.regionserver.storefiletracker;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNameTestRule;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestStoreFileListFilePrinter {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestStoreFileListFilePrinter.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  @Rule
  public final TableNameTestRule tableName = new TableNameTestRule();
  public static byte[] family = Bytes.toBytes("F");;

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testPrintWithDirectPath() throws IOException {
    createTable();
    TableName tn = tableName.getTableName();
    String fileName = getStoreFileName(tn, family);

    String cf = new String(family);

    Configuration conf = UTIL.getConfiguration();
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(stream);
    System.setOut(ps);
    StoreFileListFilePrettyPrinter sftPrinter = new StoreFileListFilePrettyPrinter(conf);

    FileSystem fs = Iterables.getOnlyElement(UTIL.getMiniHBaseCluster().getRegions(tn))
      .getRegionFileSystem().getFileSystem();
    Path regionPath = Iterables.getOnlyElement(UTIL.getMiniHBaseCluster().getRegions(tn))
      .getRegionFileSystem().getRegionDir();
    Path cfPath = new Path(regionPath, cf);
    Path path = new Path(cfPath, StoreFileListFile.TRACK_FILE_DIR);
    RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(path, false);
    while (iterator.hasNext()) {
      LocatedFileStatus lfs = iterator.next();
      if (lfs.getPath().getName().contains("f2") || lfs.getPath().getName().contains("f1")) {
        String[] argsF = { "-f", lfs.getPath().toString() };
        sftPrinter.run(argsF);
        String result = new String(stream.toByteArray());
        String expect = fileName + "\n";
        assertEquals(expect, result);
      }
    }
  }

  @Test
  public void testPrintWithRegionOption() throws IOException {
    createTable();
    String cf = new String(family);
    TableName tn = tableName.getTableName();
    String fileName = getStoreFileName(tn, family);

    List<HRegion> regions = UTIL.getMiniHBaseCluster().getRegions(tableName.getTableName());
    String rn = regions.get(0).getRegionInfo().getEncodedName();
    String table = tableName.getTableName().toString();

    Configuration conf = UTIL.getConfiguration();
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(stream);
    System.setOut(ps);
    StoreFileListFilePrettyPrinter sftPrinter = new StoreFileListFilePrettyPrinter(conf);
    String[] args = { "-r", rn, "-t", table, "-cf", cf };
    sftPrinter.run(args);
    String result = new String(stream.toByteArray());

    FileSystem fs = Iterables.getOnlyElement(UTIL.getMiniHBaseCluster().getRegions(tn))
      .getRegionFileSystem().getFileSystem();
    Path regionPath = Iterables.getOnlyElement(UTIL.getMiniHBaseCluster().getRegions(tn))
      .getRegionFileSystem().getRegionDir();
    Path cfPath = new Path(regionPath, cf);
    Path path = new Path(cfPath, StoreFileListFile.TRACK_FILE_DIR);
    RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(path, false);
    String expect = "";
    while (iterator.hasNext()) {
      LocatedFileStatus lfs = iterator.next();
      if (lfs.getPath().getName().contains("f2") || lfs.getPath().getName().contains("f1")) {
        expect = expect + "Printing contents for file " + lfs.getPath() + "\n" + fileName + "\n";
      }
    }
    assertEquals(expect, result);
  }

  private String getStoreFileName(TableName table, byte[] family) {
    return Iterables
      .getOnlyElement(Iterables.getOnlyElement(UTIL.getMiniHBaseCluster().getRegions(table))
        .getStore(family).getStorefiles())
      .getPath().getName();
  }

  private void createTable() throws IOException {
    TableName tn = tableName.getTableName();
    byte[] row = Bytes.toBytes("row");
    byte[] qualifier = Bytes.toBytes("qualifier");
    byte[] value = Bytes.toBytes("value");
    TableDescriptor td = TableDescriptorBuilder.newBuilder(tn)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(family))
      .setValue(StoreFileTrackerFactory.TRACKER_IMPL, StoreFileTrackerFactory.Trackers.FILE.name())
      .build();
    UTIL.getAdmin().createTable(td);
    try (Table table = UTIL.getConnection().getTable(tn)) {
      table.put(new Put(row).addColumn(family, qualifier, value));
    }
    UTIL.flush(tn);
  }
}
