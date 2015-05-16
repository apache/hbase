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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestMobStoreScanner {

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static byte [] row1 = Bytes.toBytes("row1");
  private final static byte [] row2 = Bytes.toBytes("row2");
  private final static byte [] family = Bytes.toBytes("family");
  private final static byte [] qf1 = Bytes.toBytes("qualifier1");
  private final static byte [] qf2 = Bytes.toBytes("qualifier2");
  protected final byte[] qf3 = Bytes.toBytes("qualifier3");
  private static Table table;
  private static HBaseAdmin admin;
  private static HColumnDescriptor hcd;
  private static HTableDescriptor desc;
  private static Random random = new Random();
  private static long defaultThreshold = 10;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt("hbase.master.info.port", 0);
    TEST_UTIL.getConfiguration().setBoolean("hbase.regionserver.info.port.auto", true);
    TEST_UTIL.getConfiguration().setInt("hbase.client.keyvalue.maxsize", 100*1024*1024);

    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  public void setUp(long threshold, TableName tn) throws Exception {
    desc = new HTableDescriptor(tn);
    hcd = new HColumnDescriptor(family);
    hcd.setMobEnabled(true);
    hcd.setMobThreshold(threshold);
    hcd.setMaxVersions(4);
    desc.addFamily(hcd);
    admin = TEST_UTIL.getHBaseAdmin();
    admin.createTable(desc);
    table = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration())
            .getTable(tn);
  }

  /**
   * Generate the mob value.
   *
   * @param size the size of the value
   * @return the mob value generated
   */
  private static byte[] generateMobValue(int size) {
    byte[] mobVal = new byte[size];
    random.nextBytes(mobVal);
    return mobVal;
  }

  /**
   * Set the scan attribute
   *
   * @param reversed if true, scan will be backward order
   * @param mobScanRaw if true, scan will get the mob reference
   * @return this
   */
  public void setScan(Scan scan, boolean reversed, boolean mobScanRaw) {
    scan.setReversed(reversed);
    scan.setMaxVersions(4);
    if(mobScanRaw) {
      scan.setAttribute(MobConstants.MOB_SCAN_RAW, Bytes.toBytes(Boolean.TRUE));
    }
  }

  @Test
  public void testMobStoreScanner() throws Exception {
	  testGetFromFiles(false);
	  testGetFromMemStore(false);
    testGetReferences(false);
    testMobThreshold(false);
    testGetFromArchive(false);
  }

  @Test
  public void testReversedMobStoreScanner() throws Exception {
	  testGetFromFiles(true);
	  testGetFromMemStore(true);
    testGetReferences(true);
    testMobThreshold(true);
    testGetFromArchive(true);
  }

  @Test(timeout=60000)
  public void testGetMassive() throws Exception {
    setUp(defaultThreshold, TableName.valueOf("testGetMassive"));

    // Put some data 5 10, 15, 20  mb ok  (this would be right below protobuf default max size of 64MB.
    // 25, 30, 40 fail.  these is above protobuf max size of 64MB
    byte[] bigValue = new byte[25*1024*1024];

    Put put = new Put(row1);
    put.addColumn(family, qf1, bigValue);
    put.addColumn(family, qf2, bigValue);
    put.addColumn(family, qf3, bigValue);
    table.put(put);

    Get g = new Get(row1);
    Result r = table.get(g);
    // should not have blown up.
  }

  @Test
  public void testReadPt() throws Exception {
    TableName tn = TableName.valueOf("testReadPt");
    setUp(0L, tn);
    long ts = System.currentTimeMillis();
    byte[] value1 = Bytes.toBytes("value1");
    Put put1 = new Put(row1);
    put1.addColumn(family, qf1, ts, value1);
    table.put(put1);
    Put put2 = new Put(row2);
    byte[] value2 = Bytes.toBytes("value2");
    put2.addColumn(family, qf1, ts, value2);
    table.put(put2);

    Scan scan = new Scan();
    scan.setCaching(1);
    ResultScanner rs = table.getScanner(scan);

    Put put3 = new Put(row1);
    byte[] value3 = Bytes.toBytes("value3");
    put3.addColumn(family, qf1, ts, value3);
    table.put(put3);
    Put put4 = new Put(row2);
    byte[] value4 = Bytes.toBytes("value4");
    put4.addColumn(family, qf1, ts, value4);
    table.put(put4);
    Result result = rs.next();
    Cell cell = result.getColumnLatestCell(family, qf1);
    Assert.assertEquals("value1", Bytes.toString(cell.getValue()));

    admin.flush(tn);
    result = rs.next();
    cell = result.getColumnLatestCell(family, qf1);
    Assert.assertEquals("value2", Bytes.toString(cell.getValue()));
  }

  private void testGetFromFiles(boolean reversed) throws Exception {
    TableName tn = TableName.valueOf("testGetFromFiles" + reversed);
    setUp(defaultThreshold, tn);
    long ts1 = System.currentTimeMillis();
    long ts2 = ts1 + 1;
    long ts3 = ts1 + 2;
    byte [] value = generateMobValue((int)defaultThreshold+1);

    Put put1 = new Put(row1);
    put1.addColumn(family, qf1, ts3, value);
    put1.addColumn(family, qf2, ts2, value);
    put1.addColumn(family, qf3, ts1, value);
    table.put(put1);

    admin.flush(tn);

    Scan scan = new Scan();
    setScan(scan, reversed, false);

    ResultScanner results = table.getScanner(scan);
    int count = 0;
    for (Result res : results) {
      List<Cell> cells = res.listCells();
      for(Cell cell : cells) {
        // Verify the value
        Assert.assertEquals(Bytes.toString(value),
            Bytes.toString(CellUtil.cloneValue(cell)));
        count++;
      }
    }
    results.close();
    Assert.assertEquals(3, count);
  }

  private void testGetFromMemStore(boolean reversed) throws Exception {
    setUp(defaultThreshold, TableName.valueOf("testGetFromMemStore" + reversed));
    long ts1 = System.currentTimeMillis();
    long ts2 = ts1 + 1;
    long ts3 = ts1 + 2;
    byte [] value = generateMobValue((int)defaultThreshold+1);;

    Put put1 = new Put(row1);
    put1.addColumn(family, qf1, ts3, value);
    put1.addColumn(family, qf2, ts2, value);
    put1.addColumn(family, qf3, ts1, value);
    table.put(put1);

    Scan scan = new Scan();
    setScan(scan, reversed, false);

    ResultScanner results = table.getScanner(scan);
    int count = 0;
    for (Result res : results) {
      List<Cell> cells = res.listCells();
      for(Cell cell : cells) {
        // Verify the value
        Assert.assertEquals(Bytes.toString(value),
            Bytes.toString(CellUtil.cloneValue(cell)));
        count++;
      }
    }
    results.close();
    Assert.assertEquals(3, count);
  }

  private void testGetReferences(boolean reversed) throws Exception {
    TableName tn = TableName.valueOf("testGetReferences" + reversed);
    setUp(defaultThreshold, tn);
    long ts1 = System.currentTimeMillis();
    long ts2 = ts1 + 1;
    long ts3 = ts1 + 2;
    byte [] value = generateMobValue((int)defaultThreshold+1);;

    Put put1 = new Put(row1);
    put1.addColumn(family, qf1, ts3, value);
    put1.addColumn(family, qf2, ts2, value);
    put1.addColumn(family, qf3, ts1, value);
    table.put(put1);

    admin.flush(tn);

    Scan scan = new Scan();
    setScan(scan, reversed, true);

    ResultScanner results = table.getScanner(scan);
    int count = 0;
    for (Result res : results) {
      List<Cell> cells = res.listCells();
      for(Cell cell : cells) {
        // Verify the value
        assertIsMobReference(cell, row1, family, value, tn);
        count++;
      }
    }
    results.close();
    Assert.assertEquals(3, count);
  }

  private void testMobThreshold(boolean reversed) throws Exception {
    TableName tn = TableName.valueOf("testMobThreshold" + reversed);
    setUp(defaultThreshold, tn);
    byte [] valueLess = generateMobValue((int)defaultThreshold-1);
    byte [] valueEqual = generateMobValue((int)defaultThreshold);
    byte [] valueGreater = generateMobValue((int)defaultThreshold+1);
    long ts1 = System.currentTimeMillis();
    long ts2 = ts1 + 1;
    long ts3 = ts1 + 2;

    Put put1 = new Put(row1);
    put1.addColumn(family, qf1, ts3, valueLess);
    put1.addColumn(family, qf2, ts2, valueEqual);
    put1.addColumn(family, qf3, ts1, valueGreater);
    table.put(put1);

    admin.flush(tn);

    Scan scan = new Scan();
    setScan(scan, reversed, true);

    Cell cellLess= null;
    Cell cellEqual = null;
    Cell cellGreater = null;
    ResultScanner results = table.getScanner(scan);
    int count = 0;
    for (Result res : results) {
      List<Cell> cells = res.listCells();
      for(Cell cell : cells) {
        // Verify the value
        String qf = Bytes.toString(CellUtil.cloneQualifier(cell));
        if(qf.equals(Bytes.toString(qf1))) {
          cellLess = cell;
        }
        if(qf.equals(Bytes.toString(qf2))) {
          cellEqual = cell;
        }
        if(qf.equals(Bytes.toString(qf3))) {
          cellGreater = cell;
        }
        count++;
      }
    }
    Assert.assertEquals(3, count);
    assertNotMobReference(cellLess, row1, family, valueLess);
    assertNotMobReference(cellEqual, row1, family, valueEqual);
    assertIsMobReference(cellGreater, row1, family, valueGreater, tn);
    results.close();
  }

  private void testGetFromArchive(boolean reversed) throws Exception {
    TableName tn = TableName.valueOf("testGetFromArchive" + reversed);
    setUp(defaultThreshold, tn);
    long ts1 = System.currentTimeMillis();
    long ts2 = ts1 + 1;
    long ts3 = ts1 + 2;
    byte [] value = generateMobValue((int)defaultThreshold+1);;
    // Put some data
    Put put1 = new Put(row1);
    put1.addColumn(family, qf1, ts3, value);
    put1.addColumn(family, qf2, ts2, value);
    put1.addColumn(family, qf3, ts1, value);
    table.put(put1);

    admin.flush(tn);

    // Get the files in the mob path
    Path mobFamilyPath;
    mobFamilyPath = new Path(MobUtils.getMobRegionPath(TEST_UTIL.getConfiguration(), tn),
      hcd.getNameAsString());
    FileSystem fs = FileSystem.get(TEST_UTIL.getConfiguration());
    FileStatus[] files = fs.listStatus(mobFamilyPath);

    // Get the archive path
    Path rootDir = FSUtils.getRootDir(TEST_UTIL.getConfiguration());
    Path tableDir = FSUtils.getTableDir(rootDir, tn);
    HRegionInfo regionInfo = MobUtils.getMobRegionInfo(tn);
    Path storeArchiveDir = HFileArchiveUtil.getStoreArchivePath(TEST_UTIL.getConfiguration(),
        regionInfo, tableDir, family);

    // Move the files from mob path to archive path
    fs.mkdirs(storeArchiveDir);
    int fileCount = 0;
    for(FileStatus file : files) {
      fileCount++;
      Path filePath = file.getPath();
      Path src = new Path(mobFamilyPath, filePath.getName());
      Path dst = new Path(storeArchiveDir, filePath.getName());
      fs.rename(src, dst);
    }

    // Verify the moving success
    FileStatus[] files1 = fs.listStatus(mobFamilyPath);
    Assert.assertEquals(0, files1.length);
    FileStatus[] files2 = fs.listStatus(storeArchiveDir);
    Assert.assertEquals(fileCount, files2.length);

    // Scan from archive
    Scan scan = new Scan();
    setScan(scan, reversed, false);
    ResultScanner results = table.getScanner(scan);
    int count = 0;
    for (Result res : results) {
      List<Cell> cells = res.listCells();
      for(Cell cell : cells) {
        // Verify the value
        Assert.assertEquals(Bytes.toString(value),
            Bytes.toString(CellUtil.cloneValue(cell)));
        count++;
      }
    }
    results.close();
    Assert.assertEquals(3, count);
  }

  /**
   * Assert the value is not store in mob.
   */
  private static void assertNotMobReference(Cell cell, byte[] row, byte[] family,
      byte[] value) throws IOException {
    Assert.assertEquals(Bytes.toString(row),
        Bytes.toString(CellUtil.cloneRow(cell)));
    Assert.assertEquals(Bytes.toString(family),
        Bytes.toString(CellUtil.cloneFamily(cell)));
    Assert.assertTrue(Bytes.toString(value).equals(
        Bytes.toString(CellUtil.cloneValue(cell))));
  }

  /**
   * Assert the value is store in mob.
   */
  private static void assertIsMobReference(Cell cell, byte[] row, byte[] family,
      byte[] value, TableName tn) throws IOException {
    Assert.assertEquals(Bytes.toString(row),
        Bytes.toString(CellUtil.cloneRow(cell)));
    Assert.assertEquals(Bytes.toString(family),
        Bytes.toString(CellUtil.cloneFamily(cell)));
    Assert.assertFalse(Bytes.toString(value).equals(
        Bytes.toString(CellUtil.cloneValue(cell))));
    byte[] referenceValue = CellUtil.cloneValue(cell);
    String fileName = Bytes.toString(referenceValue, Bytes.SIZEOF_INT,
        referenceValue.length - Bytes.SIZEOF_INT);
    int valLen = Bytes.toInt(referenceValue, 0, Bytes.SIZEOF_INT);
    Assert.assertEquals(value.length, valLen);
    Path mobFamilyPath;
    mobFamilyPath = new Path(MobUtils.getMobRegionPath(TEST_UTIL.getConfiguration(),
        tn), hcd.getNameAsString());
    Path targetPath = new Path(mobFamilyPath, fileName);
    FileSystem fs = FileSystem.get(TEST_UTIL.getConfiguration());
    Assert.assertTrue(fs.exists(targetPath));
  }
}
