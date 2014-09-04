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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestMobStoreScanner {

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static byte [] row1 = Bytes.toBytes("row1");
  private final static byte [] family = Bytes.toBytes("family");
  private final static byte [] qf1 = Bytes.toBytes("qualifier1");
  private final static byte [] qf2 = Bytes.toBytes("qualifier2");
  protected final byte[] qf3 = Bytes.toBytes("qualifier3");
  private static HTable table;
  private static HBaseAdmin admin;
  private static HColumnDescriptor hcd;
  private static HTableDescriptor desc;
  private static Random random = new Random();
  private static long defaultThreshold = 10;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt("hbase.master.info.port", 0);
    TEST_UTIL.getConfiguration().setBoolean("hbase.regionserver.info.port.auto", true);

    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  public void setUp(long threshold, String TN) throws Exception {
    desc = new HTableDescriptor(TableName.valueOf(TN));
    hcd = new HColumnDescriptor(family);
    hcd.setValue(MobConstants.IS_MOB, Bytes.toBytes(Boolean.TRUE));
    hcd.setValue(MobConstants.MOB_THRESHOLD, Bytes.toBytes(threshold));
    hcd.setMaxVersions(4);
    desc.addFamily(hcd);
    admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    admin.createTable(desc);
    table = new HTable(TEST_UTIL.getConfiguration(), TN);
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
  }

  @Test
  public void testReversedMobStoreScanner() throws Exception {
	  testGetFromFiles(true);
	  testGetFromMemStore(true);
    testGetReferences(true);
    testMobThreshold(true);
  }

  public void testGetFromFiles(boolean reversed) throws Exception {
    String TN = "testGetFromFiles" + reversed;
    setUp(defaultThreshold, TN);
    long ts1 = System.currentTimeMillis();
    long ts2 = ts1 + 1;
    long ts3 = ts1 + 2;
    byte [] value = generateMobValue((int)defaultThreshold+1);

    Put put1 = new Put(row1);
    put1.add(family, qf1, ts3, value);
    put1.add(family, qf2, ts2, value);
    put1.add(family, qf3, ts1, value);
    table.put(put1);

    table.flushCommits();
    admin.flush(TN);

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

  public void testGetFromMemStore(boolean reversed) throws Exception {
    String TN = "testGetFromMemStore" + reversed;
    setUp(defaultThreshold, TN);
    long ts1 = System.currentTimeMillis();
    long ts2 = ts1 + 1;
    long ts3 = ts1 + 2;
    byte [] value = generateMobValue((int)defaultThreshold+1);;

    Put put1 = new Put(row1);
    put1.add(family, qf1, ts3, value);
    put1.add(family, qf2, ts2, value);
    put1.add(family, qf3, ts1, value);
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

  public void testGetReferences(boolean reversed) throws Exception {
    String TN = "testGetReferences" + reversed;
    setUp(defaultThreshold, TN);
    long ts1 = System.currentTimeMillis();
    long ts2 = ts1 + 1;
    long ts3 = ts1 + 2;
    byte [] value = generateMobValue((int)defaultThreshold+1);;

    Put put1 = new Put(row1);
    put1.add(family, qf1, ts3, value);
    put1.add(family, qf2, ts2, value);
    put1.add(family, qf3, ts1, value);
    table.put(put1);

    table.flushCommits();
    admin.flush(TN);

    Scan scan = new Scan();
    setScan(scan, reversed, true);

    ResultScanner results = table.getScanner(scan);
    int count = 0;
    for (Result res : results) {
      List<Cell> cells = res.listCells();
      for(Cell cell : cells) {
        // Verify the value
        assertIsMobReference(cell, row1, family, value, TN);
        count++;
      }
    }
    results.close();
    Assert.assertEquals(3, count);
  }

  public void testMobThreshold(boolean reversed) throws Exception {
    String TN = "testMobThreshold" + reversed;
    setUp(defaultThreshold, TN);
    byte [] valueLess = generateMobValue((int)defaultThreshold-1);
    byte [] valueEqual = generateMobValue((int)defaultThreshold);
    byte [] valueGreater = generateMobValue((int)defaultThreshold+1);
    long ts1 = System.currentTimeMillis();
    long ts2 = ts1 + 1;
    long ts3 = ts1 + 2;

    Put put1 = new Put(row1);
    put1.add(family, qf1, ts3, valueLess);
    put1.add(family, qf2, ts2, valueEqual);
    put1.add(family, qf3, ts1, valueGreater);
    table.put(put1);

    table.flushCommits();
    admin.flush(TN);

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
    assertIsMobReference(cellGreater, row1, family, valueGreater, TN);
    results.close();
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
      byte[] value, String TN) throws IOException {
    Assert.assertEquals(Bytes.toString(row),
        Bytes.toString(CellUtil.cloneRow(cell)));
    Assert.assertEquals(Bytes.toString(family),
        Bytes.toString(CellUtil.cloneFamily(cell)));
    Assert.assertFalse(Bytes.toString(value).equals(
        Bytes.toString(CellUtil.cloneValue(cell))));
    byte[] referenceValue = CellUtil.cloneValue(cell);
    String fileName = Bytes.toString(referenceValue, 8, referenceValue.length-8);
    Path mobFamilyPath;
    mobFamilyPath = new Path(MobUtils.getMobRegionPath(TEST_UTIL.getConfiguration(),
        TableName.valueOf(TN)), hcd.getNameAsString());
    Path targetPath = new Path(mobFamilyPath, fileName);
    FileSystem fs = FileSystem.get(TEST_UTIL.getConfiguration());
    Assert.assertTrue(fs.exists(targetPath));
  }
}
