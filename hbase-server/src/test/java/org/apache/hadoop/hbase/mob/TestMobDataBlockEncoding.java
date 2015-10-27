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
package org.apache.hadoop.hbase.mob;

import java.util.Random;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestMobDataBlockEncoding {

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static byte [] row1 = Bytes.toBytes("row1");
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

    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  public void setUp(long threshold, String TN, DataBlockEncoding encoding)
      throws Exception {
    desc = new HTableDescriptor(TableName.valueOf(TN));
    hcd = new HColumnDescriptor(family);
    hcd.setMobEnabled(true);
    hcd.setMobThreshold(threshold);
    hcd.setMaxVersions(4);
    hcd.setDataBlockEncoding(encoding);
    desc.addFamily(hcd);
    admin = TEST_UTIL.getHBaseAdmin();
    admin.createTable(desc);
    table = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration())
            .getTable(TableName.valueOf(TN));
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

  @Test
  public void testDataBlockEncoding() throws Exception {
    for (DataBlockEncoding encoding : DataBlockEncoding.values()) {
      testDataBlockEncoding(encoding);
    }
  }

  public void testDataBlockEncoding(DataBlockEncoding encoding) throws Exception {
    String TN = "testDataBlockEncoding" + encoding;
    setUp(defaultThreshold, TN, encoding);
    long ts1 = System.currentTimeMillis();
    long ts2 = ts1 + 1;
    long ts3 = ts1 + 2;
    byte[] value = generateMobValue((int) defaultThreshold + 1);

    Put put1 = new Put(row1);
    put1.addColumn(family, qf1, ts3, value);
    put1.addColumn(family, qf2, ts2, value);
    put1.addColumn(family, qf3, ts1, value);
    table.put(put1);
    admin.flush(TableName.valueOf(TN));

    Scan scan = new Scan();
    scan.setMaxVersions(4);
    MobTestUtil.assertCellsValue(table, scan, value, 3);
  }
}
