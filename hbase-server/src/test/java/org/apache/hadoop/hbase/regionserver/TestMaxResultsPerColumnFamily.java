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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RestTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Category({RestTests.class, MediumTests.class})
public class TestMaxResultsPerColumnFamily {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMaxResultsPerColumnFamily.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestMaxResultsPerColumnFamily.class);

  private static final byte [][] FAMILIES = {
    Bytes.toBytes("1"), Bytes.toBytes("2")
  };

  private static final byte [][] VALUES = {
    Bytes.toBytes("testValueOne"), Bytes.toBytes("testValueTwo")
  };

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  public static byte[] toByteArray(String s) {
    return Bytes.toBytes(s);
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(3);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void tesSetMaxResultsPerColumnFamily() throws Exception {
    Admin admin = TEST_UTIL.getAdmin();
    TableName tableName = TableName.valueOf("TestScannersWithFilters1");
    if (!admin.tableExists(tableName)) {
      HTableDescriptor htd = new HTableDescriptor(tableName);
      htd.addFamily(new HColumnDescriptor(FAMILIES[0]));
      htd.addFamily(new HColumnDescriptor(FAMILIES[1]));
      admin.createTable(htd);
      Table table = TEST_UTIL.getConnection().getTable(tableName);

      for(int i = 0; i<20000; i++) {
        byte [] ROW = toByteArray("" + i);
        Put p = new Put(ROW);
        p.setDurability(Durability.SKIP_WAL);

        int rows = 4;
        if (i % 12 == 5 || i % 17 == 3) {
          rows = 2;
        }
        if (i % 15 == 4 || i % 7 == 4) {
          rows = 6;
        }

        int q1 = (i/2) + 1;
        int q2 = (i/2) + 1;
        int q3 = (i/2) + 2;

        if (i % 11 == 10 || i % 27 == 1) {
          q1 = (q1/2);
          q2 =(q2/2) + 1;
        }

        if(rows>4) {
          p.addColumn(FAMILIES[0], toByteArray(""+ q3 + rows), VALUES[1]);
          p.addColumn(FAMILIES[1], toByteArray(""+ q3 + rows), VALUES[0]);
        }
        if(rows>2) {
          p.addColumn(FAMILIES[1], toByteArray(""+ q1 + rows), VALUES[1]);
          p.addColumn(FAMILIES[1], toByteArray(""+ q2 + rows), VALUES[0]);
        }
        p.addColumn(FAMILIES[0], toByteArray(""+ q1 + rows), VALUES[0]);
        p.addColumn(FAMILIES[0], toByteArray(""+ q2 +rows), VALUES[1]);

        table.put(p);
      }
      table.close();
    }

    Table t = TEST_UTIL.getConnection().getTable(tableName);
    int expected = 20000 - 12355 + 10000 - 1235 + 1000 - 124 + 100 - 13 + 10 - 2;

    byte[] start = toByteArray("12354");
    byte[] stop = toByteArray("F");

    Scan limits = new Scan(start, stop).setReadType(Scan.ReadType.PREAD);
    limits.setMaxResultsPerColumnFamily(1);
    Scan noLimits = new Scan(start, stop).setReadType(Scan.ReadType.PREAD);

    int count1 = countScanRows(t, limits);
    int count2 = countScanRows(t, noLimits);

    assertEquals(expected, count2);
    assertEquals(count1, count2);
  }

  @Test
  public void tesSetMaxResultsPerColumnFamilyBigger() throws Exception {
    TableName tableName = TableName.valueOf("TestScannersWithFilters2");
    Admin admin = TEST_UTIL.getAdmin();
    if (!admin.tableExists(tableName)) {
      HTableDescriptor htd = new HTableDescriptor(tableName);
      htd.addFamily(new HColumnDescriptor(FAMILIES[0]));
      htd.addFamily(new HColumnDescriptor(FAMILIES[1]));
      admin.createTable(htd);
      Table table = TEST_UTIL.getConnection().getTable(tableName);

      for(int i = 0; i<100000; i++) {
        byte [] ROW = toByteArray("" + i);
        Put p = new Put(ROW);
        p.setDurability(Durability.SKIP_WAL);


        int rows = 4;
        if (i % 12 == 5 || i % 17 == 3) {
          rows = 2;
        }
        if (i % 15 == 4 || i % 7 == 4) {
          rows = 6;
        }
        if (i % 15 == 7 || i % 27 == 1) {
          rows = 8;
        }

        int q1 = (i/2);
        int q2 = (i/2) + 1;
        int q3 = (i/2) + 2;
        int q4 = (i/2) + 3;

        if (i % 11 == 10 || i % 27 == 1) {
          q1 = (q1/2);
          q2 = (q2/2) + 1;
          q3 = (q3/2) + 1;
          q4 = (q4/2) + 1;
        }

        if(rows>6) {
          p.addColumn(FAMILIES[0], toByteArray(""+ q4 + "-" + rows), VALUES[0]);
          p.addColumn(FAMILIES[1], toByteArray(""+ q4 + "-" + rows), VALUES[1]);
        }
        if(rows>4) {
          p.addColumn(FAMILIES[0], toByteArray(""+ q3 + "-" + rows), VALUES[1]);
          p.addColumn(FAMILIES[1], toByteArray(""+ q3 + "-" + rows), VALUES[0]);
        }
        if(rows>2) {
          p.addColumn(FAMILIES[1], toByteArray(""+ q1 + "-" + rows), VALUES[1]);
          p.addColumn(FAMILIES[1], toByteArray(""+ q2 + "-" + rows), VALUES[0]);
        }
        p.addColumn(FAMILIES[0], toByteArray(""+ q2 + "-" + rows), VALUES[0]);
        p.addColumn(FAMILIES[0], toByteArray(""+ q1 +"-" + rows), VALUES[1]);

        table.put(p);
      }
      table.close();
    }

    Table t = TEST_UTIL.getConnection().getTable(tableName);
    int expected = 76531 - 12355 + 7653 - 1235 + 765 - 122 + 76 - 12 + 7 - 1;

    byte[] start = toByteArray("12354");
    byte[] stop = toByteArray("76531");

    Scan limits = new Scan(start, stop).setReadType(Scan.ReadType.PREAD);
    limits.setMaxResultsPerColumnFamily(1);
    Scan noLimits = new Scan(start, stop).setReadType(Scan.ReadType.PREAD);

    int count1 = countScanRows(t, limits);
    int count2 = countScanRows(t, noLimits);

    assertEquals(expected, count2);
    assertEquals(count1, count2);
  }

  @Test
  public void tesSetMaxResultsPerColumnFamilySimpleBig() throws Exception {
    TableName tableName = TableName.valueOf("TestScannersWithFilters3");
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setInt(HConstants.MAJOR_COMPACTION_PERIOD, 0);
    Admin admin = TEST_UTIL.getAdmin();
    if (!admin.tableExists(tableName)) {
      HTableDescriptor htd = new HTableDescriptor(tableName);
      htd.addFamily(new HColumnDescriptor(FAMILIES[0]));
      htd.addFamily(new HColumnDescriptor(FAMILIES[1]));
      admin.createTable(htd);
      Table table = TEST_UTIL.getConnection().getTable(tableName);

      for(int i = 0; i<100000; i++) {
        byte [] ROW = toByteArray("" + i);
        Put p = new Put(ROW);
        p.setDurability(Durability.SKIP_WAL);

        int q1 = 1;
        int q2 = 2;

        p.addColumn(FAMILIES[1], toByteArray(""+ q1), VALUES[1]);
        p.addColumn(FAMILIES[1], toByteArray(""+ q2), VALUES[0]);

        p.addColumn(FAMILIES[0], toByteArray(""+ q2), VALUES[0]);
        p.addColumn(FAMILIES[0], toByteArray(""+ q1), VALUES[1]);

        table.put(p);
      }

      table.close();
    }

    Table t = TEST_UTIL.getConnection().getTable(tableName);
    int expected = 76531 - 12354 + 7653 - 1235 + 765 - 123 + 76 - 12 + 7 - 1;

    byte[] start = toByteArray("12354");
    byte[] stop = toByteArray("76531");

    Scan limits = new Scan(start, stop).setReadType(Scan.ReadType.PREAD);
    limits.setMaxResultsPerColumnFamily(1);

    int count1 = countScanRows(t, limits);
    assertEquals(expected, count1);
  }

  static int countScanRows(Table t, Scan scan) throws Exception {

    int count = 0;
    try(ResultScanner scanner = t.getScanner(scan)) {
      for(Result r:scanner) {
        count ++;
        r.advance();
      }
    }
    return count;
  }
}
