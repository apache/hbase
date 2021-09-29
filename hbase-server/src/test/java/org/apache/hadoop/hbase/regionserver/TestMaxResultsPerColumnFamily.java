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
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category(MediumTests.class)
public class TestMaxResultsPerColumnFamily {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMaxResultsPerColumnFamily.class);

  private static final byte [][] FAMILIES = {
    Bytes.toBytes("1"), Bytes.toBytes("2")
  };

  private static final byte [][] VALUES = {
    Bytes.toBytes("testValueOne"), Bytes.toBytes("testValueTwo")
  };

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Rule
  public TestName name = new TestName();

  @Test
  public void testSetMaxResultsPerColumnFamilySimple() throws Exception {
    TableName tableName = TableName.valueOf(name.getMethodName());
    Admin admin = UTIL.getAdmin();
    ColumnFamilyDescriptorBuilder cfBuilder0 =
    ColumnFamilyDescriptorBuilder.newBuilder(FAMILIES[0]);

    TableDescriptor tableDescriptor =
      TableDescriptorBuilder.newBuilder(tableName).setColumnFamily(cfBuilder0.build()).build();
    admin.createTable(tableDescriptor);
    try (Table table = UTIL.getConnection().getTable(tableName)) {

      for (int i = 0; i < 30000; i++) {
        byte[] ROW = Bytes.toBytes("" + i);
        Put p = new Put(ROW);

        p.addColumn(FAMILIES[0], Bytes.toBytes("" + 1), VALUES[1]);
        p.addColumn(FAMILIES[0], Bytes.toBytes("" + 2), VALUES[0]);

        table.put(p);
      }
    }

    try (Table t = UTIL.getConnection().getTable(tableName)) {
      int expected = 30000;

      Scan limits = new Scan().setReadType(Scan.ReadType.PREAD);
      limits.setMaxResultsPerColumnFamily(1);

      int count1 = countScanRows(t, limits);
      assertEquals(expected, count1);
    }
  }

  static int countScanRows(Table t, Scan scan) throws Exception {

    int count = 0;
    try(ResultScanner scanner = t.getScanner(scan)) {
      for(Result r:scanner) {
        count ++;
      }
    }
    return count;
  }
}
