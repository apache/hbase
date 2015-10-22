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
package org.apache.hadoop.hbase.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category(LargeTests.class)
public class TestResultSizeEstimation {

  final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  final static int TAG_DATA_SIZE = 2048;
  final static int SCANNER_DATA_LIMIT = TAG_DATA_SIZE + 256;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    // Need HFileV3
    conf.setInt(HFile.FORMAT_VERSION_KEY, HFile.MIN_FORMAT_VERSION_WITH_TAGS);
    // effectively limit max result size to one entry if it has tags
    conf.setLong(HConstants.HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE_KEY, SCANNER_DATA_LIMIT);
    conf.setBoolean(ScannerCallable.LOG_SCANNER_ACTIVITY, true);
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testResultSizeEstimation() throws Exception {
    byte [] ROW1 = Bytes.toBytes("testRow1");
    byte [] ROW2 = Bytes.toBytes("testRow2");
    byte [] FAMILY = Bytes.toBytes("testFamily");
    byte [] QUALIFIER = Bytes.toBytes("testQualifier");
    byte [] VALUE = Bytes.toBytes("testValue");

    TableName TABLE = TableName.valueOf("testResultSizeEstimation");
    byte[][] FAMILIES = new byte[][] { FAMILY };
    Table table = TEST_UTIL.createTable(TABLE, FAMILIES);
    Put p = new Put(ROW1);
    p.add(new KeyValue(ROW1, FAMILY, QUALIFIER, Long.MAX_VALUE, VALUE));
    table.put(p);
    p = new Put(ROW2);
    p.add(new KeyValue(ROW2, FAMILY, QUALIFIER, Long.MAX_VALUE, VALUE));
    table.put(p);

    Scan s = new Scan();
    s.setMaxResultSize(SCANNER_DATA_LIMIT);
    ResultScanner rs = table.getScanner(s);
    int count = 0;
    while(rs.next() != null) {
      count++;
    }
    assertEquals("Result size estimation did not work properly", 2, count);
    rs.close();
    table.close();
  }

  @Test
  public void testResultSizeEstimationWithTags() throws Exception {
    byte [] ROW1 = Bytes.toBytes("testRow1");
    byte [] ROW2 = Bytes.toBytes("testRow2");
    byte [] FAMILY = Bytes.toBytes("testFamily");
    byte [] QUALIFIER = Bytes.toBytes("testQualifier");
    byte [] VALUE = Bytes.toBytes("testValue");

    TableName TABLE = TableName.valueOf("testResultSizeEstimationWithTags");
    byte[][] FAMILIES = new byte[][] { FAMILY };
    Table table = TEST_UTIL.createTable(TABLE, FAMILIES);
    Put p = new Put(ROW1);
    p.add(new KeyValue(ROW1, FAMILY, QUALIFIER, Long.MAX_VALUE, VALUE,
      new Tag[] { new Tag((byte)1, new byte[TAG_DATA_SIZE]) } ));
    table.put(p);
    p = new Put(ROW2);
    p.add(new KeyValue(ROW2, FAMILY, QUALIFIER, Long.MAX_VALUE, VALUE,
      new Tag[] { new Tag((byte)1, new byte[TAG_DATA_SIZE]) } ));
    table.put(p);

    Scan s = new Scan();
    s.setMaxResultSize(SCANNER_DATA_LIMIT);
    ResultScanner rs = table.getScanner(s);
    int count = 0;
    while(rs.next() != null) {
      count++;
    }
    assertEquals("Result size estimation did not work properly", 2, count);
    rs.close();
    table.close();
  }
}