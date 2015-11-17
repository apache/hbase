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
package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.TestTableName;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

@Category(MediumTests.class)
public class TestBufferedHTable {

  public static final Configuration CONF = HBaseConfiguration.create();
  public static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility(CONF);

  private static final byte[] CF = Bytes.toBytes("cf");
  private static final byte[] QUAL = Bytes.toBytes("qual");
  private static final byte[] VAL = Bytes.toBytes("val");
  private static final byte[] ROW1 = Bytes.toBytes("row1");
  private static final byte[] ROW2 = Bytes.toBytes("row2");
  private static final byte[] ROW3 = Bytes.toBytes("row3");
  private static final byte[] ROW4 = Bytes.toBytes("row4");

  @Rule
  public TestTableName name = new TestTableName();

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @BeforeClass
  public static void startMiniCluster() throws Exception {
    TEST_UTIL.startMiniCluster();
  }

  @AfterClass
  public static void shutdownMiniCluster() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Verifies Puts/Deletes are buffered together and automatically flushed when write buffer
   * size limit is reached.
   */
  @Test
  public void testMutationBuffering() throws IOException {
    TEST_UTIL.createTable(name.getTableName(), CF);
    BufferedHTable table = null;
    ResultScanner scanner = null;
    try {
      table = createBufferedHTable();
      // verify that mutations are buffered, and ordered correctly
      Put put1 = createPut(ROW1);
      table.put(put1);
      Delete delete2 = new Delete(ROW2);
      table.delete(delete2);
      Put put2 = createPut(ROW2);
      Put put3 = createPut(ROW3);
      table.put(Arrays.asList(put2, put3));
      Delete delete1 = new Delete(ROW1);
      Delete delete3 = new Delete(ROW3);
      table.delete(Arrays.asList(delete1, delete3));
      List<Row> buffer = table.getWriteBuffer();
      List<Mutation> expected = Arrays.asList(put1, delete2, put2, put3, delete1, delete3);
      Assert.assertEquals(expected.size(), buffer.size());
      for (int i = 0; i < buffer.size(); i++) {
        Assert.assertEquals(expected.get(i), buffer.get(i));
      }
      // add another mutation and resize the buffer
      table.put(createPut(ROW4));
      table.setWriteBufferSize(0L);
      // verify that mutations are flushed
      Assert.assertEquals(0, buffer.size());
      Assert.assertEquals(0, table.getCurrentWriteBufferSize());
      // only the last row should be visible
      scanner = table.getScanner(new Scan());
      for (Result result : scanner) {
        Assert.assertArrayEquals(ROW4, result.getRow());
      }
      scanner.close();
      // verify that delete mutations are auto-flushed when buffer limit is reached (last row
      // should no longer be visible)
      table.delete(new Delete(ROW4));
      scanner = table.getScanner(new Scan());
      Assert.assertNull(scanner.next());
    } finally {
      if (scanner != null) scanner.close();
      if (table != null) table.close();
    }
  }

  /**
   * Verifies that Deletes are rejected after BufferedHTable is closed.
   */
  @Test
  public void testDeletesAfterCloseNotAllowed() throws IOException {
    TEST_UTIL.createTable(name.getTableName(), CF);
    BufferedHTable table = null;
    try {
      table = createBufferedHTable();
      table.close();
      thrown.expect(IllegalStateException.class);
      table.delete(new Delete(ROW1));
    } finally {
      if (table != null) table.close();
    }
  }

  private BufferedHTable createBufferedHTable() throws IOException {
    BufferedHTable table = new BufferedHTable(CONF, name.getTableName().getName());
    // disabling auto-flush is required for buffering
    table.setAutoFlush(false, false);
    return table;
  }

  private Put createPut(byte[] row) throws IOException {
    Put put = new Put(row);
    put.add(CF, QUAL, VAL);
    return put;
  }
}
