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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestSizeFailures {
  final Log LOG = LogFactory.getLog(getClass());
  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static byte [] FAMILY = Bytes.toBytes("testFamily");
  protected static int SLAVES = 1;

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Uncomment the following lines if more verbosity is needed for
    // debugging (see HBASE-12285 for details).
    //((Log4JLogger)RpcServer.LOG).getLogger().setLevel(Level.ALL);
    //((Log4JLogger)RpcClient.LOG).getLogger().setLevel(Level.ALL);
    //((Log4JLogger)ScannerCallable.LOG).getLogger().setLevel(Level.ALL);
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setBoolean("hbase.table.sanity.checks", true); // ignore sanity checks in the server
    TEST_UTIL.startMiniCluster(SLAVES);
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Basic client side validation of HBASE-13262
   */
   @Test
   public void testScannerSeesAllRecords() throws Exception {
     final int NUM_ROWS = 1000 * 1000, NUM_COLS = 10;
     final TableName TABLENAME = TableName.valueOf("testScannerSeesAllRecords");
     List<byte[]> qualifiers = new ArrayList<>();
     for (int i = 1; i <= 10; i++) {
       qualifiers.add(Bytes.toBytes(Integer.toString(i)));
     }

     HColumnDescriptor hcd = new HColumnDescriptor(FAMILY);
     HTableDescriptor desc = new HTableDescriptor(TABLENAME);
     desc.addFamily(hcd);
     byte[][] splits = new byte[9][2];
     for (int i = 1; i < 10; i++) {
       int split = 48 + i;
       splits[i - 1][0] = (byte) (split >>> 8);
       splits[i - 1][0] = (byte) (split);
     }
     TEST_UTIL.getHBaseAdmin().createTable(desc, splits);
     Connection conn = TEST_UTIL.getConnection();

     try (Table table = conn.getTable(TABLENAME)) {
       List<Put> puts = new LinkedList<>();
       for (int i = 0; i < NUM_ROWS; i++) {
         Put p = new Put(Bytes.toBytes(Integer.toString(i)));
         for (int j = 0; j < NUM_COLS; j++) {
           byte[] value = new byte[50];
           Bytes.random(value);
           p.addColumn(FAMILY, Bytes.toBytes(Integer.toString(j)), value);
         }
         puts.add(p);

         if (puts.size() == 1000) {
           Object[] results = new Object[1000];
           try {
             table.batch(puts, results);
           } catch (IOException e) {
             LOG.error("Failed to write data", e);
             LOG.debug("Errors: " +  Arrays.toString(results));
           }

           puts.clear();
         }
       }

       if (puts.size() > 0) {
         Object[] results = new Object[puts.size()];
         try {
           table.batch(puts, results);
         } catch (IOException e) {
           LOG.error("Failed to write data", e);
           LOG.debug("Errors: " +  Arrays.toString(results));
         }
       }

       // Flush the memstore to disk
       TEST_UTIL.getHBaseAdmin().flush(TABLENAME);

       TreeSet<Integer> rows = new TreeSet<>();
       long rowsObserved = 0l;
       long entriesObserved = 0l;
       Scan s = new Scan();
       s.addFamily(FAMILY);
       s.setMaxResultSize(-1);
       s.setBatch(-1);
       s.setCaching(500);
       ResultScanner scanner = table.getScanner(s);
       // Read all the records in the table
       for (Result result : scanner) {
         rowsObserved++;
         String row = new String(result.getRow());
         rows.add(Integer.parseInt(row));
         while (result.advance()) {
           entriesObserved++;
           // result.current();
         }
       }

       // Verify that we see 1M rows and 10M cells
       assertEquals(NUM_ROWS, rowsObserved);
       assertEquals(NUM_ROWS * NUM_COLS, entriesObserved);
     }

     conn.close();
   }
}
