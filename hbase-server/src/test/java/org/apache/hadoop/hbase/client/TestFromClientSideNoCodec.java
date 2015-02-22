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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.ipc.AbstractRpcClient;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Do some ops and prove that client and server can work w/o codecs; that we can pb all the time.
 * Good for third-party clients or simple scripts that want to talk direct to hbase.
 */
@Category({MediumTests.class, ClientTests.class})
public class TestFromClientSideNoCodec {
  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Turn off codec use
    TEST_UTIL.getConfiguration().set("hbase.client.default.rpc.codec", "");
    TEST_UTIL.startMiniCluster(1);
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testBasics() throws IOException {
    final byte [] t = Bytes.toBytes("testBasics");
    final byte [][] fs = new byte[][] {Bytes.toBytes("cf1"), Bytes.toBytes("cf2"),
      Bytes.toBytes("cf3") };
    HTable ht = TEST_UTIL.createTable(t, fs);
    // Check put and get.
    final byte [] row = Bytes.toBytes("row");
    Put p = new Put(row);
    for (byte [] f: fs) p.add(f, f, f);
    ht.put(p);
    Result r = ht.get(new Get(row));
    int i = 0;
    for (CellScanner cellScanner = r.cellScanner(); cellScanner.advance();) {
      Cell cell = cellScanner.current();
      byte [] f = fs[i++];
      assertTrue(Bytes.toString(f),
        Bytes.equals(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(),
          f, 0, f.length));
    }
    // Check getRowOrBefore
    byte [] f = fs[0];
    r = ht.getRowOrBefore(row, f);
    assertTrue(r.toString(), r.containsColumn(f, f));
    // Check scan.
    ResultScanner scanner = ht.getScanner(new Scan());
    int count = 0;
    while ((r = scanner.next()) != null) {
      assertTrue(r.listCells().size() == 3);
      count++;
    }
    assertTrue(count == 1);
  }

  @Test
  public void testNoCodec() {
    Configuration c = new Configuration();
    c.set("hbase.client.default.rpc.codec", "");
    String codec = AbstractRpcClient.getDefaultCodec(c);
    assertTrue(codec == null || codec.length() == 0);
  }
}