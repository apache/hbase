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

package org.apache.hadoop.hbase.coprocessor;

import static org.junit.Assert.assertEquals;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;

/**
 * Verifies ProcessRowEndpoint works.
 * The tested RowProcessor performs two scans and a read-modify-write.
 */
@Category(SmallTests.class)
public class TestProcessRowEndpoint {

  static final Log LOG = LogFactory.getLog(TestProcessRowEndpoint.class);

  private static final byte[] TABLE = Bytes.toBytes("testtable");
  private static final byte[] TABLE2 = Bytes.toBytes("testtable2");
  private final static byte[] ROW = Bytes.toBytes("testrow");
  private final static byte[] FAM = Bytes.toBytes("friendlist");

  // Column names
  private final static byte[] A = Bytes.toBytes("a");
  private final static byte[] B = Bytes.toBytes("b");
  private final static byte[] C = Bytes.toBytes("c");
  private final static byte[] D = Bytes.toBytes("d");
  private final static byte[] E = Bytes.toBytes("e");
  private final static byte[] F = Bytes.toBytes("f");
  private final static byte[] G = Bytes.toBytes("g");
  private final static byte[] REQUESTS = Bytes.toBytes("requests");

  private static HBaseTestingUtility util = new HBaseTestingUtility();
  private volatile int numRequests;

  private CountDownLatch startSignal;
  private CountDownLatch doneSignal;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration conf = util.getConfiguration();
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        FriendsOfFriendsEndpoint.class.getName());
    util.startMiniCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    util.shutdownMiniCluster();
  }

  @Test
  public void testSingle() throws Throwable {
    HTable table = prepareTestData(TABLE, util);
    verifyProcessRow(table);
    assertEquals(1, numRequests);
  }

  private void verifyProcessRow(HTable table) throws Throwable {

    FriendsOfFriendsProtocol processor =
      table.coprocessorProxy(FriendsOfFriendsProtocol.class, ROW);
    Result result = processor.query(ROW, A);

    Set<String> friendsOfFriends = new HashSet<String>();
    for (KeyValue kv : result.raw()) {
      if (Bytes.equals(kv.getQualifier(), REQUESTS)) {
        numRequests = Bytes.toInt(kv.getValue());
        continue;
      }
      for (byte val : kv.getValue()) {
        friendsOfFriends.add((char)val + "");
      }
    }
    Set<String> expected =
      new HashSet<String>(Arrays.asList(new String[]{"d", "e", "f", "g"}));
    assertEquals(expected, friendsOfFriends);
  }

  @Test
  public void testThreads() throws Exception {
    HTable table = prepareTestData(TABLE2, util);
    int numThreads = 1000;
    startSignal = new CountDownLatch(numThreads);
    doneSignal = new CountDownLatch(numThreads);
    for (int i = 0; i < numThreads; ++i) {
      new Thread(new QueryRunner(table)).start();
      startSignal.countDown();
    }
    doneSignal.await();
    Get get = new Get(ROW);
    LOG.debug("row keyvalues:" + stringifyKvs(table.get(get).list()));
    assertEquals(numThreads, numRequests);
  }

  class QueryRunner implements Runnable {
    final HTable table;
    QueryRunner(final HTable table) {
      this.table = table;
    }
    @Override
    public void run() {
      try {
        startSignal.await();
        verifyProcessRow(table);
      } catch (Throwable e) {
        e.printStackTrace();
      }
      doneSignal.countDown();
    }
  }

  static HTable prepareTestData(byte[] tableName, HBaseTestingUtility util)
      throws Exception {
    HTable table = util.createTable(tableName, FAM);
    Put put = new Put(ROW);
    put.add(FAM, A, Bytes.add(B, C));    // B, C are friends of A
    put.add(FAM, B, Bytes.add(D, E, F)); // D, E, F are friends of B
    put.add(FAM, C, G);                  // G is a friend of C
    table.put(put);
    return table;
  }

  /**
   * Coprocessor protocol that finds friends of friends of a person and
   * update the number of requests.
   */
  public static interface FriendsOfFriendsProtocol extends CoprocessorProtocol {
    
    /**
     * Query a person's friends of friends
     */
    Result query(byte[] row, byte[] person) throws IOException;
  }

  /**
   * Finds friends of friends of a person and update the number of requests.
   */
  public static class FriendsOfFriendsEndpoint extends BaseEndpointCoprocessor
      implements FriendsOfFriendsProtocol, RowProcessor<Result> {
    byte[] row = null;
    byte[] person = null;
    Result result = null;

    //
    // FriendsOfFriendsProtocol method
    //

    @Override
    public Result query(byte[] row, byte[] person) throws IOException {
      this.row = row;
      this.person = person;
      HRegion region =
        ((RegionCoprocessorEnvironment) getEnvironment()).getRegion();
      region.processRow(this);
      return this.getResult();
    }

    //
    // RowProcessor methods
    //

    FriendsOfFriendsEndpoint() {
    }

    @Override
    public byte[] getRow() {
      return row;
    }

    @Override
    public Result getResult() {
      return result;
    }

    @Override
    public boolean readOnly() {
      return false;
    }

    @Override
    public void process(long now, RowProcessor.RowScanner scanner,
        List<KeyValue> mutations, WALEdit walEdit) throws IOException {
      List<KeyValue> kvs = new ArrayList<KeyValue>();
      { // First scan to get friends of the person and numRequests
        Scan scan = new Scan(row, row);
        scan.addColumn(FAM, person);
        scan.addColumn(FAM, REQUESTS);
        scanner.doScan(scan, kvs);
      }
      LOG.debug("first scan:" + stringifyKvs(kvs));
      int numRequests = 0;
      // Second scan to get friends of friends
      Scan scan = new Scan(row, row);
      for (KeyValue kv : kvs) {
        if (Bytes.equals(kv.getQualifier(), REQUESTS)) {
          numRequests = Bytes.toInt(kv.getValue());
          continue;
        }
        byte[] friends = kv.getValue();
        for (byte f : friends) {
          scan.addColumn(FAM, new byte[]{f});
        }
      }
      scanner.doScan(scan, kvs);

      LOG.debug("second scan:" + stringifyKvs(kvs));
      numRequests += 1;
      // Construct mutations and Result
      KeyValue kv = new KeyValue(
          row, FAM, REQUESTS, now, Bytes.toBytes(numRequests));
      mutations.clear();
      mutations.add(kv);
      kvs.add(kv);
      LOG.debug("final result:" + stringifyKvs(kvs) +
                " mutations:" + stringifyKvs(mutations));
      result = new Result(kvs);
      // Inject some meta data to the walEdit
      KeyValue metaKv = new KeyValue(
          getRow(), HLog.METAFAMILY,
          Bytes.toBytes("FriendsOfFriends query"),
          person);
      walEdit.add(metaKv);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      this.person = Bytes.readByteArray(in);
      this.row = Bytes.readByteArray(in);
      this.result = new Result();
      result.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      Bytes.writeByteArray(out, person);
      Bytes.writeByteArray(out, row);
      if (result == null) {
        new Result().write(out);
      } else {
        result.write(out);
      }
    }

    @Override
    public UUID getClusterId() {
      return HConstants.DEFAULT_CLUSTER_ID;
    }
  }

  static String stringifyKvs(Collection<KeyValue> kvs) {
    StringBuilder out = new StringBuilder();
    out.append("[");
    for (KeyValue kv : kvs) {
      byte[] col = kv.getQualifier();
      byte[] val = kv.getValue();
      if (Bytes.equals(col, REQUESTS)) {
        out.append(Bytes.toStringBinary(col) + ":" +
                   Bytes.toInt(val) + " ");
      } else {
        out.append(Bytes.toStringBinary(col) + ":" +
                   Bytes.toStringBinary(val) + " ");
      }
    }
    out.append("]");
    return out.toString();
  }

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}
