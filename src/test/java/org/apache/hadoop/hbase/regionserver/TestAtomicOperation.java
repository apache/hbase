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


import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;


/**
 * Testing of HRegion.incrementColumnValue, HRegion.increment,
 * and HRegion.append
 */
public class TestAtomicOperation extends HBaseTestCase {
  static final Log LOG = LogFactory.getLog(TestAtomicOperation.class);

  HRegion region = null;
  private HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final String DIR = TEST_UTIL.getDataTestDir("TestIncrement").toString();


  private final int MAX_VERSIONS = 2;

  // Test names
  static final byte[] tableName = Bytes.toBytes("testtable");;
  static final byte[] qual1 = Bytes.toBytes("qual1");
  static final byte[] qual2 = Bytes.toBytes("qual2");
  static final byte[] qual3 = Bytes.toBytes("qual3");
  static final byte[] value1 = Bytes.toBytes("value1");
  static final byte[] value2 = Bytes.toBytes("value2");
  static final byte [] row = Bytes.toBytes("rowA");
  static final byte [] row2 = Bytes.toBytes("rowB");

  /**
   * @see org.apache.hadoop.hbase.HBaseTestCase#setUp()
   */
  @Override
  protected void setUp() throws Exception {
    super.setUp();
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    EnvironmentEdgeManagerTestHelper.reset();
  }

  //////////////////////////////////////////////////////////////////////////////
  // New tests that doesn't spin up a mini cluster but rather just test the
  // individual code pieces in the HRegion. 
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Test basic append operation.
   * More tests in
   * @see org.apache.hadoop.hbase.client.TestFromClientSide#testAppend()
   */
  public void testAppend() throws IOException {
    initHRegion(tableName, getName(), fam1);
    String v1 = "Ultimate Answer to the Ultimate Question of Life,"+
    " The Universe, and Everything";
    String v2 = " is... 42.";
    Append a = new Append(row);
    a.setReturnResults(false);
    a.add(fam1, qual1, Bytes.toBytes(v1));
    a.add(fam1, qual2, Bytes.toBytes(v2));
    assertNull(region.append(a, null, true));
    a = new Append(row);
    a.add(fam1, qual1, Bytes.toBytes(v2));
    a.add(fam1, qual2, Bytes.toBytes(v1));
    Result result = region.append(a, null, true);
    assertEquals(0, Bytes.compareTo(Bytes.toBytes(v1+v2), result.getValue(fam1, qual1)));
    assertEquals(0, Bytes.compareTo(Bytes.toBytes(v2+v1), result.getValue(fam1, qual2)));
  }

  /**
   * Test one increment command.
   */
  public void testIncrementColumnValue() throws IOException {
    LOG.info("Starting test testIncrementColumnValue");
    initHRegion(tableName, getName(), fam1);

    long value = 1L;
    long amount = 3L;

    Put put = new Put(row);
    put.add(fam1, qual1, Bytes.toBytes(value));
    region.put(put);

    long result = region.incrementColumnValue(row, fam1, qual1, amount, true);

    assertEquals(value+amount, result);

    Store store = region.getStore(fam1);
    // ICV removes any extra values floating around in there.
    assertEquals(1, store.memstore.kvset.size());
    assertTrue(store.memstore.snapshot.isEmpty());

    assertICV(row, fam1, qual1, value+amount);
  }

  /**
   * Test multi-threaded increments.
   */
  public void testIncrementMultiThreads() throws IOException {

    LOG.info("Starting test testIncrementMultiThreads");
    initHRegion(tableName, getName(), fam1);

    // create 100 threads, each will increment by its own quantity
    int numThreads = 100;
    int incrementsPerThread = 1000;
    Incrementer[] all = new Incrementer[numThreads];
    int expectedTotal = 0;

    // create all threads
    for (int i = 0; i < numThreads; i++) {
      all[i] = new Incrementer(region, i, i, incrementsPerThread);
      expectedTotal += (i * incrementsPerThread);
    }

    // run all threads
    for (int i = 0; i < numThreads; i++) {
      all[i].start();
    }

    // wait for all threads to finish
    for (int i = 0; i < numThreads; i++) {
      try {
        all[i].join();
      } catch (InterruptedException e) {
      }
    }
    assertICV(row, fam1, qual1, expectedTotal);
    LOG.info("testIncrementMultiThreads successfully verified that total is " +
             expectedTotal);
  }


  private void assertICV(byte [] row,
                         byte [] familiy,
                         byte[] qualifier,
                         long amount) throws IOException {
    // run a get and see?
    Get get = new Get(row);
    get.addColumn(familiy, qualifier);
    Result result = region.get(get, null);
    assertEquals(1, result.size());

    KeyValue kv = result.raw()[0];
    long r = Bytes.toLong(kv.getValue());
    assertEquals(amount, r);
  }

  private void initHRegion (byte [] tableName, String callingMethod,
    byte[] ... families)
  throws IOException {
    initHRegion(tableName, callingMethod, HBaseConfiguration.create(), families);
  }

  private void initHRegion (byte [] tableName, String callingMethod,
    Configuration conf, byte [] ... families)
  throws IOException{
    HTableDescriptor htd = new HTableDescriptor(tableName);
    for(byte [] family : families) {
      htd.addFamily(new HColumnDescriptor(family));
    }
    HRegionInfo info = new HRegionInfo(htd.getName(), null, null, false);
    Path path = new Path(DIR + callingMethod);
    if (fs.exists(path)) {
      if (!fs.delete(path, true)) {
        throw new IOException("Failed delete of " + path);
      }
    }
    region = HRegion.createHRegion(info, path, conf, htd);
  }

  /**
   * A thread that makes a few increment calls
   */
  public static class Incrementer extends Thread {

    private final HRegion region;
    private final int threadNumber;
    private final int numIncrements;
    private final int amount;

    private int count;

    public Incrementer(HRegion region, 
        int threadNumber, int amount, int numIncrements) {
      this.region = region;
      this.threadNumber = threadNumber;
      this.numIncrements = numIncrements;
      this.count = 0;
      this.amount = amount;
      setDaemon(true);
    }

    @Override
    public void run() {
      for (int i=0; i<numIncrements; i++) {
        try {
          long result = region.incrementColumnValue(row, fam1, qual1, amount, true);
          // LOG.info("thread:" + threadNumber + " iter:" + i);
        } catch (IOException e) {
          e.printStackTrace();
        }
        count++;
      }
    }
  }


}
