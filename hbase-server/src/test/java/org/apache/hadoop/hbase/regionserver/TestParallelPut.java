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

import static org.apache.hadoop.hbase.HBaseTestingUtility.fam1;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HConstants.OperationStatusCode;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;


/**
 * Testing of multiPut in parallel.
 *
 */
@Category({RegionServerTests.class, MediumTests.class})
public class TestParallelPut {
  static final Log LOG = LogFactory.getLog(TestParallelPut.class);
  @Rule public TestName name = new TestName(); 
  
  private HRegion region = null;
  private static HBaseTestingUtility HBTU = new HBaseTestingUtility();
  private static final int THREADS100 = 100;

  // Test names
  static byte[] tableName;
  static final byte[] qual1 = Bytes.toBytes("qual1");
  static final byte[] qual2 = Bytes.toBytes("qual2");
  static final byte[] qual3 = Bytes.toBytes("qual3");
  static final byte[] value1 = Bytes.toBytes("value1");
  static final byte[] value2 = Bytes.toBytes("value2");
  static final byte [] row = Bytes.toBytes("rowA");
  static final byte [] row2 = Bytes.toBytes("rowB");

  @BeforeClass
  public static void beforeClass() {
    // Make sure enough handlers.
    HBTU.getConfiguration().setInt(HConstants.REGION_SERVER_HANDLER_COUNT, THREADS100);
  }


  /**
   * @see org.apache.hadoop.hbase.HBaseTestCase#setUp()
   */
  @Before
  public void setUp() throws Exception {
    tableName = Bytes.toBytes(name.getMethodName());
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentEdgeManagerTestHelper.reset();
    if (region != null) region.close(true);
  }
  
  public String getName() {
    return name.getMethodName();
  }

  //////////////////////////////////////////////////////////////////////////////
  // New tests that don't spin up a mini cluster but rather just test the
  // individual code pieces in the HRegion. 
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Test one put command.
   */
  @Test
  public void testPut() throws IOException {
    LOG.info("Starting testPut");
    this.region = initHRegion(tableName, getName(), fam1);

    long value = 1L;

    Put put = new Put(row);
    put.add(fam1, qual1, Bytes.toBytes(value));
    region.put(put);

    assertGet(this.region, row, fam1, qual1, Bytes.toBytes(value));
  }

  /**
   * Test multi-threaded Puts.
   */
  @Test
  public void testParallelPuts() throws IOException {

    LOG.info("Starting testParallelPuts");

    this.region = initHRegion(tableName, getName(), fam1);
    int numOps = 1000; // these many operations per thread

    // create 100 threads, each will do its own puts
    Putter[] all = new Putter[THREADS100];

    // create all threads
    for (int i = 0; i < THREADS100; i++) {
      all[i] = new Putter(region, i, numOps);
    }

    // run all threads
    for (int i = 0; i < THREADS100; i++) {
      all[i].start();
    }

    // wait for all threads to finish
    for (int i = 0; i < THREADS100; i++) {
      try {
        all[i].join();
      } catch (InterruptedException e) {
        LOG.warn("testParallelPuts encountered InterruptedException." +
                 " Ignoring....", e);
      }
    }
    LOG.info("testParallelPuts successfully verified " + 
             (numOps * THREADS100) + " put operations.");
  }


  private static void assertGet(final HRegion region, byte [] row, byte [] familiy,
      byte[] qualifier, byte[] value) throws IOException {
    // run a get and see if the value matches
    Get get = new Get(row);
    get.addColumn(familiy, qualifier);
    Result result = region.get(get);
    assertEquals(1, result.size());

    Cell kv = result.rawCells()[0];
    byte[] r = CellUtil.cloneValue(kv);
    assertTrue(Bytes.compareTo(r, value) == 0);
  }

  private HRegion initHRegion(byte [] tableName, String callingMethod,
    byte[] ... families)
  throws IOException {
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
    for(byte [] family : families) {
      htd.addFamily(new HColumnDescriptor(family));
    }
    HRegionInfo info = new HRegionInfo(htd.getTableName(), null, null, false);
    return HBTU.createLocalHRegion(info, htd);
  }

  /**
   * A thread that makes a few put calls
   */
  public static class Putter extends Thread {

    private final HRegion region;
    private final int threadNumber;
    private final int numOps;
    private final Random rand = new Random();
    byte [] rowkey = null;

    public Putter(HRegion region, int threadNumber, int numOps) {
      this.region = region;
      this.threadNumber = threadNumber;
      this.numOps = numOps;
      this.rowkey = Bytes.toBytes((long)threadNumber); // unique rowid per thread
      setDaemon(true);
    }

    @Override
    public void run() {
      byte[] value = new byte[100];
      Put[]  in = new Put[1];

      // iterate for the specified number of operations
      for (int i=0; i<numOps; i++) {
        // generate random bytes
        rand.nextBytes(value);  

        // put the randombytes and verify that we can read it. This is one
        // way of ensuring that rwcc manipulation in HRegion.put() is fine.
        Put put = new Put(rowkey);
        put.add(fam1, qual1, value);
        in[0] = put;
        try {
          OperationStatus[] ret = region.batchMutate(in);
          assertEquals(1, ret.length);
          assertEquals(OperationStatusCode.SUCCESS, ret[0].getOperationStatusCode());
          assertGet(this.region, rowkey, fam1, qual1, value);
        } catch (IOException e) {
          assertTrue("Thread id " + threadNumber + " operation " + i + " failed.",
                     false);
        }
      }
    }
  }
}