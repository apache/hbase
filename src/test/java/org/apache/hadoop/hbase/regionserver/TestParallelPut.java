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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.HConstants.OperationStatusCode;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MultithreadedTestUtil;
import org.apache.hadoop.hbase.MultithreadedTestUtil.TestThread;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.NullComparator;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.regionserver.HRegion.RegionScannerImpl;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
import org.apache.hadoop.hbase.util.IncrementingEnvironmentEdge;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.Test;

import com.google.common.collect.Lists;


/**
 * Testing of multiPut in parallel.
 *
 */
public class TestParallelPut extends HBaseTestCase {
  static final Log LOG = LogFactory.getLog(TestParallelPut.class);

  private static HRegion region = null;
  private static HBaseTestingUtility hbtu = new HBaseTestingUtility();
  private static final String DIR = hbtu.getDataTestDir() + "/TestParallelPut/";

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
  // New tests that don't spin up a mini cluster but rather just test the
  // individual code pieces in the HRegion. 
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Test one put command.
   */
  public void testPut() throws IOException {
    LOG.info("Starting testPut");
    initHRegion(tableName, getName(), fam1);

    long value = 1L;

    Put put = new Put(row);
    put.add(fam1, qual1, Bytes.toBytes(value));
    region.put(put);

    assertGet(row, fam1, qual1, Bytes.toBytes(value));
  }

  /**
   * Test multi-threaded Puts.
   */
  public void testParallelPuts() throws IOException {

    LOG.info("Starting testParallelPuts");
    initHRegion(tableName, getName(), fam1);
    int numOps = 1000; // these many operations per thread

    // create 100 threads, each will do its own puts
    int numThreads = 100;
    Putter[] all = new Putter[numThreads];

    // create all threads
    for (int i = 0; i < numThreads; i++) {
      all[i] = new Putter(region, i, numOps);
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
        LOG.warn("testParallelPuts encountered InterruptedException." +
                 " Ignoring....", e);
      }
    }
    LOG.info("testParallelPuts successfully verified " + 
             (numOps * numThreads) + " put operations.");
  }


  static private void assertGet(byte [] row,
                         byte [] familiy,
                         byte[] qualifier,
                         byte[] value) throws IOException {
    // run a get and see if the value matches
    Get get = new Get(row);
    get.addColumn(familiy, qualifier);
    Result result = region.get(get, null);
    assertEquals(1, result.size());

    KeyValue kv = result.raw()[0];
    byte[] r = kv.getValue();
    assertTrue(Bytes.compareTo(r, value) == 0);
  }

  private void initHRegion(byte [] tableName, String callingMethod,
    byte[] ... families)
  throws IOException {
    initHRegion(tableName, callingMethod, HBaseConfiguration.create(), families);
  }

  private void initHRegion(byte [] tableName, String callingMethod,
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
          OperationStatus[] ret = region.put(in);
          assertEquals(1, ret.length);
          assertEquals(OperationStatusCode.SUCCESS, ret[0].getOperationStatusCode());
          assertGet(rowkey, fam1, qual1, value);
        } catch (IOException e) {
          assertTrue("Thread id " + threadNumber + " operation " + i + " failed.",
                     false);
        }
      }
    }
  }
}
