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

package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({RegionServerTests.class, LargeTests.class})
public class TestRegionInterrupt {

  private static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final Log LOG = LogFactory.getLog(TestRegionInterrupt.class);

  static final int SLEEP_TIME = 10 * 1000;
  static final byte[] FAMILY = Bytes.toBytes("info");

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setClass(HConstants.REGION_IMPL, InterruptInterceptingHRegion.class, Region.class);
    conf.setBoolean(HRegion.CLOSE_WAIT_ABORT, true);
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
  }

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.startMiniCluster();
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test(timeout=120000)
  public void testCloseInterruptScanning() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    LOG.info("Creating table " + tableName);
    try (Table table = TEST_UTIL.createTable(tableName, FAMILY)) {
      // load some data
      TEST_UTIL.waitUntilAllRegionsAssigned(tableName);
      TEST_UTIL.loadTable(table, FAMILY);
      final AtomicBoolean expectedExceptionCaught = new AtomicBoolean(false);
      // scan the table in the background
      Thread scanner = new Thread(new Runnable() {
        @Override
        public void run() {
          Scan scan = new Scan();
          scan.addFamily(FAMILY);
          scan.setFilter(new DelayingFilter());
          try {
            LOG.info("Starting scan");
            try (ResultScanner rs = table.getScanner(scan)) {
              Result r;
              do {
                r = rs.next();
                if (r != null) {
                  LOG.info("Scanned row " + Bytes.toStringBinary(r.getRow()));
                }
              } while (r != null);
            }
          } catch (IOException e) {
            LOG.info("Scanner caught exception", e);
            Throwable t;
            if (e instanceof RetriesExhaustedWithDetailsException) {
              t = ((RetriesExhaustedWithDetailsException)e).getCause(0);
            } else {
              t = e.getCause();
            }
            if (t instanceof NotServingRegionException) {
              expectedExceptionCaught.set(true);
            }
          } finally {
            LOG.info("Finished scan");
          }
        }
      });
      scanner.start();

      // Wait for the filter to begin sleeping
      LOG.info("Waiting for scanner to start");
      Waiter.waitFor(TEST_UTIL.getConfiguration(), 10*1000, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          return DelayingFilter.isSleeping();
        }
      });

      // Offline the table, this will trigger closing
      LOG.info("Offlining table " + tableName);
      TEST_UTIL.getHBaseAdmin().disableTable(tableName);

      // Wait for scanner termination
      scanner.join();

      // When we get here the region has closed and the table is offline
      assertTrue("Region operations were not interrupted",
        InterruptInterceptingHRegion.wasInterrupted());
      assertTrue("Scanner did not catch expected exception", expectedExceptionCaught.get());
    }
  }

  @Test(timeout=120000)
  public void testCloseInterruptMutation() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final Admin admin = TEST_UTIL.getHBaseAdmin();
    // Create the test table
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor(FAMILY));
    htd.addCoprocessor(MutationDelayingCoprocessor.class.getName());
    LOG.info("Creating table " + tableName);
    admin.createTable(htd);
    TEST_UTIL.waitUntilAllRegionsAssigned(tableName);

    // Insert some data in the background
    LOG.info("Starting writes to table " + tableName);
    final int NUM_ROWS = 100;
    final AtomicBoolean expectedExceptionCaught = new AtomicBoolean(false);
    Thread inserter = new Thread(new Runnable() {
      @Override
      public void run() {
        try (BufferedMutator t = admin.getConnection().getBufferedMutator(tableName)) {
          for (int i = 0; i < NUM_ROWS; i++) {
            LOG.info("Writing row " + i + " to " + tableName);
            byte[] value = new byte[10], row = Bytes.toBytes(Integer.toString(i));
            Bytes.random(value);
            t.mutate(new Put(row).addColumn(FAMILY, HConstants.EMPTY_BYTE_ARRAY, value));
            t.flush();
          }
        } catch (IOException e) {
          LOG.info("Inserter caught exception", e);
          Throwable t;
          if (e instanceof RetriesExhaustedWithDetailsException) {
            t = ((RetriesExhaustedWithDetailsException)e).getCause(0);
          } else {
            t = e.getCause();
          }
          if (t instanceof NotServingRegionException) {
            expectedExceptionCaught.set(true);
          }
        }
      }
    });
    inserter.start();

    // Wait for delayed insertion to begin
    LOG.info("Waiting for mutations to start");
    Waiter.waitFor(TEST_UTIL.getConfiguration(), 10*1000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return MutationDelayingCoprocessor.isSleeping();
      }
    });

    // Offline the table, this will trigger closing
    LOG.info("Offlining table " + tableName);
    admin.disableTable(tableName);

    // Wait for the inserter to finish
    inserter.join();

    // When we get here the region has closed and the table is offline
    assertTrue("Region operations were not interrupted",
      InterruptInterceptingHRegion.wasInterrupted());
    assertTrue("Inserter did not catch expected exception", expectedExceptionCaught.get());
  }

  public static class InterruptInterceptingHRegion extends HRegion {

    private static boolean interrupted = false;

    public static boolean wasInterrupted() {
      return interrupted;
    }

    public InterruptInterceptingHRegion(Path tableDir, WAL wal, FileSystem fs,
        Configuration conf, HRegionInfo regionInfo, HTableDescriptor htd,
        RegionServerServices rsServices) {
      super(tableDir, wal, fs, conf, regionInfo, htd, rsServices);
    }

    public InterruptInterceptingHRegion(HRegionFileSystem fs, WAL wal, Configuration conf,
        HTableDescriptor htd, RegionServerServices rsServices) {
      super(fs, wal, conf, htd, rsServices);
    }

    @Override
    void checkInterrupt() throws NotServingRegionException, InterruptedIOException {
      try {
        super.checkInterrupt();
      } catch (NotServingRegionException | InterruptedIOException e) {
        interrupted = true;
        throw e;
      }
    }

    @Override
    void throwOnInterrupt(Throwable t) throws NotServingRegionException, InterruptedIOException {
      interrupted = true;
      super.throwOnInterrupt(t);
    }

  }

  public static class DelayingFilter extends FilterBase {

    static volatile boolean sleeping = false;

    public static boolean isSleeping() {
      return sleeping;
    }

    @Override
    public ReturnCode filterKeyValue(Cell v) throws IOException {
      LOG.info("Starting sleep on " + v);
      sleeping = true;
      try {
        Thread.sleep(SLEEP_TIME);
      } catch (InterruptedException e) {
        // restore interrupt status so region scanner can handle it as expected
        Thread.currentThread().interrupt();
        LOG.info("Interrupted during sleep on " + v);
      } finally {
        LOG.info("Done sleep on " + v);
        sleeping = false;
      }
      return ReturnCode.INCLUDE;
    }

    public static DelayingFilter parseFrom(final byte [] pbBytes)
        throws DeserializationException {
      // Just return a new instance.
      return new DelayingFilter();
    }

  }

  public static class MutationDelayingCoprocessor extends BaseRegionObserver {

    static volatile boolean sleeping = false;

    public static boolean isSleeping() {
      return sleeping;
    }

    private void doSleep(Region.Operation op) {
      LOG.info("Starting sleep for " + op);
      sleeping = true;
      try {
        Thread.sleep(SLEEP_TIME);
      } catch (InterruptedException e) {
        // restore interrupt status so doMiniBatchMutation etc. can handle it as expected
        Thread.currentThread().interrupt();
        LOG.info("Interrupted during " + op);
      } finally {
        LOG.info("Done");
        sleeping = false;
      }
    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit,
        Durability durability) throws IOException {
      doSleep(Region.Operation.PUT);
      super.prePut(c, put, edit, durability);
    }

    @Override
    public void preDelete(ObserverContext<RegionCoprocessorEnvironment> c, Delete delete,
        WALEdit edit, Durability durability) throws IOException {
      doSleep(Region.Operation.DELETE);
      super.preDelete(c, delete, edit, durability);
    }

    @Override
    public Result preAppend(ObserverContext<RegionCoprocessorEnvironment> c, Append append)
        throws IOException {
      doSleep(Region.Operation.APPEND);
      return super.preAppend(c, append);
    }

    @Override
    public Result preIncrement(ObserverContext<RegionCoprocessorEnvironment> c, Increment increment)
        throws IOException {
      doSleep(Region.Operation.INCREMENT);
      return super.preIncrement(c, increment);
    }

  }

}
