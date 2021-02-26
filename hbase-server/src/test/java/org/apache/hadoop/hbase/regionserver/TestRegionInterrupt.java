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
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNameTestRule;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({RegionServerTests.class, LargeTests.class})
public class TestRegionInterrupt {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionInterrupt.class);

  private static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final Logger LOG = LoggerFactory.getLogger(TestRegionInterrupt.class);

  static final byte[] FAMILY = Bytes.toBytes("info");

  static long sleepTime;

  @Rule
  public TableNameTestRule name = new TableNameTestRule();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    conf.setClass(HConstants.REGION_IMPL, InterruptInterceptingHRegion.class, Region.class);
    conf.setBoolean(HRegion.CLOSE_WAIT_ABORT, true);
    // Ensure the sleep interval is long enough for interrupts to occur.
    long waitInterval = conf.getLong(HRegion.CLOSE_WAIT_INTERVAL,
      HRegion.DEFAULT_CLOSE_WAIT_INTERVAL);
    sleepTime = waitInterval * 2;
    // Try to bound the running time of this unit if expected actions do not take place.
    conf.setLong(HRegion.CLOSE_WAIT_TIME, sleepTime * 2);
  }

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.startMiniCluster();
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testCloseInterruptScanning() throws Exception {
    final TableName tableName = name.getTableName();
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
            expectedExceptionCaught.set(true);
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

  @Test
  public void testCloseInterruptMutation() throws Exception {
    final TableName tableName = name.getTableName();
    final Admin admin = TEST_UTIL.getAdmin();
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
          expectedExceptionCaught.set(true);
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
        Configuration conf, RegionInfo regionInfo, TableDescriptor htd,
        RegionServerServices rsServices) {
      super(tableDir, wal, fs, conf, regionInfo, htd, rsServices);
    }

    public InterruptInterceptingHRegion(HRegionFileSystem fs, WAL wal, Configuration conf,
        TableDescriptor htd, RegionServerServices rsServices) {
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
    IOException throwOnInterrupt(Throwable t) {
      interrupted = true;
      return super.throwOnInterrupt(t);
    }

  }

  public static class DelayingFilter extends FilterBase {

    static volatile boolean sleeping = false;

    public static boolean isSleeping() {
      return sleeping;
    }

    @Override
    public ReturnCode filterCell(Cell v) throws IOException {
      LOG.info("Starting sleep on " + v);
      sleeping = true;
      try {
        Thread.sleep(sleepTime);
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

  public static class MutationDelayingCoprocessor implements RegionCoprocessor, RegionObserver {

    static volatile boolean sleeping = false;

    public static boolean isSleeping() {
      return sleeping;
    }

    private void doSleep(Region.Operation op) {
      LOG.info("Starting sleep for " + op);
      sleeping = true;
      try {
        Thread.sleep(sleepTime);
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
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit,
        Durability durability) throws IOException {
      doSleep(Region.Operation.PUT);
      RegionObserver.super.prePut(c, put, edit, durability);
    }

    @Override
    public void preDelete(ObserverContext<RegionCoprocessorEnvironment> c, Delete delete,
        WALEdit edit, Durability durability) throws IOException {
      doSleep(Region.Operation.DELETE);
      RegionObserver.super.preDelete(c, delete, edit, durability);
    }

    @Override
    public Result preAppend(ObserverContext<RegionCoprocessorEnvironment> c, Append append)
        throws IOException {
      doSleep(Region.Operation.APPEND);
      return RegionObserver.super.preAppend(c, append);
    }

    @Override
    public Result preIncrement(ObserverContext<RegionCoprocessorEnvironment> c, Increment increment)
        throws IOException {
      doSleep(Region.Operation.INCREMENT);
      return RegionObserver.super.preIncrement(c, increment);
    }

  }

}
