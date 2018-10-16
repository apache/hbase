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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import org.apache.hadoop.hbase.Cell.Type;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Confirm that the function of FlushLifeCycleTracker is OK as we do not use it in our own code.
 */
@Category({ CoprocessorTests.class, MediumTests.class })
public class TestFlushLifeCycleTracker {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestFlushLifeCycleTracker.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static final TableName NAME =
      TableName.valueOf(TestFlushLifeCycleTracker.class.getSimpleName());

  private static final byte[] CF = Bytes.toBytes("CF");

  private static final byte[] QUALIFIER = Bytes.toBytes("CQ");

  private HRegion region;

  private static FlushLifeCycleTracker TRACKER;

  private static volatile CountDownLatch ARRIVE;

  private static volatile CountDownLatch BLOCK;

  public static final class FlushObserver implements RegionObserver, RegionCoprocessor {

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void preFlush(ObserverContext<RegionCoprocessorEnvironment> c,
        FlushLifeCycleTracker tracker) throws IOException {
      if (TRACKER != null) {
        assertSame(tracker, TRACKER);
      }
    }

    @Override
    public InternalScanner preFlush(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
        InternalScanner scanner, FlushLifeCycleTracker tracker) throws IOException {
      if (TRACKER != null) {
        assertSame(tracker, TRACKER);
      }
      return scanner;
    }

    @Override
    public void postFlush(ObserverContext<RegionCoprocessorEnvironment> c,
        FlushLifeCycleTracker tracker) throws IOException {
      if (TRACKER != null) {
        assertSame(tracker, TRACKER);
      }
    }

    @Override
    public void postFlush(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
        StoreFile resultFile, FlushLifeCycleTracker tracker) throws IOException {
      if (TRACKER != null) {
        assertSame(tracker, TRACKER);
      }
      // inject here so we can make a flush request to fail because of we already have a flush
      // ongoing.
      CountDownLatch arrive = ARRIVE;
      if (arrive != null) {
        arrive.countDown();
        try {
          BLOCK.await();
        } catch (InterruptedException e) {
          throw new InterruptedIOException();
        }
      }
    }
  }

  private static final class Tracker implements FlushLifeCycleTracker {

    private String reason;

    private boolean beforeExecutionCalled;

    private boolean afterExecutionCalled;

    private boolean completed = false;

    @Override
    public synchronized void notExecuted(String reason) {
      this.reason = reason;
      completed = true;
      notifyAll();
    }

    @Override
    public void beforeExecution() {
      this.beforeExecutionCalled = true;
    }

    @Override
    public synchronized void afterExecution() {
      this.afterExecutionCalled = true;
      completed = true;
      notifyAll();
    }

    public synchronized void await() throws InterruptedException {
      while (!completed) {
        wait();
      }
    }
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    UTIL.startMiniCluster(3);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws IOException {
    UTIL.getAdmin()
        .createTable(TableDescriptorBuilder.newBuilder(NAME)
            .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF))
            .setCoprocessor(FlushObserver.class.getName()).build());
    region = UTIL.getHBaseCluster().getRegions(NAME).get(0);
  }

  @After
  public void tearDown() throws IOException {
    region = null;
    TRACKER = null;
    UTIL.deleteTable(NAME);
  }

  @Test
  public void test() throws IOException, InterruptedException {
    try (Table table = UTIL.getConnection().getTable(NAME)) {
      for (int i = 0; i < 100; i++) {
        byte[] row = Bytes.toBytes(i);
        table.put(new Put(row, true)
                    .add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
                        .setRow(row)
                        .setFamily(CF)
                        .setQualifier(QUALIFIER)
                        .setTimestamp(HConstants.LATEST_TIMESTAMP)
                        .setType(Type.Put)
                        .setValue(Bytes.toBytes(i))
                        .build()));
      }
    }
    Tracker tracker = new Tracker();
    TRACKER = tracker;
    region.requestFlush(tracker);
    tracker.await();
    assertNull(tracker.reason);
    assertTrue(tracker.beforeExecutionCalled);
    assertTrue(tracker.afterExecutionCalled);

    // request flush on a region with empty memstore should still success
    tracker = new Tracker();
    TRACKER = tracker;
    region.requestFlush(tracker);
    tracker.await();
    assertNull(tracker.reason);
    assertTrue(tracker.beforeExecutionCalled);
    assertTrue(tracker.afterExecutionCalled);
  }

  @Test
  public void testNotExecuted() throws IOException, InterruptedException {
    try (Table table = UTIL.getConnection().getTable(NAME)) {
      for (int i = 0; i < 100; i++) {
        byte[] row = Bytes.toBytes(i);
        table.put(new Put(row, true)
                    .add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
                        .setRow(row)
                        .setFamily(CF)
                        .setQualifier(QUALIFIER)
                        .setTimestamp(HConstants.LATEST_TIMESTAMP)
                        .setType(Type.Put)
                        .setValue(Bytes.toBytes(i))
                        .build()));
      }
    }
    // here we may have overlap when calling the CP hooks so we do not assert on TRACKER
    Tracker tracker1 = new Tracker();
    ARRIVE = new CountDownLatch(1);
    BLOCK = new CountDownLatch(1);
    region.requestFlush(tracker1);
    ARRIVE.await();

    Tracker tracker2 = new Tracker();
    region.requestFlush(tracker2);
    tracker2.await();
    assertNotNull(tracker2.reason);
    assertFalse(tracker2.beforeExecutionCalled);
    assertFalse(tracker2.afterExecutionCalled);

    BLOCK.countDown();
    tracker1.await();
    assertNull(tracker1.reason);
    assertTrue(tracker1.beforeExecutionCalled);
    assertTrue(tracker1.afterExecutionCalled);
  }
}
