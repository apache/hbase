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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.hbase.Cell;
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
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshot;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshot.SpaceQuotaStatus;
import org.apache.hadoop.hbase.quotas.SpaceViolationPolicy;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionConfiguration;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Confirm that the function of CompactionLifeCycleTracker is OK as we do not use it in our own
 * code.
 */
@Category({ CoprocessorTests.class, MediumTests.class })
public class TestCompactionLifeCycleTracker {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCompactionLifeCycleTracker.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static final TableName NAME =
      TableName.valueOf(TestCompactionLifeCycleTracker.class.getSimpleName());

  private static final byte[] CF1 = Bytes.toBytes("CF1");

  private static final byte[] CF2 = Bytes.toBytes("CF2");

  private static final byte[] QUALIFIER = Bytes.toBytes("CQ");

  private HRegion region;

  private static CompactionLifeCycleTracker TRACKER = null;

  // make sure that we pass the correct CompactionLifeCycleTracker to CP hooks.
  public static final class CompactionObserver implements RegionObserver, RegionCoprocessor {

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void preCompactSelection(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
        List<? extends StoreFile> candidates, CompactionLifeCycleTracker tracker)
        throws IOException {
      if (TRACKER != null) {
        assertSame(tracker, TRACKER);
      }
    }

    @Override
    public void postCompactSelection(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
        List<? extends StoreFile> selected, CompactionLifeCycleTracker tracker,
        CompactionRequest request) {
      if (TRACKER != null) {
        assertSame(tracker, TRACKER);
      }
    }

    @Override
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
        InternalScanner scanner, ScanType scanType, CompactionLifeCycleTracker tracker,
        CompactionRequest request) throws IOException {
      if (TRACKER != null) {
        assertSame(tracker, TRACKER);
      }
      return scanner;
    }

    @Override
    public void postCompact(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
        StoreFile resultFile, CompactionLifeCycleTracker tracker, CompactionRequest request)
        throws IOException {
      if (TRACKER != null) {
        assertSame(tracker, TRACKER);
      }
    }
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    UTIL.getConfiguration().setInt(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MIN_KEY, 2);
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
            .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF1))
            .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF2))
            .setCoprocessor(CompactionObserver.class.getName()).build());
    try (Table table = UTIL.getConnection().getTable(NAME)) {
      for (int i = 0; i < 100; i++) {
        byte[] row = Bytes.toBytes(i);
        table.put(new Put(row)
                    .add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
                        .setRow(row)
                        .setFamily(CF1)
                        .setQualifier(QUALIFIER)
                        .setTimestamp(HConstants.LATEST_TIMESTAMP)
                        .setType(Cell.Type.Put)
                        .setValue(Bytes.toBytes(i))
                        .build()));
      }
      UTIL.getAdmin().flush(NAME);
      for (int i = 100; i < 200; i++) {
        byte[] row = Bytes.toBytes(i);
        table.put(new Put(row)
                    .add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
                        .setRow(row)
                        .setFamily(CF1)
                        .setQualifier(QUALIFIER)
                        .setTimestamp(HConstants.LATEST_TIMESTAMP)
                        .setType(Type.Put)
                        .setValue(Bytes.toBytes(i))
                        .build()));
      }
      UTIL.getAdmin().flush(NAME);
    }
    region = UTIL.getHBaseCluster().getRegions(NAME).get(0);
    assertEquals(2, region.getStore(CF1).getStorefilesCount());
    assertEquals(0, region.getStore(CF2).getStorefilesCount());
  }

  @After
  public void tearDown() throws IOException {
    region = null;
    TRACKER = null;
    UTIL.deleteTable(NAME);
  }

  private static final class Tracker implements CompactionLifeCycleTracker {

    final List<Pair<Store, String>> notExecutedStores = new ArrayList<>();

    final List<Store> beforeExecuteStores = new ArrayList<>();

    final List<Store> afterExecuteStores = new ArrayList<>();

    private boolean completed = false;

    @Override
    public void notExecuted(Store store, String reason) {
      notExecutedStores.add(Pair.newPair(store, reason));
    }

    @Override
    public void beforeExecution(Store store) {
      beforeExecuteStores.add(store);
    }

    @Override
    public void afterExecution(Store store) {
      afterExecuteStores.add(store);
    }

    @Override
    public synchronized void completed() {
      completed = true;
      notifyAll();
    }

    public synchronized void await() throws InterruptedException {
      while (!completed) {
        wait();
      }
    }
  }

  @Test
  public void testRequestOnRegion() throws IOException, InterruptedException {
    Tracker tracker = new Tracker();
    TRACKER = tracker;
    region.requestCompaction("test", Store.PRIORITY_USER, false, tracker);
    tracker.await();
    assertEquals(1, tracker.notExecutedStores.size());
    assertEquals(Bytes.toString(CF2),
      tracker.notExecutedStores.get(0).getFirst().getColumnFamilyName());
    assertThat(tracker.notExecutedStores.get(0).getSecond(),
      containsString("compaction request was cancelled"));

    assertEquals(1, tracker.beforeExecuteStores.size());
    assertEquals(Bytes.toString(CF1), tracker.beforeExecuteStores.get(0).getColumnFamilyName());

    assertEquals(1, tracker.afterExecuteStores.size());
    assertEquals(Bytes.toString(CF1), tracker.afterExecuteStores.get(0).getColumnFamilyName());
  }

  @Test
  public void testRequestOnStore() throws IOException, InterruptedException {
    Tracker tracker = new Tracker();
    TRACKER = tracker;
    region.requestCompaction(CF1, "test", Store.PRIORITY_USER, false, tracker);
    tracker.await();
    assertTrue(tracker.notExecutedStores.isEmpty());
    assertEquals(1, tracker.beforeExecuteStores.size());
    assertEquals(Bytes.toString(CF1), tracker.beforeExecuteStores.get(0).getColumnFamilyName());
    assertEquals(1, tracker.afterExecuteStores.size());
    assertEquals(Bytes.toString(CF1), tracker.afterExecuteStores.get(0).getColumnFamilyName());

    tracker = new Tracker();
    TRACKER = tracker;
    region.requestCompaction(CF2, "test", Store.PRIORITY_USER, false, tracker);
    tracker.await();
    assertEquals(1, tracker.notExecutedStores.size());
    assertEquals(Bytes.toString(CF2),
      tracker.notExecutedStores.get(0).getFirst().getColumnFamilyName());
    assertThat(tracker.notExecutedStores.get(0).getSecond(),
      containsString("compaction request was cancelled"));
    assertTrue(tracker.beforeExecuteStores.isEmpty());
    assertTrue(tracker.afterExecuteStores.isEmpty());
  }

  // This test assumes that compaction wouldn't happen with null user.
  // But null user means system generated compaction so compaction should happen
  // even if the space quota is violated. So this test should be removed/ignored.
  @Ignore @Test
  public void testSpaceQuotaViolation() throws IOException, InterruptedException {
    region.getRegionServerServices().getRegionServerSpaceQuotaManager().enforceViolationPolicy(NAME,
      new SpaceQuotaSnapshot(new SpaceQuotaStatus(SpaceViolationPolicy.NO_WRITES_COMPACTIONS), 10L,
          100L));
    Tracker tracker = new Tracker();
    TRACKER = tracker;
    region.requestCompaction("test", Store.PRIORITY_USER, false, tracker);
    tracker.await();
    assertEquals(2, tracker.notExecutedStores.size());
    tracker.notExecutedStores.sort((p1, p2) -> p1.getFirst().getColumnFamilyName()
        .compareTo(p2.getFirst().getColumnFamilyName()));

    assertEquals(Bytes.toString(CF2),
      tracker.notExecutedStores.get(1).getFirst().getColumnFamilyName());
    assertThat(tracker.notExecutedStores.get(1).getSecond(),
      containsString("space quota violation"));

    assertTrue(tracker.beforeExecuteStores.isEmpty());
    assertTrue(tracker.afterExecuteStores.isEmpty());
  }
}
