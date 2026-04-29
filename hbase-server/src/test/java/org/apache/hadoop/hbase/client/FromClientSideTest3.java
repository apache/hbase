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
package org.apache.hadoop.hbase.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.MultiRowMutationEndpoint;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.exceptions.UnknownProtocolException;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.jupiter.api.TestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MultiRowMutationProtos;

public class FromClientSideTest3 extends FromClientSideTestBase {

  protected FromClientSideTest3(Class<? extends ConnectionRegistry> registryImpl,
    int numHedgedReqs) {
    super(registryImpl, numHedgedReqs);
  }

  private static final Logger LOG = LoggerFactory.getLogger(FromClientSideTest3.class);

  private static int WAITTABLE_MILLIS;

  protected static void startCluster(Class<?>... cps) throws Exception {
    WAITTABLE_MILLIS = 10000;
    SLAVES = 3;
    initialize(cps);
  }

  private void randomCFPuts(Table table, byte[] row, byte[] family, int nPuts) throws Exception {
    Put put = new Put(row);
    Random rand = ThreadLocalRandom.current();
    for (int i = 0; i < nPuts; i++) {
      byte[] qualifier = Bytes.toBytes(rand.nextInt());
      byte[] value = Bytes.toBytes(rand.nextInt());
      put.addColumn(family, qualifier, value);
    }
    table.put(put);
  }

  private void performMultiplePutAndFlush(Admin admin, Table table, byte[] row, byte[] family,
    int nFlushes, int nPuts) throws Exception {
    for (int i = 0; i < nFlushes; i++) {
      randomCFPuts(table, row, family, nPuts);
      admin.flush(table.getName());
    }
  }

  private static List<Cell> toList(ResultScanner scanner) {
    try {
      List<Cell> cells = new ArrayList<>();
      for (Result r : scanner) {
        cells.addAll(r.listCells());
      }
      return cells;
    } finally {
      scanner.close();
    }
  }

  @TestTemplate
  public void testScanAfterDeletingSpecifiedRow() throws IOException, InterruptedException {
    TEST_UTIL.createTable(tableName, new byte[][] { FAMILY });
    TEST_UTIL.waitTableAvailable(tableName, WAITTABLE_MILLIS);
    try (Connection conn = getConnection(); Table table = conn.getTable(tableName)) {
      TEST_UTIL.waitTableAvailable(tableName, WAITTABLE_MILLIS);
      byte[] row = Bytes.toBytes("SpecifiedRow");
      byte[] value0 = Bytes.toBytes("value_0");
      byte[] value1 = Bytes.toBytes("value_1");
      Put put = new Put(row);
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      table.put(put);
      Delete d = new Delete(row);
      table.delete(d);
      put = new Put(row);
      put.addColumn(FAMILY, null, value0);
      table.put(put);
      put = new Put(row);
      put.addColumn(FAMILY, null, value1);
      table.put(put);
      List<Cell> cells = toList(table.getScanner(new Scan()));
      assertEquals(1, cells.size());
      assertEquals("value_1", Bytes.toString(CellUtil.cloneValue(cells.get(0))));

      cells = toList(table.getScanner(new Scan().addFamily(FAMILY)));
      assertEquals(1, cells.size());
      assertEquals("value_1", Bytes.toString(CellUtil.cloneValue(cells.get(0))));

      cells = toList(table.getScanner(new Scan().addColumn(FAMILY, QUALIFIER)));
      assertEquals(0, cells.size());

      TEST_UTIL.getAdmin().flush(tableName);
      cells = toList(table.getScanner(new Scan()));
      assertEquals(1, cells.size());
      assertEquals("value_1", Bytes.toString(CellUtil.cloneValue(cells.get(0))));

      cells = toList(table.getScanner(new Scan().addFamily(FAMILY)));
      assertEquals(1, cells.size());
      assertEquals("value_1", Bytes.toString(CellUtil.cloneValue(cells.get(0))));

      cells = toList(table.getScanner(new Scan().addColumn(FAMILY, QUALIFIER)));
      assertEquals(0, cells.size());
    }
  }

  @TestTemplate
  public void testScanAfterDeletingSpecifiedRowV2() throws IOException, InterruptedException {
    TEST_UTIL.createTable(tableName, new byte[][] { FAMILY });
    TEST_UTIL.waitTableAvailable(tableName, WAITTABLE_MILLIS);
    try (Connection conn = getConnection(); Table table = conn.getTable(tableName)) {
      byte[] row = Bytes.toBytes("SpecifiedRow");
      byte[] qual0 = Bytes.toBytes("qual0");
      byte[] qual1 = Bytes.toBytes("qual1");
      long now = EnvironmentEdgeManager.currentTime();
      Delete d = new Delete(row, now);
      table.delete(d);

      Put put = new Put(row);
      put.addColumn(FAMILY, null, now + 1, VALUE);
      table.put(put);

      put = new Put(row);
      put.addColumn(FAMILY, qual1, now + 2, qual1);
      table.put(put);

      put = new Put(row);
      put.addColumn(FAMILY, qual0, now + 3, qual0);
      table.put(put);

      Result r = table.get(new Get(row));
      assertEquals(3, r.size(), r.toString());
      assertEquals("testValue", Bytes.toString(CellUtil.cloneValue(r.rawCells()[0])));
      assertEquals("qual0", Bytes.toString(CellUtil.cloneValue(r.rawCells()[1])));
      assertEquals("qual1", Bytes.toString(CellUtil.cloneValue(r.rawCells()[2])));

      TEST_UTIL.getAdmin().flush(tableName);
      r = table.get(new Get(row));
      assertEquals(3, r.size());
      assertEquals("testValue", Bytes.toString(CellUtil.cloneValue(r.rawCells()[0])));
      assertEquals("qual0", Bytes.toString(CellUtil.cloneValue(r.rawCells()[1])));
      assertEquals("qual1", Bytes.toString(CellUtil.cloneValue(r.rawCells()[2])));
    }
  }

  private int getStoreFileCount(Admin admin, ServerName serverName, RegionInfo region)
    throws IOException {
    for (RegionMetrics metrics : admin.getRegionMetrics(serverName, region.getTable())) {
      if (Bytes.equals(region.getRegionName(), metrics.getRegionName())) {
        return metrics.getStoreFileCount();
      }
    }
    return 0;
  }

  // override the config settings at the CF level and ensure priority
  @TestTemplate
  public void testAdvancedConfigOverride() throws Exception {
    /*
     * Overall idea: (1) create 3 store files and issue a compaction. config's compaction.min == 3,
     * so should work. (2) Increase the compaction.min toggle in the HTD to 5 and modify table. If
     * we use the HTD value instead of the default config value, adding 3 files and issuing a
     * compaction SHOULD NOT work (3) Decrease the compaction.min toggle in the HCD to 2 and modify
     * table. The CF schema should override the Table schema and now cause a minor compaction.
     */
    TEST_UTIL.getConfiguration().setInt("hbase.hstore.compaction.min", 3);
    TEST_UTIL.createTable(tableName, FAMILY, 10);
    TEST_UTIL.waitTableAvailable(tableName, WAITTABLE_MILLIS);
    try (Connection conn = getConnection(); Table table = conn.getTable(tableName);
      Admin admin = conn.getAdmin()) {
      // Create 3 store files.
      byte[] row = Bytes.toBytes(ThreadLocalRandom.current().nextInt());
      performMultiplePutAndFlush(admin, table, row, FAMILY, 3, 100);

      try (RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(tableName)) {
        // Verify we have multiple store files.
        HRegionLocation loc = locator.getRegionLocation(row, true);
        assertTrue(getStoreFileCount(admin, loc.getServerName(), loc.getRegion()) > 1);

        // Issue a compaction request
        admin.compact(tableName);

        // poll wait for the compactions to happen
        for (int i = 0; i < 10 * 1000 / 40; ++i) {
          // The number of store files after compaction should be lesser.
          loc = locator.getRegionLocation(row, true);
          if (!loc.getRegion().isOffline()) {
            if (getStoreFileCount(admin, loc.getServerName(), loc.getRegion()) <= 1) {
              break;
            }
          }
          Thread.sleep(40);
        }
        // verify the compactions took place and that we didn't just time out
        assertTrue(getStoreFileCount(admin, loc.getServerName(), loc.getRegion()) <= 1);

        // change the compaction.min config option for this table to 5
        LOG.info("hbase.hstore.compaction.min should now be 5");
        TableDescriptor htd = TableDescriptorBuilder.newBuilder(table.getDescriptor())
          .setValue("hbase.hstore.compaction.min", String.valueOf(5)).build();
        admin.modifyTable(htd);
        LOG.info("alter status finished");

        // Create 3 more store files.
        performMultiplePutAndFlush(admin, table, row, FAMILY, 3, 10);

        // Issue a compaction request
        admin.compact(tableName);

        // This time, the compaction request should not happen
        Thread.sleep(10 * 1000);
        loc = locator.getRegionLocation(row, true);
        int sfCount = getStoreFileCount(admin, loc.getServerName(), loc.getRegion());
        assertTrue(sfCount > 1);

        // change an individual CF's config option to 2 & online schema update
        LOG.info("hbase.hstore.compaction.min should now be 2");
        htd = TableDescriptorBuilder.newBuilder(htd)
          .modifyColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(htd.getColumnFamily(FAMILY))
            .setValue("hbase.hstore.compaction.min", String.valueOf(2)).build())
          .build();
        admin.modifyTable(htd);
        LOG.info("alter status finished");

        // Issue a compaction request
        admin.compact(tableName);

        // poll wait for the compactions to happen
        for (int i = 0; i < 10 * 1000 / 40; ++i) {
          loc = locator.getRegionLocation(row, true);
          try {
            if (getStoreFileCount(admin, loc.getServerName(), loc.getRegion()) < sfCount) {
              break;
            }
          } catch (Exception e) {
            LOG.debug("Waiting for region to come online: "
              + Bytes.toStringBinary(loc.getRegion().getRegionName()));
          }
          Thread.sleep(40);
        }

        // verify the compaction took place and that we didn't just time out
        assertTrue(getStoreFileCount(admin, loc.getServerName(), loc.getRegion()) < sfCount);

        // Finally, ensure that we can remove a custom config value after we made it
        LOG.info("Removing CF config value");
        LOG.info("hbase.hstore.compaction.min should now be 5");
        htd = TableDescriptorBuilder.newBuilder(htd)
          .modifyColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(htd.getColumnFamily(FAMILY))
            .setValue("hbase.hstore.compaction.min", null).build())
          .build();
        admin.modifyTable(htd);
        LOG.info("alter status finished");
        assertNull(table.getDescriptor().getColumnFamily(FAMILY)
          .getValue(Bytes.toBytes("hbase.hstore.compaction.min")));
      }
    }
  }

  @TestTemplate
  public void testPutWithPreBatchMutate() throws Exception {
    testPreBatchMutate(tableName, () -> {
      try (Connection conn = getConnection(); Table t = conn.getTable(tableName)) {
        Put put = new Put(ROW);
        put.addColumn(FAMILY, QUALIFIER, VALUE);
        t.put(put);
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    });
  }

  @TestTemplate
  public void testRowMutationsWithPreBatchMutate() throws Exception {
    testPreBatchMutate(tableName, () -> {
      try (Connection conn = getConnection(); Table t = conn.getTable(tableName)) {
        RowMutations rm = new RowMutations(ROW, 1);
        Put put = new Put(ROW);
        put.addColumn(FAMILY, QUALIFIER, VALUE);
        rm.add(put);
        t.mutateRow(rm);
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    });
  }

  private void testPreBatchMutate(TableName tableName, Runnable rn) throws Exception {
    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY))
      .setCoprocessor(WaitingForScanObserver.class.getName()).build();
    try (Connection conn = getConnection(); Admin admin = conn.getAdmin()) {
      admin.createTable(tableDescriptor);
      // Don't use waitTableAvailable(), because the scanner will mess up the co-processor

      ExecutorService service = Executors.newFixedThreadPool(2);
      service.execute(rn);
      final List<Cell> cells = new ArrayList<>();
      service.execute(() -> {
        try {
          // waiting for update.
          TimeUnit.SECONDS.sleep(3);
          try (Table t = conn.getTable(tableName)) {
            Scan scan = new Scan();
            try (ResultScanner scanner = t.getScanner(scan)) {
              for (Result r : scanner) {
                cells.addAll(Arrays.asList(r.rawCells()));
              }
            }
          }
        } catch (IOException | InterruptedException ex) {
          throw new RuntimeException(ex);
        }
      });
      service.shutdown();
      service.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
      assertEquals(0, cells.size(), "The write is blocking by RegionObserver#postBatchMutate"
        + ", so the data is invisible to reader");
    }
  }

  @TestTemplate
  public void testLockLeakWithDelta() throws Exception, Throwable {
    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY))
      .setCoprocessor(WaitingForMultiMutationsObserver.class.getName())
      .setValue("hbase.rowlock.wait.duration", String.valueOf(5000)).build();
    TEST_UTIL.getAdmin().createTable(tableDescriptor);
    TEST_UTIL.waitTableAvailable(tableName, WAITTABLE_MILLIS);

    // new a connection for lower retry number.
    Configuration copy = getClientConf();
    copy.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);
    try (Connection con = ConnectionFactory.createConnection(copy)) {
      HRegion region = (HRegion) find(tableName);
      region.setTimeoutForWriteLock(10);
      ExecutorService putService = Executors.newSingleThreadExecutor();
      putService.execute(() -> {
        try (Table table = con.getTable(tableName)) {
          Put put = new Put(ROW);
          put.addColumn(FAMILY, QUALIFIER, VALUE);
          // the put will be blocked by WaitingForMultiMutationsObserver.
          table.put(put);
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
      });
      ExecutorService appendService = Executors.newSingleThreadExecutor();
      appendService.execute(() -> {
        Append append = new Append(ROW);
        append.addColumn(FAMILY, QUALIFIER, VALUE);
        try (Table table = con.getTable(tableName)) {
          table.append(append);
          fail("The APPEND should fail because the target lock is blocked by previous put");
        } catch (Exception ex) {
        }
      });
      appendService.shutdown();
      appendService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
      WaitingForMultiMutationsObserver observer =
        find(tableName, WaitingForMultiMutationsObserver.class);
      observer.latch.countDown();
      putService.shutdown();
      putService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
      try (Table table = con.getTable(tableName)) {
        Result r = table.get(new Get(ROW));
        assertFalse(r.isEmpty());
        assertTrue(Bytes.equals(r.getValue(FAMILY, QUALIFIER), VALUE));
      }
    }
    HRegion region = (HRegion) find(tableName);
    int readLockCount = region.getReadLockCount();
    LOG.info("readLockCount:" + readLockCount);
    assertEquals(0, readLockCount);
  }

  @TestTemplate
  public void testMultiRowMutations() throws Exception, Throwable {
    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY))
      .setCoprocessor(MultiRowMutationEndpoint.class.getName())
      .setCoprocessor(WaitingForMultiMutationsObserver.class.getName())
      .setValue("hbase.rowlock.wait.duration", String.valueOf(5000)).build();
    TEST_UTIL.getAdmin().createTable(tableDescriptor);
    TEST_UTIL.waitTableAvailable(tableName, WAITTABLE_MILLIS);

    // new a connection for lower retry number.
    Configuration copy = getClientConf();
    copy.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);
    try (Connection con = ConnectionFactory.createConnection(copy)) {
      byte[] row = Bytes.toBytes("ROW-0");
      byte[] rowLocked = Bytes.toBytes("ROW-1");
      byte[] value0 = Bytes.toBytes("VALUE-0");
      byte[] value1 = Bytes.toBytes("VALUE-1");
      byte[] value2 = Bytes.toBytes("VALUE-2");
      assertNoLocks(tableName);
      ExecutorService putService = Executors.newSingleThreadExecutor();
      putService.execute(() -> {
        try (Table table = con.getTable(tableName)) {
          Put put0 = new Put(rowLocked);
          put0.addColumn(FAMILY, QUALIFIER, value0);
          // the put will be blocked by WaitingForMultiMutationsObserver.
          table.put(put0);
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
      });
      ExecutorService cpService = Executors.newSingleThreadExecutor();
      AtomicBoolean exceptionDuringMutateRows = new AtomicBoolean();
      cpService.execute(() -> {
        Put put1 = new Put(row);
        Put put2 = new Put(rowLocked);
        put1.addColumn(FAMILY, QUALIFIER, value1);
        put2.addColumn(FAMILY, QUALIFIER, value2);
        try (Table table = con.getTable(tableName)) {
          MultiRowMutationProtos.MutateRowsRequest request =
            MultiRowMutationProtos.MutateRowsRequest.newBuilder()
              .addMutationRequest(
                ProtobufUtil.toMutation(ClientProtos.MutationProto.MutationType.PUT, put1))
              .addMutationRequest(
                ProtobufUtil.toMutation(ClientProtos.MutationProto.MutationType.PUT, put2))
              .build();
          table.coprocessorService(MultiRowMutationProtos.MultiRowMutationService.class, ROW, ROW,
            (MultiRowMutationProtos.MultiRowMutationService exe) -> {
              ServerRpcController controller = new ServerRpcController();
              CoprocessorRpcUtils.BlockingRpcCallback<
                MultiRowMutationProtos.MutateRowsResponse> rpcCallback =
                  new CoprocessorRpcUtils.BlockingRpcCallback<>();
              exe.mutateRows(controller, request, rpcCallback);
              if (
                controller.failedOnException()
                  && !(controller.getFailedOn() instanceof UnknownProtocolException)
              ) {
                exceptionDuringMutateRows.set(true);
              }
              return rpcCallback.get();
            });
        } catch (Throwable ex) {
          LOG.error("encountered " + ex);
        }
      });
      cpService.shutdown();
      cpService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
      WaitingForMultiMutationsObserver observer =
        find(tableName, WaitingForMultiMutationsObserver.class);
      observer.latch.countDown();
      putService.shutdown();
      putService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
      try (Table table = con.getTable(tableName)) {
        Get g0 = new Get(row);
        Get g1 = new Get(rowLocked);
        Result r0 = table.get(g0);
        Result r1 = table.get(g1);
        assertTrue(r0.isEmpty());
        assertFalse(r1.isEmpty());
        assertTrue(Bytes.equals(r1.getValue(FAMILY, QUALIFIER), value0));
      }
      assertNoLocks(tableName);
      if (!exceptionDuringMutateRows.get()) {
        fail("This cp should fail because the target lock is blocked by previous put");
      }
    }
  }

  /**
   * A test case for issue HBASE-17482 After combile seqid with mvcc readpoint, seqid/mvcc is
   * acquired and stamped onto cells in the append thread, a countdown latch is used to ensure that
   * happened before cells can be put into memstore. But the MVCCPreAssign patch(HBASE-16698) make
   * the seqid/mvcc acquirement in handler thread and stamping in append thread No countdown latch
   * to assure cells in memstore are stamped with seqid/mvcc. If cells without mvcc(A.K.A mvcc=0)
   * are put into memstore, then a scanner with a smaller readpoint can see these data, which
   * disobey the multi version concurrency control rules. This test case is to reproduce this
   * scenario.
   */
  @TestTemplate
  public void testMVCCUsingMVCCPreAssign() throws IOException, InterruptedException {
    TEST_UTIL.createTable(tableName, new byte[][] { FAMILY });
    TEST_UTIL.waitTableAvailable(tableName, WAITTABLE_MILLIS);
    try (Connection conn = getConnection(); Table table = conn.getTable(tableName)) {
      // put two row first to init the scanner
      Put put = new Put(Bytes.toBytes("0"));
      put.addColumn(FAMILY, Bytes.toBytes(""), Bytes.toBytes("0"));
      table.put(put);
      put = new Put(Bytes.toBytes("00"));
      put.addColumn(FAMILY, Bytes.toBytes(""), Bytes.toBytes("0"));
      table.put(put);
      Scan scan = new Scan();
      scan.setTimeRange(0, Long.MAX_VALUE);
      scan.setCaching(1);
      try (ResultScanner scanner = table.getScanner(scan)) {
        int rowNum = scanner.next() != null ? 1 : 0;
        // the started scanner shouldn't see the rows put below
        for (int i = 1; i < 1000; i++) {
          put = new Put(Bytes.toBytes(String.valueOf(i)));
          put.setDurability(Durability.ASYNC_WAL);
          put.addColumn(FAMILY, Bytes.toBytes(""), Bytes.toBytes(i));
          table.put(put);
        }
        rowNum += Iterables.size(scanner);
        // scanner should only see two rows
        assertEquals(2, rowNum);
      }
      try (ResultScanner scanner = table.getScanner(scan)) {
        int rowNum = Iterables.size(scanner);
        // the new scanner should see all rows
        assertEquals(1001, rowNum);
      }
    }
  }

  private static void assertNoLocks(final TableName tableName)
    throws IOException, InterruptedException {
    HRegion region = (HRegion) find(tableName);
    assertEquals(0, region.getLockedRows().size());
  }

  private static HRegion find(final TableName tableName) throws IOException, InterruptedException {
    HRegionServer rs = TEST_UTIL.getRSForFirstRegionInTable(tableName);
    List<HRegion> regions = rs.getRegions(tableName);
    assertEquals(1, regions.size());
    return regions.get(0);
  }

  private static <T extends RegionObserver> T find(final TableName tableName, Class<T> clz)
    throws IOException, InterruptedException {
    HRegion region = find(tableName);
    Coprocessor cp = region.getCoprocessorHost().findCoprocessor(clz.getName());
    assertTrue(clz.isInstance(cp), "The cp instance should be " + clz.getName()
      + ", current instance is " + cp.getClass().getName());
    return clz.cast(cp);
  }

  public static class WaitingForMultiMutationsObserver
    implements RegionCoprocessor, RegionObserver {
    final CountDownLatch latch = new CountDownLatch(1);

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void postBatchMutate(final ObserverContext<? extends RegionCoprocessorEnvironment> c,
      final MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
      try {
        latch.await();
      } catch (InterruptedException ex) {
        throw new IOException(ex);
      }
    }
  }

  public static class WaitingForScanObserver implements RegionCoprocessor, RegionObserver {
    private final CountDownLatch latch = new CountDownLatch(1);

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void postBatchMutate(final ObserverContext<? extends RegionCoprocessorEnvironment> c,
      final MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
      try {
        // waiting for scanner
        latch.await();
      } catch (InterruptedException ex) {
        throw new IOException(ex);
      }
    }

    @Override
    public RegionScanner postScannerOpen(
      final ObserverContext<? extends RegionCoprocessorEnvironment> e, final Scan scan,
      final RegionScanner s) throws IOException {
      latch.countDown();
      return s;
    }
  }

  static byte[] generateHugeValue(int size) {
    Random rand = ThreadLocalRandom.current();
    byte[] value = new byte[size];
    for (int i = 0; i < value.length; i++) {
      value[i] = (byte) rand.nextInt(256);
    }
    return value;
  }

  @TestTemplate
  public void testScanWithBatchSizeReturnIncompleteCells()
    throws IOException, InterruptedException {
    TableDescriptor hd = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(FAMILY).setMaxVersions(3).build())
      .build();
    try (Table table = TEST_UTIL.createTable(hd, null)) {
      TEST_UTIL.waitTableAvailable(tableName, WAITTABLE_MILLIS);

      Put put = new Put(ROW);
      put.addColumn(FAMILY, Bytes.toBytes(0), generateHugeValue(3 * 1024 * 1024));
      table.put(put);

      put = new Put(ROW);
      put.addColumn(FAMILY, Bytes.toBytes(1), generateHugeValue(4 * 1024 * 1024));
      table.put(put);

      for (int i = 2; i < 5; i++) {
        for (int version = 0; version < 2; version++) {
          put = new Put(ROW);
          put.addColumn(FAMILY, Bytes.toBytes(i), generateHugeValue(1024));
          table.put(put);
        }
      }

      Scan scan = new Scan();
      scan.withStartRow(ROW).withStopRow(ROW, true).addFamily(FAMILY).setBatch(3)
        .setMaxResultSize(4 * 1024 * 1024);
      Result result;
      try (ResultScanner scanner = table.getScanner(scan)) {
        List<Result> list = new ArrayList<>();
        /*
         * The first scan rpc should return a result with 2 cells, because 3MB + 4MB > 4MB; The
         * second scan rpc should return a result with 3 cells, because reach the batch limit = 3;
         * The mayHaveMoreCellsInRow in last result should be false in the scan rpc. BTW, the
         * moreResultsInRegion also would be false. Finally, the client should collect all the cells
         * into two result: 2+3 -> 3+2;
         */
        while ((result = scanner.next()) != null) {
          list.add(result);
        }

        assertEquals(5, list.stream().mapToInt(Result::size).sum());
        assertEquals(2, list.size());
        assertEquals(3, list.get(0).size());
        assertEquals(2, list.get(1).size());
      }

      scan = new Scan();
      scan.withStartRow(ROW).withStopRow(ROW, true).addFamily(FAMILY).setBatch(2)
        .setMaxResultSize(4 * 1024 * 1024);
      try (ResultScanner scanner = table.getScanner(scan)) {
        List<Result> list = new ArrayList<>();
        while ((result = scanner.next()) != null) {
          list.add(result);
        }
        assertEquals(5, list.stream().mapToInt(Result::size).sum());
        assertEquals(3, list.size());
        assertEquals(2, list.get(0).size());
        assertEquals(2, list.get(1).size());
        assertEquals(1, list.get(2).size());
      }
    }
  }
}
