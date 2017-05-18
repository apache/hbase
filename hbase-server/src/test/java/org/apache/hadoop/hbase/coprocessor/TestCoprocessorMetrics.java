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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter.Predicate;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.metrics.Counter;
import org.apache.hadoop.hbase.metrics.Metric;
import org.apache.hadoop.hbase.metrics.MetricRegistries;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.metrics.MetricRegistryInfo;
import org.apache.hadoop.hbase.metrics.Timer;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos.MultiRowMutationService;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos.MutateRowsRequest;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos.MutateRowsResponse;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * Testing of coprocessor metrics end-to-end.
 */
@Category({CoprocessorTests.class, MediumTests.class})
public class TestCoprocessorMetrics {

  private static final Log LOG = LogFactory.getLog(TestCoprocessorMetrics.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static final byte[] foo = Bytes.toBytes("foo");
  private static final byte[] bar = Bytes.toBytes("bar");
  /**
   * MasterObserver that has a Timer metric for create table operation.
   */
  public static class CustomMasterObserver extends BaseMasterObserver {
    private Timer createTableTimer;
    private long start = Long.MIN_VALUE;

    @Override
    public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
                               HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
      super.preCreateTable(ctx, desc, regions);

      // we rely on the fact that there is only 1 instance of our MasterObserver
      this.start = System.currentTimeMillis();
    }

    @Override
    public void postCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
      super.postCreateTable(ctx, desc, regions);
      if (this.start > 0) {
        long time = System.currentTimeMillis() - start;
        LOG.info("Create table took: " + time);
        createTableTimer.updateMillis(time);
      }
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
      super.start(env);
      if (env instanceof MasterCoprocessorEnvironment) {
        MetricRegistry registry =
            ((MasterCoprocessorEnvironment) env).getMetricRegistryForMaster();

        createTableTimer  = registry.timer("CreateTable");
      }
    }
  }

  /**
   * RegionServerObserver that has a Counter for rollWAL requests.
   */
  public static class CustomRegionServerObserver extends BaseRegionServerObserver {
    /** This is the Counter metric object to keep track of the current count across invocations */
    private Counter rollWALCounter;
    @Override
    public void postRollWALWriterRequest(ObserverContext<RegionServerCoprocessorEnvironment> ctx)
        throws IOException {
      // Increment the Counter whenever the coprocessor is called
      rollWALCounter.increment();
      super.postRollWALWriterRequest(ctx);
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
      super.start(env);
      if (env instanceof RegionServerCoprocessorEnvironment) {
        MetricRegistry registry =
            ((RegionServerCoprocessorEnvironment) env).getMetricRegistryForRegionServer();

        if (rollWALCounter == null) {
          rollWALCounter = registry.counter("rollWALRequests");
        }
      }
    }
  }

  /**
   * WALObserver that has a Counter for walEdits written.
   */
  public static class CustomWALObserver extends BaseWALObserver {
    private Counter walEditsCount;

    @Override
    public void postWALWrite(ObserverContext<? extends WALCoprocessorEnvironment> ctx,
                             HRegionInfo info, org.apache.hadoop.hbase.wal.WALKey logKey,
                             WALEdit logEdit) throws IOException {
      super.postWALWrite(ctx, info, logKey, logEdit);
      walEditsCount.increment();
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
      super.start(env);
      if (env instanceof WALCoprocessorEnvironment) {
        MetricRegistry registry =
            ((WALCoprocessorEnvironment) env).getMetricRegistryForRegionServer();

        if (walEditsCount == null) {
          walEditsCount = registry.counter("walEditsCount");
        }
      }
    }
  }

  /**
   * RegionObserver that has a Counter for preGet()
   */
  public static class CustomRegionObserver extends BaseRegionObserver {
    private Counter preGetCounter;

    @Override
    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get,
                         List<Cell> results) throws IOException {
      super.preGetOp(e, get, results);
      preGetCounter.increment();
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
      super.start(env);

      if (env instanceof RegionCoprocessorEnvironment) {
        MetricRegistry registry =
            ((RegionCoprocessorEnvironment) env).getMetricRegistryForRegionServer();

        if (preGetCounter == null) {
          preGetCounter = registry.counter("preGetRequests");
        }
      }
    }
  }

  public static class CustomRegionObserver2 extends CustomRegionObserver {
  }

  /**
   * RegionEndpoint to test metrics from endpoint calls
   */
  public static class CustomRegionEndpoint extends MultiRowMutationEndpoint {

    private Timer endpointExecution;

    @Override
    public void mutateRows(RpcController controller, MutateRowsRequest request,
                           RpcCallback<MutateRowsResponse> done) {
      long start = System.nanoTime();
      super.mutateRows(controller, request, done);
      endpointExecution.updateNanos(System.nanoTime() - start);
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
      super.start(env);

      if (env instanceof RegionCoprocessorEnvironment) {
        MetricRegistry registry =
            ((RegionCoprocessorEnvironment) env).getMetricRegistryForRegionServer();

        if (endpointExecution == null) {
          endpointExecution = registry.timer("EndpointExecution");
        }
      }
    }
  }

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    // inject master, regionserver and WAL coprocessors
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
        CustomMasterObserver.class.getName());
    conf.set(CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY,
        CustomRegionServerObserver.class.getName());
    conf.set(CoprocessorHost.WAL_COPROCESSOR_CONF_KEY,
        CustomWALObserver.class.getName());
    conf.setBoolean(CoprocessorHost.ABORT_ON_ERROR_KEY, true);
    UTIL.startMiniCluster();
  }

  @AfterClass
  public static void teardownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Before
  public void setup() throws IOException {
    try (Connection connection = ConnectionFactory.createConnection(UTIL.getConfiguration());
         Admin admin = connection.getAdmin()) {
      for (HTableDescriptor htd : admin.listTables()) {
        UTIL.deleteTable(htd.getTableName());
      }
    }
  }

  @Test
  public void testMasterObserver() throws IOException {
    // Find out the MetricRegistry used by the CP using the global registries
    MetricRegistryInfo info = MetricsCoprocessor.createRegistryInfoForMasterCoprocessor(
        CustomMasterObserver.class.getName());
    Optional<MetricRegistry> registry =  MetricRegistries.global().get(info);
    assertTrue(registry.isPresent());

    Optional<Metric> metric = registry.get().get("CreateTable");
    assertTrue(metric.isPresent());

    try (Connection connection = ConnectionFactory.createConnection(UTIL.getConfiguration());
         Admin admin = connection.getAdmin()) {

      Timer createTableTimer = (Timer)metric.get();
      long prevCount = createTableTimer.getHistogram().getCount();
      LOG.info("Creating table");
      admin.createTable(
          new HTableDescriptor("testMasterObserver")
              .addFamily(new HColumnDescriptor("foo")));

      assertEquals(1, createTableTimer.getHistogram().getCount() - prevCount);
    }
  }

  @Test
  public void testRegionServerObserver() throws IOException {
    try (Connection connection = ConnectionFactory.createConnection(UTIL.getConfiguration());
         Admin admin = connection.getAdmin()) {
      LOG.info("Rolling WALs");
      admin.rollWALWriter(UTIL.getMiniHBaseCluster().getServerHoldingMeta());
    }

    // Find out the MetricRegistry used by the CP using the global registries
    MetricRegistryInfo info = MetricsCoprocessor.createRegistryInfoForRSCoprocessor(
        CustomRegionServerObserver.class.getName());

    Optional<MetricRegistry> registry =  MetricRegistries.global().get(info);
    assertTrue(registry.isPresent());

    Optional<Metric> metric = registry.get().get("rollWALRequests");
    assertTrue(metric.isPresent());

    Counter rollWalRequests = (Counter)metric.get();
    assertEquals(1, rollWalRequests.getCount());
  }

  @Test
  public void testWALObserver() throws IOException {
    // Find out the MetricRegistry used by the CP using the global registries
    MetricRegistryInfo info = MetricsCoprocessor.createRegistryInfoForWALCoprocessor(
        CustomWALObserver.class.getName());

    Optional<MetricRegistry> registry =  MetricRegistries.global().get(info);
    assertTrue(registry.isPresent());

    Optional<Metric> metric = registry.get().get("walEditsCount");
    assertTrue(metric.isPresent());

    try (Connection connection = ConnectionFactory.createConnection(UTIL.getConfiguration());
         Admin admin = connection.getAdmin()) {
      admin.createTable(
          new HTableDescriptor("testWALObserver")
              .addFamily(new HColumnDescriptor("foo")));

      Counter rollWalRequests = (Counter)metric.get();
      long prevCount = rollWalRequests.getCount();
      assertTrue(prevCount > 0);

      try (Table table = connection.getTable(TableName.valueOf("testWALObserver"))) {
        table.put(new Put(foo).addColumn(foo, foo, foo));
      }

      assertEquals(1, rollWalRequests.getCount() - prevCount);
    }
  }

  /**
   * Helper for below tests
   */
  private void assertPreGetRequestsCounter(Class<?> coprocClass) {
    // Find out the MetricRegistry used by the CP using the global registries
    MetricRegistryInfo info = MetricsCoprocessor.createRegistryInfoForRegionCoprocessor(
        coprocClass.getName());

    Optional<MetricRegistry> registry =  MetricRegistries.global().get(info);
    assertTrue(registry.isPresent());

    Optional<Metric> metric = registry.get().get("preGetRequests");
    assertTrue(metric.isPresent());

    Counter preGetRequests = (Counter)metric.get();
    assertEquals(2, preGetRequests.getCount());
  }

  @Test
  public void testRegionObserverSingleRegion() throws IOException {
    TableName tableName = TableName.valueOf("testRegionObserverSingleRegion");
    try (Connection connection = ConnectionFactory.createConnection(UTIL.getConfiguration());
         Admin admin = connection.getAdmin()) {
      admin.createTable(
          new HTableDescriptor(tableName)
              .addFamily(new HColumnDescriptor(foo))
              // add the coprocessor for the region
              .addCoprocessor(CustomRegionObserver.class.getName()));
      try (Table table = connection.getTable(tableName)) {
        table.get(new Get(foo));
        table.get(new Get(foo)); // 2 gets
      }
    }

    assertPreGetRequestsCounter(CustomRegionObserver.class);
  }

  @Test
  public void testRegionObserverMultiRegion() throws IOException {
    TableName tableName = TableName.valueOf("testRegionObserverMultiRegion");
    try (Connection connection = ConnectionFactory.createConnection(UTIL.getConfiguration());
         Admin admin = connection.getAdmin()) {
      admin.createTable(
          new HTableDescriptor(tableName)
              .addFamily(new HColumnDescriptor(foo))
              // add the coprocessor for the region
              .addCoprocessor(CustomRegionObserver.class.getName())
          , new byte[][]{foo}); // create with 2 regions
      try (Table table = connection.getTable(tableName);
           RegionLocator locator = connection.getRegionLocator(tableName)) {
        table.get(new Get(bar));
        table.get(new Get(foo)); // 2 gets to 2 separate regions
        assertEquals(2, locator.getAllRegionLocations().size());
        assertNotEquals(locator.getRegionLocation(bar).getRegionInfo(),
            locator.getRegionLocation(foo).getRegionInfo());
      }
    }

    assertPreGetRequestsCounter(CustomRegionObserver.class);
  }

  @Test
  public void testRegionObserverMultiTable() throws IOException {
    TableName tableName1 = TableName.valueOf("testRegionObserverMultiTable1");
    TableName tableName2 = TableName.valueOf("testRegionObserverMultiTable2");
    try (Connection connection = ConnectionFactory.createConnection(UTIL.getConfiguration());
         Admin admin = connection.getAdmin()) {
      admin.createTable(
          new HTableDescriptor(tableName1)
              .addFamily(new HColumnDescriptor(foo))
              // add the coprocessor for the region
              .addCoprocessor(CustomRegionObserver.class.getName()));
      admin.createTable(
          new HTableDescriptor(tableName2)
              .addFamily(new HColumnDescriptor(foo))
              // add the coprocessor for the region
              .addCoprocessor(CustomRegionObserver.class.getName()));
      try (Table table1 = connection.getTable(tableName1);
           Table table2 = connection.getTable(tableName2);) {
        table1.get(new Get(bar));
        table2.get(new Get(foo)); // 2 gets to 2 separate tables
      }
    }
    assertPreGetRequestsCounter(CustomRegionObserver.class);
  }

  @Test
  public void testRegionObserverMultiCoprocessor() throws IOException {
    TableName tableName = TableName.valueOf("testRegionObserverMultiCoprocessor");
    try (Connection connection = ConnectionFactory.createConnection(UTIL.getConfiguration());
         Admin admin = connection.getAdmin()) {
      admin.createTable(
          new HTableDescriptor(tableName)
              .addFamily(new HColumnDescriptor(foo))
              // add the coprocessor for the region. We add two different coprocessors
              .addCoprocessor(CustomRegionObserver.class.getName())
              .addCoprocessor(CustomRegionObserver2.class.getName()));
      try (Table table = connection.getTable(tableName)) {
        table.get(new Get(foo));
        table.get(new Get(foo)); // 2 gets
      }
    }

    // we will have two counters coming from two coprocs, in two different MetricRegistries
    assertPreGetRequestsCounter(CustomRegionObserver.class);
    assertPreGetRequestsCounter(CustomRegionObserver2.class);
  }

  @Test
  public void testRegionObserverAfterRegionClosed() throws IOException {
    TableName tableName = TableName.valueOf("testRegionObserverAfterRegionClosed");
    try (Connection connection = ConnectionFactory.createConnection(UTIL.getConfiguration());
         Admin admin = connection.getAdmin()) {
      admin.createTable(
          new HTableDescriptor(tableName)
              .addFamily(new HColumnDescriptor(foo))
              // add the coprocessor for the region
              .addCoprocessor(CustomRegionObserver.class.getName())
          , new byte[][]{foo}); // create with 2 regions
      try (Table table = connection.getTable(tableName)) {
        table.get(new Get(foo));
        table.get(new Get(foo)); // 2 gets
      }

      assertPreGetRequestsCounter(CustomRegionObserver.class);

      // close one of the regions
      try (RegionLocator locator = connection.getRegionLocator(tableName)) {
        final HRegionLocation loc = locator.getRegionLocation(foo);
        admin.closeRegion(loc.getServerName(), loc.getRegionInfo());

        final HRegionServer server = UTIL.getMiniHBaseCluster().getRegionServer(loc.getServerName());
        UTIL.waitFor(30000,new Predicate<IOException>() {
          @Override
          public boolean evaluate() throws IOException {
            return server.getOnlineRegion(loc.getRegionInfo().getRegionName()) == null;
          }
        });
        assertNull(server.getOnlineRegion(loc.getRegionInfo().getRegionName()));
      }

      // with only 1 region remaining, we should still be able to find the Counter
      assertPreGetRequestsCounter(CustomRegionObserver.class);

      // close the table
      admin.disableTable(tableName);

      MetricRegistryInfo info = MetricsCoprocessor.createRegistryInfoForRegionCoprocessor(
          CustomRegionObserver.class.getName());

      // ensure that MetricRegistry is deleted
      Optional<MetricRegistry> registry =  MetricRegistries.global().get(info);
      assertFalse(registry.isPresent());
    }
  }

  @Test
  public void testRegionObserverEndpoint() throws IOException, ServiceException {
    TableName tableName = TableName.valueOf("testRegionObserverEndpoint");
    try (Connection connection = ConnectionFactory.createConnection(UTIL.getConfiguration());
         Admin admin = connection.getAdmin()) {
      admin.createTable(
          new HTableDescriptor(tableName)
              .addFamily(new HColumnDescriptor(foo))
              // add the coprocessor for the region
              .addCoprocessor(CustomRegionEndpoint.class.getName()));

      try (Table table = connection.getTable(tableName)) {
        List<Put> mutations = Lists.newArrayList(new Put(foo), new Put(bar));
        MutateRowsRequest.Builder mrmBuilder = MutateRowsRequest.newBuilder();

        for (Mutation mutation : mutations) {
          mrmBuilder.addMutationRequest(ProtobufUtil.toMutation(
              ClientProtos.MutationProto.MutationType.PUT, mutation));
        }

        CoprocessorRpcChannel channel = table.coprocessorService(bar);
        MultiRowMutationService.BlockingInterface service =
            MultiRowMutationService.newBlockingStub(channel);
        MutateRowsRequest mrm = mrmBuilder.build();
        service.mutateRows(null, mrm);
      }
    }

    // Find out the MetricRegistry used by the CP using the global registries
    MetricRegistryInfo info = MetricsCoprocessor.createRegistryInfoForRegionCoprocessor(
        CustomRegionEndpoint.class.getName());

    Optional<MetricRegistry> registry =  MetricRegistries.global().get(info);
    assertTrue(registry.isPresent());

    Optional<Metric> metric = registry.get().get("EndpointExecution");
    assertTrue(metric.isPresent());

    Timer endpointExecutions = (Timer)metric.get();
    assertEquals(1, endpointExecutions.getHistogram().getCount());
  }
}
