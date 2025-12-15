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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.StartTestingClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.MetaTableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.ipc.CallRunner;
import org.apache.hadoop.hbase.ipc.PluggableBlockingQueue;
import org.apache.hadoop.hbase.ipc.PriorityFunction;
import org.apache.hadoop.hbase.ipc.TestPluggableQueueImpl;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestRegionServerRejectDuringAbort {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionServerRejectDuringAbort.class);

  private static final Logger LOG =
    LoggerFactory.getLogger(TestRegionServerRejectDuringAbort.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  private static TableName TABLE_NAME = TableName.valueOf("RSRejectOnAbort");

  private static byte[] CF = Bytes.toBytes("cf");

  private static final int REGIONS_NUM = 5;

  private static final AtomicReference<Exception> THROWN_EXCEPTION = new AtomicReference<>(null);

  private static volatile boolean shouldThrowTooBig = false;

  @BeforeClass
  public static void setUp() throws Exception {
    // Will schedule a abort timeout task after SLEEP_TIME_WHEN_CLOSE_REGION ms
    UTIL.getConfiguration().set("hbase.ipc.server.callqueue.type", "pluggable");
    UTIL.getConfiguration().setClass("hbase.ipc.server.callqueue.pluggable.queue.class.name",
      CallQueueTooBigThrowingQueue.class, PluggableBlockingQueue.class);
    StartTestingClusterOption option =
      StartTestingClusterOption.builder().numRegionServers(2).build();
    UTIL.startMiniCluster(option);
    TableDescriptor td = TableDescriptorBuilder.newBuilder(TABLE_NAME)
      .setCoprocessor(SleepWhenCloseCoprocessor.class.getName())
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(CF).build()).build();
    UTIL.getAdmin().createTable(td, Bytes.toBytes("0"), Bytes.toBytes("9"), REGIONS_NUM);
  }

  public static final class CallQueueTooBigThrowingQueue extends TestPluggableQueueImpl {

    public CallQueueTooBigThrowingQueue(int maxQueueLength, PriorityFunction priority,
      Configuration conf) {
      super(maxQueueLength, priority, conf);
    }

    @Override
    public boolean offer(CallRunner callRunner) {
      if (shouldThrowTooBig && callRunner.getRpcCall().getRequestAttribute("test") != null) {
        return false;
      }
      return super.offer(callRunner);
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  /**
   * Tests that the logic in ServerRpcConnection works such that if the server is aborted, it short
   * circuits any other logic. This means we no longer even attempt to enqueue the request onto the
   * call queue. We verify this by using a special call queue which we can trigger to always return
   * CallQueueTooBigException. If the logic works, despite forcing those exceptions, we should not
   * see them.
   */
  @Test
  public void testRejectRequestsOnAbort() throws Exception {
    // We don't want to disrupt the server carrying meta, because we plan to disrupt requests to
    // the server. Disrupting meta requests messes with the test.
    HRegionServer serverWithoutMeta = null;
    for (JVMClusterUtil.RegionServerThread regionServerThread : UTIL.getMiniHBaseCluster()
      .getRegionServerThreads()) {
      HRegionServer regionServer = regionServerThread.getRegionServer();
      if (
        regionServer.getRegions(MetaTableName.getInstance()).isEmpty()
          && !regionServer.getRegions(TABLE_NAME).isEmpty()
      ) {
        serverWithoutMeta = regionServer;
        break;
      }
    }

    assertNotNull("couldn't find a server without meta, but with test table regions",
      serverWithoutMeta);

    Thread writer = new Thread(getWriterThreadRunnable(serverWithoutMeta.getServerName()));
    writer.setDaemon(true);
    writer.start();

    // Trigger the abort. Our WriterThread will detect the first RegionServerAbortedException
    // and trigger our custom queue to reject any more requests. This would typically result in
    // CallQueueTooBigException, unless our logic in ServerRpcConnection to preempt the processing
    // of a request is working.
    serverWithoutMeta.abort("Abort RS for test");

    UTIL.waitFor(60_000, () -> THROWN_EXCEPTION.get() != null);
    assertEquals(THROWN_EXCEPTION.get().getCause().getClass(), RegionServerAbortedException.class);
  }

  private Runnable getWriterThreadRunnable(ServerName loadServer) {
    return () -> {
      try {
        Configuration conf = UTIL.getConfiguration();
        conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 5);
        try (Connection conn = ConnectionFactory.createConnection(conf);
          Table table = conn.getTableBuilder(TABLE_NAME, null)
            .setRequestAttribute("test", new byte[] { 0 }).build()) {
          // find the first region to exist on our test server, then submit requests to it
          for (HRegionLocation regionLocation : table.getRegionLocator().getAllRegionLocations()) {
            if (regionLocation.getServerName().equals(loadServer)) {
              submitRequestsToRegion(table, regionLocation.getRegion());
              return;
            }
          }
          throw new RuntimeException("Failed to find any regions for loadServer " + loadServer);
        }
      } catch (Exception e) {
        LOG.warn("Failed to load data", e);
        synchronized (THROWN_EXCEPTION) {
          THROWN_EXCEPTION.set(e);
          THROWN_EXCEPTION.notifyAll();
        }
      }
    };
  }

  private void submitRequestsToRegion(Table table, RegionInfo regionInfo) throws IOException {
    // We will block closes of the regions with a CP, so no need to worry about the region getting
    // reassigned. Just use the same rowkey always.
    byte[] rowKey = getRowKeyWithin(regionInfo);

    int i = 0;
    while (true) {
      try {
        i++;
        table.put(new Put(rowKey).addColumn(CF, Bytes.toBytes(i), Bytes.toBytes(i)));
      } catch (IOException e) {
        // only catch RegionServerAbortedException once. After that, the next exception thrown
        // is our test case
        if (
          !shouldThrowTooBig && e instanceof RetriesExhaustedException
            && e.getCause() instanceof RegionServerAbortedException
        ) {
          shouldThrowTooBig = true;
        } else {
          throw e;
        }
      }

      // small sleep to relieve pressure
      Threads.sleep(10);
    }
  }

  private byte[] getRowKeyWithin(RegionInfo regionInfo) {
    byte[] rowKey;
    // region is start of table, find one after start key
    if (regionInfo.getStartKey().length == 0) {
      if (regionInfo.getEndKey().length == 0) {
        // doesn't matter, single region table
        return Bytes.toBytes(1);
      } else {
        // find a row just before endkey
        rowKey = Bytes.copy(regionInfo.getEndKey());
        rowKey[rowKey.length - 1]--;
        return rowKey;
      }
    } else {
      return regionInfo.getStartKey();
    }
  }

  public static class SleepWhenCloseCoprocessor implements RegionCoprocessor, RegionObserver {

    public SleepWhenCloseCoprocessor() {
    }

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void preClose(ObserverContext<? extends RegionCoprocessorEnvironment> c,
      boolean abortRequested) throws IOException {
      // Wait so that the region can't close until we get the information we need from our test
      UTIL.waitFor(60_000, () -> THROWN_EXCEPTION.get() != null);
    }
  }
}
