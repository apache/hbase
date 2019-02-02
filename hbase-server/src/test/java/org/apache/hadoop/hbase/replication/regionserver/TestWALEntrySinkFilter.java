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
package org.apache.hadoop.hbase.replication.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilder;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AdvancedScanResultConsumer;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.ClusterConnectionFactory;
import org.apache.hadoop.hbase.client.DummyAsyncClusterConnection;
import org.apache.hadoop.hbase.client.DummyAsyncRegistry;
import org.apache.hadoop.hbase.client.DummyAsyncTable;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;

/**
 * Simple test of sink-side wal entry filter facility.
 */
@Category({ ReplicationTests.class, SmallTests.class })
public class TestWALEntrySinkFilter {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestWALEntrySinkFilter.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestReplicationSink.class);
  @Rule
  public TestName name = new TestName();
  static final int BOUNDARY = 5;
  static final AtomicInteger UNFILTERED = new AtomicInteger();
  static final AtomicInteger FILTERED = new AtomicInteger();

  /**
   * Implemetentation of Stoppable to pass into ReplicationSink.
   */
  private static Stoppable STOPPABLE = new Stoppable() {
    private final AtomicBoolean stop = new AtomicBoolean(false);

    @Override
    public boolean isStopped() {
      return this.stop.get();
    }

    @Override
    public void stop(String why) {
      LOG.info("STOPPING BECAUSE: " + why);
      this.stop.set(true);
    }
  };

  /**
   * Test filter. Filter will filter out any write time that is <= 5 (BOUNDARY). We count how many
   * items we filter out and we count how many cells make it through for distribution way down below
   * in the Table#batch implementation. Puts in place a custom DevNullConnection so we can insert
   * our counting Table.
   * @throws IOException
   */
  @Test
  public void testWALEntryFilter() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    // Make it so our filter is instantiated on construction of ReplicationSink.
    conf.setClass(DummyAsyncRegistry.REGISTRY_IMPL_CONF_KEY, DevNullAsyncRegistry.class,
      DummyAsyncRegistry.class);
    conf.setClass(WALEntrySinkFilter.WAL_ENTRY_FILTER_KEY,
      IfTimeIsGreaterThanBOUNDARYWALEntrySinkFilterImpl.class, WALEntrySinkFilter.class);
    conf.setClass(ClusterConnectionFactory.HBASE_SERVER_CLUSTER_CONNECTION_IMPL,
      DevNullAsyncClusterConnection.class, AsyncClusterConnection.class);
    ReplicationSink sink = new ReplicationSink(conf, STOPPABLE);
    // Create some dumb walentries.
    List<AdminProtos.WALEntry> entries = new ArrayList<>();
    AdminProtos.WALEntry.Builder entryBuilder = AdminProtos.WALEntry.newBuilder();
    // Need a tablename.
    ByteString tableName =
      ByteString.copyFromUtf8(TableName.valueOf(this.name.getMethodName()).toString());
    // Add WALEdit Cells to Cells List. The way edits arrive at the sink is with protos
    // describing the edit with all Cells from all edits aggregated in a single CellScanner.
    final List<Cell> cells = new ArrayList<>();
    int count = BOUNDARY * 2;
    for (int i = 0; i < count; i++) {
      byte[] bytes = Bytes.toBytes(i);
      // Create a wal entry. Everything is set to the current index as bytes or int/long.
      entryBuilder.clear();
      entryBuilder.setKey(entryBuilder.getKeyBuilder().setLogSequenceNumber(i)
        .setEncodedRegionName(ByteString.copyFrom(bytes)).setWriteTime(i).setTableName(tableName)
        .build());
      // Lets have one Cell associated with each WALEdit.
      entryBuilder.setAssociatedCellCount(1);
      entries.add(entryBuilder.build());
      // We need to add a Cell per WALEdit to the cells array.
      CellBuilder cellBuilder = CellBuilderFactory.create(CellBuilderType.DEEP_COPY);
      // Make cells whose row, family, cell, value, and ts are == 'i'.
      Cell cell = cellBuilder.setRow(bytes).setFamily(bytes).setQualifier(bytes)
        .setType(Cell.Type.Put).setTimestamp(i).setValue(bytes).build();
      cells.add(cell);
    }
    // Now wrap our cells array in a CellScanner that we can pass in to replicateEntries. It has
    // all Cells from all the WALEntries made above.
    CellScanner cellScanner = new CellScanner() {
      // Set to -1 because advance gets called before current.
      int index = -1;

      @Override
      public Cell current() {
        return cells.get(index);
      }

      @Override
      public boolean advance() throws IOException {
        index++;
        return index < cells.size();
      }
    };
    // Call our sink.
    sink.replicateEntries(entries, cellScanner, null, null, null);
    // Check what made it through and what was filtered.
    assertTrue(FILTERED.get() > 0);
    assertTrue(UNFILTERED.get() > 0);
    assertEquals(count, FILTERED.get() + UNFILTERED.get());
  }

  /**
   * Simple filter that will filter out any entry wholse writeTime is <= 5.
   */
  public static class IfTimeIsGreaterThanBOUNDARYWALEntrySinkFilterImpl
      implements WALEntrySinkFilter {
    public IfTimeIsGreaterThanBOUNDARYWALEntrySinkFilterImpl() {
    }

    @Override
    public void init(AsyncConnection conn) {
      // Do nothing.
    }

    @Override
    public boolean filter(TableName table, long writeTime) {
      boolean b = writeTime <= BOUNDARY;
      if (b) {
        FILTERED.incrementAndGet();
      }
      return b;
    }
  }

  public static class DevNullAsyncRegistry extends DummyAsyncRegistry {

    public DevNullAsyncRegistry(Configuration conf) {
    }

    @Override
    public CompletableFuture<String> getClusterId() {
      return CompletableFuture.completedFuture("test");
    }
  }

  public static class DevNullAsyncClusterConnection extends DummyAsyncClusterConnection {

    private final Configuration conf;

    public DevNullAsyncClusterConnection(Configuration conf, Object registry, String clusterId,
        SocketAddress localAddress, User user) {
      this.conf = conf;
    }

    @Override
    public AsyncTable<AdvancedScanResultConsumer> getTable(TableName tableName) {
      return new DummyAsyncTable<AdvancedScanResultConsumer>() {

        @Override
        public <T> CompletableFuture<List<T>> batchAll(List<? extends Row> actions) {
          List<T> list = new ArrayList<>(actions.size());
          for (Row action : actions) {
            // Row is the index of the loop above where we make WALEntry and Cells.
            int row = Bytes.toInt(action.getRow());
            assertTrue("" + row, row > BOUNDARY);
            UNFILTERED.incrementAndGet();
            list.add(null);
          }
          return CompletableFuture.completedFuture(list);
        }
      };
    }

    @Override
    public Configuration getConfiguration() {
      return conf;
    }
  }
}
