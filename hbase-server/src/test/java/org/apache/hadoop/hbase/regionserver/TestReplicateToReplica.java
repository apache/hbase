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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNameTestRule;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.executor.ExecutorType;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.protobuf.ReplicationProtobufUtil;
import org.apache.hadoop.hbase.regionserver.HRegion.FlushResult;
import org.apache.hadoop.hbase.regionserver.regionreplication.RegionReplicationBufferManager;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ReplicateWALEntryRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.WALEntry;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestReplicateToReplica {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReplicateToReplica.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  private static byte[] FAMILY = Bytes.toBytes("family");

  private static byte[] QUAL = Bytes.toBytes("qualifier");

  private static ExecutorService EXEC;

  @Rule
  public final TableNameTestRule name = new TableNameTestRule();

  private TableName tableName;

  private Path testDir;

  private TableDescriptor td;

  private RegionServerServices rss;

  private AsyncClusterConnection conn;

  private RegionReplicationBufferManager manager;

  private FlushRequester flushRequester;

  private HRegion primary;

  private HRegion secondary;

  private WALFactory walFactory;

  private boolean queueReqAndResps;

  private Queue<Pair<List<WAL.Entry>, CompletableFuture<Void>>> reqAndResps;

  private static List<Put> TO_ADD_AFTER_PREPARE_FLUSH;

  public static final class HRegionForTest extends HRegion {

    public HRegionForTest(HRegionFileSystem fs, WAL wal, Configuration confParam,
      TableDescriptor htd, RegionServerServices rsServices) {
      super(fs, wal, confParam, htd, rsServices);
    }

    @SuppressWarnings("deprecation")
    public HRegionForTest(Path tableDir, WAL wal, FileSystem fs, Configuration confParam,
      RegionInfo regionInfo, TableDescriptor htd, RegionServerServices rsServices) {
      super(tableDir, wal, fs, confParam, regionInfo, htd, rsServices);
    }

    @Override
    protected PrepareFlushResult internalPrepareFlushCache(WAL wal, long myseqid,
      Collection<HStore> storesToFlush, MonitoredTask status, boolean writeFlushWalMarker,
      FlushLifeCycleTracker tracker) throws IOException {
      PrepareFlushResult result = super.internalPrepareFlushCache(wal, myseqid, storesToFlush,
        status, writeFlushWalMarker, tracker);
      for (Put put : TO_ADD_AFTER_PREPARE_FLUSH) {
        put(put);
      }
      TO_ADD_AFTER_PREPARE_FLUSH.clear();
      return result;
    }

  }

  @BeforeClass
  public static void setUpBeforeClass() {
    Configuration conf = UTIL.getConfiguration();
    conf.setInt("hbase.region.read-replica.sink.flush.min-interval.secs", 1);
    conf.setBoolean(ServerRegionReplicaUtil.REGION_REPLICA_REPLICATION_CONF_KEY, true);
    conf.setBoolean(ServerRegionReplicaUtil.REGION_REPLICA_REPLICATION_CATALOG_CONF_KEY, true);
    conf.setClass(HConstants.REGION_IMPL, HRegionForTest.class, HRegion.class);
    EXEC = new ExecutorService("test");
    EXEC.startExecutorService(EXEC.new ExecutorConfig().setCorePoolSize(1)
      .setExecutorType(ExecutorType.RS_COMPACTED_FILES_DISCHARGER));
    ChunkCreator.initialize(MemStoreLAB.CHUNK_SIZE_DEFAULT, false, 0, 0, 0, null,
      MemStoreLAB.INDEX_CHUNK_SIZE_PERCENTAGE_DEFAULT);
  }

  @AfterClass
  public static void tearDownAfterClass() {
    EXEC.shutdown();
    UTIL.cleanupTestDir();
  }

  @Before
  public void setUp() throws IOException {
    TO_ADD_AFTER_PREPARE_FLUSH = new ArrayList<>();
    tableName = name.getTableName();
    testDir = UTIL.getDataTestDir(tableName.getNameAsString());
    Configuration conf = UTIL.getConfiguration();
    conf.set(HConstants.HBASE_DIR, testDir.toString());

    td = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY)).setRegionReplication(2)
      .setRegionMemStoreReplication(true).build();

    reqAndResps = new ArrayDeque<>();
    queueReqAndResps = true;
    conn = mock(AsyncClusterConnection.class);
    when(conn.replicate(any(), anyList(), anyInt(), anyLong(), anyLong())).thenAnswer(i -> {
      if (queueReqAndResps) {
        @SuppressWarnings("unchecked")
        List<WAL.Entry> entries = i.getArgument(1, List.class);
        CompletableFuture<Void> future = new CompletableFuture<>();
        reqAndResps.add(Pair.newPair(entries, future));
        return future;
      } else {
        return CompletableFuture.completedFuture(null);
      }
    });

    flushRequester = mock(FlushRequester.class);

    rss = mock(RegionServerServices.class);
    when(rss.getServerName()).thenReturn(ServerName.valueOf("foo", 1, 1));
    when(rss.getConfiguration()).thenReturn(conf);
    when(rss.getRegionServerAccounting()).thenReturn(new RegionServerAccounting(conf));
    when(rss.getExecutorService()).thenReturn(EXEC);
    when(rss.getAsyncClusterConnection()).thenReturn(conn);
    when(rss.getFlushRequester()).thenReturn(flushRequester);

    manager = new RegionReplicationBufferManager(rss);
    when(rss.getRegionReplicationBufferManager()).thenReturn(manager);

    RegionInfo primaryHri = RegionInfoBuilder.newBuilder(td.getTableName()).build();
    RegionInfo secondaryHri = RegionReplicaUtil.getRegionInfoForReplica(primaryHri, 1);

    walFactory = new WALFactory(conf, UUID.randomUUID().toString());
    WAL wal = walFactory.getWAL(primaryHri);
    primary = HRegion.createHRegion(primaryHri, testDir, conf, td, wal);
    primary.close();

    primary = HRegion.openHRegion(testDir, primaryHri, td, wal, conf, rss, null);
    secondary = HRegion.openHRegion(secondaryHri, td, null, conf, rss, null);

    when(rss.getRegions()).then(i -> {
      return Arrays.asList(primary, secondary);
    });

    // process the open events
    replicateAll();
  }

  @After
  public void tearDown() throws IOException {
    // close region will issue a flush, which will enqueue an edit into the replication sink so we
    // need to complete it otherwise the test will hang.
    queueReqAndResps = false;
    failAll();
    HBaseTestingUtil.closeRegionAndWAL(primary);
    HBaseTestingUtil.closeRegionAndWAL(secondary);
    if (walFactory != null) {
      walFactory.close();
    }
  }

  private FlushResult flushPrimary() throws IOException {
    return primary.flushcache(true, true, FlushLifeCycleTracker.DUMMY);
  }

  private void replicate(Pair<List<WAL.Entry>, CompletableFuture<Void>> pair) throws IOException {
    Pair<ReplicateWALEntryRequest, CellScanner> params = ReplicationProtobufUtil
      .buildReplicateWALEntryRequest(pair.getFirst().toArray(new WAL.Entry[0]),
        secondary.getRegionInfo().getEncodedNameAsBytes(), null, null, null);
    for (WALEntry entry : params.getFirst().getEntryList()) {
      secondary.replayWALEntry(entry, params.getSecond());
    }
    pair.getSecond().complete(null);
  }

  private void replicateOne() throws IOException {
    replicate(reqAndResps.remove());
  }

  private void replicateAll() throws IOException {
    for (Pair<List<WAL.Entry>, CompletableFuture<Void>> pair;;) {
      pair = reqAndResps.poll();
      if (pair == null) {
        break;
      }
      replicate(pair);
    }
  }

  private void failOne() {
    reqAndResps.remove().getSecond().completeExceptionally(new IOException("Inject error"));
  }

  private void failAll() {
    for (Pair<List<WAL.Entry>, CompletableFuture<Void>> pair;;) {
      pair = reqAndResps.poll();
      if (pair == null) {
        break;
      }
      pair.getSecond().completeExceptionally(new IOException("Inject error"));
    }
  }

  @Test
  public void testNormalReplicate() throws IOException {
    byte[] row = Bytes.toBytes(0);
    primary.put(new Put(row).addColumn(FAMILY, QUAL, Bytes.toBytes(1)));
    replicateOne();
    assertEquals(1, Bytes.toInt(secondary.get(new Get(row)).getValue(FAMILY, QUAL)));
  }

  @Test
  public void testNormalFlush() throws IOException {
    byte[] row = Bytes.toBytes(0);
    primary.put(new Put(row).addColumn(FAMILY, QUAL, Bytes.toBytes(1)));
    TO_ADD_AFTER_PREPARE_FLUSH.add(new Put(row).addColumn(FAMILY, QUAL, Bytes.toBytes(2)));
    flushPrimary();
    replicateAll();
    assertEquals(2, Bytes.toInt(secondary.get(new Get(row)).getValue(FAMILY, QUAL)));

    // we should have the same memstore size, i.e, the secondary should have also dropped the
    // snapshot
    assertEquals(primary.getMemStoreDataSize(), secondary.getMemStoreDataSize());
  }

  @Test
  public void testErrorBeforeFlushStart() throws IOException {
    byte[] row = Bytes.toBytes(0);
    primary.put(new Put(row).addColumn(FAMILY, QUAL, Bytes.toBytes(1)));
    failOne();
    verify(flushRequester, times(1)).requestFlush(any(), anyList(), any());
    TO_ADD_AFTER_PREPARE_FLUSH.add(new Put(row).addColumn(FAMILY, QUAL, Bytes.toBytes(2)));
    flushPrimary();
    // this also tests start flush with empty memstore at secondary replica side
    replicateAll();
    assertEquals(2, Bytes.toInt(secondary.get(new Get(row)).getValue(FAMILY, QUAL)));
    assertEquals(primary.getMemStoreDataSize(), secondary.getMemStoreDataSize());
  }

  @Test
  public void testErrorAfterFlushStartBeforeFlushCommit() throws IOException {
    primary.put(new Put(Bytes.toBytes(0)).addColumn(FAMILY, QUAL, Bytes.toBytes(1)));
    replicateAll();
    TO_ADD_AFTER_PREPARE_FLUSH
      .add(new Put(Bytes.toBytes(1)).addColumn(FAMILY, QUAL, Bytes.toBytes(2)));
    flushPrimary();
    // replicate the start flush edit
    replicateOne();
    // fail the remaining edits, the put and the commit flush edit
    failOne();
    verify(flushRequester, times(1)).requestFlush(any(), anyList(), any());
    primary.put(new Put(Bytes.toBytes(2)).addColumn(FAMILY, QUAL, Bytes.toBytes(3)));
    flushPrimary();
    replicateAll();
    for (int i = 0; i < 3; i++) {
      assertEquals(i + 1,
        Bytes.toInt(secondary.get(new Get(Bytes.toBytes(i))).getValue(FAMILY, QUAL)));
    }
    // should have nothing in memstore
    assertEquals(0, secondary.getMemStoreDataSize());
  }

  @Test
  public void testCatchUpWithCannotFlush() throws IOException, InterruptedException {
    byte[] row = Bytes.toBytes(0);
    primary.put(new Put(row).addColumn(FAMILY, QUAL, Bytes.toBytes(1)));
    failOne();
    verify(flushRequester, times(1)).requestFlush(any(), anyList(), any());
    flushPrimary();
    failAll();
    Thread.sleep(2000);
    // we will request flush the second time
    verify(flushRequester, times(2)).requestFlush(any(), anyList(), any());
    // we can not flush because no content in memstore
    FlushResult result = flushPrimary();
    assertEquals(FlushResult.Result.CANNOT_FLUSH_MEMSTORE_EMPTY, result.getResult());
    // the secondary replica does not have this row yet
    assertFalse(secondary.get(new Get(row).setCheckExistenceOnly(true)).getExists().booleanValue());
    // replicate the can not flush edit
    replicateOne();
    // we should have the row now
    assertEquals(1, Bytes.toInt(secondary.get(new Get(row)).getValue(FAMILY, QUAL)));
  }

  @Test
  public void testCatchUpWithReopen() throws IOException {
    byte[] row = Bytes.toBytes(0);
    primary.put(new Put(row).addColumn(FAMILY, QUAL, Bytes.toBytes(1)));
    failOne();
    primary.close();
    // the secondary replica does not have this row yet, although the above close has flushed the
    // data out
    assertFalse(secondary.get(new Get(row).setCheckExistenceOnly(true)).getExists().booleanValue());

    // reopen
    primary = HRegion.openHRegion(testDir, primary.getRegionInfo(), td, primary.getWAL(),
      UTIL.getConfiguration(), rss, null);
    replicateAll();
    // we should have the row now
    assertEquals(1, Bytes.toInt(secondary.get(new Get(row)).getValue(FAMILY, QUAL)));
  }
}
