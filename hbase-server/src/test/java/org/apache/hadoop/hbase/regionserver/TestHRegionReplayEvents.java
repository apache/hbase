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

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.apache.hadoop.hbase.regionserver.TestHRegion.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.BulkLoadDescriptor;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.CompactionDescriptor;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.FlushDescriptor;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.FlushDescriptor.StoreFlushDescriptor;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.RegionEventDescriptor;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.FlushDescriptor.FlushAction;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.RegionEventDescriptor.EventType;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.StoreDescriptor;
import org.apache.hadoop.hbase.regionserver.HRegion.FlushResultImpl;
import org.apache.hadoop.hbase.regionserver.HRegion.PrepareFlushResult;
import org.apache.hadoop.hbase.regionserver.compactions.NoLimitCompactionThroughputController;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.DefaultWALProvider;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALSplitter.MutationReplay;
import org.apache.hadoop.util.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;

/**
 * Tests of HRegion methods for replaying flush, compaction, region open, etc events for secondary
 * region replicas
 */
@Category(MediumTests.class)
public class TestHRegionReplayEvents {

  private static final Log LOG = LogFactory.getLog(TestHRegion.class);
  @Rule public TestName name = new TestName();

  private static HBaseTestingUtility TEST_UTIL;

  public static Configuration CONF ;
  private String dir;
  private static FileSystem FILESYSTEM;

  private byte[][] families = new byte[][] {
      Bytes.toBytes("cf1"), Bytes.toBytes("cf2"), Bytes.toBytes("cf3")};

  // Test names
  protected byte[] tableName;
  protected String method;
  protected final byte[] row = Bytes.toBytes("rowA");
  protected final byte[] row2 = Bytes.toBytes("rowB");
  protected byte[] cq = Bytes.toBytes("cq");

  // per test fields
  private Path rootDir;
  private HTableDescriptor htd;
  private long time;
  private RegionServerServices rss;
  private HRegionInfo primaryHri, secondaryHri;
  private HRegion primaryRegion, secondaryRegion;
  private WALFactory wals;
  private WAL walPrimary, walSecondary;
  private WAL.Reader reader;

  @Before
  public void setup() throws IOException {
    TEST_UTIL = HBaseTestingUtility.createLocalHTU();
    FILESYSTEM = TEST_UTIL.getTestFileSystem();
    CONF = TEST_UTIL.getConfiguration();
    dir = TEST_UTIL.getDataTestDir("TestHRegionReplayEvents").toString();
    method = name.getMethodName();
    tableName = Bytes.toBytes(name.getMethodName());
    rootDir = new Path(dir + method);
    TEST_UTIL.getConfiguration().set(HConstants.HBASE_DIR, rootDir.toString());
    method = name.getMethodName();

    htd = new HTableDescriptor(TableName.valueOf(method));
    for (byte[] family : families) {
      htd.addFamily(new HColumnDescriptor(family));
    }

    time = System.currentTimeMillis();

    primaryHri = new HRegionInfo(htd.getTableName(),
      HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW,
      false, time, 0);
    secondaryHri = new HRegionInfo(htd.getTableName(),
      HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW,
      false, time, 1);

    wals = TestHRegion.createWALFactory(CONF, rootDir);
    walPrimary = wals.getWAL(primaryHri.getEncodedNameAsBytes(),
        primaryHri.getTable().getNamespace());
    walSecondary = wals.getWAL(secondaryHri.getEncodedNameAsBytes(),
        secondaryHri.getTable().getNamespace());

    rss = mock(RegionServerServices.class);
    when(rss.getServerName()).thenReturn(ServerName.valueOf("foo", 1, 1));
    when(rss.getConfiguration()).thenReturn(CONF);
    when(rss.getRegionServerAccounting()).thenReturn(new RegionServerAccounting());
    String string = org.apache.hadoop.hbase.executor.EventType.RS_COMPACTED_FILES_DISCHARGER
        .toString();
    ExecutorService es = new ExecutorService(string);
    es.startExecutorService(
      string+"-"+string, 1);
    when(rss.getExecutorService()).thenReturn(es);
    primaryRegion = HRegion.createHRegion(primaryHri, rootDir, CONF, htd, walPrimary);
    primaryRegion.close();
    List<Region> regions = new ArrayList<Region>();
    regions.add(primaryRegion);
    when(rss.getOnlineRegions()).thenReturn(regions);

    primaryRegion = HRegion.openHRegion(rootDir, primaryHri, htd, walPrimary, CONF, rss, null);
    secondaryRegion = HRegion.openHRegion(secondaryHri, htd, null, CONF, rss, null);

    reader = null;
  }

  @After
  public void tearDown() throws Exception {
    if (reader != null) {
      reader.close();
    }

    if (primaryRegion != null) {
      HBaseTestingUtility.closeRegionAndWAL(primaryRegion);
    }
    if (secondaryRegion != null) {
      HBaseTestingUtility.closeRegionAndWAL(secondaryRegion);
    }

    EnvironmentEdgeManagerTestHelper.reset();
    LOG.info("Cleaning test directory: " + TEST_UTIL.getDataTestDir());
    TEST_UTIL.cleanupTestDir();
  }

  String getName() {
    return name.getMethodName();
  }

  // Some of the test cases are as follows:
  // 1. replay flush start marker again
  // 2. replay flush with smaller seqId than what is there in memstore snapshot
  // 3. replay flush with larger seqId than what is there in memstore snapshot
  // 4. replay flush commit without flush prepare. non droppable memstore
  // 5. replay flush commit without flush prepare. droppable memstore
  // 6. replay open region event
  // 7. replay open region event after flush start
  // 8. replay flush form an earlier seqId (test ignoring seqIds)
  // 9. start flush does not prevent region from closing.

  @Test
  public void testRegionReplicaSecondaryCannotFlush() throws IOException {
    // load some data and flush ensure that the secondary replica will not execute the flush

    // load some data to secondary by replaying
    putDataByReplay(secondaryRegion, 0, 1000, cq, families);

    verifyData(secondaryRegion, 0, 1000, cq, families);

    // flush region
    FlushResultImpl flush = (FlushResultImpl)secondaryRegion.flush(true);
    assertEquals(flush.result, FlushResultImpl.Result.CANNOT_FLUSH);

    verifyData(secondaryRegion, 0, 1000, cq, families);

    // close the region, and inspect that it has not flushed
    Map<byte[], List<StoreFile>> files = secondaryRegion.close(false);
    // assert that there are no files (due to flush)
    for (List<StoreFile> f : files.values()) {
      assertTrue(f.isEmpty());
    }
  }

  /**
   * Tests a case where we replay only a flush start marker, then the region is closed. This region
   * should not block indefinitely
   */
  @Test (timeout = 60000)
  public void testOnlyReplayingFlushStartDoesNotHoldUpRegionClose() throws IOException {
    // load some data to primary and flush
    int start = 0;
    LOG.info("-- Writing some data to primary from " +  start + " to " + (start+100));
    putData(primaryRegion, Durability.SYNC_WAL, start, 100, cq, families);
    LOG.info("-- Flushing primary, creating 3 files for 3 stores");
    primaryRegion.flush(true);

    // now replay the edits and the flush marker
    reader = createWALReaderForPrimary();

    LOG.info("-- Replaying edits and flush events in secondary");
    while (true) {
      WAL.Entry entry = reader.next();
      if (entry == null) {
        break;
      }
      FlushDescriptor flushDesc
        = WALEdit.getFlushDescriptor(entry.getEdit().getCells().get(0));
      if (flushDesc != null) {
        if (flushDesc.getAction() == FlushAction.START_FLUSH) {
          LOG.info("-- Replaying flush start in secondary");
          PrepareFlushResult result = secondaryRegion.replayWALFlushStartMarker(flushDesc);
        } else if (flushDesc.getAction() == FlushAction.COMMIT_FLUSH) {
          LOG.info("-- NOT Replaying flush commit in secondary");
        }
      } else {
        replayEdit(secondaryRegion, entry);
      }
    }

    assertTrue(rss.getRegionServerAccounting().getGlobalMemstoreSize() > 0);
    // now close the region which should not cause hold because of un-committed flush
    secondaryRegion.close();

    // verify that the memstore size is back to what it was
    assertEquals(0, rss.getRegionServerAccounting().getGlobalMemstoreSize());
  }

  static int replayEdit(HRegion region, WAL.Entry entry) throws IOException {
    if (WALEdit.isMetaEditFamily(entry.getEdit().getCells().get(0))) {
      return 0; // handled elsewhere
    }
    Put put = new Put(CellUtil.cloneRow(entry.getEdit().getCells().get(0)));
    for (Cell cell : entry.getEdit().getCells()) put.add(cell);
    put.setDurability(Durability.SKIP_WAL);
    MutationReplay mutation = new MutationReplay(MutationType.PUT, put, 0, 0);
    region.batchReplay(new MutationReplay[] {mutation},
      entry.getKey().getLogSeqNum());
    return Integer.parseInt(Bytes.toString(put.getRow()));
  }

  WAL.Reader createWALReaderForPrimary() throws FileNotFoundException, IOException {
    return wals.createReader(TEST_UTIL.getTestFileSystem(),
      DefaultWALProvider.getCurrentFileName(walPrimary),
      TEST_UTIL.getConfiguration());
  }

  @Test
  public void testReplayFlushesAndCompactions() throws IOException {
    // initiate a secondary region with some data.

    // load some data to primary and flush. 3 flushes and some more unflushed data
    putDataWithFlushes(primaryRegion, 100, 300, 100);

    // compaction from primary
    LOG.info("-- Compacting primary, only 1 store");
    primaryRegion.compactStore(Bytes.toBytes("cf1"),
      NoLimitCompactionThroughputController.INSTANCE);

    // now replay the edits and the flush marker
    reader = createWALReaderForPrimary();

    LOG.info("-- Replaying edits and flush events in secondary");
    int lastReplayed = 0;
    int expectedStoreFileCount = 0;
    while (true) {
      WAL.Entry entry = reader.next();
      if (entry == null) {
        break;
      }
      FlushDescriptor flushDesc
      = WALEdit.getFlushDescriptor(entry.getEdit().getCells().get(0));
      CompactionDescriptor compactionDesc
      = WALEdit.getCompaction(entry.getEdit().getCells().get(0));
      if (flushDesc != null) {
        // first verify that everything is replayed and visible before flush event replay
        verifyData(secondaryRegion, 0, lastReplayed, cq, families);
        Store store = secondaryRegion.getStore(Bytes.toBytes("cf1"));
        long storeMemstoreSize = store.getMemStoreSize();
        long regionMemstoreSize = secondaryRegion.getMemstoreSize();
        long storeFlushableSize = store.getFlushableSize();
        long storeSize = store.getSize();
        long storeSizeUncompressed = store.getStoreSizeUncompressed();
        if (flushDesc.getAction() == FlushAction.START_FLUSH) {
          LOG.info("-- Replaying flush start in secondary");
          PrepareFlushResult result = secondaryRegion.replayWALFlushStartMarker(flushDesc);
          assertNull(result.result);
          assertEquals(result.flushOpSeqId, flushDesc.getFlushSequenceNumber());

          // assert that the store memstore is smaller now
          long newStoreMemstoreSize = store.getMemStoreSize();
          LOG.info("Memstore size reduced by:"
              + StringUtils.humanReadableInt(newStoreMemstoreSize - storeMemstoreSize));
          assertTrue(storeMemstoreSize > newStoreMemstoreSize);

        } else if (flushDesc.getAction() == FlushAction.COMMIT_FLUSH) {
          LOG.info("-- Replaying flush commit in secondary");
          secondaryRegion.replayWALFlushCommitMarker(flushDesc);

          // assert that the flush files are picked
          expectedStoreFileCount++;
          for (Store s : secondaryRegion.getStores()) {
            assertEquals(expectedStoreFileCount, s.getStorefilesCount());
          }
          long newFlushableSize = store.getFlushableSize();
          assertTrue(storeFlushableSize > newFlushableSize);

          // assert that the region memstore is smaller now
          long newRegionMemstoreSize = secondaryRegion.getMemstoreSize();
          assertTrue(regionMemstoreSize > newRegionMemstoreSize);

          // assert that the store sizes are bigger
          assertTrue(store.getSize() > storeSize);
          assertTrue(store.getStoreSizeUncompressed() > storeSizeUncompressed);
          assertEquals(store.getSize(), store.getStorefilesSize());
        }
        // after replay verify that everything is still visible
        verifyData(secondaryRegion, 0, lastReplayed+1, cq, families);
      } else if (compactionDesc != null) {
        secondaryRegion.replayWALCompactionMarker(compactionDesc, true, false, Long.MAX_VALUE);

        // assert that the compaction is applied
        for (Store store : secondaryRegion.getStores()) {
          if (store.getColumnFamilyName().equals("cf1")) {
            assertEquals(1, store.getStorefilesCount());
          } else {
            assertEquals(expectedStoreFileCount, store.getStorefilesCount());
          }
        }
      } else {
        lastReplayed = replayEdit(secondaryRegion, entry);;
      }
    }

    assertEquals(400-1, lastReplayed);
    LOG.info("-- Verifying edits from secondary");
    verifyData(secondaryRegion, 0, 400, cq, families);

    LOG.info("-- Verifying edits from primary. Ensuring that files are not deleted");
    verifyData(primaryRegion, 0, lastReplayed, cq, families);
    for (Store store : primaryRegion.getStores()) {
      if (store.getColumnFamilyName().equals("cf1")) {
        assertEquals(1, store.getStorefilesCount());
      } else {
        assertEquals(expectedStoreFileCount, store.getStorefilesCount());
      }
    }
  }

  /**
   * Tests cases where we prepare a flush with some seqId and we receive other flush start markers
   * equal to, greater or less than the previous flush start marker.
   */
  @Test
  public void testReplayFlushStartMarkers() throws IOException {
    // load some data to primary and flush. 1 flush and some more unflushed data
    putDataWithFlushes(primaryRegion, 100, 100, 100);
    int numRows = 200;

    // now replay the edits and the flush marker
    reader =  createWALReaderForPrimary();

    LOG.info("-- Replaying edits and flush events in secondary");

    FlushDescriptor startFlushDesc = null;

    int lastReplayed = 0;
    while (true) {
      WAL.Entry entry = reader.next();
      if (entry == null) {
        break;
      }
      FlushDescriptor flushDesc
      = WALEdit.getFlushDescriptor(entry.getEdit().getCells().get(0));
      if (flushDesc != null) {
        // first verify that everything is replayed and visible before flush event replay
        Store store = secondaryRegion.getStore(Bytes.toBytes("cf1"));
        long storeMemstoreSize = store.getMemStoreSize();
        long regionMemstoreSize = secondaryRegion.getMemstoreSize();
        long storeFlushableSize = store.getFlushableSize();

        if (flushDesc.getAction() == FlushAction.START_FLUSH) {
          startFlushDesc = flushDesc;
          LOG.info("-- Replaying flush start in secondary");
          PrepareFlushResult result = secondaryRegion.replayWALFlushStartMarker(startFlushDesc);
          assertNull(result.result);
          assertEquals(result.flushOpSeqId, startFlushDesc.getFlushSequenceNumber());
          assertTrue(regionMemstoreSize > 0);
          assertTrue(storeFlushableSize > 0);

          // assert that the store memstore is smaller now
          long newStoreMemstoreSize = store.getMemStoreSize();
          LOG.info("Memstore size reduced by:"
              + StringUtils.humanReadableInt(newStoreMemstoreSize - storeMemstoreSize));
          assertTrue(storeMemstoreSize > newStoreMemstoreSize);
          verifyData(secondaryRegion, 0, lastReplayed+1, cq, families);

        }
        // after replay verify that everything is still visible
        verifyData(secondaryRegion, 0, lastReplayed+1, cq, families);
      } else {
        lastReplayed = replayEdit(secondaryRegion, entry);
      }
    }

    // at this point, there should be some data (rows 0-100) in memstore snapshot
    // and some more data in memstores (rows 100-200)

    verifyData(secondaryRegion, 0, numRows, cq, families);

    // Test case 1: replay the same flush start marker again
    LOG.info("-- Replaying same flush start in secondary again");
    PrepareFlushResult result = secondaryRegion.replayWALFlushStartMarker(startFlushDesc);
    assertNull(result); // this should return null. Ignoring the flush start marker
    // assert that we still have prepared flush with the previous setup.
    assertNotNull(secondaryRegion.getPrepareFlushResult());
    assertEquals(secondaryRegion.getPrepareFlushResult().flushOpSeqId,
      startFlushDesc.getFlushSequenceNumber());
    assertTrue(secondaryRegion.getMemstoreSize() > 0); // memstore is not empty
    verifyData(secondaryRegion, 0, numRows, cq, families);

    // Test case 2: replay a flush start marker with a smaller seqId
    FlushDescriptor startFlushDescSmallerSeqId
      = clone(startFlushDesc, startFlushDesc.getFlushSequenceNumber() - 50);
    LOG.info("-- Replaying same flush start in secondary again " + startFlushDescSmallerSeqId);
    result = secondaryRegion.replayWALFlushStartMarker(startFlushDescSmallerSeqId);
    assertNull(result); // this should return null. Ignoring the flush start marker
    // assert that we still have prepared flush with the previous setup.
    assertNotNull(secondaryRegion.getPrepareFlushResult());
    assertEquals(secondaryRegion.getPrepareFlushResult().flushOpSeqId,
      startFlushDesc.getFlushSequenceNumber());
    assertTrue(secondaryRegion.getMemstoreSize() > 0); // memstore is not empty
    verifyData(secondaryRegion, 0, numRows, cq, families);

    // Test case 3: replay a flush start marker with a larger seqId
    FlushDescriptor startFlushDescLargerSeqId
      = clone(startFlushDesc, startFlushDesc.getFlushSequenceNumber() + 50);
    LOG.info("-- Replaying same flush start in secondary again " + startFlushDescLargerSeqId);
    result = secondaryRegion.replayWALFlushStartMarker(startFlushDescLargerSeqId);
    assertNull(result); // this should return null. Ignoring the flush start marker
    // assert that we still have prepared flush with the previous setup.
    assertNotNull(secondaryRegion.getPrepareFlushResult());
    assertEquals(secondaryRegion.getPrepareFlushResult().flushOpSeqId,
      startFlushDesc.getFlushSequenceNumber());
    assertTrue(secondaryRegion.getMemstoreSize() > 0); // memstore is not empty
    verifyData(secondaryRegion, 0, numRows, cq, families);

    LOG.info("-- Verifying edits from secondary");
    verifyData(secondaryRegion, 0, numRows, cq, families);

    LOG.info("-- Verifying edits from primary.");
    verifyData(primaryRegion, 0, numRows, cq, families);
  }

  /**
   * Tests the case where we prepare a flush with some seqId and we receive a flush commit marker
   * less than the previous flush start marker.
   */
  @Test
  public void testReplayFlushCommitMarkerSmallerThanFlushStartMarker() throws IOException {
    // load some data to primary and flush. 2 flushes and some more unflushed data
    putDataWithFlushes(primaryRegion, 100, 200, 100);
    int numRows = 300;

    // now replay the edits and the flush marker
    reader =  createWALReaderForPrimary();

    LOG.info("-- Replaying edits and flush events in secondary");
    FlushDescriptor startFlushDesc = null;
    FlushDescriptor commitFlushDesc = null;

    int lastReplayed = 0;
    while (true) {
      System.out.println(lastReplayed);
      WAL.Entry entry = reader.next();
      if (entry == null) {
        break;
      }
      FlushDescriptor flushDesc
      = WALEdit.getFlushDescriptor(entry.getEdit().getCells().get(0));
      if (flushDesc != null) {
        if (flushDesc.getAction() == FlushAction.START_FLUSH) {
          // don't replay the first flush start marker, hold on to it, replay the second one
          if (startFlushDesc == null) {
            startFlushDesc = flushDesc;
          } else {
            LOG.info("-- Replaying flush start in secondary");
            startFlushDesc = flushDesc;
            PrepareFlushResult result = secondaryRegion.replayWALFlushStartMarker(startFlushDesc);
            assertNull(result.result);
          }
        } else if (flushDesc.getAction() == FlushAction.COMMIT_FLUSH) {
          // do not replay any flush commit yet
          if (commitFlushDesc == null) {
            commitFlushDesc = flushDesc; // hold on to the first flush commit marker
          }
        }
        // after replay verify that everything is still visible
        verifyData(secondaryRegion, 0, lastReplayed+1, cq, families);
      } else {
        lastReplayed = replayEdit(secondaryRegion, entry);
      }
    }

    // at this point, there should be some data (rows 0-200) in memstore snapshot
    // and some more data in memstores (rows 200-300)
    verifyData(secondaryRegion, 0, numRows, cq, families);

    // no store files in the region
    int expectedStoreFileCount = 0;
    for (Store s : secondaryRegion.getStores()) {
      assertEquals(expectedStoreFileCount, s.getStorefilesCount());
    }
    long regionMemstoreSize = secondaryRegion.getMemstoreSize();

    // Test case 1: replay the a flush commit marker smaller than what we have prepared
    LOG.info("Testing replaying flush COMMIT " + commitFlushDesc + " on top of flush START"
        + startFlushDesc);
    assertTrue(commitFlushDesc.getFlushSequenceNumber() < startFlushDesc.getFlushSequenceNumber());

    LOG.info("-- Replaying flush commit in secondary" + commitFlushDesc);
    secondaryRegion.replayWALFlushCommitMarker(commitFlushDesc);

    // assert that the flush files are picked
    expectedStoreFileCount++;
    for (Store s : secondaryRegion.getStores()) {
      assertEquals(expectedStoreFileCount, s.getStorefilesCount());
    }
    Store store = secondaryRegion.getStore(Bytes.toBytes("cf1"));
    long newFlushableSize = store.getFlushableSize();
    assertTrue(newFlushableSize > 0); // assert that the memstore is not dropped

    // assert that the region memstore is same as before
    long newRegionMemstoreSize = secondaryRegion.getMemstoreSize();
    assertEquals(regionMemstoreSize, newRegionMemstoreSize);

    assertNotNull(secondaryRegion.getPrepareFlushResult()); // not dropped

    LOG.info("-- Verifying edits from secondary");
    verifyData(secondaryRegion, 0, numRows, cq, families);

    LOG.info("-- Verifying edits from primary.");
    verifyData(primaryRegion, 0, numRows, cq, families);
  }

  /**
   * Tests the case where we prepare a flush with some seqId and we receive a flush commit marker
   * larger than the previous flush start marker.
   */
  @Test
  public void testReplayFlushCommitMarkerLargerThanFlushStartMarker() throws IOException {
    // load some data to primary and flush. 1 flush and some more unflushed data
    putDataWithFlushes(primaryRegion, 100, 100, 100);
    int numRows = 200;

    // now replay the edits and the flush marker
    reader =  createWALReaderForPrimary();

    LOG.info("-- Replaying edits and flush events in secondary");
    FlushDescriptor startFlushDesc = null;
    FlushDescriptor commitFlushDesc = null;

    int lastReplayed = 0;
    while (true) {
      WAL.Entry entry = reader.next();
      if (entry == null) {
        break;
      }
      FlushDescriptor flushDesc
      = WALEdit.getFlushDescriptor(entry.getEdit().getCells().get(0));
      if (flushDesc != null) {
        if (flushDesc.getAction() == FlushAction.START_FLUSH) {
          if (startFlushDesc == null) {
            LOG.info("-- Replaying flush start in secondary");
            startFlushDesc = flushDesc;
            PrepareFlushResult result = secondaryRegion.replayWALFlushStartMarker(startFlushDesc);
            assertNull(result.result);
          }
        } else if (flushDesc.getAction() == FlushAction.COMMIT_FLUSH) {
          // do not replay any flush commit yet
          // hold on to the flush commit marker but simulate a larger
          // flush commit seqId
          commitFlushDesc =
              FlushDescriptor.newBuilder(flushDesc)
              .setFlushSequenceNumber(flushDesc.getFlushSequenceNumber() + 50)
              .build();
        }
        // after replay verify that everything is still visible
        verifyData(secondaryRegion, 0, lastReplayed+1, cq, families);
      } else {
        lastReplayed = replayEdit(secondaryRegion, entry);
      }
    }

    // at this point, there should be some data (rows 0-100) in memstore snapshot
    // and some more data in memstores (rows 100-200)
    verifyData(secondaryRegion, 0, numRows, cq, families);

    // no store files in the region
    int expectedStoreFileCount = 0;
    for (Store s : secondaryRegion.getStores()) {
      assertEquals(expectedStoreFileCount, s.getStorefilesCount());
    }
    long regionMemstoreSize = secondaryRegion.getMemstoreSize();

    // Test case 1: replay the a flush commit marker larger than what we have prepared
    LOG.info("Testing replaying flush COMMIT " + commitFlushDesc + " on top of flush START"
        + startFlushDesc);
    assertTrue(commitFlushDesc.getFlushSequenceNumber() > startFlushDesc.getFlushSequenceNumber());

    LOG.info("-- Replaying flush commit in secondary" + commitFlushDesc);
    secondaryRegion.replayWALFlushCommitMarker(commitFlushDesc);

    // assert that the flush files are picked
    expectedStoreFileCount++;
    for (Store s : secondaryRegion.getStores()) {
      assertEquals(expectedStoreFileCount, s.getStorefilesCount());
    }
    Store store = secondaryRegion.getStore(Bytes.toBytes("cf1"));
    long newFlushableSize = store.getFlushableSize();
    assertTrue(newFlushableSize > 0); // assert that the memstore is not dropped

    // assert that the region memstore is smaller than before, but not empty
    long newRegionMemstoreSize = secondaryRegion.getMemstoreSize();
    assertTrue(newRegionMemstoreSize > 0);
    assertTrue(regionMemstoreSize > newRegionMemstoreSize);

    assertNull(secondaryRegion.getPrepareFlushResult()); // prepare snapshot should be dropped

    LOG.info("-- Verifying edits from secondary");
    verifyData(secondaryRegion, 0, numRows, cq, families);

    LOG.info("-- Verifying edits from primary.");
    verifyData(primaryRegion, 0, numRows, cq, families);
  }

  /**
   * Tests the case where we receive a flush commit before receiving any flush prepare markers.
   * The memstore edits should be dropped after the flush commit replay since they should be in
   * flushed files
   */
  @Test
  public void testReplayFlushCommitMarkerWithoutFlushStartMarkerDroppableMemstore()
      throws IOException {
    testReplayFlushCommitMarkerWithoutFlushStartMarker(true);
  }

  /**
   * Tests the case where we receive a flush commit before receiving any flush prepare markers.
   * The memstore edits should be not dropped after the flush commit replay since not every edit
   * will be in flushed files (based on seqId)
   */
  @Test
  public void testReplayFlushCommitMarkerWithoutFlushStartMarkerNonDroppableMemstore()
      throws IOException {
    testReplayFlushCommitMarkerWithoutFlushStartMarker(false);
  }

  /**
   * Tests the case where we receive a flush commit before receiving any flush prepare markers
   */
  public void testReplayFlushCommitMarkerWithoutFlushStartMarker(boolean droppableMemstore)
      throws IOException {
    // load some data to primary and flush. 1 flushes and some more unflushed data.
    // write more data after flush depending on whether droppableSnapshot
    putDataWithFlushes(primaryRegion, 100, 100, droppableMemstore ? 0 : 100);
    int numRows = droppableMemstore ? 100 : 200;

    // now replay the edits and the flush marker
    reader =  createWALReaderForPrimary();

    LOG.info("-- Replaying edits and flush events in secondary");
    FlushDescriptor commitFlushDesc = null;

    int lastReplayed = 0;
    while (true) {
      WAL.Entry entry = reader.next();
      if (entry == null) {
        break;
      }
      FlushDescriptor flushDesc
      = WALEdit.getFlushDescriptor(entry.getEdit().getCells().get(0));
      if (flushDesc != null) {
        if (flushDesc.getAction() == FlushAction.START_FLUSH) {
          // do not replay flush start marker
        } else if (flushDesc.getAction() == FlushAction.COMMIT_FLUSH) {
          commitFlushDesc = flushDesc; // hold on to the flush commit marker
        }
        // after replay verify that everything is still visible
        verifyData(secondaryRegion, 0, lastReplayed+1, cq, families);
      } else {
        lastReplayed = replayEdit(secondaryRegion, entry);
      }
    }

    // at this point, there should be some data (rows 0-200) in the memstore without snapshot
    // and some more data in memstores (rows 100-300)
    verifyData(secondaryRegion, 0, numRows, cq, families);

    // no store files in the region
    int expectedStoreFileCount = 0;
    for (Store s : secondaryRegion.getStores()) {
      assertEquals(expectedStoreFileCount, s.getStorefilesCount());
    }
    long regionMemstoreSize = secondaryRegion.getMemstoreSize();

    // Test case 1: replay a flush commit marker without start flush marker
    assertNull(secondaryRegion.getPrepareFlushResult());
    assertTrue(commitFlushDesc.getFlushSequenceNumber() > 0);

    // ensure all files are visible in secondary
    for (Store store : secondaryRegion.getStores()) {
      assertTrue(store.getMaxSequenceId() <= secondaryRegion.getSequenceId());
    }

    LOG.info("-- Replaying flush commit in secondary" + commitFlushDesc);
    secondaryRegion.replayWALFlushCommitMarker(commitFlushDesc);

    // assert that the flush files are picked
    expectedStoreFileCount++;
    for (Store s : secondaryRegion.getStores()) {
      assertEquals(expectedStoreFileCount, s.getStorefilesCount());
    }
    Store store = secondaryRegion.getStore(Bytes.toBytes("cf1"));
    long newFlushableSize = store.getFlushableSize();
    if (droppableMemstore) {
      assertTrue(newFlushableSize == 0); // assert that the memstore is dropped
    } else {
      assertTrue(newFlushableSize > 0); // assert that the memstore is not dropped
    }

    // assert that the region memstore is same as before (we could not drop)
    long newRegionMemstoreSize = secondaryRegion.getMemstoreSize();
    if (droppableMemstore) {
      assertTrue(0 == newRegionMemstoreSize);
    } else {
      assertTrue(regionMemstoreSize == newRegionMemstoreSize);
    }

    LOG.info("-- Verifying edits from secondary");
    verifyData(secondaryRegion, 0, numRows, cq, families);

    LOG.info("-- Verifying edits from primary.");
    verifyData(primaryRegion, 0, numRows, cq, families);
  }

  private FlushDescriptor clone(FlushDescriptor flush, long flushSeqId) {
    return FlushDescriptor.newBuilder(flush)
        .setFlushSequenceNumber(flushSeqId)
        .build();
  }

  /**
   * Tests replaying region open markers from primary region. Checks whether the files are picked up
   */
  @Test
  public void testReplayRegionOpenEvent() throws IOException {
    putDataWithFlushes(primaryRegion, 100, 0, 100); // no flush
    int numRows = 100;

    // close the region and open again.
    primaryRegion.close();
    primaryRegion = HRegion.openHRegion(rootDir, primaryHri, htd, walPrimary, CONF, rss, null);

    // now replay the edits and the flush marker
    reader =  createWALReaderForPrimary();
    List<RegionEventDescriptor> regionEvents = Lists.newArrayList();

    LOG.info("-- Replaying edits and region events in secondary");
    while (true) {
      WAL.Entry entry = reader.next();
      if (entry == null) {
        break;
      }
      FlushDescriptor flushDesc
        = WALEdit.getFlushDescriptor(entry.getEdit().getCells().get(0));
      RegionEventDescriptor regionEventDesc
        = WALEdit.getRegionEventDescriptor(entry.getEdit().getCells().get(0));

      if (flushDesc != null) {
        // don't replay flush events
      } else if (regionEventDesc != null) {
        regionEvents.add(regionEventDesc);
      } else {
        // don't replay edits
      }
    }

    // we should have 1 open, 1 close and 1 open event
    assertEquals(3, regionEvents.size());

    // replay the first region open event.
    secondaryRegion.replayWALRegionEventMarker(regionEvents.get(0));

    // replay the close event as well
    secondaryRegion.replayWALRegionEventMarker(regionEvents.get(1));

    // no store files in the region
    int expectedStoreFileCount = 0;
    for (Store s : secondaryRegion.getStores()) {
      assertEquals(expectedStoreFileCount, s.getStorefilesCount());
    }
    long regionMemstoreSize = secondaryRegion.getMemstoreSize();
    assertTrue(regionMemstoreSize == 0);

    // now replay the region open event that should contain new file locations
    LOG.info("Testing replaying region open event " + regionEvents.get(2));
    secondaryRegion.replayWALRegionEventMarker(regionEvents.get(2));

    // assert that the flush files are picked
    expectedStoreFileCount++;
    for (Store s : secondaryRegion.getStores()) {
      assertEquals(expectedStoreFileCount, s.getStorefilesCount());
    }
    Store store = secondaryRegion.getStore(Bytes.toBytes("cf1"));
    long newFlushableSize = store.getFlushableSize();
    assertTrue(newFlushableSize == 0);

    // assert that the region memstore is empty
    long newRegionMemstoreSize = secondaryRegion.getMemstoreSize();
    assertTrue(newRegionMemstoreSize == 0);

    assertNull(secondaryRegion.getPrepareFlushResult()); //prepare snapshot should be dropped if any

    LOG.info("-- Verifying edits from secondary");
    verifyData(secondaryRegion, 0, numRows, cq, families);

    LOG.info("-- Verifying edits from primary.");
    verifyData(primaryRegion, 0, numRows, cq, families);
  }

  /**
   * Tests the case where we replay a region open event after a flush start but before receiving
   * flush commit
   */
  @Test
  public void testReplayRegionOpenEventAfterFlushStart() throws IOException {
    putDataWithFlushes(primaryRegion, 100, 100, 100);
    int numRows = 200;

    // close the region and open again.
    primaryRegion.close();
    primaryRegion = HRegion.openHRegion(rootDir, primaryHri, htd, walPrimary, CONF, rss, null);

    // now replay the edits and the flush marker
    reader =  createWALReaderForPrimary();
    List<RegionEventDescriptor> regionEvents = Lists.newArrayList();

    LOG.info("-- Replaying edits and region events in secondary");
    while (true) {
      WAL.Entry entry = reader.next();
      if (entry == null) {
        break;
      }
      FlushDescriptor flushDesc
        = WALEdit.getFlushDescriptor(entry.getEdit().getCells().get(0));
      RegionEventDescriptor regionEventDesc
        = WALEdit.getRegionEventDescriptor(entry.getEdit().getCells().get(0));

      if (flushDesc != null) {
        // only replay flush start
        if (flushDesc.getAction() == FlushAction.START_FLUSH) {
          secondaryRegion.replayWALFlushStartMarker(flushDesc);
        }
      } else if (regionEventDesc != null) {
        regionEvents.add(regionEventDesc);
      } else {
        replayEdit(secondaryRegion, entry);
      }
    }

    // at this point, there should be some data (rows 0-100) in the memstore snapshot
    // and some more data in memstores (rows 100-200)
    verifyData(secondaryRegion, 0, numRows, cq, families);

    // we should have 1 open, 1 close and 1 open event
    assertEquals(3, regionEvents.size());

    // no store files in the region
    int expectedStoreFileCount = 0;
    for (Store s : secondaryRegion.getStores()) {
      assertEquals(expectedStoreFileCount, s.getStorefilesCount());
    }

    // now replay the region open event that should contain new file locations
    LOG.info("Testing replaying region open event " + regionEvents.get(2));
    secondaryRegion.replayWALRegionEventMarker(regionEvents.get(2));

    // assert that the flush files are picked
    expectedStoreFileCount = 2; // two flushes happened
    for (Store s : secondaryRegion.getStores()) {
      assertEquals(expectedStoreFileCount, s.getStorefilesCount());
    }
    Store store = secondaryRegion.getStore(Bytes.toBytes("cf1"));
    long newSnapshotSize = store.getSnapshotSize();
    assertTrue(newSnapshotSize == 0);

    // assert that the region memstore is empty
    long newRegionMemstoreSize = secondaryRegion.getMemstoreSize();
    assertTrue(newRegionMemstoreSize == 0);

    assertNull(secondaryRegion.getPrepareFlushResult()); //prepare snapshot should be dropped if any

    LOG.info("-- Verifying edits from secondary");
    verifyData(secondaryRegion, 0, numRows, cq, families);

    LOG.info("-- Verifying edits from primary.");
    verifyData(primaryRegion, 0, numRows, cq, families);
  }

  /**
   * Tests whether edits coming in for replay are skipped which have smaller seq id than the seqId
   * of the last replayed region open event.
   */
  @Test
  public void testSkippingEditsWithSmallerSeqIdAfterRegionOpenEvent() throws IOException {
    putDataWithFlushes(primaryRegion, 100, 100, 0);
    int numRows = 100;

    // close the region and open again.
    primaryRegion.close();
    primaryRegion = HRegion.openHRegion(rootDir, primaryHri, htd, walPrimary, CONF, rss, null);

    // now replay the edits and the flush marker
    reader =  createWALReaderForPrimary();
    List<RegionEventDescriptor> regionEvents = Lists.newArrayList();
    List<WAL.Entry> edits = Lists.newArrayList();

    LOG.info("-- Replaying edits and region events in secondary");
    while (true) {
      WAL.Entry entry = reader.next();
      if (entry == null) {
        break;
      }
      FlushDescriptor flushDesc
        = WALEdit.getFlushDescriptor(entry.getEdit().getCells().get(0));
      RegionEventDescriptor regionEventDesc
        = WALEdit.getRegionEventDescriptor(entry.getEdit().getCells().get(0));

      if (flushDesc != null) {
        // don't replay flushes
      } else if (regionEventDesc != null) {
        regionEvents.add(regionEventDesc);
      } else {
        edits.add(entry);
      }
    }

    // replay the region open of first open, but with the seqid of the second open
    // this way non of the flush files will be picked up.
    secondaryRegion.replayWALRegionEventMarker(
      RegionEventDescriptor.newBuilder(regionEvents.get(0)).setLogSequenceNumber(
        regionEvents.get(2).getLogSequenceNumber()).build());


    // replay edits from the before region close. If replay does not
    // skip these the following verification will NOT fail.
    for (WAL.Entry entry: edits) {
      replayEdit(secondaryRegion, entry);
    }

    boolean expectedFail = false;
    try {
      verifyData(secondaryRegion, 0, numRows, cq, families);
    } catch (AssertionError e) {
      expectedFail = true; // expected
    }
    if (!expectedFail) {
      fail("Should have failed this verification");
    }
  }

  @Test
  public void testReplayFlushSeqIds() throws IOException {
    // load some data to primary and flush
    int start = 0;
    LOG.info("-- Writing some data to primary from " +  start + " to " + (start+100));
    putData(primaryRegion, Durability.SYNC_WAL, start, 100, cq, families);
    LOG.info("-- Flushing primary, creating 3 files for 3 stores");
    primaryRegion.flush(true);

    // now replay the flush marker
    reader =  createWALReaderForPrimary();

    long flushSeqId = -1;
    LOG.info("-- Replaying flush events in secondary");
    while (true) {
      WAL.Entry entry = reader.next();
      if (entry == null) {
        break;
      }
      FlushDescriptor flushDesc
        = WALEdit.getFlushDescriptor(entry.getEdit().getCells().get(0));
      if (flushDesc != null) {
        if (flushDesc.getAction() == FlushAction.START_FLUSH) {
          LOG.info("-- Replaying flush start in secondary");
          secondaryRegion.replayWALFlushStartMarker(flushDesc);
          flushSeqId = flushDesc.getFlushSequenceNumber();
        } else if (flushDesc.getAction() == FlushAction.COMMIT_FLUSH) {
          LOG.info("-- Replaying flush commit in secondary");
          secondaryRegion.replayWALFlushCommitMarker(flushDesc);
          assertEquals(flushSeqId, flushDesc.getFlushSequenceNumber());
        }
      }
      // else do not replay
    }

    // TODO: what to do with this?
    // assert that the newly picked up flush file is visible
    long readPoint = secondaryRegion.getMVCC().getReadPoint();
    assertEquals(flushSeqId, readPoint);

    // after replay verify that everything is still visible
    verifyData(secondaryRegion, 0, 100, cq, families);
  }

  @Test
  public void testSeqIdsFromReplay() throws IOException {
    // test the case where seqId's coming from replayed WALEdits are made persisted with their
    // original seqIds and they are made visible through mvcc read point upon replay
    String method = name.getMethodName();
    byte[] tableName = Bytes.toBytes(method);
    byte[] family = Bytes.toBytes("family");

    HRegion region = initHRegion(tableName, method, family);
    try {
      // replay an entry that is bigger than current read point
      long readPoint = region.getMVCC().getReadPoint();
      long origSeqId = readPoint + 100;

      Put put = new Put(row).addColumn(family, row, row);
      put.setDurability(Durability.SKIP_WAL); // we replay with skip wal
      replay(region, put, origSeqId);

      // read point should have advanced to this seqId
      assertGet(region, family, row);

      // region seqId should have advanced at least to this seqId
      assertEquals(origSeqId, region.getSequenceId());

      // replay an entry that is smaller than current read point
      // caution: adding an entry below current read point might cause partial dirty reads. Normal
      // replay does not allow reads while replay is going on.
      put = new Put(row2).addColumn(family, row2, row2);
      put.setDurability(Durability.SKIP_WAL);
      replay(region, put, origSeqId - 50);

      assertGet(region, family, row2);
    } finally {
      region.close();
    }
  }

  /**
   * Tests that a region opened in secondary mode would not write region open / close
   * events to its WAL.
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testSecondaryRegionDoesNotWriteRegionEventsToWAL() throws IOException {
    secondaryRegion.close();
    walSecondary = spy(walSecondary);

    // test for region open and close
    secondaryRegion = HRegion.openHRegion(secondaryHri, htd, walSecondary, CONF, rss, null);
    verify(walSecondary, times(0)).append((HTableDescriptor)any(), (HRegionInfo)any(),
      (WALKey)any(), (WALEdit)any(),  anyBoolean());

    // test for replay prepare flush
    putDataByReplay(secondaryRegion, 0, 10, cq, families);
    secondaryRegion.replayWALFlushStartMarker(FlushDescriptor.newBuilder().
      setFlushSequenceNumber(10)
      .setTableName(ByteString.copyFrom(primaryRegion.getTableDesc().getTableName().getName()))
      .setAction(FlushAction.START_FLUSH)
      .setEncodedRegionName(
        ByteString.copyFrom(primaryRegion.getRegionInfo().getEncodedNameAsBytes()))
      .setRegionName(ByteString.copyFrom(primaryRegion.getRegionInfo().getRegionName()))
      .build());

    verify(walSecondary, times(0)).append((HTableDescriptor)any(), (HRegionInfo)any(),
      (WALKey)any(), (WALEdit)any(), anyBoolean());

    secondaryRegion.close();
    verify(walSecondary, times(0)).append((HTableDescriptor)any(), (HRegionInfo)any(),
      (WALKey)any(), (WALEdit)any(),  anyBoolean());
  }

  /**
   * Tests the reads enabled flag for the region. When unset all reads should be rejected
   */
  @Test
  public void testRegionReadsEnabledFlag() throws IOException {

    putDataByReplay(secondaryRegion, 0, 100, cq, families);

    verifyData(secondaryRegion, 0, 100, cq, families);

    // now disable reads
    secondaryRegion.setReadsEnabled(false);
    try {
      verifyData(secondaryRegion, 0, 100, cq, families);
      fail("Should have failed with IOException");
    } catch(IOException ex) {
      // expected
    }

    // verify that we can still replay data
    putDataByReplay(secondaryRegion, 100, 100, cq, families);

    // now enable reads again
    secondaryRegion.setReadsEnabled(true);
    verifyData(secondaryRegion, 0, 200, cq, families);
  }

  /**
   * Tests the case where a request for flush cache is sent to the region, but region cannot flush.
   * It should write the flush request marker instead.
   */
  @Test
  public void testWriteFlushRequestMarker() throws IOException {
    // primary region is empty at this point. Request a flush with writeFlushRequestWalMarker=false
    FlushResultImpl result = (FlushResultImpl)((HRegion)primaryRegion).flushcache(true, false);
    assertNotNull(result);
    assertEquals(result.result, FlushResultImpl.Result.CANNOT_FLUSH_MEMSTORE_EMPTY);
    assertFalse(result.wroteFlushWalMarker);

    // request flush again, but this time with writeFlushRequestWalMarker = true
    result = (FlushResultImpl)((HRegion)primaryRegion).flushcache(true, true);
    assertNotNull(result);
    assertEquals(result.result, FlushResultImpl.Result.CANNOT_FLUSH_MEMSTORE_EMPTY);
    assertTrue(result.wroteFlushWalMarker);

    List<FlushDescriptor> flushes = Lists.newArrayList();
    reader = createWALReaderForPrimary();
    while (true) {
      WAL.Entry entry = reader.next();
      if (entry == null) {
        break;
      }
      FlushDescriptor flush = WALEdit.getFlushDescriptor(entry.getEdit().getCells().get(0));
      if (flush != null) {
        flushes.add(flush);
      }
    }

    assertEquals(1, flushes.size());
    assertNotNull(flushes.get(0));
    assertEquals(FlushDescriptor.FlushAction.CANNOT_FLUSH, flushes.get(0).getAction());
  }

  /**
   * Test the case where the secondary region replica is not in reads enabled state because it is
   * waiting for a flush or region open marker from primary region. Replaying CANNOT_FLUSH
   * flush marker entry should restore the reads enabled status in the region and allow the reads
   * to continue.
   */
  @Test
  public void testReplayingFlushRequestRestoresReadsEnabledState() throws IOException {
    disableReads(secondaryRegion);

    // Test case 1: Test that replaying CANNOT_FLUSH request marker assuming this came from
    // triggered flush restores readsEnabled
    primaryRegion.flushcache(true, true);
    reader = createWALReaderForPrimary();
    while (true) {
      WAL.Entry entry = reader.next();
      if (entry == null) {
        break;
      }
      FlushDescriptor flush = WALEdit.getFlushDescriptor(entry.getEdit().getCells().get(0));
      if (flush != null) {
        secondaryRegion.replayWALFlushMarker(flush, entry.getKey().getLogSeqNum());
      }
    }

    // now reads should be enabled
    secondaryRegion.get(new Get(Bytes.toBytes(0)));
  }

  /**
   * Test the case where the secondary region replica is not in reads enabled state because it is
   * waiting for a flush or region open marker from primary region. Replaying flush start and commit
   * entries should restore the reads enabled status in the region and allow the reads
   * to continue.
   */
  @Test
  public void testReplayingFlushRestoresReadsEnabledState() throws IOException {
    // Test case 2: Test that replaying FLUSH_START and FLUSH_COMMIT markers assuming these came
    // from triggered flush restores readsEnabled
    disableReads(secondaryRegion);

    // put some data in primary
    putData(primaryRegion, Durability.SYNC_WAL, 0, 100, cq, families);
    primaryRegion.flush(true);

    reader = createWALReaderForPrimary();
    while (true) {
      WAL.Entry entry = reader.next();
      if (entry == null) {
        break;
      }
      FlushDescriptor flush = WALEdit.getFlushDescriptor(entry.getEdit().getCells().get(0));
      if (flush != null) {
        secondaryRegion.replayWALFlushMarker(flush, entry.getKey().getLogSeqNum());
      } else {
        replayEdit(secondaryRegion, entry);
      }
    }

    // now reads should be enabled
    verifyData(secondaryRegion, 0, 100, cq, families);
  }

  /**
   * Test the case where the secondary region replica is not in reads enabled state because it is
   * waiting for a flush or region open marker from primary region. Replaying flush start and commit
   * entries should restore the reads enabled status in the region and allow the reads
   * to continue.
   */
  @Test
  public void testReplayingFlushWithEmptyMemstoreRestoresReadsEnabledState() throws IOException {
    // Test case 2: Test that replaying FLUSH_START and FLUSH_COMMIT markers assuming these came
    // from triggered flush restores readsEnabled
    disableReads(secondaryRegion);

    // put some data in primary
    putData(primaryRegion, Durability.SYNC_WAL, 0, 100, cq, families);
    primaryRegion.flush(true);

    reader = createWALReaderForPrimary();
    while (true) {
      WAL.Entry entry = reader.next();
      if (entry == null) {
        break;
      }
      FlushDescriptor flush = WALEdit.getFlushDescriptor(entry.getEdit().getCells().get(0));
      if (flush != null) {
        secondaryRegion.replayWALFlushMarker(flush, entry.getKey().getLogSeqNum());
      }
    }

    // now reads should be enabled
    verifyData(secondaryRegion, 0, 100, cq, families);
  }

  /**
   * Test the case where the secondary region replica is not in reads enabled state because it is
   * waiting for a flush or region open marker from primary region. Replaying region open event
   * entry from primary should restore the reads enabled status in the region and allow the reads
   * to continue.
   */
  @Test
  public void testReplayingRegionOpenEventRestoresReadsEnabledState() throws IOException {
    // Test case 3: Test that replaying region open event markers restores readsEnabled
    disableReads(secondaryRegion);

    primaryRegion.close();
    primaryRegion = HRegion.openHRegion(rootDir, primaryHri, htd, walPrimary, CONF, rss, null);

    reader = createWALReaderForPrimary();
    while (true) {
      WAL.Entry entry = reader.next();
      if (entry == null) {
        break;
      }

      RegionEventDescriptor regionEventDesc
        = WALEdit.getRegionEventDescriptor(entry.getEdit().getCells().get(0));

      if (regionEventDesc != null) {
        secondaryRegion.replayWALRegionEventMarker(regionEventDesc);
      }
    }

    // now reads should be enabled
    secondaryRegion.get(new Get(Bytes.toBytes(0)));
  }

  @Test
  public void testRefreshStoreFiles() throws IOException {
    assertEquals(0, primaryRegion.getStoreFileList(families).size());
    assertEquals(0, secondaryRegion.getStoreFileList(families).size());

    // Test case 1: refresh with an empty region
    secondaryRegion.refreshStoreFiles();
    assertEquals(0, secondaryRegion.getStoreFileList(families).size());

    // do one flush
    putDataWithFlushes(primaryRegion, 100, 100, 0);
    int numRows = 100;

    // refresh the store file list, and ensure that the files are picked up.
    secondaryRegion.refreshStoreFiles();
    assertPathListsEqual(primaryRegion.getStoreFileList(families),
      secondaryRegion.getStoreFileList(families));
    assertEquals(families.length, secondaryRegion.getStoreFileList(families).size());

    LOG.info("-- Verifying edits from secondary");
    verifyData(secondaryRegion, 0, numRows, cq, families);

    // Test case 2: 3 some more flushes
    putDataWithFlushes(primaryRegion, 100, 300, 0);
    numRows = 300;

    // refresh the store file list, and ensure that the files are picked up.
    secondaryRegion.refreshStoreFiles();
    assertPathListsEqual(primaryRegion.getStoreFileList(families),
      secondaryRegion.getStoreFileList(families));
    assertEquals(families.length * 4, secondaryRegion.getStoreFileList(families).size());

    LOG.info("-- Verifying edits from secondary");
    verifyData(secondaryRegion, 0, numRows, cq, families);

    if (FSUtils.WINDOWS) {
      // compaction cannot move files while they are open in secondary on windows. Skip remaining.
      return;
    }

    // Test case 3: compact primary files
    primaryRegion.compactStores();
    List<Region> regions = new ArrayList<Region>();
    regions.add(primaryRegion);
    when(rss.getOnlineRegions()).thenReturn(regions);
    CompactedHFilesDischarger cleaner = new CompactedHFilesDischarger(100, null, rss, false);
    cleaner.chore();
    secondaryRegion.refreshStoreFiles();
    assertPathListsEqual(primaryRegion.getStoreFileList(families),
      secondaryRegion.getStoreFileList(families));
    assertEquals(families.length, secondaryRegion.getStoreFileList(families).size());

    LOG.info("-- Verifying edits from secondary");
    verifyData(secondaryRegion, 0, numRows, cq, families);

    LOG.info("-- Replaying edits in secondary");

    // Test case 4: replay some edits, ensure that memstore is dropped.
    assertTrue(secondaryRegion.getMemstoreSize() == 0);
    putDataWithFlushes(primaryRegion, 400, 400, 0);
    numRows = 400;

    reader =  createWALReaderForPrimary();
    while (true) {
      WAL.Entry entry = reader.next();
      if (entry == null) {
        break;
      }
      FlushDescriptor flush = WALEdit.getFlushDescriptor(entry.getEdit().getCells().get(0));
      if (flush != null) {
        // do not replay flush
      } else {
        replayEdit(secondaryRegion, entry);
      }
    }

    assertTrue(secondaryRegion.getMemstoreSize() > 0);

    secondaryRegion.refreshStoreFiles();

    assertTrue(secondaryRegion.getMemstoreSize() == 0);

    LOG.info("-- Verifying edits from primary");
    verifyData(primaryRegion, 0, numRows, cq, families);
    LOG.info("-- Verifying edits from secondary");
    verifyData(secondaryRegion, 0, numRows, cq, families);
  }

  /**
   * Paths can be qualified or not. This does the assertion using String->Path conversion.
   */
  private void assertPathListsEqual(List<String> list1, List<String> list2) {
    List<Path> l1 = new ArrayList<>(list1.size());
    for (String path : list1) {
      l1.add(Path.getPathWithoutSchemeAndAuthority(new Path(path)));
    }
    List<Path> l2 = new ArrayList<>(list2.size());
    for (String path : list2) {
      l2.add(Path.getPathWithoutSchemeAndAuthority(new Path(path)));
    }
    assertEquals(l1, l2);
  }

  private void disableReads(HRegion region) {
    region.setReadsEnabled(false);
    try {
      verifyData(region, 0, 1, cq, families);
      fail("Should have failed with IOException");
    } catch(IOException ex) {
      // expected
    }
  }

  private void replay(HRegion region, Put put, long replaySeqId) throws IOException {
    put.setDurability(Durability.SKIP_WAL);
    MutationReplay mutation = new MutationReplay(MutationType.PUT, put, 0, 0);
    region.batchReplay(new MutationReplay[] {mutation}, replaySeqId);
  }

  /**
   * Tests replaying region open markers from primary region. Checks whether the files are picked up
   */
  @Test
  public void testReplayBulkLoadEvent() throws IOException {
    LOG.info("testReplayBulkLoadEvent starts");
    putDataWithFlushes(primaryRegion, 100, 0, 100); // no flush

    // close the region and open again.
    primaryRegion.close();
    primaryRegion = HRegion.openHRegion(rootDir, primaryHri, htd, walPrimary, CONF, rss, null);

    // bulk load a file into primary region
    Random random = new Random();
    byte[] randomValues = new byte[20];
    random.nextBytes(randomValues);
    Path testPath = TEST_UTIL.getDataTestDirOnTestFS();

    List<Pair<byte[], String>> familyPaths = new ArrayList<Pair<byte[], String>>();
    int expectedLoadFileCount = 0;
    for (byte[] family : families) {
      familyPaths.add(new Pair<byte[], String>(family, createHFileForFamilies(testPath, family,
        randomValues)));
      expectedLoadFileCount++;
    }
    primaryRegion.bulkLoadHFiles(familyPaths, false, null);

    // now replay the edits and the bulk load marker
    reader = createWALReaderForPrimary();

    LOG.info("-- Replaying edits and region events in secondary");
    BulkLoadDescriptor bulkloadEvent = null;
    while (true) {
      WAL.Entry entry = reader.next();
      if (entry == null) {
        break;
      }
      bulkloadEvent = WALEdit.getBulkLoadDescriptor(entry.getEdit().getCells().get(0));
      if (bulkloadEvent != null) {
        break;
      }
    }

    // we should have 1 bulk load event
    assertTrue(bulkloadEvent != null);
    assertEquals(expectedLoadFileCount, bulkloadEvent.getStoresCount());

    // replay the bulk load event
    secondaryRegion.replayWALBulkLoadEventMarker(bulkloadEvent);


    List<String> storeFileName = new ArrayList<String>();
    for (StoreDescriptor storeDesc : bulkloadEvent.getStoresList()) {
      storeFileName.addAll(storeDesc.getStoreFileList());
    }
    // assert that the bulk loaded files are picked
    for (Store s : secondaryRegion.getStores()) {
      for (StoreFile sf : s.getStorefiles()) {
        storeFileName.remove(sf.getPath().getName());
      }
    }
    assertTrue("Found some store file isn't loaded:" + storeFileName, storeFileName.isEmpty());

    LOG.info("-- Verifying edits from secondary");
    for (byte[] family : families) {
      assertGet(secondaryRegion, family, randomValues);
    }
  }

  @Test
  public void testReplayingFlushCommitWithFileAlreadyDeleted() throws IOException {
    // tests replaying flush commit marker, but the flush file has already been compacted
    // from primary and also deleted from the archive directory
    secondaryRegion.replayWALFlushCommitMarker(FlushDescriptor.newBuilder().
      setFlushSequenceNumber(Long.MAX_VALUE)
      .setTableName(ByteString.copyFrom(primaryRegion.getTableDesc().getTableName().getName()))
      .setAction(FlushAction.COMMIT_FLUSH)
      .setEncodedRegionName(
        ByteString.copyFrom(primaryRegion.getRegionInfo().getEncodedNameAsBytes()))
      .setRegionName(ByteString.copyFrom(primaryRegion.getRegionInfo().getRegionName()))
      .addStoreFlushes(StoreFlushDescriptor.newBuilder()
        .setFamilyName(ByteString.copyFrom(families[0]))
        .setStoreHomeDir("/store_home_dir")
        .addFlushOutput("/foo/baz/bar")
        .build())
      .build());
  }

  @Test
  public void testReplayingCompactionWithFileAlreadyDeleted() throws IOException {
    // tests replaying compaction marker, but the compaction output file has already been compacted
    // from primary and also deleted from the archive directory
    secondaryRegion.replayWALCompactionMarker(CompactionDescriptor.newBuilder()
      .setTableName(ByteString.copyFrom(primaryRegion.getTableDesc().getTableName().getName()))
      .setEncodedRegionName(
        ByteString.copyFrom(primaryRegion.getRegionInfo().getEncodedNameAsBytes()))
      .setFamilyName(ByteString.copyFrom(families[0]))
      .addCompactionInput("/foo")
      .addCompactionOutput("/bar")
      .setStoreHomeDir("/store_home_dir")
      .setRegionName(ByteString.copyFrom(primaryRegion.getRegionInfo().getRegionName()))
      .build()
      , true, true, Long.MAX_VALUE);
  }

  @Test
  public void testReplayingRegionOpenEventWithFileAlreadyDeleted() throws IOException {
    // tests replaying region open event marker, but the region files have already been compacted
    // from primary and also deleted from the archive directory
    secondaryRegion.replayWALRegionEventMarker(RegionEventDescriptor.newBuilder()
      .setTableName(ByteString.copyFrom(primaryRegion.getTableDesc().getTableName().getName()))
      .setEncodedRegionName(
        ByteString.copyFrom(primaryRegion.getRegionInfo().getEncodedNameAsBytes()))
      .setRegionName(ByteString.copyFrom(primaryRegion.getRegionInfo().getRegionName()))
      .setEventType(EventType.REGION_OPEN)
      .setServer(ProtobufUtil.toServerName(ServerName.valueOf("foo", 1, 1)))
      .setLogSequenceNumber(Long.MAX_VALUE)
      .addStores(StoreDescriptor.newBuilder()
        .setFamilyName(ByteString.copyFrom(families[0]))
        .setStoreHomeDir("/store_home_dir")
        .addStoreFile("/foo")
        .build())
      .build());
  }

  @Test
  public void testReplayingBulkLoadEventWithFileAlreadyDeleted() throws IOException {
    // tests replaying bulk load event marker, but the bulk load files have already been compacted
    // from primary and also deleted from the archive directory
    secondaryRegion.replayWALBulkLoadEventMarker(BulkLoadDescriptor.newBuilder()
      .setTableName(ProtobufUtil.toProtoTableName(primaryRegion.getTableDesc().getTableName()))
      .setEncodedRegionName(
        ByteString.copyFrom(primaryRegion.getRegionInfo().getEncodedNameAsBytes()))
      .setBulkloadSeqNum(Long.MAX_VALUE)
      .addStores(StoreDescriptor.newBuilder()
        .setFamilyName(ByteString.copyFrom(families[0]))
        .setStoreHomeDir("/store_home_dir")
        .addStoreFile("/foo")
        .build())
      .build());
  }

  private String createHFileForFamilies(Path testPath, byte[] family,
      byte[] valueBytes) throws IOException {
    HFile.WriterFactory hFileFactory = HFile.getWriterFactoryNoCache(TEST_UTIL.getConfiguration());
    // TODO We need a way to do this without creating files
    Path testFile = new Path(testPath, UUID.randomUUID().toString());
    FSDataOutputStream out = TEST_UTIL.getTestFileSystem().create(testFile);
    try {
      hFileFactory.withOutputStream(out);
      hFileFactory.withFileContext(new HFileContext());
      HFile.Writer writer = hFileFactory.create();
      try {
        writer.append(new KeyValue(CellUtil.createCell(valueBytes, family, valueBytes, 0l,
          KeyValue.Type.Put.getCode(), valueBytes)));
      } finally {
        writer.close();
      }
    } finally {
      out.close();
    }
    return testFile.toString();
  }

  /** Puts a total of numRows + numRowsAfterFlush records indexed with numeric row keys. Does
   * a flush every flushInterval number of records. Then it puts numRowsAfterFlush number of
   * more rows but does not execute flush after
   * @throws IOException */
  private void putDataWithFlushes(HRegion region, int flushInterval,
      int numRows, int numRowsAfterFlush) throws IOException {
    int start = 0;
    for (; start < numRows; start += flushInterval) {
      LOG.info("-- Writing some data to primary from " +  start + " to " + (start+flushInterval));
      putData(region, Durability.SYNC_WAL, start, flushInterval, cq, families);
      LOG.info("-- Flushing primary, creating 3 files for 3 stores");
      region.flush(true);
    }
    LOG.info("-- Writing some more data to primary, not flushing");
    putData(region, Durability.SYNC_WAL, start, numRowsAfterFlush, cq, families);
  }

  private void putDataByReplay(HRegion region,
      int startRow, int numRows, byte[] qf, byte[]... families) throws IOException {
    for (int i = startRow; i < startRow + numRows; i++) {
      Put put = new Put(Bytes.toBytes("" + i));
      put.setDurability(Durability.SKIP_WAL);
      for (byte[] family : families) {
        put.addColumn(family, qf, EnvironmentEdgeManager.currentTime(), null);
      }
      replay(region, put, i+1);
    }
  }

  private static HRegion initHRegion(byte[] tableName,
      String callingMethod, byte[]... families) throws IOException {
    return initHRegion(tableName, HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW,
      callingMethod, TEST_UTIL.getConfiguration(), false, Durability.SYNC_WAL, null, families);
  }

  private static HRegion initHRegion(byte[] tableName, byte[] startKey, byte[] stopKey,
      String callingMethod, Configuration conf, boolean isReadOnly, Durability durability,
      WAL wal, byte[]... families) throws IOException {
    return TEST_UTIL.createLocalHRegion(tableName, startKey, stopKey, callingMethod, conf,
      isReadOnly, durability, wal, families);
  }
}
