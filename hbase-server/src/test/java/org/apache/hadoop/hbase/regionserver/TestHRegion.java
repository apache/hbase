/**
 *
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


import static org.apache.hadoop.hbase.HBaseTestingUtility.COLUMNS;
import static org.apache.hadoop.hbase.HBaseTestingUtility.FIRST_CHAR;
import static org.apache.hadoop.hbase.HBaseTestingUtility.LAST_CHAR;
import static org.apache.hadoop.hbase.HBaseTestingUtility.START_KEY;
import static org.apache.hadoop.hbase.HBaseTestingUtility.fam1;
import static org.apache.hadoop.hbase.HBaseTestingUtility.fam2;
import static org.apache.hadoop.hbase.HBaseTestingUtility.fam3;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.DroppedSnapshotException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HConstants.OperationStatusCode;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.MultithreadedTestUtil;
import org.apache.hadoop.hbase.MultithreadedTestUtil.RepeatingTestThread;
import org.apache.hadoop.hbase.MultithreadedTestUtil.TestThread;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.exceptions.FailedSanityCheckException;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.NullComparator;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.CompactionDescriptor;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.FlushDescriptor;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.FlushDescriptor.FlushAction;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.FlushDescriptor.StoreFlushDescriptor;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.RegionEventDescriptor;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.StoreDescriptor;
import org.apache.hadoop.hbase.regionserver.HRegion.RegionScannerImpl;
import org.apache.hadoop.hbase.regionserver.InternalScanner.NextState;
import org.apache.hadoop.hbase.regionserver.Region.RowLock;
import org.apache.hadoop.hbase.regionserver.TestStore.FaultyFileSystem;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.handler.FinishRegionRecoveringHandler;
import org.apache.hadoop.hbase.regionserver.wal.MetricsWAL;
import org.apache.hadoop.hbase.regionserver.wal.MetricsWALSource;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.regionserver.wal.WALUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.VerySlowRegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.IncrementingEnvironmentEdge;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.DefaultWALProvider;
import org.apache.hadoop.hbase.wal.FaultyFSLog;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALProvider;
import org.apache.hadoop.hbase.wal.WALSplitter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;

/**
 * Basic stand-alone testing of HRegion.  No clusters!
 *
 * A lot of the meta information for an HRegion now lives inside other HRegions
 * or in the HBaseMaster, so only basic testing is possible.
 */
@Category({VerySlowRegionServerTests.class, LargeTests.class})
@SuppressWarnings("deprecation")
public class TestHRegion {
  // Do not spin up clusters in here. If you need to spin up a cluster, do it
  // over in TestHRegionOnCluster.
  static final Log LOG = LogFactory.getLog(TestHRegion.class);
  @Rule public TestName name = new TestName();

  private static final String COLUMN_FAMILY = "MyCF";
  private static final byte [] COLUMN_FAMILY_BYTES = Bytes.toBytes(COLUMN_FAMILY);

  HRegion region = null;
  // Do not run unit tests in parallel (? Why not?  It don't work?  Why not?  St.Ack)
  private static HBaseTestingUtility TEST_UTIL;
  public static Configuration CONF ;
  private String dir;
  private static FileSystem FILESYSTEM;
  private final int MAX_VERSIONS = 2;

  // Test names
  protected byte[] tableName;
  protected String method;
  protected final byte[] qual1 = Bytes.toBytes("qual1");
  protected final byte[] qual2 = Bytes.toBytes("qual2");
  protected final byte[] qual3 = Bytes.toBytes("qual3");
  protected final byte[] value1 = Bytes.toBytes("value1");
  protected final byte[] value2 = Bytes.toBytes("value2");
  protected final byte[] row = Bytes.toBytes("rowA");
  protected final byte[] row2 = Bytes.toBytes("rowB");

  protected final MetricsAssertHelper metricsAssertHelper = CompatibilitySingletonFactory
      .getInstance(MetricsAssertHelper.class);

  @Before
  public void setup() throws IOException {
    TEST_UTIL = HBaseTestingUtility.createLocalHTU();
    FILESYSTEM = TEST_UTIL.getTestFileSystem();
    CONF = TEST_UTIL.getConfiguration();
    dir = TEST_UTIL.getDataTestDir("TestHRegion").toString();
    method = name.getMethodName();
    tableName = Bytes.toBytes(name.getMethodName());
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentEdgeManagerTestHelper.reset();
    LOG.info("Cleaning test directory: " + TEST_UTIL.getDataTestDir());
    TEST_UTIL.cleanupTestDir();
  }

  String getName() {
    return name.getMethodName();
  }

  /**
   * Test for Bug 2 of HBASE-10466.
   * "Bug 2: Conditions for the first flush of region close (so-called pre-flush) If memstoreSize
   * is smaller than a certain value, or when region close starts a flush is ongoing, the first
   * flush is skipped and only the second flush takes place. However, two flushes are required in
   * case previous flush fails and leaves some data in snapshot. The bug could cause loss of data
   * in current memstore. The fix is removing all conditions except abort check so we ensure 2
   * flushes for region close."
   * @throws IOException
   */
  @Test (timeout=60000)
  public void testCloseCarryingSnapshot() throws IOException {
    HRegion region = initHRegion(tableName, name.getMethodName(), CONF, COLUMN_FAMILY_BYTES);
    Store store = region.getStore(COLUMN_FAMILY_BYTES);
    // Get some random bytes.
    byte [] value = Bytes.toBytes(name.getMethodName());
    // Make a random put against our cf.
    Put put = new Put(value);
    put.add(COLUMN_FAMILY_BYTES, null, value);
    // First put something in current memstore, which will be in snapshot after flusher.prepare()
    region.put(put);
    StoreFlushContext storeFlushCtx = store.createFlushContext(12345);
    storeFlushCtx.prepare();
    // Second put something in current memstore
    put.add(COLUMN_FAMILY_BYTES, Bytes.toBytes("abc"), value);
    region.put(put);
    // Close with something in memstore and something in the snapshot.  Make sure all is cleared.
    region.close();
    assertEquals(0, region.getMemstoreSize());
    HBaseTestingUtility.closeRegionAndWAL(region);
  }

  /*
   * This test is for verifying memstore snapshot size is correctly updated in case of rollback
   * See HBASE-10845
   */
  @Test (timeout=60000)
  public void testMemstoreSnapshotSize() throws IOException {
    class MyFaultyFSLog extends FaultyFSLog {
      StoreFlushContext storeFlushCtx;
      public MyFaultyFSLog(FileSystem fs, Path rootDir, String logName, Configuration conf)
          throws IOException {
        super(fs, rootDir, logName, conf);
      }

      void setStoreFlushCtx(StoreFlushContext storeFlushCtx) {
        this.storeFlushCtx = storeFlushCtx;
      }

      @Override
      public void sync(long txid) throws IOException {
        storeFlushCtx.prepare();
        super.sync(txid);
      }
    }

    FileSystem fs = FileSystem.get(CONF);
    Path rootDir = new Path(dir + "testMemstoreSnapshotSize");
    MyFaultyFSLog faultyLog = new MyFaultyFSLog(fs, rootDir, "testMemstoreSnapshotSize", CONF);
    HRegion region = initHRegion(tableName, null, null, name.getMethodName(),
      CONF, false, Durability.SYNC_WAL, faultyLog, COLUMN_FAMILY_BYTES);

    Store store = region.getStore(COLUMN_FAMILY_BYTES);
    // Get some random bytes.
    byte [] value = Bytes.toBytes(name.getMethodName());
    faultyLog.setStoreFlushCtx(store.createFlushContext(12345));

    Put put = new Put(value);
    put.add(COLUMN_FAMILY_BYTES, Bytes.toBytes("abc"), value);
    faultyLog.setFailureType(FaultyFSLog.FailureType.SYNC);

    boolean threwIOE = false;
    try {
      region.put(put);
    } catch (IOException ioe) {
      threwIOE = true;
    } finally {
      assertTrue("The regionserver should have thrown an exception", threwIOE);
    }
    long sz = store.getFlushableSize();
    assertTrue("flushable size should be zero, but it is " + sz, sz == 0);
    HBaseTestingUtility.closeRegionAndWAL(region);
  }

  /**
   * Create a WAL outside of the usual helper in
   * {@link HBaseTestingUtility#createWal(Configuration, Path, HRegionInfo)} because that method
   * doesn't play nicely with FaultyFileSystem. Call this method before overriding
   * {@code fs.file.impl}.
   * @param callingMethod a unique component for the path, probably the name of the test method.
   */
  private static WAL createWALCompatibleWithFaultyFileSystem(String callingMethod,
      Configuration conf, byte[] tableName) throws IOException {
    final Path logDir = TEST_UTIL.getDataTestDirOnTestFS(callingMethod + ".log");
    final Configuration walConf = new Configuration(conf);
    FSUtils.setRootDir(walConf, logDir);
    return (new WALFactory(walConf,
        Collections.<WALActionsListener>singletonList(new MetricsWAL()), callingMethod))
        .getWAL(tableName);
  }

  /**
   * Test we do not lose data if we fail a flush and then close.
   * Part of HBase-10466.  Tests the following from the issue description:
   * "Bug 1: Wrong calculation of HRegion.memstoreSize: When a flush fails, data to be flushed is
   * kept in each MemStore's snapshot and wait for next flush attempt to continue on it. But when
   * the next flush succeeds, the counter of total memstore size in HRegion is always deduced by
   * the sum of current memstore sizes instead of snapshots left from previous failed flush. This
   * calculation is problematic that almost every time there is failed flush, HRegion.memstoreSize
   * gets reduced by a wrong value. If region flush could not proceed for a couple cycles, the size
   * in current memstore could be much larger than the snapshot. It's likely to drift memstoreSize
   * much smaller than expected. In extreme case, if the error accumulates to even bigger than
   * HRegion's memstore size limit, any further flush is skipped because flush does not do anything
   * if memstoreSize is not larger than 0."
   * @throws Exception
   */
  @Test (timeout=60000)
  public void testFlushSizeAccounting() throws Exception {
    final Configuration conf = HBaseConfiguration.create(CONF);
    final String callingMethod = name.getMethodName();
    final WAL wal = createWALCompatibleWithFaultyFileSystem(callingMethod, conf, tableName);
    // Only retry once.
    conf.setInt("hbase.hstore.flush.retries.number", 1);
    final User user =
      User.createUserForTesting(conf, this.name.getMethodName(), new String[]{"foo"});
    // Inject our faulty LocalFileSystem
    conf.setClass("fs.file.impl", FaultyFileSystem.class, FileSystem.class);
    user.runAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        // Make sure it worked (above is sensitive to caching details in hadoop core)
        FileSystem fs = FileSystem.get(conf);
        Assert.assertEquals(FaultyFileSystem.class, fs.getClass());
        FaultyFileSystem ffs = (FaultyFileSystem)fs;
        HRegion region = null;
        try {
          // Initialize region
          region = initHRegion(tableName, null, null, callingMethod, conf, false,
              Durability.SYNC_WAL, wal, COLUMN_FAMILY_BYTES);
          long size = region.getMemstoreSize();
          Assert.assertEquals(0, size);
          // Put one item into memstore.  Measure the size of one item in memstore.
          Put p1 = new Put(row);
          p1.add(new KeyValue(row, COLUMN_FAMILY_BYTES, qual1, 1, (byte[])null));
          region.put(p1);
          final long sizeOfOnePut = region.getMemstoreSize();
          // Fail a flush which means the current memstore will hang out as memstore 'snapshot'.
          try {
            LOG.info("Flushing");
            region.flush(true);
            Assert.fail("Didn't bubble up IOE!");
          } catch (DroppedSnapshotException dse) {
            // What we are expecting
          }
          // Make it so all writes succeed from here on out
          ffs.fault.set(false);
          // Check sizes.  Should still be the one entry.
          Assert.assertEquals(sizeOfOnePut, region.getMemstoreSize());
          // Now add two entries so that on this next flush that fails, we can see if we
          // subtract the right amount, the snapshot size only.
          Put p2 = new Put(row);
          p2.add(new KeyValue(row, COLUMN_FAMILY_BYTES, qual2, 2, (byte[])null));
          p2.add(new KeyValue(row, COLUMN_FAMILY_BYTES, qual3, 3, (byte[])null));
          region.put(p2);
          Assert.assertEquals(sizeOfOnePut * 3, region.getMemstoreSize());
          // Do a successful flush.  It will clear the snapshot only.  Thats how flushes work.
          // If already a snapshot, we clear it else we move the memstore to be snapshot and flush
          // it
          region.flush(true);
          // Make sure our memory accounting is right.
          Assert.assertEquals(sizeOfOnePut * 2, region.getMemstoreSize());
        } finally {
          HBaseTestingUtility.closeRegionAndWAL(region);
        }
        return null;
      }
    });
    FileSystem.closeAllForUGI(user.getUGI());
  }

  @Test (timeout=60000)
  public void testCloseWithFailingFlush() throws Exception {
    final Configuration conf = HBaseConfiguration.create(CONF);
    final String callingMethod = name.getMethodName();
    final WAL wal = createWALCompatibleWithFaultyFileSystem(callingMethod, conf, tableName);
    // Only retry once.
    conf.setInt("hbase.hstore.flush.retries.number", 1);
    final User user =
      User.createUserForTesting(conf, this.name.getMethodName(), new String[]{"foo"});
    // Inject our faulty LocalFileSystem
    conf.setClass("fs.file.impl", FaultyFileSystem.class, FileSystem.class);
    user.runAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        // Make sure it worked (above is sensitive to caching details in hadoop core)
        FileSystem fs = FileSystem.get(conf);
        Assert.assertEquals(FaultyFileSystem.class, fs.getClass());
        FaultyFileSystem ffs = (FaultyFileSystem)fs;
        HRegion region = null;
        try {
          // Initialize region
          region = initHRegion(tableName, null, null, callingMethod, conf, false,
              Durability.SYNC_WAL, wal, COLUMN_FAMILY_BYTES);
          long size = region.getMemstoreSize();
          Assert.assertEquals(0, size);
          // Put one item into memstore.  Measure the size of one item in memstore.
          Put p1 = new Put(row);
          p1.add(new KeyValue(row, COLUMN_FAMILY_BYTES, qual1, 1, (byte[])null));
          region.put(p1);
          // Manufacture an outstanding snapshot -- fake a failed flush by doing prepare step only.
          Store store = region.getStore(COLUMN_FAMILY_BYTES);
          StoreFlushContext storeFlushCtx = store.createFlushContext(12345);
          storeFlushCtx.prepare();
          // Now add two entries to the foreground memstore.
          Put p2 = new Put(row);
          p2.add(new KeyValue(row, COLUMN_FAMILY_BYTES, qual2, 2, (byte[])null));
          p2.add(new KeyValue(row, COLUMN_FAMILY_BYTES, qual3, 3, (byte[])null));
          region.put(p2);
          // Now try close on top of a failing flush.
          region.close();
          fail();
        } catch (DroppedSnapshotException dse) {
          // Expected
          LOG.info("Expected DroppedSnapshotException");
        } finally {
          // Make it so all writes succeed from here on out so can close clean
          ffs.fault.set(false);
          HBaseTestingUtility.closeRegionAndWAL(region);
        }
        return null;
      }
    });
    FileSystem.closeAllForUGI(user.getUGI());
  }

  @Test
  public void testCompactionAffectedByScanners() throws Exception {
    byte[] family = Bytes.toBytes("family");
    this.region = initHRegion(tableName, method, CONF, family);

    Put put = new Put(Bytes.toBytes("r1"));
    put.add(family, Bytes.toBytes("q1"), Bytes.toBytes("v1"));
    region.put(put);
    region.flush(true);

    Scan scan = new Scan();
    scan.setMaxVersions(3);
    // open the first scanner
    RegionScanner scanner1 = region.getScanner(scan);

    Delete delete = new Delete(Bytes.toBytes("r1"));
    region.delete(delete);
    region.flush(true);

    // open the second scanner
    RegionScanner scanner2 = region.getScanner(scan);

    List<Cell> results = new ArrayList<Cell>();

    System.out.println("Smallest read point:" + region.getSmallestReadPoint());

    // make a major compaction
    region.compact(true);

    // open the third scanner
    RegionScanner scanner3 = region.getScanner(scan);

    // get data from scanner 1, 2, 3 after major compaction
    scanner1.next(results);
    System.out.println(results);
    assertEquals(1, results.size());

    results.clear();
    scanner2.next(results);
    System.out.println(results);
    assertEquals(0, results.size());

    results.clear();
    scanner3.next(results);
    System.out.println(results);
    assertEquals(0, results.size());
  }

  @Test
  public void testToShowNPEOnRegionScannerReseek() throws Exception {
    byte[] family = Bytes.toBytes("family");
    this.region = initHRegion(tableName, method, CONF, family);

    Put put = new Put(Bytes.toBytes("r1"));
    put.add(family, Bytes.toBytes("q1"), Bytes.toBytes("v1"));
    region.put(put);
    put = new Put(Bytes.toBytes("r2"));
    put.add(family, Bytes.toBytes("q1"), Bytes.toBytes("v1"));
    region.put(put);
    region.flush(true);

    Scan scan = new Scan();
    scan.setMaxVersions(3);
    // open the first scanner
    RegionScanner scanner1 = region.getScanner(scan);

    System.out.println("Smallest read point:" + region.getSmallestReadPoint());

    region.compact(true);

    scanner1.reseek(Bytes.toBytes("r2"));
    List<Cell> results = new ArrayList<Cell>();
    scanner1.next(results);
    Cell keyValue = results.get(0);
    Assert.assertTrue(Bytes.compareTo(CellUtil.cloneRow(keyValue), Bytes.toBytes("r2")) == 0);
    scanner1.close();
  }

  @Test
  public void testSkipRecoveredEditsReplay() throws Exception {
    String method = "testSkipRecoveredEditsReplay";
    TableName tableName = TableName.valueOf(method);
    byte[] family = Bytes.toBytes("family");
    this.region = initHRegion(tableName, method, CONF, family);
    final WALFactory wals = new WALFactory(CONF, null, method);
    try {
      Path regiondir = region.getRegionFileSystem().getRegionDir();
      FileSystem fs = region.getRegionFileSystem().getFileSystem();
      byte[] regionName = region.getRegionInfo().getEncodedNameAsBytes();

      Path recoveredEditsDir = WALSplitter.getRegionDirRecoveredEditsDir(regiondir);

      long maxSeqId = 1050;
      long minSeqId = 1000;

      for (long i = minSeqId; i <= maxSeqId; i += 10) {
        Path recoveredEdits = new Path(recoveredEditsDir, String.format("%019d", i));
        fs.create(recoveredEdits);
        WALProvider.Writer writer = wals.createRecoveredEditsWriter(fs, recoveredEdits);

        long time = System.nanoTime();
        WALEdit edit = new WALEdit();
        edit.add(new KeyValue(row, family, Bytes.toBytes(i), time, KeyValue.Type.Put, Bytes
            .toBytes(i)));
        writer.append(new WAL.Entry(new HLogKey(regionName, tableName, i, time,
            HConstants.DEFAULT_CLUSTER_ID), edit));

        writer.close();
      }
      MonitoredTask status = TaskMonitor.get().createStatus(method);
      Map<byte[], Long> maxSeqIdInStores = new TreeMap<byte[], Long>(Bytes.BYTES_COMPARATOR);
      for (Store store : region.getStores()) {
        maxSeqIdInStores.put(store.getColumnFamilyName().getBytes(), minSeqId - 1);
      }
      long seqId = region.replayRecoveredEditsIfAny(regiondir, maxSeqIdInStores, null, status);
      assertEquals(maxSeqId, seqId);
      region.getMVCC().initialize(seqId);
      Get get = new Get(row);
      Result result = region.get(get);
      for (long i = minSeqId; i <= maxSeqId; i += 10) {
        List<Cell> kvs = result.getColumnCells(family, Bytes.toBytes(i));
        assertEquals(1, kvs.size());
        assertArrayEquals(Bytes.toBytes(i), CellUtil.cloneValue(kvs.get(0)));
      }
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
      wals.close();
    }
  }

  @Test
  public void testSkipRecoveredEditsReplaySomeIgnored() throws Exception {
    String method = "testSkipRecoveredEditsReplaySomeIgnored";
    TableName tableName = TableName.valueOf(method);
    byte[] family = Bytes.toBytes("family");
    this.region = initHRegion(tableName, method, CONF, family);
    final WALFactory wals = new WALFactory(CONF, null, method);
    try {
      Path regiondir = region.getRegionFileSystem().getRegionDir();
      FileSystem fs = region.getRegionFileSystem().getFileSystem();
      byte[] regionName = region.getRegionInfo().getEncodedNameAsBytes();

      Path recoveredEditsDir = WALSplitter.getRegionDirRecoveredEditsDir(regiondir);

      long maxSeqId = 1050;
      long minSeqId = 1000;

      for (long i = minSeqId; i <= maxSeqId; i += 10) {
        Path recoveredEdits = new Path(recoveredEditsDir, String.format("%019d", i));
        fs.create(recoveredEdits);
        WALProvider.Writer writer = wals.createRecoveredEditsWriter(fs, recoveredEdits);

        long time = System.nanoTime();
        WALEdit edit = new WALEdit();
        edit.add(new KeyValue(row, family, Bytes.toBytes(i), time, KeyValue.Type.Put, Bytes
            .toBytes(i)));
        writer.append(new WAL.Entry(new HLogKey(regionName, tableName, i, time,
            HConstants.DEFAULT_CLUSTER_ID), edit));

        writer.close();
      }
      long recoverSeqId = 1030;
      MonitoredTask status = TaskMonitor.get().createStatus(method);
      Map<byte[], Long> maxSeqIdInStores = new TreeMap<byte[], Long>(Bytes.BYTES_COMPARATOR);
      for (Store store : region.getStores()) {
        maxSeqIdInStores.put(store.getColumnFamilyName().getBytes(), recoverSeqId - 1);
      }
      long seqId = region.replayRecoveredEditsIfAny(regiondir, maxSeqIdInStores, null, status);
      assertEquals(maxSeqId, seqId);
      region.getMVCC().initialize(seqId);
      Get get = new Get(row);
      Result result = region.get(get);
      for (long i = minSeqId; i <= maxSeqId; i += 10) {
        List<Cell> kvs = result.getColumnCells(family, Bytes.toBytes(i));
        if (i < recoverSeqId) {
          assertEquals(0, kvs.size());
        } else {
          assertEquals(1, kvs.size());
          assertArrayEquals(Bytes.toBytes(i), CellUtil.cloneValue(kvs.get(0)));
        }
      }
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
      wals.close();
    }
  }

  @Test
  public void testSkipRecoveredEditsReplayAllIgnored() throws Exception {
    byte[] family = Bytes.toBytes("family");
    this.region = initHRegion(tableName, method, CONF, family);
    try {
      Path regiondir = region.getRegionFileSystem().getRegionDir();
      FileSystem fs = region.getRegionFileSystem().getFileSystem();

      Path recoveredEditsDir = WALSplitter.getRegionDirRecoveredEditsDir(regiondir);
      for (int i = 1000; i < 1050; i += 10) {
        Path recoveredEdits = new Path(recoveredEditsDir, String.format("%019d", i));
        FSDataOutputStream dos = fs.create(recoveredEdits);
        dos.writeInt(i);
        dos.close();
      }
      long minSeqId = 2000;
      Path recoveredEdits = new Path(recoveredEditsDir, String.format("%019d", minSeqId - 1));
      FSDataOutputStream dos = fs.create(recoveredEdits);
      dos.close();

      Map<byte[], Long> maxSeqIdInStores = new TreeMap<byte[], Long>(Bytes.BYTES_COMPARATOR);
      for (Store store : region.getStores()) {
        maxSeqIdInStores.put(store.getColumnFamilyName().getBytes(), minSeqId);
      }
      long seqId = region.replayRecoveredEditsIfAny(regiondir, maxSeqIdInStores, null, null);
      assertEquals(minSeqId, seqId);
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  @Test
  public void testSkipRecoveredEditsReplayTheLastFileIgnored() throws Exception {
    String method = "testSkipRecoveredEditsReplayTheLastFileIgnored";
    TableName tableName = TableName.valueOf(method);
    byte[] family = Bytes.toBytes("family");
    this.region = initHRegion(tableName, method, CONF, family);
    final WALFactory wals = new WALFactory(CONF, null, method);
    try {
      Path regiondir = region.getRegionFileSystem().getRegionDir();
      FileSystem fs = region.getRegionFileSystem().getFileSystem();
      byte[] regionName = region.getRegionInfo().getEncodedNameAsBytes();
      byte[][] columns = region.getTableDesc().getFamiliesKeys().toArray(new byte[0][]);

      assertEquals(0, region.getStoreFileList(columns).size());

      Path recoveredEditsDir = WALSplitter.getRegionDirRecoveredEditsDir(regiondir);

      long maxSeqId = 1050;
      long minSeqId = 1000;

      for (long i = minSeqId; i <= maxSeqId; i += 10) {
        Path recoveredEdits = new Path(recoveredEditsDir, String.format("%019d", i));
        fs.create(recoveredEdits);
        WALProvider.Writer writer = wals.createRecoveredEditsWriter(fs, recoveredEdits);

        long time = System.nanoTime();
        WALEdit edit = null;
        if (i == maxSeqId) {
          edit = WALEdit.createCompaction(region.getRegionInfo(),
          CompactionDescriptor.newBuilder()
          .setTableName(ByteString.copyFrom(tableName.getName()))
          .setFamilyName(ByteString.copyFrom(regionName))
          .setEncodedRegionName(ByteString.copyFrom(regionName))
          .setStoreHomeDirBytes(ByteString.copyFrom(Bytes.toBytes(regiondir.toString())))
          .setRegionName(ByteString.copyFrom(region.getRegionInfo().getRegionName()))
          .build());
        } else {
          edit = new WALEdit();
          edit.add(new KeyValue(row, family, Bytes.toBytes(i), time, KeyValue.Type.Put, Bytes
            .toBytes(i)));
        }
        writer.append(new WAL.Entry(new HLogKey(regionName, tableName, i, time,
            HConstants.DEFAULT_CLUSTER_ID), edit));
        writer.close();
      }

      long recoverSeqId = 1030;
      Map<byte[], Long> maxSeqIdInStores = new TreeMap<byte[], Long>(Bytes.BYTES_COMPARATOR);
      MonitoredTask status = TaskMonitor.get().createStatus(method);
      for (Store store : region.getStores()) {
        maxSeqIdInStores.put(store.getColumnFamilyName().getBytes(), recoverSeqId - 1);
      }
      long seqId = region.replayRecoveredEditsIfAny(regiondir, maxSeqIdInStores, null, status);
      assertEquals(maxSeqId, seqId);

      // assert that the files are flushed
      assertEquals(1, region.getStoreFileList(columns).size());

    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
      wals.close();
    }
  }

  @Test
  public void testRecoveredEditsReplayCompaction() throws Exception {
    String method = name.getMethodName();
    TableName tableName = TableName.valueOf(method);
    byte[] family = Bytes.toBytes("family");
    this.region = initHRegion(tableName, method, CONF, family);
    final WALFactory wals = new WALFactory(CONF, null, method);
    try {
      Path regiondir = region.getRegionFileSystem().getRegionDir();
      FileSystem fs = region.getRegionFileSystem().getFileSystem();
      byte[] regionName = region.getRegionInfo().getEncodedNameAsBytes();

      long maxSeqId = 3;
      long minSeqId = 0;

      for (long i = minSeqId; i < maxSeqId; i++) {
        Put put = new Put(Bytes.toBytes(i));
        put.add(family, Bytes.toBytes(i), Bytes.toBytes(i));
        region.put(put);
        region.flush(true);
      }

      // this will create a region with 3 files
      assertEquals(3, region.getStore(family).getStorefilesCount());
      List<Path> storeFiles = new ArrayList<Path>(3);
      for (StoreFile sf : region.getStore(family).getStorefiles()) {
        storeFiles.add(sf.getPath());
      }

      // disable compaction completion
      CONF.setBoolean("hbase.hstore.compaction.complete", false);
      region.compactStores();

      // ensure that nothing changed
      assertEquals(3, region.getStore(family).getStorefilesCount());

      // now find the compacted file, and manually add it to the recovered edits
      Path tmpDir = region.getRegionFileSystem().getTempDir();
      FileStatus[] files = FSUtils.listStatus(fs, tmpDir);
      String errorMsg = "Expected to find 1 file in the region temp directory "
          + "from the compaction, could not find any";
      assertNotNull(errorMsg, files);
      assertEquals(errorMsg, 1, files.length);
      // move the file inside region dir
      Path newFile = region.getRegionFileSystem().commitStoreFile(Bytes.toString(family),
          files[0].getPath());

      CompactionDescriptor compactionDescriptor = ProtobufUtil.toCompactionDescriptor(this.region
          .getRegionInfo(), family, storeFiles, Lists.newArrayList(newFile), region
          .getRegionFileSystem().getStoreDir(Bytes.toString(family)));

      WALUtil.writeCompactionMarker(region.getWAL(), this.region.getTableDesc(),
          this.region.getRegionInfo(), compactionDescriptor, new AtomicLong(1));

      Path recoveredEditsDir = WALSplitter.getRegionDirRecoveredEditsDir(regiondir);

      Path recoveredEdits = new Path(recoveredEditsDir, String.format("%019d", 1000));
      fs.create(recoveredEdits);
      WALProvider.Writer writer = wals.createRecoveredEditsWriter(fs, recoveredEdits);

      long time = System.nanoTime();

      writer.append(new WAL.Entry(new HLogKey(regionName, tableName, 10, time,
          HConstants.DEFAULT_CLUSTER_ID), WALEdit.createCompaction(region.getRegionInfo(),
          compactionDescriptor)));
      writer.close();

      // close the region now, and reopen again
      region.getTableDesc();
      region.getRegionInfo();
      region.close();
      region = HRegion.openHRegion(region, null);

      // now check whether we have only one store file, the compacted one
      Collection<StoreFile> sfs = region.getStore(family).getStorefiles();
      for (StoreFile sf : sfs) {
        LOG.info(sf.getPath());
      }
      assertEquals(1, region.getStore(family).getStorefilesCount());
      files = FSUtils.listStatus(fs, tmpDir);
      assertTrue("Expected to find 0 files inside " + tmpDir, files == null || files.length == 0);

      for (long i = minSeqId; i < maxSeqId; i++) {
        Get get = new Get(Bytes.toBytes(i));
        Result result = region.get(get);
        byte[] value = result.getValue(family, Bytes.toBytes(i));
        assertArrayEquals(Bytes.toBytes(i), value);
      }
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
      wals.close();
    }
  }

  @Test
  public void testFlushMarkers() throws Exception {
    // tests that flush markers are written to WAL and handled at recovered edits
    String method = name.getMethodName();
    TableName tableName = TableName.valueOf(method);
    byte[] family = Bytes.toBytes("family");
    Path logDir = TEST_UTIL.getDataTestDirOnTestFS(method + ".log");
    final Configuration walConf = new Configuration(TEST_UTIL.getConfiguration());
    FSUtils.setRootDir(walConf, logDir);
    final WALFactory wals = new WALFactory(walConf, null, method);
    final WAL wal = wals.getWAL(tableName.getName());

    this.region = initHRegion(tableName.getName(), HConstants.EMPTY_START_ROW,
      HConstants.EMPTY_END_ROW, method, CONF, false, Durability.USE_DEFAULT, wal, family);
    try {
      Path regiondir = region.getRegionFileSystem().getRegionDir();
      FileSystem fs = region.getRegionFileSystem().getFileSystem();
      byte[] regionName = region.getRegionInfo().getEncodedNameAsBytes();

      long maxSeqId = 3;
      long minSeqId = 0;

      for (long i = minSeqId; i < maxSeqId; i++) {
        Put put = new Put(Bytes.toBytes(i));
        put.add(family, Bytes.toBytes(i), Bytes.toBytes(i));
        region.put(put);
        region.flush(true);
      }

      // this will create a region with 3 files from flush
      assertEquals(3, region.getStore(family).getStorefilesCount());
      List<String> storeFiles = new ArrayList<String>(3);
      for (StoreFile sf : region.getStore(family).getStorefiles()) {
        storeFiles.add(sf.getPath().getName());
      }

      // now verify that the flush markers are written
      wal.shutdown();
      WAL.Reader reader = wals.createReader(fs, DefaultWALProvider.getCurrentFileName(wal),
        TEST_UTIL.getConfiguration());
      try {
        List<WAL.Entry> flushDescriptors = new ArrayList<WAL.Entry>();
        long lastFlushSeqId = -1;
        while (true) {
          WAL.Entry entry = reader.next();
          if (entry == null) {
            break;
          }
          Cell cell = entry.getEdit().getCells().get(0);
          if (WALEdit.isMetaEditFamily(cell)) {
            FlushDescriptor flushDesc = WALEdit.getFlushDescriptor(cell);
            assertNotNull(flushDesc);
            assertArrayEquals(tableName.getName(), flushDesc.getTableName().toByteArray());
            if (flushDesc.getAction() == FlushAction.START_FLUSH) {
              assertTrue(flushDesc.getFlushSequenceNumber() > lastFlushSeqId);
            } else if (flushDesc.getAction() == FlushAction.COMMIT_FLUSH) {
              assertTrue(flushDesc.getFlushSequenceNumber() == lastFlushSeqId);
            }
            lastFlushSeqId = flushDesc.getFlushSequenceNumber();
            assertArrayEquals(regionName, flushDesc.getEncodedRegionName().toByteArray());
            assertEquals(1, flushDesc.getStoreFlushesCount()); //only one store
            StoreFlushDescriptor storeFlushDesc = flushDesc.getStoreFlushes(0);
            assertArrayEquals(family, storeFlushDesc.getFamilyName().toByteArray());
            assertEquals("family", storeFlushDesc.getStoreHomeDir());
            if (flushDesc.getAction() == FlushAction.START_FLUSH) {
              assertEquals(0, storeFlushDesc.getFlushOutputCount());
            } else {
              assertEquals(1, storeFlushDesc.getFlushOutputCount()); //only one file from flush
              assertTrue(storeFiles.contains(storeFlushDesc.getFlushOutput(0)));
            }

            flushDescriptors.add(entry);
          }
        }

        assertEquals(3 * 2, flushDescriptors.size()); // START_FLUSH and COMMIT_FLUSH per flush

        // now write those markers to the recovered edits again.

        Path recoveredEditsDir = WALSplitter.getRegionDirRecoveredEditsDir(regiondir);

        Path recoveredEdits = new Path(recoveredEditsDir, String.format("%019d", 1000));
        fs.create(recoveredEdits);
        WALProvider.Writer writer = wals.createRecoveredEditsWriter(fs, recoveredEdits);

        for (WAL.Entry entry : flushDescriptors) {
          writer.append(entry);
        }
        writer.close();
      } finally {
        if (null != reader) {
          try {
            reader.close();
          } catch (IOException exception) {
            LOG.warn("Problem closing wal: " + exception.getMessage());
            LOG.debug("exception details", exception);
          }
        }
      }


      // close the region now, and reopen again
      region.close();
      region = HRegion.openHRegion(region, null);

      // now check whether we have can read back the data from region
      for (long i = minSeqId; i < maxSeqId; i++) {
        Get get = new Get(Bytes.toBytes(i));
        Result result = region.get(get);
        byte[] value = result.getValue(family, Bytes.toBytes(i));
        assertArrayEquals(Bytes.toBytes(i), value);
      }
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
      wals.close();
    }
  }

  class IsFlushWALMarker extends ArgumentMatcher<WALEdit> {
    volatile FlushAction[] actions;
    public IsFlushWALMarker(FlushAction... actions) {
      this.actions = actions;
    }
    @Override
    public boolean matches(Object edit) {
      List<Cell> cells = ((WALEdit)edit).getCells();
      if (cells.isEmpty()) {
        return false;
      }
      if (WALEdit.isMetaEditFamily(cells.get(0))) {
        FlushDescriptor desc = null;
        try {
          desc = WALEdit.getFlushDescriptor(cells.get(0));
        } catch (IOException e) {
          LOG.warn(e);
          return false;
        }
        if (desc != null) {
          for (FlushAction action : actions) {
            if (desc.getAction() == action) {
              return true;
            }
          }
        }
      }
      return false;
    }
    public IsFlushWALMarker set(FlushAction... actions) {
      this.actions = actions;
      return this;
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testFlushMarkersWALFail() throws Exception {
    // test the cases where the WAL append for flush markers fail.
    String method = name.getMethodName();
    TableName tableName = TableName.valueOf(method);
    byte[] family = Bytes.toBytes("family");

    // spy an actual WAL implementation to throw exception (was not able to mock)
    Path logDir = TEST_UTIL.getDataTestDirOnTestFS(method + "log");

    final Configuration walConf = new Configuration(TEST_UTIL.getConfiguration());
    FSUtils.setRootDir(walConf, logDir);
    final WALFactory wals = new WALFactory(walConf, null, method);
    WAL wal = spy(wals.getWAL(tableName.getName()));

    this.region = initHRegion(tableName.getName(), HConstants.EMPTY_START_ROW,
      HConstants.EMPTY_END_ROW, method, CONF, false, Durability.USE_DEFAULT, wal, family);
    try {
      int i = 0;
      Put put = new Put(Bytes.toBytes(i));
      put.setDurability(Durability.SKIP_WAL); // have to skip mocked wal
      put.add(family, Bytes.toBytes(i), Bytes.toBytes(i));
      region.put(put);

      // 1. Test case where START_FLUSH throws exception
      IsFlushWALMarker isFlushWALMarker = new IsFlushWALMarker(FlushAction.START_FLUSH);

      // throw exceptions if the WalEdit is a start flush action
      when(wal.append((HTableDescriptor)any(), (HRegionInfo)any(), (WALKey)any(),
        (WALEdit)argThat(isFlushWALMarker), (AtomicLong)any(), Mockito.anyBoolean(),
        (List<Cell>)any()))
          .thenThrow(new IOException("Fail to append flush marker"));

      // start cache flush will throw exception
      try {
        region.flush(true);
        fail("This should have thrown exception");
      } catch (DroppedSnapshotException unexpected) {
        // this should not be a dropped snapshot exception. Meaning that RS will not abort
        throw unexpected;
      } catch (IOException expected) {
        // expected
      }

      // 2. Test case where START_FLUSH succeeds but COMMIT_FLUSH will throw exception
      isFlushWALMarker.set(FlushAction.COMMIT_FLUSH);

      try {
        region.flush(true);
        fail("This should have thrown exception");
      } catch (DroppedSnapshotException expected) {
        // we expect this exception, since we were able to write the snapshot, but failed to
        // write the flush marker to WAL
      } catch (IOException unexpected) {
        throw unexpected;
      }

      region.close();
      this.region = initHRegion(tableName.getName(), HConstants.EMPTY_START_ROW,
        HConstants.EMPTY_END_ROW, method, CONF, false, Durability.USE_DEFAULT, wal, family);
      region.put(put);

      // 3. Test case where ABORT_FLUSH will throw exception.
      // Even if ABORT_FLUSH throws exception, we should not fail with IOE, but continue with
      // DroppedSnapshotException. Below COMMMIT_FLUSH will cause flush to abort
      isFlushWALMarker.set(FlushAction.COMMIT_FLUSH, FlushAction.ABORT_FLUSH);

      try {
        region.flush(true);
        fail("This should have thrown exception");
      } catch (DroppedSnapshotException expected) {
        // we expect this exception, since we were able to write the snapshot, but failed to
        // write the flush marker to WAL
      } catch (IOException unexpected) {
        throw unexpected;
      }

    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  @Test
  public void testGetWhileRegionClose() throws IOException {
    TableName tableName = TableName.valueOf(name.getMethodName());
    Configuration hc = initSplit();
    int numRows = 100;
    byte[][] families = { fam1, fam2, fam3 };

    // Setting up region
    String method = name.getMethodName();
    this.region = initHRegion(tableName, method, hc, families);
    try {
      // Put data in region
      final int startRow = 100;
      putData(startRow, numRows, qual1, families);
      putData(startRow, numRows, qual2, families);
      putData(startRow, numRows, qual3, families);
      final AtomicBoolean done = new AtomicBoolean(false);
      final AtomicInteger gets = new AtomicInteger(0);
      GetTillDoneOrException[] threads = new GetTillDoneOrException[10];
      try {
        // Set ten threads running concurrently getting from the region.
        for (int i = 0; i < threads.length / 2; i++) {
          threads[i] = new GetTillDoneOrException(i, Bytes.toBytes("" + startRow), done, gets);
          threads[i].setDaemon(true);
          threads[i].start();
        }
        // Artificially make the condition by setting closing flag explicitly.
        // I can't make the issue happen with a call to region.close().
        this.region.closing.set(true);
        for (int i = threads.length / 2; i < threads.length; i++) {
          threads[i] = new GetTillDoneOrException(i, Bytes.toBytes("" + startRow), done, gets);
          threads[i].setDaemon(true);
          threads[i].start();
        }
      } finally {
        if (this.region != null) {
          HBaseTestingUtility.closeRegionAndWAL(this.region);
        }
      }
      done.set(true);
      for (GetTillDoneOrException t : threads) {
        try {
          t.join();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        if (t.e != null) {
          LOG.info("Exception=" + t.e);
          assertFalse("Found a NPE in " + t.getName(), t.e instanceof NullPointerException);
        }
      }
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  /*
   * Thread that does get on single row until 'done' flag is flipped. If an
   * exception causes us to fail, it records it.
   */
  class GetTillDoneOrException extends Thread {
    private final Get g;
    private final AtomicBoolean done;
    private final AtomicInteger count;
    private Exception e;

    GetTillDoneOrException(final int i, final byte[] r, final AtomicBoolean d, final AtomicInteger c) {
      super("getter." + i);
      this.g = new Get(r);
      this.done = d;
      this.count = c;
    }

    @Override
    public void run() {
      while (!this.done.get()) {
        try {
          assertTrue(region.get(g).size() > 0);
          this.count.incrementAndGet();
        } catch (Exception e) {
          this.e = e;
          break;
        }
      }
    }
  }

  /*
   * An involved filter test. Has multiple column families and deletes in mix.
   */
  @Test
  public void testWeirdCacheBehaviour() throws Exception {
    byte[] TABLE = Bytes.toBytes("testWeirdCacheBehaviour");
    byte[][] FAMILIES = new byte[][] { Bytes.toBytes("trans-blob"), Bytes.toBytes("trans-type"),
        Bytes.toBytes("trans-date"), Bytes.toBytes("trans-tags"), Bytes.toBytes("trans-group") };
    this.region = initHRegion(TABLE, getName(), CONF, FAMILIES);
    try {
      String value = "this is the value";
      String value2 = "this is some other value";
      String keyPrefix1 = "prefix1";
      String keyPrefix2 = "prefix2";
      String keyPrefix3 = "prefix3";
      putRows(this.region, 3, value, keyPrefix1);
      putRows(this.region, 3, value, keyPrefix2);
      putRows(this.region, 3, value, keyPrefix3);
      putRows(this.region, 3, value2, keyPrefix1);
      putRows(this.region, 3, value2, keyPrefix2);
      putRows(this.region, 3, value2, keyPrefix3);
      System.out.println("Checking values for key: " + keyPrefix1);
      assertEquals("Got back incorrect number of rows from scan", 3,
          getNumberOfRows(keyPrefix1, value2, this.region));
      System.out.println("Checking values for key: " + keyPrefix2);
      assertEquals("Got back incorrect number of rows from scan", 3,
          getNumberOfRows(keyPrefix2, value2, this.region));
      System.out.println("Checking values for key: " + keyPrefix3);
      assertEquals("Got back incorrect number of rows from scan", 3,
          getNumberOfRows(keyPrefix3, value2, this.region));
      deleteColumns(this.region, value2, keyPrefix1);
      deleteColumns(this.region, value2, keyPrefix2);
      deleteColumns(this.region, value2, keyPrefix3);
      System.out.println("Starting important checks.....");
      assertEquals("Got back incorrect number of rows from scan: " + keyPrefix1, 0,
          getNumberOfRows(keyPrefix1, value2, this.region));
      assertEquals("Got back incorrect number of rows from scan: " + keyPrefix2, 0,
          getNumberOfRows(keyPrefix2, value2, this.region));
      assertEquals("Got back incorrect number of rows from scan: " + keyPrefix3, 0,
          getNumberOfRows(keyPrefix3, value2, this.region));
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  @Test
  public void testAppendWithReadOnlyTable() throws Exception {
    byte[] TABLE = Bytes.toBytes("readOnlyTable");
    this.region = initHRegion(TABLE, getName(), CONF, true, Bytes.toBytes("somefamily"));
    boolean exceptionCaught = false;
    Append append = new Append(Bytes.toBytes("somerow"));
    append.setDurability(Durability.SKIP_WAL);
    append.add(Bytes.toBytes("somefamily"), Bytes.toBytes("somequalifier"),
        Bytes.toBytes("somevalue"));
    try {
      region.append(append);
    } catch (IOException e) {
      exceptionCaught = true;
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
    assertTrue(exceptionCaught == true);
  }

  @Test
  public void testIncrWithReadOnlyTable() throws Exception {
    byte[] TABLE = Bytes.toBytes("readOnlyTable");
    this.region = initHRegion(TABLE, getName(), CONF, true, Bytes.toBytes("somefamily"));
    boolean exceptionCaught = false;
    Increment inc = new Increment(Bytes.toBytes("somerow"));
    inc.setDurability(Durability.SKIP_WAL);
    inc.addColumn(Bytes.toBytes("somefamily"), Bytes.toBytes("somequalifier"), 1L);
    try {
      region.increment(inc);
    } catch (IOException e) {
      exceptionCaught = true;
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
    assertTrue(exceptionCaught == true);
  }

  private void deleteColumns(HRegion r, String value, String keyPrefix) throws IOException {
    InternalScanner scanner = buildScanner(keyPrefix, value, r);
    int count = 0;
    boolean more = false;
    List<Cell> results = new ArrayList<Cell>();
    do {
      more = NextState.hasMoreValues(scanner.next(results));
      if (results != null && !results.isEmpty())
        count++;
      else
        break;
      Delete delete = new Delete(CellUtil.cloneRow(results.get(0)));
      delete.deleteColumn(Bytes.toBytes("trans-tags"), Bytes.toBytes("qual2"));
      r.delete(delete);
      results.clear();
    } while (more);
    assertEquals("Did not perform correct number of deletes", 3, count);
  }

  private int getNumberOfRows(String keyPrefix, String value, HRegion r) throws Exception {
    InternalScanner resultScanner = buildScanner(keyPrefix, value, r);
    int numberOfResults = 0;
    List<Cell> results = new ArrayList<Cell>();
    boolean more = false;
    do {
      more = NextState.hasMoreValues(resultScanner.next(results));
      if (results != null && !results.isEmpty())
        numberOfResults++;
      else
        break;
      for (Cell kv : results) {
        System.out.println("kv=" + kv.toString() + ", " + Bytes.toString(CellUtil.cloneValue(kv)));
      }
      results.clear();
    } while (more);
    return numberOfResults;
  }

  private InternalScanner buildScanner(String keyPrefix, String value, HRegion r)
      throws IOException {
    // Defaults FilterList.Operator.MUST_PASS_ALL.
    FilterList allFilters = new FilterList();
    allFilters.addFilter(new PrefixFilter(Bytes.toBytes(keyPrefix)));
    // Only return rows where this column value exists in the row.
    SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("trans-tags"),
        Bytes.toBytes("qual2"), CompareOp.EQUAL, Bytes.toBytes(value));
    filter.setFilterIfMissing(true);
    allFilters.addFilter(filter);
    Scan scan = new Scan();
    scan.addFamily(Bytes.toBytes("trans-blob"));
    scan.addFamily(Bytes.toBytes("trans-type"));
    scan.addFamily(Bytes.toBytes("trans-date"));
    scan.addFamily(Bytes.toBytes("trans-tags"));
    scan.addFamily(Bytes.toBytes("trans-group"));
    scan.setFilter(allFilters);
    return r.getScanner(scan);
  }

  private void putRows(HRegion r, int numRows, String value, String key) throws IOException {
    for (int i = 0; i < numRows; i++) {
      String row = key + "_" + i/* UUID.randomUUID().toString() */;
      System.out.println(String.format("Saving row: %s, with value %s", row, value));
      Put put = new Put(Bytes.toBytes(row));
      put.setDurability(Durability.SKIP_WAL);
      put.add(Bytes.toBytes("trans-blob"), null, Bytes.toBytes("value for blob"));
      put.add(Bytes.toBytes("trans-type"), null, Bytes.toBytes("statement"));
      put.add(Bytes.toBytes("trans-date"), null, Bytes.toBytes("20090921010101999"));
      put.add(Bytes.toBytes("trans-tags"), Bytes.toBytes("qual2"), Bytes.toBytes(value));
      put.add(Bytes.toBytes("trans-group"), null, Bytes.toBytes("adhocTransactionGroupId"));
      r.put(put);
    }
  }

  @Test
  public void testFamilyWithAndWithoutColon() throws Exception {
    byte[] b = Bytes.toBytes(getName());
    byte[] cf = Bytes.toBytes(COLUMN_FAMILY);
    this.region = initHRegion(b, getName(), CONF, cf);
    try {
      Put p = new Put(b);
      byte[] cfwithcolon = Bytes.toBytes(COLUMN_FAMILY + ":");
      p.add(cfwithcolon, cfwithcolon, cfwithcolon);
      boolean exception = false;
      try {
        this.region.put(p);
      } catch (NoSuchColumnFamilyException e) {
        exception = true;
      }
      assertTrue(exception);
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  @Test
  public void testBatchPut_whileNoRowLocksHeld() throws IOException {
    byte[] cf = Bytes.toBytes(COLUMN_FAMILY);
    byte[] qual = Bytes.toBytes("qual");
    byte[] val = Bytes.toBytes("val");
    this.region = initHRegion(Bytes.toBytes(getName()), getName(), CONF, cf);
    MetricsWALSource source = CompatibilitySingletonFactory.getInstance(MetricsWALSource.class);
    try {
      long syncs = metricsAssertHelper.getCounter("syncTimeNumOps", source);
      metricsAssertHelper.assertCounter("syncTimeNumOps", syncs, source);

      LOG.info("First a batch put with all valid puts");
      final Put[] puts = new Put[10];
      for (int i = 0; i < 10; i++) {
        puts[i] = new Put(Bytes.toBytes("row_" + i));
        puts[i].add(cf, qual, val);
      }

      OperationStatus[] codes = this.region.batchMutate(puts);
      assertEquals(10, codes.length);
      for (int i = 0; i < 10; i++) {
        assertEquals(OperationStatusCode.SUCCESS, codes[i].getOperationStatusCode());
      }
      metricsAssertHelper.assertCounter("syncTimeNumOps", syncs + 1, source);

      LOG.info("Next a batch put with one invalid family");
      puts[5].add(Bytes.toBytes("BAD_CF"), qual, val);
      codes = this.region.batchMutate(puts);
      assertEquals(10, codes.length);
      for (int i = 0; i < 10; i++) {
        assertEquals((i == 5) ? OperationStatusCode.BAD_FAMILY : OperationStatusCode.SUCCESS,
            codes[i].getOperationStatusCode());
      }

      metricsAssertHelper.assertCounter("syncTimeNumOps", syncs + 2, source);
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  @Test
  public void testBatchPut_whileMultipleRowLocksHeld() throws Exception {
    byte[] cf = Bytes.toBytes(COLUMN_FAMILY);
    byte[] qual = Bytes.toBytes("qual");
    byte[] val = Bytes.toBytes("val");
    this.region = initHRegion(Bytes.toBytes(getName()), getName(), CONF, cf);
    MetricsWALSource source = CompatibilitySingletonFactory.getInstance(MetricsWALSource.class);
    try {
      long syncs = metricsAssertHelper.getCounter("syncTimeNumOps", source);
      metricsAssertHelper.assertCounter("syncTimeNumOps", syncs, source);

      final Put[] puts = new Put[10];
      for (int i = 0; i < 10; i++) {
        puts[i] = new Put(Bytes.toBytes("row_" + i));
        puts[i].add(cf, qual, val);
      }
      puts[5].add(Bytes.toBytes("BAD_CF"), qual, val);

      LOG.info("batchPut will have to break into four batches to avoid row locks");
      RowLock rowLock1 = region.getRowLock(Bytes.toBytes("row_2"));
      RowLock rowLock2 = region.getRowLock(Bytes.toBytes("row_4"));
      RowLock rowLock3 = region.getRowLock(Bytes.toBytes("row_6"));

      MultithreadedTestUtil.TestContext ctx = new MultithreadedTestUtil.TestContext(CONF);
      final AtomicReference<OperationStatus[]> retFromThread = new AtomicReference<OperationStatus[]>();
      TestThread putter = new TestThread(ctx) {
        @Override
        public void doWork() throws IOException {
          retFromThread.set(region.batchMutate(puts));
        }
      };
      LOG.info("...starting put thread while holding locks");
      ctx.addThread(putter);
      ctx.startThreads();

      LOG.info("...waiting for put thread to sync 1st time");
      waitForCounter(source, "syncTimeNumOps", syncs + 1);

      // Now attempt to close the region from another thread.  Prior to HBASE-12565
      // this would cause the in-progress batchMutate operation to to fail with
      // exception because it use to release and re-acquire the close-guard lock
      // between batches.  Caller then didn't get status indicating which writes succeeded.
      // We now expect this thread to block until the batchMutate call finishes.
      Thread regionCloseThread = new Thread() {
        @Override
        public void run() {
          try {
            HBaseTestingUtility.closeRegionAndWAL(region);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      };
      regionCloseThread.start();

      LOG.info("...releasing row lock 1, which should let put thread continue");
      rowLock1.release();

      LOG.info("...waiting for put thread to sync 2nd time");
      waitForCounter(source, "syncTimeNumOps", syncs + 2);

      LOG.info("...releasing row lock 2, which should let put thread continue");
      rowLock2.release();

      LOG.info("...waiting for put thread to sync 3rd time");
      waitForCounter(source, "syncTimeNumOps", syncs + 3);

      LOG.info("...releasing row lock 3, which should let put thread continue");
      rowLock3.release();

      LOG.info("...waiting for put thread to sync 4th time");
      waitForCounter(source, "syncTimeNumOps", syncs + 4);

      LOG.info("...joining on put thread");
      ctx.stop();
      regionCloseThread.join();

      OperationStatus[] codes = retFromThread.get();
      for (int i = 0; i < codes.length; i++) {
        assertEquals((i == 5) ? OperationStatusCode.BAD_FAMILY : OperationStatusCode.SUCCESS,
            codes[i].getOperationStatusCode());
      }
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  private void waitForCounter(MetricsWALSource source, String metricName, long expectedCount)
      throws InterruptedException {
    long startWait = System.currentTimeMillis();
    long currentCount;
    while ((currentCount = metricsAssertHelper.getCounter(metricName, source)) < expectedCount) {
      Thread.sleep(100);
      if (System.currentTimeMillis() - startWait > 10000) {
        fail(String.format("Timed out waiting for '%s' >= '%s', currentCount=%s", metricName,
          expectedCount, currentCount));
      }
    }
  }

  @Test
  public void testBatchPutWithTsSlop() throws Exception {
    byte[] b = Bytes.toBytes(getName());
    byte[] cf = Bytes.toBytes(COLUMN_FAMILY);
    byte[] qual = Bytes.toBytes("qual");
    byte[] val = Bytes.toBytes("val");

    // add data with a timestamp that is too recent for range. Ensure assert
    CONF.setInt("hbase.hregion.keyvalue.timestamp.slop.millisecs", 1000);
    this.region = initHRegion(b, getName(), CONF, cf);

    try {
      MetricsWALSource source = CompatibilitySingletonFactory.getInstance(MetricsWALSource.class);
      long syncs = metricsAssertHelper.getCounter("syncTimeNumOps", source);
      metricsAssertHelper.assertCounter("syncTimeNumOps", syncs, source);

      final Put[] puts = new Put[10];
      for (int i = 0; i < 10; i++) {
        puts[i] = new Put(Bytes.toBytes("row_" + i), Long.MAX_VALUE - 100);
        puts[i].add(cf, qual, val);
      }

      OperationStatus[] codes = this.region.batchMutate(puts);
      assertEquals(10, codes.length);
      for (int i = 0; i < 10; i++) {
        assertEquals(OperationStatusCode.SANITY_CHECK_FAILURE, codes[i].getOperationStatusCode());
      }
      metricsAssertHelper.assertCounter("syncTimeNumOps", syncs, source);

    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }

  }

  // ////////////////////////////////////////////////////////////////////////////
  // checkAndMutate tests
  // ////////////////////////////////////////////////////////////////////////////
  @Test
  public void testCheckAndMutate_WithEmptyRowValue() throws IOException {
    byte[] row1 = Bytes.toBytes("row1");
    byte[] fam1 = Bytes.toBytes("fam1");
    byte[] qf1 = Bytes.toBytes("qualifier");
    byte[] emptyVal = new byte[] {};
    byte[] val1 = Bytes.toBytes("value1");
    byte[] val2 = Bytes.toBytes("value2");

    // Setting up region
    String method = this.getName();
    this.region = initHRegion(tableName, method, CONF, fam1);
    try {
      // Putting empty data in key
      Put put = new Put(row1);
      put.add(fam1, qf1, emptyVal);

      // checkAndPut with empty value
      boolean res = region.checkAndMutate(row1, fam1, qf1, CompareOp.EQUAL, new BinaryComparator(
          emptyVal), put, true);
      assertTrue(res);

      // Putting data in key
      put = new Put(row1);
      put.add(fam1, qf1, val1);

      // checkAndPut with correct value
      res = region.checkAndMutate(row1, fam1, qf1, CompareOp.EQUAL, new BinaryComparator(emptyVal),
          put, true);
      assertTrue(res);

      // not empty anymore
      res = region.checkAndMutate(row1, fam1, qf1, CompareOp.EQUAL, new BinaryComparator(emptyVal),
          put, true);
      assertFalse(res);

      Delete delete = new Delete(row1);
      delete.deleteColumn(fam1, qf1);
      res = region.checkAndMutate(row1, fam1, qf1, CompareOp.EQUAL, new BinaryComparator(emptyVal),
          delete, true);
      assertFalse(res);

      put = new Put(row1);
      put.add(fam1, qf1, val2);
      // checkAndPut with correct value
      res = region.checkAndMutate(row1, fam1, qf1, CompareOp.EQUAL, new BinaryComparator(val1),
          put, true);
      assertTrue(res);

      // checkAndDelete with correct value
      delete = new Delete(row1);
      delete.deleteColumn(fam1, qf1);
      delete.deleteColumn(fam1, qf1);
      res = region.checkAndMutate(row1, fam1, qf1, CompareOp.EQUAL, new BinaryComparator(val2),
          delete, true);
      assertTrue(res);

      delete = new Delete(row1);
      res = region.checkAndMutate(row1, fam1, qf1, CompareOp.EQUAL, new BinaryComparator(emptyVal),
          delete, true);
      assertTrue(res);

      // checkAndPut looking for a null value
      put = new Put(row1);
      put.add(fam1, qf1, val1);

      res = region
          .checkAndMutate(row1, fam1, qf1, CompareOp.EQUAL, new NullComparator(), put, true);
      assertTrue(res);
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  @Test
  public void testCheckAndMutate_WithWrongValue() throws IOException {
    byte[] row1 = Bytes.toBytes("row1");
    byte[] fam1 = Bytes.toBytes("fam1");
    byte[] qf1 = Bytes.toBytes("qualifier");
    byte[] val1 = Bytes.toBytes("value1");
    byte[] val2 = Bytes.toBytes("value2");

    // Setting up region
    String method = this.getName();
    this.region = initHRegion(tableName, method, CONF, fam1);
    try {
      // Putting data in key
      Put put = new Put(row1);
      put.add(fam1, qf1, val1);
      region.put(put);

      // checkAndPut with wrong value
      boolean res = region.checkAndMutate(row1, fam1, qf1, CompareOp.EQUAL, new BinaryComparator(
          val2), put, true);
      assertEquals(false, res);

      // checkAndDelete with wrong value
      Delete delete = new Delete(row1);
      delete.deleteFamily(fam1);
      res = region.checkAndMutate(row1, fam1, qf1, CompareOp.EQUAL, new BinaryComparator(val2),
          put, true);
      assertEquals(false, res);
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  @Test
  public void testCheckAndMutate_WithCorrectValue() throws IOException {
    byte[] row1 = Bytes.toBytes("row1");
    byte[] fam1 = Bytes.toBytes("fam1");
    byte[] qf1 = Bytes.toBytes("qualifier");
    byte[] val1 = Bytes.toBytes("value1");

    // Setting up region
    String method = this.getName();
    this.region = initHRegion(tableName, method, CONF, fam1);
    try {
      // Putting data in key
      Put put = new Put(row1);
      put.add(fam1, qf1, val1);
      region.put(put);

      // checkAndPut with correct value
      boolean res = region.checkAndMutate(row1, fam1, qf1, CompareOp.EQUAL, new BinaryComparator(
          val1), put, true);
      assertEquals(true, res);

      // checkAndDelete with correct value
      Delete delete = new Delete(row1);
      delete.deleteColumn(fam1, qf1);
      res = region.checkAndMutate(row1, fam1, qf1, CompareOp.EQUAL, new BinaryComparator(val1),
          delete, true);
      assertEquals(true, res);
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  @Test
  public void testCheckAndMutate_WithNonEqualCompareOp() throws IOException {
    byte[] row1 = Bytes.toBytes("row1");
    byte[] fam1 = Bytes.toBytes("fam1");
    byte[] qf1 = Bytes.toBytes("qualifier");
    byte[] val1 = Bytes.toBytes("value1");
    byte[] val2 = Bytes.toBytes("value2");
    byte[] val3 = Bytes.toBytes("value3");
    byte[] val4 = Bytes.toBytes("value4");

    // Setting up region
    String method = this.getName();
    this.region = initHRegion(tableName, method, CONF, fam1);
    try {
      // Putting val3 in key
      Put put = new Put(row1);
      put.add(fam1, qf1, val3);
      region.put(put);

      // Test CompareOp.LESS: original = val3, compare with val3, fail
      boolean res = region.checkAndMutate(row1, fam1, qf1, CompareOp.LESS,
          new BinaryComparator(val3), put, true);
      assertEquals(false, res);

      // Test CompareOp.LESS: original = val3, compare with val4, fail
      res = region.checkAndMutate(row1, fam1, qf1, CompareOp.LESS,
          new BinaryComparator(val4), put, true);
      assertEquals(false, res);

      // Test CompareOp.LESS: original = val3, compare with val2,
      // succeed (now value = val2)
      put = new Put(row1);
      put.add(fam1, qf1, val2);
      res = region.checkAndMutate(row1, fam1, qf1, CompareOp.LESS,
          new BinaryComparator(val2), put, true);
      assertEquals(true, res);

      // Test CompareOp.LESS_OR_EQUAL: original = val2, compare with val3, fail
      res = region.checkAndMutate(row1, fam1, qf1, CompareOp.LESS_OR_EQUAL,
          new BinaryComparator(val3), put, true);
      assertEquals(false, res);

      // Test CompareOp.LESS_OR_EQUAL: original = val2, compare with val2,
      // succeed (value still = val2)
      res = region.checkAndMutate(row1, fam1, qf1, CompareOp.LESS_OR_EQUAL,
          new BinaryComparator(val2), put, true);
      assertEquals(true, res);

      // Test CompareOp.LESS_OR_EQUAL: original = val2, compare with val1,
      // succeed (now value = val3)
      put = new Put(row1);
      put.add(fam1, qf1, val3);
      res = region.checkAndMutate(row1, fam1, qf1, CompareOp.LESS_OR_EQUAL,
          new BinaryComparator(val1), put, true);
      assertEquals(true, res);

      // Test CompareOp.GREATER: original = val3, compare with val3, fail
      res = region.checkAndMutate(row1, fam1, qf1, CompareOp.GREATER,
          new BinaryComparator(val3), put, true);
      assertEquals(false, res);

      // Test CompareOp.GREATER: original = val3, compare with val2, fail
      res = region.checkAndMutate(row1, fam1, qf1, CompareOp.GREATER,
          new BinaryComparator(val2), put, true);
      assertEquals(false, res);

      // Test CompareOp.GREATER: original = val3, compare with val4,
      // succeed (now value = val2)
      put = new Put(row1);
      put.add(fam1, qf1, val2);
      res = region.checkAndMutate(row1, fam1, qf1, CompareOp.GREATER,
          new BinaryComparator(val4), put, true);
      assertEquals(true, res);

      // Test CompareOp.GREATER_OR_EQUAL: original = val2, compare with val1, fail
      res = region.checkAndMutate(row1, fam1, qf1, CompareOp.GREATER_OR_EQUAL,
          new BinaryComparator(val1), put, true);
      assertEquals(false, res);

      // Test CompareOp.GREATER_OR_EQUAL: original = val2, compare with val2,
      // succeed (value still = val2)
      res = region.checkAndMutate(row1, fam1, qf1, CompareOp.GREATER_OR_EQUAL,
          new BinaryComparator(val2), put, true);
      assertEquals(true, res);

      // Test CompareOp.GREATER_OR_EQUAL: original = val2, compare with val3, succeed
      res = region.checkAndMutate(row1, fam1, qf1, CompareOp.GREATER_OR_EQUAL,
          new BinaryComparator(val3), put, true);
      assertEquals(true, res);
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  @Test
  public void testCheckAndPut_ThatPutWasWritten() throws IOException {
    byte[] row1 = Bytes.toBytes("row1");
    byte[] fam1 = Bytes.toBytes("fam1");
    byte[] fam2 = Bytes.toBytes("fam2");
    byte[] qf1 = Bytes.toBytes("qualifier");
    byte[] val1 = Bytes.toBytes("value1");
    byte[] val2 = Bytes.toBytes("value2");

    byte[][] families = { fam1, fam2 };

    // Setting up region
    String method = this.getName();
    this.region = initHRegion(tableName, method, CONF, families);
    try {
      // Putting data in the key to check
      Put put = new Put(row1);
      put.add(fam1, qf1, val1);
      region.put(put);

      // Creating put to add
      long ts = System.currentTimeMillis();
      KeyValue kv = new KeyValue(row1, fam2, qf1, ts, KeyValue.Type.Put, val2);
      put = new Put(row1);
      put.add(kv);

      // checkAndPut with wrong value
      boolean res = region.checkAndMutate(row1, fam1, qf1, CompareOp.EQUAL, new BinaryComparator(
          val1), put, true);
      assertEquals(true, res);

      Get get = new Get(row1);
      get.addColumn(fam2, qf1);
      Cell[] actual = region.get(get).rawCells();

      Cell[] expected = { kv };

      assertEquals(expected.length, actual.length);
      for (int i = 0; i < actual.length; i++) {
        assertEquals(expected[i], actual[i]);
      }
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  @Test
  public void testCheckAndPut_wrongRowInPut() throws IOException {
    TableName tableName = TableName.valueOf(name.getMethodName());
    this.region = initHRegion(tableName, this.getName(), CONF, COLUMNS);
    try {
      Put put = new Put(row2);
      put.add(fam1, qual1, value1);
      try {
        region.checkAndMutate(row, fam1, qual1, CompareOp.EQUAL,
            new BinaryComparator(value2), put, false);
        fail();
      } catch (org.apache.hadoop.hbase.DoNotRetryIOException expected) {
        // expected exception.
      }
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  @Test
  public void testCheckAndDelete_ThatDeleteWasWritten() throws IOException {
    byte[] row1 = Bytes.toBytes("row1");
    byte[] fam1 = Bytes.toBytes("fam1");
    byte[] fam2 = Bytes.toBytes("fam2");
    byte[] qf1 = Bytes.toBytes("qualifier1");
    byte[] qf2 = Bytes.toBytes("qualifier2");
    byte[] qf3 = Bytes.toBytes("qualifier3");
    byte[] val1 = Bytes.toBytes("value1");
    byte[] val2 = Bytes.toBytes("value2");
    byte[] val3 = Bytes.toBytes("value3");
    byte[] emptyVal = new byte[] {};

    byte[][] families = { fam1, fam2 };

    // Setting up region
    String method = this.getName();
    this.region = initHRegion(tableName, method, CONF, families);
    try {
      // Put content
      Put put = new Put(row1);
      put.add(fam1, qf1, val1);
      region.put(put);
      Threads.sleep(2);

      put = new Put(row1);
      put.add(fam1, qf1, val2);
      put.add(fam2, qf1, val3);
      put.add(fam2, qf2, val2);
      put.add(fam2, qf3, val1);
      put.add(fam1, qf3, val1);
      region.put(put);

      // Multi-column delete
      Delete delete = new Delete(row1);
      delete.deleteColumn(fam1, qf1);
      delete.deleteColumn(fam2, qf1);
      delete.deleteColumn(fam1, qf3);
      boolean res = region.checkAndMutate(row1, fam1, qf1, CompareOp.EQUAL, new BinaryComparator(
          val2), delete, true);
      assertEquals(true, res);

      Get get = new Get(row1);
      get.addColumn(fam1, qf1);
      get.addColumn(fam1, qf3);
      get.addColumn(fam2, qf2);
      Result r = region.get(get);
      assertEquals(2, r.size());
      assertArrayEquals(val1, r.getValue(fam1, qf1));
      assertArrayEquals(val2, r.getValue(fam2, qf2));

      // Family delete
      delete = new Delete(row1);
      delete.deleteFamily(fam2);
      res = region.checkAndMutate(row1, fam2, qf1, CompareOp.EQUAL, new BinaryComparator(emptyVal),
          delete, true);
      assertEquals(true, res);

      get = new Get(row1);
      r = region.get(get);
      assertEquals(1, r.size());
      assertArrayEquals(val1, r.getValue(fam1, qf1));

      // Row delete
      delete = new Delete(row1);
      res = region.checkAndMutate(row1, fam1, qf1, CompareOp.EQUAL, new BinaryComparator(val1),
          delete, true);
      assertEquals(true, res);
      get = new Get(row1);
      r = region.get(get);
      assertEquals(0, r.size());
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  // ////////////////////////////////////////////////////////////////////////////
  // Delete tests
  // ////////////////////////////////////////////////////////////////////////////
  @Test
  public void testDelete_multiDeleteColumn() throws IOException {
    byte[] row1 = Bytes.toBytes("row1");
    byte[] fam1 = Bytes.toBytes("fam1");
    byte[] qual = Bytes.toBytes("qualifier");
    byte[] value = Bytes.toBytes("value");

    Put put = new Put(row1);
    put.add(fam1, qual, 1, value);
    put.add(fam1, qual, 2, value);

    String method = this.getName();
    this.region = initHRegion(tableName, method, CONF, fam1);
    try {
      region.put(put);

      // We do support deleting more than 1 'latest' version
      Delete delete = new Delete(row1);
      delete.deleteColumn(fam1, qual);
      delete.deleteColumn(fam1, qual);
      region.delete(delete);

      Get get = new Get(row1);
      get.addFamily(fam1);
      Result r = region.get(get);
      assertEquals(0, r.size());
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  @Test
  public void testDelete_CheckFamily() throws IOException {
    byte[] row1 = Bytes.toBytes("row1");
    byte[] fam1 = Bytes.toBytes("fam1");
    byte[] fam2 = Bytes.toBytes("fam2");
    byte[] fam3 = Bytes.toBytes("fam3");
    byte[] fam4 = Bytes.toBytes("fam4");

    // Setting up region
    String method = this.getName();
    this.region = initHRegion(tableName, method, CONF, fam1, fam2, fam3);
    try {
      List<Cell> kvs = new ArrayList<Cell>();
      kvs.add(new KeyValue(row1, fam4, null, null));

      // testing existing family
      byte[] family = fam2;
      try {
        NavigableMap<byte[], List<Cell>> deleteMap = new TreeMap<byte[], List<Cell>>(
            Bytes.BYTES_COMPARATOR);
        deleteMap.put(family, kvs);
        region.delete(deleteMap, Durability.SYNC_WAL);
      } catch (Exception e) {
        assertTrue("Family " + new String(family) + " does not exist", false);
      }

      // testing non existing family
      boolean ok = false;
      family = fam4;
      try {
        NavigableMap<byte[], List<Cell>> deleteMap = new TreeMap<byte[], List<Cell>>(
            Bytes.BYTES_COMPARATOR);
        deleteMap.put(family, kvs);
        region.delete(deleteMap, Durability.SYNC_WAL);
      } catch (Exception e) {
        ok = true;
      }
      assertEquals("Family " + new String(family) + " does exist", true, ok);
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  @Test
  public void testDelete_mixed() throws IOException, InterruptedException {
    byte[] fam = Bytes.toBytes("info");
    byte[][] families = { fam };
    String method = this.getName();
    this.region = initHRegion(tableName, method, CONF, families);
    try {
      EnvironmentEdgeManagerTestHelper.injectEdge(new IncrementingEnvironmentEdge());

      byte[] row = Bytes.toBytes("table_name");
      // column names
      byte[] serverinfo = Bytes.toBytes("serverinfo");
      byte[] splitA = Bytes.toBytes("splitA");
      byte[] splitB = Bytes.toBytes("splitB");

      // add some data:
      Put put = new Put(row);
      put.add(fam, splitA, Bytes.toBytes("reference_A"));
      region.put(put);

      put = new Put(row);
      put.add(fam, splitB, Bytes.toBytes("reference_B"));
      region.put(put);

      put = new Put(row);
      put.add(fam, serverinfo, Bytes.toBytes("ip_address"));
      region.put(put);

      // ok now delete a split:
      Delete delete = new Delete(row);
      delete.deleteColumns(fam, splitA);
      region.delete(delete);

      // assert some things:
      Get get = new Get(row).addColumn(fam, serverinfo);
      Result result = region.get(get);
      assertEquals(1, result.size());

      get = new Get(row).addColumn(fam, splitA);
      result = region.get(get);
      assertEquals(0, result.size());

      get = new Get(row).addColumn(fam, splitB);
      result = region.get(get);
      assertEquals(1, result.size());

      // Assert that after a delete, I can put.
      put = new Put(row);
      put.add(fam, splitA, Bytes.toBytes("reference_A"));
      region.put(put);
      get = new Get(row);
      result = region.get(get);
      assertEquals(3, result.size());

      // Now delete all... then test I can add stuff back
      delete = new Delete(row);
      region.delete(delete);
      assertEquals(0, region.get(get).size());

      region.put(new Put(row).add(fam, splitA, Bytes.toBytes("reference_A")));
      result = region.get(get);
      assertEquals(1, result.size());
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  @Test
  public void testDeleteRowWithFutureTs() throws IOException {
    byte[] fam = Bytes.toBytes("info");
    byte[][] families = { fam };
    String method = this.getName();
    this.region = initHRegion(tableName, method, CONF, families);
    try {
      byte[] row = Bytes.toBytes("table_name");
      // column names
      byte[] serverinfo = Bytes.toBytes("serverinfo");

      // add data in the far future
      Put put = new Put(row);
      put.add(fam, serverinfo, HConstants.LATEST_TIMESTAMP - 5, Bytes.toBytes("value"));
      region.put(put);

      // now delete something in the present
      Delete delete = new Delete(row);
      region.delete(delete);

      // make sure we still see our data
      Get get = new Get(row).addColumn(fam, serverinfo);
      Result result = region.get(get);
      assertEquals(1, result.size());

      // delete the future row
      delete = new Delete(row, HConstants.LATEST_TIMESTAMP - 3);
      region.delete(delete);

      // make sure it is gone
      get = new Get(row).addColumn(fam, serverinfo);
      result = region.get(get);
      assertEquals(0, result.size());
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  /**
   * Tests that the special LATEST_TIMESTAMP option for puts gets replaced by
   * the actual timestamp
   */
  @Test
  public void testPutWithLatestTS() throws IOException {
    byte[] fam = Bytes.toBytes("info");
    byte[][] families = { fam };
    String method = this.getName();
    this.region = initHRegion(tableName, method, CONF, families);
    try {
      byte[] row = Bytes.toBytes("row1");
      // column names
      byte[] qual = Bytes.toBytes("qual");

      // add data with LATEST_TIMESTAMP, put without WAL
      Put put = new Put(row);
      put.add(fam, qual, HConstants.LATEST_TIMESTAMP, Bytes.toBytes("value"));
      region.put(put);

      // Make sure it shows up with an actual timestamp
      Get get = new Get(row).addColumn(fam, qual);
      Result result = region.get(get);
      assertEquals(1, result.size());
      Cell kv = result.rawCells()[0];
      LOG.info("Got: " + kv);
      assertTrue("LATEST_TIMESTAMP was not replaced with real timestamp",
          kv.getTimestamp() != HConstants.LATEST_TIMESTAMP);

      // Check same with WAL enabled (historically these took different
      // code paths, so check both)
      row = Bytes.toBytes("row2");
      put = new Put(row);
      put.add(fam, qual, HConstants.LATEST_TIMESTAMP, Bytes.toBytes("value"));
      region.put(put);

      // Make sure it shows up with an actual timestamp
      get = new Get(row).addColumn(fam, qual);
      result = region.get(get);
      assertEquals(1, result.size());
      kv = result.rawCells()[0];
      LOG.info("Got: " + kv);
      assertTrue("LATEST_TIMESTAMP was not replaced with real timestamp",
          kv.getTimestamp() != HConstants.LATEST_TIMESTAMP);
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }

  }

  /**
   * Tests that there is server-side filtering for invalid timestamp upper
   * bound. Note that the timestamp lower bound is automatically handled for us
   * by the TTL field.
   */
  @Test
  public void testPutWithTsSlop() throws IOException {
    byte[] fam = Bytes.toBytes("info");
    byte[][] families = { fam };
    String method = this.getName();

    // add data with a timestamp that is too recent for range. Ensure assert
    CONF.setInt("hbase.hregion.keyvalue.timestamp.slop.millisecs", 1000);
    this.region = initHRegion(tableName, method, CONF, families);
    boolean caughtExcep = false;
    try {
      try {
        // no TS specified == use latest. should not error
        region.put(new Put(row).add(fam, Bytes.toBytes("qual"), Bytes.toBytes("value")));
        // TS out of range. should error
        region.put(new Put(row).add(fam, Bytes.toBytes("qual"), System.currentTimeMillis() + 2000,
            Bytes.toBytes("value")));
        fail("Expected IOE for TS out of configured timerange");
      } catch (FailedSanityCheckException ioe) {
        LOG.debug("Received expected exception", ioe);
        caughtExcep = true;
      }
      assertTrue("Should catch FailedSanityCheckException", caughtExcep);
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  @Test
  public void testScanner_DeleteOneFamilyNotAnother() throws IOException {
    byte[] fam1 = Bytes.toBytes("columnA");
    byte[] fam2 = Bytes.toBytes("columnB");
    this.region = initHRegion(tableName, getName(), CONF, fam1, fam2);
    try {
      byte[] rowA = Bytes.toBytes("rowA");
      byte[] rowB = Bytes.toBytes("rowB");

      byte[] value = Bytes.toBytes("value");

      Delete delete = new Delete(rowA);
      delete.deleteFamily(fam1);

      region.delete(delete);

      // now create data.
      Put put = new Put(rowA);
      put.add(fam2, null, value);
      region.put(put);

      put = new Put(rowB);
      put.add(fam1, null, value);
      put.add(fam2, null, value);
      region.put(put);

      Scan scan = new Scan();
      scan.addFamily(fam1).addFamily(fam2);
      InternalScanner s = region.getScanner(scan);
      List<Cell> results = new ArrayList<Cell>();
      s.next(results);
      assertTrue(CellUtil.matchingRow(results.get(0), rowA));

      results.clear();
      s.next(results);
      assertTrue(CellUtil.matchingRow(results.get(0), rowB));
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  @Test
  public void testDeleteColumns_PostInsert() throws IOException, InterruptedException {
    Delete delete = new Delete(row);
    delete.deleteColumns(fam1, qual1);
    doTestDelete_AndPostInsert(delete);
  }

  @Test
  public void testDeleteFamily_PostInsert() throws IOException, InterruptedException {
    Delete delete = new Delete(row);
    delete.deleteFamily(fam1);
    doTestDelete_AndPostInsert(delete);
  }

  public void doTestDelete_AndPostInsert(Delete delete) throws IOException, InterruptedException {
    TableName tableName = TableName.valueOf(name.getMethodName());
    this.region = initHRegion(tableName, getName(), CONF, fam1);
    try {
      EnvironmentEdgeManagerTestHelper.injectEdge(new IncrementingEnvironmentEdge());
      Put put = new Put(row);
      put.add(fam1, qual1, value1);
      region.put(put);

      // now delete the value:
      region.delete(delete);

      // ok put data:
      put = new Put(row);
      put.add(fam1, qual1, value2);
      region.put(put);

      // ok get:
      Get get = new Get(row);
      get.addColumn(fam1, qual1);

      Result r = region.get(get);
      assertEquals(1, r.size());
      assertArrayEquals(value2, r.getValue(fam1, qual1));

      // next:
      Scan scan = new Scan(row);
      scan.addColumn(fam1, qual1);
      InternalScanner s = region.getScanner(scan);

      List<Cell> results = new ArrayList<Cell>();
      assertEquals(false, NextState.hasMoreValues(s.next(results)));
      assertEquals(1, results.size());
      Cell kv = results.get(0);

      assertArrayEquals(value2, CellUtil.cloneValue(kv));
      assertArrayEquals(fam1, CellUtil.cloneFamily(kv));
      assertArrayEquals(qual1, CellUtil.cloneQualifier(kv));
      assertArrayEquals(row, CellUtil.cloneRow(kv));
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  @Test
  public void testDelete_CheckTimestampUpdated() throws IOException {
    TableName tableName = TableName.valueOf(name.getMethodName());
    byte[] row1 = Bytes.toBytes("row1");
    byte[] col1 = Bytes.toBytes("col1");
    byte[] col2 = Bytes.toBytes("col2");
    byte[] col3 = Bytes.toBytes("col3");

    // Setting up region
    String method = this.getName();
    this.region = initHRegion(tableName, method, CONF, fam1);
    try {
      // Building checkerList
      List<Cell> kvs = new ArrayList<Cell>();
      kvs.add(new KeyValue(row1, fam1, col1, null));
      kvs.add(new KeyValue(row1, fam1, col2, null));
      kvs.add(new KeyValue(row1, fam1, col3, null));

      NavigableMap<byte[], List<Cell>> deleteMap = new TreeMap<byte[], List<Cell>>(
          Bytes.BYTES_COMPARATOR);
      deleteMap.put(fam1, kvs);
      region.delete(deleteMap, Durability.SYNC_WAL);

      // extract the key values out the memstore:
      // This is kinda hacky, but better than nothing...
      long now = System.currentTimeMillis();
      DefaultMemStore memstore = (DefaultMemStore) ((HStore) region.getStore(fam1)).memstore;
      Cell firstCell = memstore.cellSet.first();
      assertTrue(firstCell.getTimestamp() <= now);
      now = firstCell.getTimestamp();
      for (Cell cell : memstore.cellSet) {
        assertTrue(cell.getTimestamp() <= now);
        now = cell.getTimestamp();
      }
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  // ////////////////////////////////////////////////////////////////////////////
  // Get tests
  // ////////////////////////////////////////////////////////////////////////////
  @Test
  public void testGet_FamilyChecker() throws IOException {
    byte[] row1 = Bytes.toBytes("row1");
    byte[] fam1 = Bytes.toBytes("fam1");
    byte[] fam2 = Bytes.toBytes("False");
    byte[] col1 = Bytes.toBytes("col1");

    // Setting up region
    String method = this.getName();
    this.region = initHRegion(tableName, method, CONF, fam1);
    try {
      Get get = new Get(row1);
      get.addColumn(fam2, col1);

      // Test
      try {
        region.get(get);
      } catch (org.apache.hadoop.hbase.DoNotRetryIOException e) {
        assertFalse(false);
        return;
      }
      assertFalse(true);
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  @Test
  public void testGet_Basic() throws IOException {
    byte[] row1 = Bytes.toBytes("row1");
    byte[] fam1 = Bytes.toBytes("fam1");
    byte[] col1 = Bytes.toBytes("col1");
    byte[] col2 = Bytes.toBytes("col2");
    byte[] col3 = Bytes.toBytes("col3");
    byte[] col4 = Bytes.toBytes("col4");
    byte[] col5 = Bytes.toBytes("col5");

    // Setting up region
    String method = this.getName();
    this.region = initHRegion(tableName, method, CONF, fam1);
    try {
      // Add to memstore
      Put put = new Put(row1);
      put.add(fam1, col1, null);
      put.add(fam1, col2, null);
      put.add(fam1, col3, null);
      put.add(fam1, col4, null);
      put.add(fam1, col5, null);
      region.put(put);

      Get get = new Get(row1);
      get.addColumn(fam1, col2);
      get.addColumn(fam1, col4);
      // Expected result
      KeyValue kv1 = new KeyValue(row1, fam1, col2);
      KeyValue kv2 = new KeyValue(row1, fam1, col4);
      KeyValue[] expected = { kv1, kv2 };

      // Test
      Result res = region.get(get);
      assertEquals(expected.length, res.size());
      for (int i = 0; i < res.size(); i++) {
        assertTrue(CellUtil.matchingRow(expected[i], res.rawCells()[i]));
        assertTrue(CellUtil.matchingFamily(expected[i], res.rawCells()[i]));
        assertTrue(CellUtil.matchingQualifier(expected[i], res.rawCells()[i]));
      }

      // Test using a filter on a Get
      Get g = new Get(row1);
      final int count = 2;
      g.setFilter(new ColumnCountGetFilter(count));
      res = region.get(g);
      assertEquals(count, res.size());
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  @Test
  public void testGet_Empty() throws IOException {
    byte[] row = Bytes.toBytes("row");
    byte[] fam = Bytes.toBytes("fam");

    String method = this.getName();
    this.region = initHRegion(tableName, method, CONF, fam);
    try {
      Get get = new Get(row);
      get.addFamily(fam);
      Result r = region.get(get);

      assertTrue(r.isEmpty());
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  // ////////////////////////////////////////////////////////////////////////////
  // Merge test
  // ////////////////////////////////////////////////////////////////////////////
  @Test
  public void testMerge() throws IOException {
    byte[][] families = { fam1, fam2, fam3 };
    Configuration hc = initSplit();
    // Setting up region
    String method = this.getName();
    this.region = initHRegion(tableName, method, hc, families);
    try {
      LOG.info("" + HBaseTestCase.addContent(region, fam3));
      region.flush(true);
      region.compactStores();
      byte[] splitRow = region.checkSplit();
      assertNotNull(splitRow);
      LOG.info("SplitRow: " + Bytes.toString(splitRow));
      HRegion[] subregions = splitRegion(region, splitRow);
      try {
        // Need to open the regions.
        for (int i = 0; i < subregions.length; i++) {
          HRegion.openHRegion(subregions[i], null);
          subregions[i].compactStores();
        }
        Path oldRegionPath = region.getRegionFileSystem().getRegionDir();
        Path oldRegion1 = subregions[0].getRegionFileSystem().getRegionDir();
        Path oldRegion2 = subregions[1].getRegionFileSystem().getRegionDir();
        long startTime = System.currentTimeMillis();
        region = HRegion.mergeAdjacent(subregions[0], subregions[1]);
        LOG.info("Merge regions elapsed time: "
            + ((System.currentTimeMillis() - startTime) / 1000.0));
        FILESYSTEM.delete(oldRegion1, true);
        FILESYSTEM.delete(oldRegion2, true);
        FILESYSTEM.delete(oldRegionPath, true);
        LOG.info("splitAndMerge completed.");
      } finally {
        for (int i = 0; i < subregions.length; i++) {
          try {
            HBaseTestingUtility.closeRegionAndWAL(subregions[i]);
          } catch (IOException e) {
            // Ignore.
          }
        }
      }
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  /**
   * @param parent
   *          Region to split.
   * @param midkey
   *          Key to split around.
   * @return The Regions we created.
   * @throws IOException
   */
  HRegion[] splitRegion(final HRegion parent, final byte[] midkey) throws IOException {
    PairOfSameType<Region> result = null;
    SplitTransaction st = new SplitTransaction(parent, midkey);
    // If prepare does not return true, for some reason -- logged inside in
    // the prepare call -- we are not ready to split just now. Just return.
    if (!st.prepare()) {
      parent.clearSplit();
      return null;
    }
    try {
      result = st.execute(null, null);
    } catch (IOException ioe) {
      try {
        LOG.info("Running rollback of failed split of " +
          parent.getRegionInfo().getRegionNameAsString() + "; " + ioe.getMessage());
        st.rollback(null, null);
        LOG.info("Successful rollback of failed split of " +
          parent.getRegionInfo().getRegionNameAsString());
        return null;
      } catch (RuntimeException e) {
        // If failed rollback, kill this server to avoid having a hole in table.
        LOG.info("Failed rollback of failed split of " +
          parent.getRegionInfo().getRegionNameAsString() + " -- aborting server", e);
      }
    }
    finally {
      parent.clearSplit();
    }
    return new HRegion[] { (HRegion)result.getFirst(), (HRegion)result.getSecond() };
  }

  // ////////////////////////////////////////////////////////////////////////////
  // Scanner tests
  // ////////////////////////////////////////////////////////////////////////////
  @Test
  public void testGetScanner_WithOkFamilies() throws IOException {
    byte[] fam1 = Bytes.toBytes("fam1");
    byte[] fam2 = Bytes.toBytes("fam2");

    byte[][] families = { fam1, fam2 };

    // Setting up region
    String method = this.getName();
    this.region = initHRegion(tableName, method, CONF, families);
    try {
      Scan scan = new Scan();
      scan.addFamily(fam1);
      scan.addFamily(fam2);
      try {
        region.getScanner(scan);
      } catch (Exception e) {
        assertTrue("Families could not be found in Region", false);
      }
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  @Test
  public void testGetScanner_WithNotOkFamilies() throws IOException {
    byte[] fam1 = Bytes.toBytes("fam1");
    byte[] fam2 = Bytes.toBytes("fam2");

    byte[][] families = { fam1 };

    // Setting up region
    String method = this.getName();
    this.region = initHRegion(tableName, method, CONF, families);
    try {
      Scan scan = new Scan();
      scan.addFamily(fam2);
      boolean ok = false;
      try {
        region.getScanner(scan);
      } catch (Exception e) {
        ok = true;
      }
      assertTrue("Families could not be found in Region", ok);
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  @Test
  public void testGetScanner_WithNoFamilies() throws IOException {
    byte[] row1 = Bytes.toBytes("row1");
    byte[] fam1 = Bytes.toBytes("fam1");
    byte[] fam2 = Bytes.toBytes("fam2");
    byte[] fam3 = Bytes.toBytes("fam3");
    byte[] fam4 = Bytes.toBytes("fam4");

    byte[][] families = { fam1, fam2, fam3, fam4 };

    // Setting up region
    String method = this.getName();
    this.region = initHRegion(tableName, method, CONF, families);
    try {

      // Putting data in Region
      Put put = new Put(row1);
      put.add(fam1, null, null);
      put.add(fam2, null, null);
      put.add(fam3, null, null);
      put.add(fam4, null, null);
      region.put(put);

      Scan scan = null;
      HRegion.RegionScannerImpl is = null;

      // Testing to see how many scanners that is produced by getScanner,
      // starting
      // with known number, 2 - current = 1
      scan = new Scan();
      scan.addFamily(fam2);
      scan.addFamily(fam4);
      is = (RegionScannerImpl) region.getScanner(scan);
      assertEquals(1, ((RegionScannerImpl) is).storeHeap.getHeap().size());

      scan = new Scan();
      is = (RegionScannerImpl) region.getScanner(scan);
      assertEquals(families.length - 1, ((RegionScannerImpl) is).storeHeap.getHeap().size());
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  /**
   * This method tests https://issues.apache.org/jira/browse/HBASE-2516.
   *
   * @throws IOException
   */
  @Test
  public void testGetScanner_WithRegionClosed() throws IOException {
    byte[] fam1 = Bytes.toBytes("fam1");
    byte[] fam2 = Bytes.toBytes("fam2");

    byte[][] families = { fam1, fam2 };

    // Setting up region
    String method = this.getName();
    try {
      this.region = initHRegion(tableName, method, CONF, families);
    } catch (IOException e) {
      e.printStackTrace();
      fail("Got IOException during initHRegion, " + e.getMessage());
    }
    try {
      region.closed.set(true);
      try {
        region.getScanner(null);
        fail("Expected to get an exception during getScanner on a region that is closed");
      } catch (NotServingRegionException e) {
        // this is the correct exception that is expected
      } catch (IOException e) {
        fail("Got wrong type of exception - should be a NotServingRegionException, but was an IOException: "
            + e.getMessage());
      }
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  @Test
  public void testRegionScanner_Next() throws IOException {
    byte[] row1 = Bytes.toBytes("row1");
    byte[] row2 = Bytes.toBytes("row2");
    byte[] fam1 = Bytes.toBytes("fam1");
    byte[] fam2 = Bytes.toBytes("fam2");
    byte[] fam3 = Bytes.toBytes("fam3");
    byte[] fam4 = Bytes.toBytes("fam4");

    byte[][] families = { fam1, fam2, fam3, fam4 };
    long ts = System.currentTimeMillis();

    // Setting up region
    String method = this.getName();
    this.region = initHRegion(tableName, method, CONF, families);
    try {
      // Putting data in Region
      Put put = null;
      put = new Put(row1);
      put.add(fam1, (byte[]) null, ts, null);
      put.add(fam2, (byte[]) null, ts, null);
      put.add(fam3, (byte[]) null, ts, null);
      put.add(fam4, (byte[]) null, ts, null);
      region.put(put);

      put = new Put(row2);
      put.add(fam1, (byte[]) null, ts, null);
      put.add(fam2, (byte[]) null, ts, null);
      put.add(fam3, (byte[]) null, ts, null);
      put.add(fam4, (byte[]) null, ts, null);
      region.put(put);

      Scan scan = new Scan();
      scan.addFamily(fam2);
      scan.addFamily(fam4);
      InternalScanner is = region.getScanner(scan);

      List<Cell> res = null;

      // Result 1
      List<Cell> expected1 = new ArrayList<Cell>();
      expected1.add(new KeyValue(row1, fam2, null, ts, KeyValue.Type.Put, null));
      expected1.add(new KeyValue(row1, fam4, null, ts, KeyValue.Type.Put, null));

      res = new ArrayList<Cell>();
      is.next(res);
      for (int i = 0; i < res.size(); i++) {
        assertTrue(CellComparator.equalsIgnoreMvccVersion(expected1.get(i), res.get(i)));
      }

      // Result 2
      List<Cell> expected2 = new ArrayList<Cell>();
      expected2.add(new KeyValue(row2, fam2, null, ts, KeyValue.Type.Put, null));
      expected2.add(new KeyValue(row2, fam4, null, ts, KeyValue.Type.Put, null));

      res = new ArrayList<Cell>();
      is.next(res);
      for (int i = 0; i < res.size(); i++) {
        assertTrue(CellComparator.equalsIgnoreMvccVersion(expected2.get(i), res.get(i)));
      }
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  @Test
  public void testScanner_ExplicitColumns_FromMemStore_EnforceVersions() throws IOException {
    byte[] row1 = Bytes.toBytes("row1");
    byte[] qf1 = Bytes.toBytes("qualifier1");
    byte[] qf2 = Bytes.toBytes("qualifier2");
    byte[] fam1 = Bytes.toBytes("fam1");
    byte[][] families = { fam1 };

    long ts1 = System.currentTimeMillis();
    long ts2 = ts1 + 1;
    long ts3 = ts1 + 2;

    // Setting up region
    String method = this.getName();
    this.region = initHRegion(tableName, method, CONF, families);
    try {
      // Putting data in Region
      Put put = null;
      KeyValue kv13 = new KeyValue(row1, fam1, qf1, ts3, KeyValue.Type.Put, null);
      KeyValue kv12 = new KeyValue(row1, fam1, qf1, ts2, KeyValue.Type.Put, null);
      KeyValue kv11 = new KeyValue(row1, fam1, qf1, ts1, KeyValue.Type.Put, null);

      KeyValue kv23 = new KeyValue(row1, fam1, qf2, ts3, KeyValue.Type.Put, null);
      KeyValue kv22 = new KeyValue(row1, fam1, qf2, ts2, KeyValue.Type.Put, null);
      KeyValue kv21 = new KeyValue(row1, fam1, qf2, ts1, KeyValue.Type.Put, null);

      put = new Put(row1);
      put.add(kv13);
      put.add(kv12);
      put.add(kv11);
      put.add(kv23);
      put.add(kv22);
      put.add(kv21);
      region.put(put);

      // Expected
      List<Cell> expected = new ArrayList<Cell>();
      expected.add(kv13);
      expected.add(kv12);

      Scan scan = new Scan(row1);
      scan.addColumn(fam1, qf1);
      scan.setMaxVersions(MAX_VERSIONS);
      List<Cell> actual = new ArrayList<Cell>();
      InternalScanner scanner = region.getScanner(scan);

      boolean hasNext = NextState.hasMoreValues(scanner.next(actual));
      assertEquals(false, hasNext);

      // Verify result
      for (int i = 0; i < expected.size(); i++) {
        assertEquals(expected.get(i), actual.get(i));
      }
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  @Test
  public void testScanner_ExplicitColumns_FromFilesOnly_EnforceVersions() throws IOException {
    byte[] row1 = Bytes.toBytes("row1");
    byte[] qf1 = Bytes.toBytes("qualifier1");
    byte[] qf2 = Bytes.toBytes("qualifier2");
    byte[] fam1 = Bytes.toBytes("fam1");
    byte[][] families = { fam1 };

    long ts1 = 1; // System.currentTimeMillis();
    long ts2 = ts1 + 1;
    long ts3 = ts1 + 2;

    // Setting up region
    String method = this.getName();
    this.region = initHRegion(tableName, method, CONF, families);
    try {
      // Putting data in Region
      Put put = null;
      KeyValue kv13 = new KeyValue(row1, fam1, qf1, ts3, KeyValue.Type.Put, null);
      KeyValue kv12 = new KeyValue(row1, fam1, qf1, ts2, KeyValue.Type.Put, null);
      KeyValue kv11 = new KeyValue(row1, fam1, qf1, ts1, KeyValue.Type.Put, null);

      KeyValue kv23 = new KeyValue(row1, fam1, qf2, ts3, KeyValue.Type.Put, null);
      KeyValue kv22 = new KeyValue(row1, fam1, qf2, ts2, KeyValue.Type.Put, null);
      KeyValue kv21 = new KeyValue(row1, fam1, qf2, ts1, KeyValue.Type.Put, null);

      put = new Put(row1);
      put.add(kv13);
      put.add(kv12);
      put.add(kv11);
      put.add(kv23);
      put.add(kv22);
      put.add(kv21);
      region.put(put);
      region.flush(true);

      // Expected
      List<Cell> expected = new ArrayList<Cell>();
      expected.add(kv13);
      expected.add(kv12);
      expected.add(kv23);
      expected.add(kv22);

      Scan scan = new Scan(row1);
      scan.addColumn(fam1, qf1);
      scan.addColumn(fam1, qf2);
      scan.setMaxVersions(MAX_VERSIONS);
      List<Cell> actual = new ArrayList<Cell>();
      InternalScanner scanner = region.getScanner(scan);

      boolean hasNext = NextState.hasMoreValues(scanner.next(actual));
      assertEquals(false, hasNext);

      // Verify result
      for (int i = 0; i < expected.size(); i++) {
        assertTrue(CellComparator.equalsIgnoreMvccVersion(expected.get(i), actual.get(i)));
      }
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  @Test
  public void testScanner_ExplicitColumns_FromMemStoreAndFiles_EnforceVersions() throws IOException {
    byte[] row1 = Bytes.toBytes("row1");
    byte[] fam1 = Bytes.toBytes("fam1");
    byte[][] families = { fam1 };
    byte[] qf1 = Bytes.toBytes("qualifier1");
    byte[] qf2 = Bytes.toBytes("qualifier2");

    long ts1 = 1;
    long ts2 = ts1 + 1;
    long ts3 = ts1 + 2;
    long ts4 = ts1 + 3;

    // Setting up region
    String method = this.getName();
    this.region = initHRegion(tableName, method, CONF, families);
    try {
      // Putting data in Region
      KeyValue kv14 = new KeyValue(row1, fam1, qf1, ts4, KeyValue.Type.Put, null);
      KeyValue kv13 = new KeyValue(row1, fam1, qf1, ts3, KeyValue.Type.Put, null);
      KeyValue kv12 = new KeyValue(row1, fam1, qf1, ts2, KeyValue.Type.Put, null);
      KeyValue kv11 = new KeyValue(row1, fam1, qf1, ts1, KeyValue.Type.Put, null);

      KeyValue kv24 = new KeyValue(row1, fam1, qf2, ts4, KeyValue.Type.Put, null);
      KeyValue kv23 = new KeyValue(row1, fam1, qf2, ts3, KeyValue.Type.Put, null);
      KeyValue kv22 = new KeyValue(row1, fam1, qf2, ts2, KeyValue.Type.Put, null);
      KeyValue kv21 = new KeyValue(row1, fam1, qf2, ts1, KeyValue.Type.Put, null);

      Put put = null;
      put = new Put(row1);
      put.add(kv14);
      put.add(kv24);
      region.put(put);
      region.flush(true);

      put = new Put(row1);
      put.add(kv23);
      put.add(kv13);
      region.put(put);
      region.flush(true);

      put = new Put(row1);
      put.add(kv22);
      put.add(kv12);
      region.put(put);
      region.flush(true);

      put = new Put(row1);
      put.add(kv21);
      put.add(kv11);
      region.put(put);

      // Expected
      List<Cell> expected = new ArrayList<Cell>();
      expected.add(kv14);
      expected.add(kv13);
      expected.add(kv12);
      expected.add(kv24);
      expected.add(kv23);
      expected.add(kv22);

      Scan scan = new Scan(row1);
      scan.addColumn(fam1, qf1);
      scan.addColumn(fam1, qf2);
      int versions = 3;
      scan.setMaxVersions(versions);
      List<Cell> actual = new ArrayList<Cell>();
      InternalScanner scanner = region.getScanner(scan);

      boolean hasNext = NextState.hasMoreValues(scanner.next(actual));
      assertEquals(false, hasNext);

      // Verify result
      for (int i = 0; i < expected.size(); i++) {
        assertTrue(CellComparator.equalsIgnoreMvccVersion(expected.get(i), actual.get(i)));
      }
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  @Test
  public void testScanner_Wildcard_FromMemStore_EnforceVersions() throws IOException {
    byte[] row1 = Bytes.toBytes("row1");
    byte[] qf1 = Bytes.toBytes("qualifier1");
    byte[] qf2 = Bytes.toBytes("qualifier2");
    byte[] fam1 = Bytes.toBytes("fam1");
    byte[][] families = { fam1 };

    long ts1 = System.currentTimeMillis();
    long ts2 = ts1 + 1;
    long ts3 = ts1 + 2;

    // Setting up region
    String method = this.getName();
    this.region = initHRegion(tableName, method, CONF, families);
    try {
      // Putting data in Region
      Put put = null;
      KeyValue kv13 = new KeyValue(row1, fam1, qf1, ts3, KeyValue.Type.Put, null);
      KeyValue kv12 = new KeyValue(row1, fam1, qf1, ts2, KeyValue.Type.Put, null);
      KeyValue kv11 = new KeyValue(row1, fam1, qf1, ts1, KeyValue.Type.Put, null);

      KeyValue kv23 = new KeyValue(row1, fam1, qf2, ts3, KeyValue.Type.Put, null);
      KeyValue kv22 = new KeyValue(row1, fam1, qf2, ts2, KeyValue.Type.Put, null);
      KeyValue kv21 = new KeyValue(row1, fam1, qf2, ts1, KeyValue.Type.Put, null);

      put = new Put(row1);
      put.add(kv13);
      put.add(kv12);
      put.add(kv11);
      put.add(kv23);
      put.add(kv22);
      put.add(kv21);
      region.put(put);

      // Expected
      List<Cell> expected = new ArrayList<Cell>();
      expected.add(kv13);
      expected.add(kv12);
      expected.add(kv23);
      expected.add(kv22);

      Scan scan = new Scan(row1);
      scan.addFamily(fam1);
      scan.setMaxVersions(MAX_VERSIONS);
      List<Cell> actual = new ArrayList<Cell>();
      InternalScanner scanner = region.getScanner(scan);

      boolean hasNext = NextState.hasMoreValues(scanner.next(actual));
      assertEquals(false, hasNext);

      // Verify result
      for (int i = 0; i < expected.size(); i++) {
        assertEquals(expected.get(i), actual.get(i));
      }
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  @Test
  public void testScanner_Wildcard_FromFilesOnly_EnforceVersions() throws IOException {
    byte[] row1 = Bytes.toBytes("row1");
    byte[] qf1 = Bytes.toBytes("qualifier1");
    byte[] qf2 = Bytes.toBytes("qualifier2");
    byte[] fam1 = Bytes.toBytes("fam1");

    long ts1 = 1; // System.currentTimeMillis();
    long ts2 = ts1 + 1;
    long ts3 = ts1 + 2;

    // Setting up region
    String method = this.getName();
    this.region = initHRegion(tableName, method, CONF, fam1);
    try {
      // Putting data in Region
      Put put = null;
      KeyValue kv13 = new KeyValue(row1, fam1, qf1, ts3, KeyValue.Type.Put, null);
      KeyValue kv12 = new KeyValue(row1, fam1, qf1, ts2, KeyValue.Type.Put, null);
      KeyValue kv11 = new KeyValue(row1, fam1, qf1, ts1, KeyValue.Type.Put, null);

      KeyValue kv23 = new KeyValue(row1, fam1, qf2, ts3, KeyValue.Type.Put, null);
      KeyValue kv22 = new KeyValue(row1, fam1, qf2, ts2, KeyValue.Type.Put, null);
      KeyValue kv21 = new KeyValue(row1, fam1, qf2, ts1, KeyValue.Type.Put, null);

      put = new Put(row1);
      put.add(kv13);
      put.add(kv12);
      put.add(kv11);
      put.add(kv23);
      put.add(kv22);
      put.add(kv21);
      region.put(put);
      region.flush(true);

      // Expected
      List<Cell> expected = new ArrayList<Cell>();
      expected.add(kv13);
      expected.add(kv12);
      expected.add(kv23);
      expected.add(kv22);

      Scan scan = new Scan(row1);
      scan.addFamily(fam1);
      scan.setMaxVersions(MAX_VERSIONS);
      List<Cell> actual = new ArrayList<Cell>();
      InternalScanner scanner = region.getScanner(scan);

      boolean hasNext = NextState.hasMoreValues(scanner.next(actual));
      assertEquals(false, hasNext);

      // Verify result
      for (int i = 0; i < expected.size(); i++) {
        assertTrue(CellComparator.equalsIgnoreMvccVersion(expected.get(i), actual.get(i)));
      }
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  @Test
  public void testScanner_StopRow1542() throws IOException {
    byte[] family = Bytes.toBytes("testFamily");
    this.region = initHRegion(tableName, getName(), CONF, family);
    try {
      byte[] row1 = Bytes.toBytes("row111");
      byte[] row2 = Bytes.toBytes("row222");
      byte[] row3 = Bytes.toBytes("row333");
      byte[] row4 = Bytes.toBytes("row444");
      byte[] row5 = Bytes.toBytes("row555");

      byte[] col1 = Bytes.toBytes("Pub111");
      byte[] col2 = Bytes.toBytes("Pub222");

      Put put = new Put(row1);
      put.add(family, col1, Bytes.toBytes(10L));
      region.put(put);

      put = new Put(row2);
      put.add(family, col1, Bytes.toBytes(15L));
      region.put(put);

      put = new Put(row3);
      put.add(family, col2, Bytes.toBytes(20L));
      region.put(put);

      put = new Put(row4);
      put.add(family, col2, Bytes.toBytes(30L));
      region.put(put);

      put = new Put(row5);
      put.add(family, col1, Bytes.toBytes(40L));
      region.put(put);

      Scan scan = new Scan(row3, row4);
      scan.setMaxVersions();
      scan.addColumn(family, col1);
      InternalScanner s = region.getScanner(scan);

      List<Cell> results = new ArrayList<Cell>();
      assertEquals(false, NextState.hasMoreValues(s.next(results)));
      assertEquals(0, results.size());
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  @Test
  public void testScanner_Wildcard_FromMemStoreAndFiles_EnforceVersions() throws IOException {
    byte[] row1 = Bytes.toBytes("row1");
    byte[] fam1 = Bytes.toBytes("fam1");
    byte[] qf1 = Bytes.toBytes("qualifier1");
    byte[] qf2 = Bytes.toBytes("quateslifier2");

    long ts1 = 1;
    long ts2 = ts1 + 1;
    long ts3 = ts1 + 2;
    long ts4 = ts1 + 3;

    // Setting up region
    String method = this.getName();
    this.region = initHRegion(tableName, method, CONF, fam1);
    try {
      // Putting data in Region
      KeyValue kv14 = new KeyValue(row1, fam1, qf1, ts4, KeyValue.Type.Put, null);
      KeyValue kv13 = new KeyValue(row1, fam1, qf1, ts3, KeyValue.Type.Put, null);
      KeyValue kv12 = new KeyValue(row1, fam1, qf1, ts2, KeyValue.Type.Put, null);
      KeyValue kv11 = new KeyValue(row1, fam1, qf1, ts1, KeyValue.Type.Put, null);

      KeyValue kv24 = new KeyValue(row1, fam1, qf2, ts4, KeyValue.Type.Put, null);
      KeyValue kv23 = new KeyValue(row1, fam1, qf2, ts3, KeyValue.Type.Put, null);
      KeyValue kv22 = new KeyValue(row1, fam1, qf2, ts2, KeyValue.Type.Put, null);
      KeyValue kv21 = new KeyValue(row1, fam1, qf2, ts1, KeyValue.Type.Put, null);

      Put put = null;
      put = new Put(row1);
      put.add(kv14);
      put.add(kv24);
      region.put(put);
      region.flush(true);

      put = new Put(row1);
      put.add(kv23);
      put.add(kv13);
      region.put(put);
      region.flush(true);

      put = new Put(row1);
      put.add(kv22);
      put.add(kv12);
      region.put(put);
      region.flush(true);

      put = new Put(row1);
      put.add(kv21);
      put.add(kv11);
      region.put(put);

      // Expected
      List<KeyValue> expected = new ArrayList<KeyValue>();
      expected.add(kv14);
      expected.add(kv13);
      expected.add(kv12);
      expected.add(kv24);
      expected.add(kv23);
      expected.add(kv22);

      Scan scan = new Scan(row1);
      int versions = 3;
      scan.setMaxVersions(versions);
      List<Cell> actual = new ArrayList<Cell>();
      InternalScanner scanner = region.getScanner(scan);

      boolean hasNext = NextState.hasMoreValues(scanner.next(actual));
      assertEquals(false, hasNext);

      // Verify result
      for (int i = 0; i < expected.size(); i++) {
        assertTrue(CellComparator.equalsIgnoreMvccVersion(expected.get(i), actual.get(i)));
      }
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  /**
   * Added for HBASE-5416
   *
   * Here we test scan optimization when only subset of CFs are used in filter
   * conditions.
   */
  @Test
  public void testScanner_JoinedScanners() throws IOException {
    byte[] cf_essential = Bytes.toBytes("essential");
    byte[] cf_joined = Bytes.toBytes("joined");
    byte[] cf_alpha = Bytes.toBytes("alpha");
    this.region = initHRegion(tableName, getName(), CONF, cf_essential, cf_joined, cf_alpha);
    try {
      byte[] row1 = Bytes.toBytes("row1");
      byte[] row2 = Bytes.toBytes("row2");
      byte[] row3 = Bytes.toBytes("row3");

      byte[] col_normal = Bytes.toBytes("d");
      byte[] col_alpha = Bytes.toBytes("a");

      byte[] filtered_val = Bytes.toBytes(3);

      Put put = new Put(row1);
      put.add(cf_essential, col_normal, Bytes.toBytes(1));
      put.add(cf_joined, col_alpha, Bytes.toBytes(1));
      region.put(put);

      put = new Put(row2);
      put.add(cf_essential, col_alpha, Bytes.toBytes(2));
      put.add(cf_joined, col_normal, Bytes.toBytes(2));
      put.add(cf_alpha, col_alpha, Bytes.toBytes(2));
      region.put(put);

      put = new Put(row3);
      put.add(cf_essential, col_normal, filtered_val);
      put.add(cf_joined, col_normal, filtered_val);
      region.put(put);

      // Check two things:
      // 1. result list contains expected values
      // 2. result list is sorted properly

      Scan scan = new Scan();
      Filter filter = new SingleColumnValueExcludeFilter(cf_essential, col_normal,
          CompareOp.NOT_EQUAL, filtered_val);
      scan.setFilter(filter);
      scan.setLoadColumnFamiliesOnDemand(true);
      InternalScanner s = region.getScanner(scan);

      List<Cell> results = new ArrayList<Cell>();
      assertTrue(NextState.hasMoreValues(s.next(results)));
      assertEquals(results.size(), 1);
      results.clear();

      assertTrue(NextState.hasMoreValues(s.next(results)));
      assertEquals(results.size(), 3);
      assertTrue("orderCheck", CellUtil.matchingFamily(results.get(0), cf_alpha));
      assertTrue("orderCheck", CellUtil.matchingFamily(results.get(1), cf_essential));
      assertTrue("orderCheck", CellUtil.matchingFamily(results.get(2), cf_joined));
      results.clear();

      assertFalse(NextState.hasMoreValues(s.next(results)));
      assertEquals(results.size(), 0);
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  /**
   * HBASE-5416
   *
   * Test case when scan limits amount of KVs returned on each next() call.
   */
  @Test
  public void testScanner_JoinedScannersWithLimits() throws IOException {
    final byte[] cf_first = Bytes.toBytes("first");
    final byte[] cf_second = Bytes.toBytes("second");

    this.region = initHRegion(tableName, getName(), CONF, cf_first, cf_second);
    try {
      final byte[] col_a = Bytes.toBytes("a");
      final byte[] col_b = Bytes.toBytes("b");

      Put put;

      for (int i = 0; i < 10; i++) {
        put = new Put(Bytes.toBytes("r" + Integer.toString(i)));
        put.add(cf_first, col_a, Bytes.toBytes(i));
        if (i < 5) {
          put.add(cf_first, col_b, Bytes.toBytes(i));
          put.add(cf_second, col_a, Bytes.toBytes(i));
          put.add(cf_second, col_b, Bytes.toBytes(i));
        }
        region.put(put);
      }

      Scan scan = new Scan();
      scan.setLoadColumnFamiliesOnDemand(true);
      Filter bogusFilter = new FilterBase() {
        @Override
        public ReturnCode filterKeyValue(Cell ignored) throws IOException {
          return ReturnCode.INCLUDE;
        }
        @Override
        public boolean isFamilyEssential(byte[] name) {
          return Bytes.equals(name, cf_first);
        }
      };

      scan.setFilter(bogusFilter);
      InternalScanner s = region.getScanner(scan);

      // Our data looks like this:
      // r0: first:a, first:b, second:a, second:b
      // r1: first:a, first:b, second:a, second:b
      // r2: first:a, first:b, second:a, second:b
      // r3: first:a, first:b, second:a, second:b
      // r4: first:a, first:b, second:a, second:b
      // r5: first:a
      // r6: first:a
      // r7: first:a
      // r8: first:a
      // r9: first:a

      // But due to next's limit set to 3, we should get this:
      // r0: first:a, first:b, second:a
      // r0: second:b
      // r1: first:a, first:b, second:a
      // r1: second:b
      // r2: first:a, first:b, second:a
      // r2: second:b
      // r3: first:a, first:b, second:a
      // r3: second:b
      // r4: first:a, first:b, second:a
      // r4: second:b
      // r5: first:a
      // r6: first:a
      // r7: first:a
      // r8: first:a
      // r9: first:a

      List<Cell> results = new ArrayList<Cell>();
      int index = 0;
      while (true) {
        boolean more = NextState.hasMoreValues(s.next(results, 3));
        if ((index >> 1) < 5) {
          if (index % 2 == 0)
            assertEquals(results.size(), 3);
          else
            assertEquals(results.size(), 1);
        } else
          assertEquals(results.size(), 1);
        results.clear();
        index++;
        if (!more)
          break;
      }
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  // ////////////////////////////////////////////////////////////////////////////
  // Split test
  // ////////////////////////////////////////////////////////////////////////////
  /**
   * Splits twice and verifies getting from each of the split regions.
   *
   * @throws Exception
   */
  @Test
  public void testBasicSplit() throws Exception {
    byte[][] families = { fam1, fam2, fam3 };

    Configuration hc = initSplit();
    // Setting up region
    String method = this.getName();
    this.region = initHRegion(tableName, method, hc, families);

    try {
      LOG.info("" + HBaseTestCase.addContent(region, fam3));
      region.flush(true);
      region.compactStores();
      byte[] splitRow = region.checkSplit();
      assertNotNull(splitRow);
      LOG.info("SplitRow: " + Bytes.toString(splitRow));
      HRegion[] regions = splitRegion(region, splitRow);
      try {
        // Need to open the regions.
        // TODO: Add an 'open' to HRegion... don't do open by constructing
        // instance.
        for (int i = 0; i < regions.length; i++) {
          regions[i] = HRegion.openHRegion(regions[i], null);
        }
        // Assert can get rows out of new regions. Should be able to get first
        // row from first region and the midkey from second region.
        assertGet(regions[0], fam3, Bytes.toBytes(START_KEY));
        assertGet(regions[1], fam3, splitRow);
        // Test I can get scanner and that it starts at right place.
        assertScan(regions[0], fam3, Bytes.toBytes(START_KEY));
        assertScan(regions[1], fam3, splitRow);
        // Now prove can't split regions that have references.
        for (int i = 0; i < regions.length; i++) {
          // Add so much data to this region, we create a store file that is >
          // than one of our unsplitable references. it will.
          for (int j = 0; j < 2; j++) {
            HBaseTestCase.addContent(regions[i], fam3);
          }
          HBaseTestCase.addContent(regions[i], fam2);
          HBaseTestCase.addContent(regions[i], fam1);
          regions[i].flush(true);
        }

        byte[][] midkeys = new byte[regions.length][];
        // To make regions splitable force compaction.
        for (int i = 0; i < regions.length; i++) {
          regions[i].compactStores();
          midkeys[i] = regions[i].checkSplit();
        }

        TreeMap<String, HRegion> sortedMap = new TreeMap<String, HRegion>();
        // Split these two daughter regions so then I'll have 4 regions. Will
        // split because added data above.
        for (int i = 0; i < regions.length; i++) {
          HRegion[] rs = null;
          if (midkeys[i] != null) {
            rs = splitRegion(regions[i], midkeys[i]);
            for (int j = 0; j < rs.length; j++) {
              sortedMap.put(Bytes.toString(rs[j].getRegionInfo().getRegionName()),
                HRegion.openHRegion(rs[j], null));
            }
          }
        }
        LOG.info("Made 4 regions");
        // The splits should have been even. Test I can get some arbitrary row
        // out of each.
        int interval = (LAST_CHAR - FIRST_CHAR) / 3;
        byte[] b = Bytes.toBytes(START_KEY);
        for (HRegion r : sortedMap.values()) {
          assertGet(r, fam3, b);
          b[0] += interval;
        }
      } finally {
        for (int i = 0; i < regions.length; i++) {
          try {
            regions[i].close();
          } catch (IOException e) {
            // Ignore.
          }
        }
      }
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  @Test
  public void testSplitRegion() throws IOException {
    byte[] qualifier = Bytes.toBytes("qualifier");
    Configuration hc = initSplit();
    int numRows = 10;
    byte[][] families = { fam1, fam3 };

    // Setting up region
    String method = this.getName();
    this.region = initHRegion(tableName, method, hc, families);

    // Put data in region
    int startRow = 100;
    putData(startRow, numRows, qualifier, families);
    int splitRow = startRow + numRows;
    putData(splitRow, numRows, qualifier, families);
    region.flush(true);

    HRegion[] regions = null;
    try {
      regions = splitRegion(region, Bytes.toBytes("" + splitRow));
      // Opening the regions returned.
      for (int i = 0; i < regions.length; i++) {
        regions[i] = HRegion.openHRegion(regions[i], null);
      }
      // Verifying that the region has been split
      assertEquals(2, regions.length);

      // Verifying that all data is still there and that data is in the right
      // place
      verifyData(regions[0], startRow, numRows, qualifier, families);
      verifyData(regions[1], splitRow, numRows, qualifier, families);

    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  @Test
  public void testClearForceSplit() throws IOException {
    byte[] qualifier = Bytes.toBytes("qualifier");
    Configuration hc = initSplit();
    int numRows = 10;
    byte[][] families = { fam1, fam3 };

    // Setting up region
    String method = this.getName();
    this.region = initHRegion(tableName, method, hc, families);

    // Put data in region
    int startRow = 100;
    putData(startRow, numRows, qualifier, families);
    int splitRow = startRow + numRows;
    byte[] splitRowBytes = Bytes.toBytes("" + splitRow);
    putData(splitRow, numRows, qualifier, families);
    region.flush(true);

    HRegion[] regions = null;
    try {
      // Set force split
      region.forceSplit(splitRowBytes);
      assertTrue(region.shouldForceSplit());
      // Split point should be the force split row
      assertTrue(Bytes.equals(splitRowBytes, region.checkSplit()));

      // Add a store that has references.
      HStore storeMock = Mockito.mock(HStore.class);
      when(storeMock.hasReferences()).thenReturn(true);
      when(storeMock.getFamily()).thenReturn(new HColumnDescriptor("cf"));
      when(storeMock.close()).thenReturn(ImmutableList.<StoreFile>of());
      when(storeMock.getColumnFamilyName()).thenReturn("cf");
      region.stores.put(Bytes.toBytes(storeMock.getColumnFamilyName()), storeMock);
      assertTrue(region.hasReferences());

      // Will not split since the store has references.
      regions = splitRegion(region, splitRowBytes);
      assertNull(regions);

      // Region force split should be cleared after the split try.
      assertFalse(region.shouldForceSplit());

      // Remove the store that has references.
      region.stores.remove(Bytes.toBytes(storeMock.getColumnFamilyName()));
      assertFalse(region.hasReferences());

      // Now we can split.
      regions = splitRegion(region, splitRowBytes);

      // Opening the regions returned.
      for (int i = 0; i < regions.length; i++) {
        regions[i] = HRegion.openHRegion(regions[i], null);
      }
      // Verifying that the region has been split
      assertEquals(2, regions.length);

      // Verifying that all data is still there and that data is in the right
      // place
      verifyData(regions[0], startRow, numRows, qualifier, families);
      verifyData(regions[1], splitRow, numRows, qualifier, families);

    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  /**
   * Flushes the cache in a thread while scanning. The tests verify that the
   * scan is coherent - e.g. the returned results are always of the same or
   * later update as the previous results.
   *
   * @throws IOException
   *           scan / compact
   * @throws InterruptedException
   *           thread join
   */
  @Test
  public void testFlushCacheWhileScanning() throws IOException, InterruptedException {
    byte[] family = Bytes.toBytes("family");
    int numRows = 1000;
    int flushAndScanInterval = 10;
    int compactInterval = 10 * flushAndScanInterval;

    String method = "testFlushCacheWhileScanning";
    this.region = initHRegion(tableName, method, CONF, family);
    try {
      FlushThread flushThread = new FlushThread();
      flushThread.start();

      Scan scan = new Scan();
      scan.addFamily(family);
      scan.setFilter(new SingleColumnValueFilter(family, qual1, CompareOp.EQUAL,
          new BinaryComparator(Bytes.toBytes(5L))));

      int expectedCount = 0;
      List<Cell> res = new ArrayList<Cell>();

      boolean toggle = true;
      for (long i = 0; i < numRows; i++) {
        Put put = new Put(Bytes.toBytes(i));
        put.setDurability(Durability.SKIP_WAL);
        put.add(family, qual1, Bytes.toBytes(i % 10));
        region.put(put);

        if (i != 0 && i % compactInterval == 0) {
          // System.out.println("iteration = " + i);
          region.compact(true);
        }

        if (i % 10 == 5L) {
          expectedCount++;
        }

        if (i != 0 && i % flushAndScanInterval == 0) {
          res.clear();
          InternalScanner scanner = region.getScanner(scan);
          if (toggle) {
            flushThread.flush();
          }
          while (NextState.hasMoreValues(scanner.next(res)))
            ;
          if (!toggle) {
            flushThread.flush();
          }
          assertEquals("i=" + i, expectedCount, res.size());
          toggle = !toggle;
        }
      }

      flushThread.done();
      flushThread.join();
      flushThread.checkNoError();
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  protected class FlushThread extends Thread {
    private volatile boolean done;
    private Throwable error = null;

    public void done() {
      done = true;
      synchronized (this) {
        interrupt();
      }
    }

    public void checkNoError() {
      if (error != null) {
        assertNull(error);
      }
    }

    @Override
    public void run() {
      done = false;
      while (!done) {
        synchronized (this) {
          try {
            wait();
          } catch (InterruptedException ignored) {
            if (done) {
              break;
            }
          }
        }
        try {
          region.flush(true);
        } catch (IOException e) {
          if (!done) {
            LOG.error("Error while flusing cache", e);
            error = e;
          }
          break;
        }
      }

    }

    public void flush() {
      synchronized (this) {
        notify();
      }

    }
  }

  /**
   * Writes very wide records and scans for the latest every time.. Flushes and
   * compacts the region every now and then to keep things realistic.
   *
   * @throws IOException
   *           by flush / scan / compaction
   * @throws InterruptedException
   *           when joining threads
   */
  @Test
  public void testWritesWhileScanning() throws IOException, InterruptedException {
    int testCount = 100;
    int numRows = 1;
    int numFamilies = 10;
    int numQualifiers = 100;
    int flushInterval = 7;
    int compactInterval = 5 * flushInterval;
    byte[][] families = new byte[numFamilies][];
    for (int i = 0; i < numFamilies; i++) {
      families[i] = Bytes.toBytes("family" + i);
    }
    byte[][] qualifiers = new byte[numQualifiers][];
    for (int i = 0; i < numQualifiers; i++) {
      qualifiers[i] = Bytes.toBytes("qual" + i);
    }

    String method = "testWritesWhileScanning";
    this.region = initHRegion(tableName, method, CONF, families);
    try {
      PutThread putThread = new PutThread(numRows, families, qualifiers);
      putThread.start();
      putThread.waitForFirstPut();

      FlushThread flushThread = new FlushThread();
      flushThread.start();

      Scan scan = new Scan(Bytes.toBytes("row0"), Bytes.toBytes("row1"));

      int expectedCount = numFamilies * numQualifiers;
      List<Cell> res = new ArrayList<Cell>();

      long prevTimestamp = 0L;
      for (int i = 0; i < testCount; i++) {

        if (i != 0 && i % compactInterval == 0) {
          region.compact(true);
        }

        if (i != 0 && i % flushInterval == 0) {
          flushThread.flush();
        }

        boolean previousEmpty = res.isEmpty();
        res.clear();
        InternalScanner scanner = region.getScanner(scan);
        while (NextState.hasMoreValues(scanner.next(res)))
          ;
        if (!res.isEmpty() || !previousEmpty || i > compactInterval) {
          assertEquals("i=" + i, expectedCount, res.size());
          long timestamp = res.get(0).getTimestamp();
          assertTrue("Timestamps were broke: " + timestamp + " prev: " + prevTimestamp,
              timestamp >= prevTimestamp);
          prevTimestamp = timestamp;
        }
      }

      putThread.done();

      region.flush(true);

      putThread.join();
      putThread.checkNoError();

      flushThread.done();
      flushThread.join();
      flushThread.checkNoError();
    } finally {
      try {
        HBaseTestingUtility.closeRegionAndWAL(this.region);
      } catch (DroppedSnapshotException dse) {
        // We could get this on way out because we interrupt the background flusher and it could
        // fail anywhere causing a DSE over in the background flusher... only it is not properly
        // dealt with so could still be memory hanging out when we get to here -- memory we can't
        // flush because the accounting is 'off' since original DSE.
      }
      this.region = null;
    }
  }

  protected class PutThread extends Thread {
    private volatile boolean done;
    private volatile int numPutsFinished = 0;

    private Throwable error = null;
    private int numRows;
    private byte[][] families;
    private byte[][] qualifiers;

    private PutThread(int numRows, byte[][] families, byte[][] qualifiers) {
      this.numRows = numRows;
      this.families = families;
      this.qualifiers = qualifiers;
    }

    /**
     * Block calling thread until this instance of PutThread has put at least one row.
     */
    public void waitForFirstPut() throws InterruptedException {
      // wait until put thread actually puts some data
      while (isAlive() && numPutsFinished == 0) {
        checkNoError();
        Thread.sleep(50);
      }
    }

    public void done() {
      done = true;
      synchronized (this) {
        interrupt();
      }
    }

    public void checkNoError() {
      if (error != null) {
        assertNull(error);
      }
    }

    @Override
    public void run() {
      done = false;
      while (!done) {
        try {
          for (int r = 0; r < numRows; r++) {
            byte[] row = Bytes.toBytes("row" + r);
            Put put = new Put(row);
            put.setDurability(Durability.SKIP_WAL);
            byte[] value = Bytes.toBytes(String.valueOf(numPutsFinished));
            for (byte[] family : families) {
              for (byte[] qualifier : qualifiers) {
                put.add(family, qualifier, (long) numPutsFinished, value);
              }
            }
            region.put(put);
            numPutsFinished++;
            if (numPutsFinished > 0 && numPutsFinished % 47 == 0) {
              System.out.println("put iteration = " + numPutsFinished);
              Delete delete = new Delete(row, (long) numPutsFinished - 30);
              region.delete(delete);
            }
            numPutsFinished++;
          }
        } catch (InterruptedIOException e) {
          // This is fine. It means we are done, or didn't get the lock on time
        } catch (IOException e) {
          LOG.error("error while putting records", e);
          error = e;
          break;
        }
      }

    }

  }

  /**
   * Writes very wide records and gets the latest row every time.. Flushes and
   * compacts the region aggressivly to catch issues.
   *
   * @throws IOException
   *           by flush / scan / compaction
   * @throws InterruptedException
   *           when joining threads
   */
  @Test
  public void testWritesWhileGetting() throws Exception {
    int testCount = 50;
    int numRows = 1;
    int numFamilies = 10;
    int numQualifiers = 100;
    int compactInterval = 100;
    byte[][] families = new byte[numFamilies][];
    for (int i = 0; i < numFamilies; i++) {
      families[i] = Bytes.toBytes("family" + i);
    }
    byte[][] qualifiers = new byte[numQualifiers][];
    for (int i = 0; i < numQualifiers; i++) {
      qualifiers[i] = Bytes.toBytes("qual" + i);
    }


    String method = "testWritesWhileGetting";
    // This test flushes constantly and can cause many files to be created,
    // possibly
    // extending over the ulimit. Make sure compactions are aggressive in
    // reducing
    // the number of HFiles created.
    Configuration conf = HBaseConfiguration.create(CONF);
    conf.setInt("hbase.hstore.compaction.min", 1);
    conf.setInt("hbase.hstore.compaction.max", 1000);
    this.region = initHRegion(tableName, method, conf, families);
    PutThread putThread = null;
    MultithreadedTestUtil.TestContext ctx = new MultithreadedTestUtil.TestContext(conf);
    try {
      putThread = new PutThread(numRows, families, qualifiers);
      putThread.start();
      putThread.waitForFirstPut();

      // Add a thread that flushes as fast as possible
      ctx.addThread(new RepeatingTestThread(ctx) {
        private int flushesSinceCompact = 0;
        private final int maxFlushesSinceCompact = 20;

        @Override
        public void doAnAction() throws Exception {
          if (region.flush(true).isCompactionNeeded()) {
            ++flushesSinceCompact;
          }
          // Compact regularly to avoid creating too many files and exceeding
          // the ulimit.
          if (flushesSinceCompact == maxFlushesSinceCompact) {
            region.compact(false);
            flushesSinceCompact = 0;
          }
        }
      });
      ctx.startThreads();

      Get get = new Get(Bytes.toBytes("row0"));
      Result result = null;

      int expectedCount = numFamilies * numQualifiers;

      long prevTimestamp = 0L;
      for (int i = 0; i < testCount; i++) {
        LOG.info("testWritesWhileGetting verify turn " + i);
        boolean previousEmpty = result == null || result.isEmpty();
        result = region.get(get);
        if (!result.isEmpty() || !previousEmpty || i > compactInterval) {
          assertEquals("i=" + i, expectedCount, result.size());
          // TODO this was removed, now what dangit?!
          // search looking for the qualifier in question?
          long timestamp = 0;
          for (Cell kv : result.rawCells()) {
            if (CellUtil.matchingFamily(kv, families[0])
                && CellUtil.matchingQualifier(kv, qualifiers[0])) {
              timestamp = kv.getTimestamp();
            }
          }
          assertTrue(timestamp >= prevTimestamp);
          prevTimestamp = timestamp;
          Cell previousKV = null;

          for (Cell kv : result.rawCells()) {
            byte[] thisValue = CellUtil.cloneValue(kv);
            if (previousKV != null) {
              if (Bytes.compareTo(CellUtil.cloneValue(previousKV), thisValue) != 0) {
                LOG.warn("These two KV should have the same value." + " Previous KV:" + previousKV
                    + "(memStoreTS:" + previousKV.getMvccVersion() + ")" + ", New KV: " + kv
                    + "(memStoreTS:" + kv.getMvccVersion() + ")");
                assertEquals(0, Bytes.compareTo(CellUtil.cloneValue(previousKV), thisValue));
              }
            }
            previousKV = kv;
          }
        }
      }
    } finally {
      if (putThread != null)
        putThread.done();

      region.flush(true);

      if (putThread != null) {
        putThread.join();
        putThread.checkNoError();
      }

      ctx.stop();
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  @Test
  public void testHolesInMeta() throws Exception {
    byte[] family = Bytes.toBytes("family");
    this.region = initHRegion(tableName, Bytes.toBytes("x"), Bytes.toBytes("z"), method, CONF,
        false, family);
    try {
      byte[] rowNotServed = Bytes.toBytes("a");
      Get g = new Get(rowNotServed);
      try {
        region.get(g);
        fail();
      } catch (WrongRegionException x) {
        // OK
      }
      byte[] row = Bytes.toBytes("y");
      g = new Get(row);
      region.get(g);
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  @Test
  public void testIndexesScanWithOneDeletedRow() throws IOException {
    byte[] family = Bytes.toBytes("family");

    // Setting up region
    String method = "testIndexesScanWithOneDeletedRow";
    this.region = initHRegion(tableName, method, CONF, family);
    try {
      Put put = new Put(Bytes.toBytes(1L));
      put.add(family, qual1, 1L, Bytes.toBytes(1L));
      region.put(put);

      region.flush(true);

      Delete delete = new Delete(Bytes.toBytes(1L), 1L);
      region.delete(delete);

      put = new Put(Bytes.toBytes(2L));
      put.add(family, qual1, 2L, Bytes.toBytes(2L));
      region.put(put);

      Scan idxScan = new Scan();
      idxScan.addFamily(family);
      idxScan.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ALL, Arrays.<Filter> asList(
          new SingleColumnValueFilter(family, qual1, CompareOp.GREATER_OR_EQUAL,
              new BinaryComparator(Bytes.toBytes(0L))), new SingleColumnValueFilter(family, qual1,
              CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes(3L))))));
      InternalScanner scanner = region.getScanner(idxScan);
      List<Cell> res = new ArrayList<Cell>();

      while (NextState.hasMoreValues(scanner.next(res)))
        ;
      assertEquals(1L, res.size());
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  // ////////////////////////////////////////////////////////////////////////////
  // Bloom filter test
  // ////////////////////////////////////////////////////////////////////////////
  @Test
  public void testBloomFilterSize() throws IOException {
    byte[] fam1 = Bytes.toBytes("fam1");
    byte[] qf1 = Bytes.toBytes("col");
    byte[] val1 = Bytes.toBytes("value1");
    // Create Table
    HColumnDescriptor hcd = new HColumnDescriptor(fam1).setMaxVersions(Integer.MAX_VALUE)
        .setBloomFilterType(BloomType.ROWCOL);

    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
    htd.addFamily(hcd);
    HRegionInfo info = new HRegionInfo(htd.getTableName(), null, null, false);
    this.region = TEST_UTIL.createLocalHRegion(info, htd);
    try {
      int num_unique_rows = 10;
      int duplicate_multiplier = 2;
      int num_storefiles = 4;

      int version = 0;
      for (int f = 0; f < num_storefiles; f++) {
        for (int i = 0; i < duplicate_multiplier; i++) {
          for (int j = 0; j < num_unique_rows; j++) {
            Put put = new Put(Bytes.toBytes("row" + j));
            put.setDurability(Durability.SKIP_WAL);
            put.add(fam1, qf1, version++, val1);
            region.put(put);
          }
        }
        region.flush(true);
      }
      // before compaction
      HStore store = (HStore) region.getStore(fam1);
      Collection<StoreFile> storeFiles = store.getStorefiles();
      for (StoreFile storefile : storeFiles) {
        StoreFile.Reader reader = storefile.getReader();
        reader.loadFileInfo();
        reader.loadBloomfilter();
        assertEquals(num_unique_rows * duplicate_multiplier, reader.getEntries());
        assertEquals(num_unique_rows, reader.getFilterEntries());
      }

      region.compact(true);

      // after compaction
      storeFiles = store.getStorefiles();
      for (StoreFile storefile : storeFiles) {
        StoreFile.Reader reader = storefile.getReader();
        reader.loadFileInfo();
        reader.loadBloomfilter();
        assertEquals(num_unique_rows * duplicate_multiplier * num_storefiles, reader.getEntries());
        assertEquals(num_unique_rows, reader.getFilterEntries());
      }
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  @Test
  public void testAllColumnsWithBloomFilter() throws IOException {
    byte[] TABLE = Bytes.toBytes("testAllColumnsWithBloomFilter");
    byte[] FAMILY = Bytes.toBytes("family");

    // Create table
    HColumnDescriptor hcd = new HColumnDescriptor(FAMILY).setMaxVersions(Integer.MAX_VALUE)
        .setBloomFilterType(BloomType.ROWCOL);
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(TABLE));
    htd.addFamily(hcd);
    HRegionInfo info = new HRegionInfo(htd.getTableName(), null, null, false);
    this.region = TEST_UTIL.createLocalHRegion(info, htd);
    try {
      // For row:0, col:0: insert versions 1 through 5.
      byte row[] = Bytes.toBytes("row:" + 0);
      byte column[] = Bytes.toBytes("column:" + 0);
      Put put = new Put(row);
      put.setDurability(Durability.SKIP_WAL);
      for (long idx = 1; idx <= 4; idx++) {
        put.add(FAMILY, column, idx, Bytes.toBytes("value-version-" + idx));
      }
      region.put(put);

      // Flush
      region.flush(true);

      // Get rows
      Get get = new Get(row);
      get.setMaxVersions();
      Cell[] kvs = region.get(get).rawCells();

      // Check if rows are correct
      assertEquals(4, kvs.length);
      checkOneCell(kvs[0], FAMILY, 0, 0, 4);
      checkOneCell(kvs[1], FAMILY, 0, 0, 3);
      checkOneCell(kvs[2], FAMILY, 0, 0, 2);
      checkOneCell(kvs[3], FAMILY, 0, 0, 1);
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  /**
   * Testcase to cover bug-fix for HBASE-2823 Ensures correct delete when
   * issuing delete row on columns with bloom filter set to row+col
   * (BloomType.ROWCOL)
   */
  @Test
  public void testDeleteRowWithBloomFilter() throws IOException {
    byte[] familyName = Bytes.toBytes("familyName");

    // Create Table
    HColumnDescriptor hcd = new HColumnDescriptor(familyName).setMaxVersions(Integer.MAX_VALUE)
        .setBloomFilterType(BloomType.ROWCOL);

    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
    htd.addFamily(hcd);
    HRegionInfo info = new HRegionInfo(htd.getTableName(), null, null, false);
    this.region = TEST_UTIL.createLocalHRegion(info, htd);
    try {
      // Insert some data
      byte row[] = Bytes.toBytes("row1");
      byte col[] = Bytes.toBytes("col1");

      Put put = new Put(row);
      put.add(familyName, col, 1, Bytes.toBytes("SomeRandomValue"));
      region.put(put);
      region.flush(true);

      Delete del = new Delete(row);
      region.delete(del);
      region.flush(true);

      // Get remaining rows (should have none)
      Get get = new Get(row);
      get.addColumn(familyName, col);

      Cell[] keyValues = region.get(get).rawCells();
      assertTrue(keyValues.length == 0);
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  @Test
  public void testgetHDFSBlocksDistribution() throws Exception {
    HBaseTestingUtility htu = new HBaseTestingUtility();
    // Why do we set the block size in this test?  If we set it smaller than the kvs, then we'll
    // break up the file in to more pieces that can be distributed across the three nodes and we
    // won't be able to have the condition this test asserts; that at least one node has
    // a copy of all replicas -- if small block size, then blocks are spread evenly across the
    // the three nodes.  hfilev3 with tags seems to put us over the block size.  St.Ack.
    // final int DEFAULT_BLOCK_SIZE = 1024;
    // htu.getConfiguration().setLong("dfs.blocksize", DEFAULT_BLOCK_SIZE);
    htu.getConfiguration().setInt("dfs.replication", 2);

    // set up a cluster with 3 nodes
    MiniHBaseCluster cluster = null;
    String dataNodeHosts[] = new String[] { "host1", "host2", "host3" };
    int regionServersCount = 3;

    try {
      cluster = htu.startMiniCluster(1, regionServersCount, dataNodeHosts);
      byte[][] families = { fam1, fam2 };
      Table ht = htu.createTable(Bytes.toBytes(this.getName()), families);

      // Setting up region
      byte row[] = Bytes.toBytes("row1");
      byte col[] = Bytes.toBytes("col1");

      Put put = new Put(row);
      put.add(fam1, col, 1, Bytes.toBytes("test1"));
      put.add(fam2, col, 1, Bytes.toBytes("test2"));
      ht.put(put);

      HRegion firstRegion = htu.getHBaseCluster().getRegions(TableName.valueOf(this.getName()))
          .get(0);
      firstRegion.flush(true);
      HDFSBlocksDistribution blocksDistribution1 = firstRegion.getHDFSBlocksDistribution();

      // Given the default replication factor is 2 and we have 2 HFiles,
      // we will have total of 4 replica of blocks on 3 datanodes; thus there
      // must be at least one host that have replica for 2 HFiles. That host's
      // weight will be equal to the unique block weight.
      long uniqueBlocksWeight1 = blocksDistribution1.getUniqueBlocksTotalWeight();
      StringBuilder sb = new StringBuilder();
      for (String host: blocksDistribution1.getTopHosts()) {
        if (sb.length() > 0) sb.append(", ");
        sb.append(host);
        sb.append("=");
        sb.append(blocksDistribution1.getWeight(host));
      }

      String topHost = blocksDistribution1.getTopHosts().get(0);
      long topHostWeight = blocksDistribution1.getWeight(topHost);
      String msg = "uniqueBlocksWeight=" + uniqueBlocksWeight1 + ", topHostWeight=" +
        topHostWeight + ", topHost=" + topHost + "; " + sb.toString();
      LOG.info(msg);
      assertTrue(msg, uniqueBlocksWeight1 == topHostWeight);

      // use the static method to compute the value, it should be the same.
      // static method is used by load balancer or other components
      HDFSBlocksDistribution blocksDistribution2 = HRegion.computeHDFSBlocksDistribution(
          htu.getConfiguration(), firstRegion.getTableDesc(), firstRegion.getRegionInfo());
      long uniqueBlocksWeight2 = blocksDistribution2.getUniqueBlocksTotalWeight();

      assertTrue(uniqueBlocksWeight1 == uniqueBlocksWeight2);

      ht.close();
    } finally {
      if (cluster != null) {
        htu.shutdownMiniCluster();
      }
    }
  }

  /**
   * Testcase to check state of region initialization task set to ABORTED or not
   * if any exceptions during initialization
   *
   * @throws Exception
   */
  @Test
  public void testStatusSettingToAbortIfAnyExceptionDuringRegionInitilization() throws Exception {
    TableName tableName = TableName.valueOf(name.getMethodName());
    HRegionInfo info = null;
    try {
      FileSystem fs = Mockito.mock(FileSystem.class);
      Mockito.when(fs.exists((Path) Mockito.anyObject())).thenThrow(new IOException());
      HTableDescriptor htd = new HTableDescriptor(tableName);
      htd.addFamily(new HColumnDescriptor("cf"));
      info = new HRegionInfo(htd.getTableName(), HConstants.EMPTY_BYTE_ARRAY,
          HConstants.EMPTY_BYTE_ARRAY, false);
      Path path = new Path(dir + "testStatusSettingToAbortIfAnyExceptionDuringRegionInitilization");
      region = HRegion.newHRegion(path, null, fs, CONF, info, htd, null);
      // region initialization throws IOException and set task state to ABORTED.
      region.initialize();
      fail("Region initialization should fail due to IOException");
    } catch (IOException io) {
      List<MonitoredTask> tasks = TaskMonitor.get().getTasks();
      for (MonitoredTask monitoredTask : tasks) {
        if (!(monitoredTask instanceof MonitoredRPCHandler)
            && monitoredTask.getDescription().contains(region.toString())) {
          assertTrue("Region state should be ABORTED.",
              monitoredTask.getState().equals(MonitoredTask.State.ABORTED));
          break;
        }
      }
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(region);
    }
  }

  /**
   * Verifies that the .regioninfo file is written on region creation and that
   * is recreated if missing during region opening.
   */
  @Test
  public void testRegionInfoFileCreation() throws IOException {
    Path rootDir = new Path(dir + "testRegionInfoFileCreation");

    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("testtb"));
    htd.addFamily(new HColumnDescriptor("cf"));

    HRegionInfo hri = new HRegionInfo(htd.getTableName());

    // Create a region and skip the initialization (like CreateTableHandler)
    HRegion region = HBaseTestingUtility.createRegionAndWAL(hri, rootDir, CONF, htd, false);
    Path regionDir = region.getRegionFileSystem().getRegionDir();
    FileSystem fs = region.getRegionFileSystem().getFileSystem();
    HBaseTestingUtility.closeRegionAndWAL(region);

    Path regionInfoFile = new Path(regionDir, HRegionFileSystem.REGION_INFO_FILE);

    // Verify that the .regioninfo file is present
    assertTrue(HRegionFileSystem.REGION_INFO_FILE + " should be present in the region dir",
        fs.exists(regionInfoFile));

    // Try to open the region
    region = HRegion.openHRegion(rootDir, hri, htd, null, CONF);
    assertEquals(regionDir, region.getRegionFileSystem().getRegionDir());
    HBaseTestingUtility.closeRegionAndWAL(region);

    // Verify that the .regioninfo file is still there
    assertTrue(HRegionFileSystem.REGION_INFO_FILE + " should be present in the region dir",
        fs.exists(regionInfoFile));

    // Remove the .regioninfo file and verify is recreated on region open
    fs.delete(regionInfoFile, true);
    assertFalse(HRegionFileSystem.REGION_INFO_FILE + " should be removed from the region dir",
        fs.exists(regionInfoFile));

    region = HRegion.openHRegion(rootDir, hri, htd, null, CONF);
//    region = TEST_UTIL.openHRegion(hri, htd);
    assertEquals(regionDir, region.getRegionFileSystem().getRegionDir());
    HBaseTestingUtility.closeRegionAndWAL(region);

    // Verify that the .regioninfo file is still there
    assertTrue(HRegionFileSystem.REGION_INFO_FILE + " should be present in the region dir",
        fs.exists(new Path(regionDir, HRegionFileSystem.REGION_INFO_FILE)));
  }

  /**
   * TestCase for increment
   */
  private static class Incrementer implements Runnable {
    private HRegion region;
    private final static byte[] incRow = Bytes.toBytes("incRow");
    private final static byte[] family = Bytes.toBytes("family");
    private final static byte[] qualifier = Bytes.toBytes("qualifier");
    private final static long ONE = 1l;
    private int incCounter;

    public Incrementer(HRegion region, int incCounter) {
      this.region = region;
      this.incCounter = incCounter;
    }

    @Override
    public void run() {
      int count = 0;
      while (count < incCounter) {
        Increment inc = new Increment(incRow);
        inc.addColumn(family, qualifier, ONE);
        count++;
        try {
          region.increment(inc);
        } catch (IOException e) {
          LOG.info("Count=" + count + ", " + e);
          break;
        }
      }
    }
  }

  /**
   * Test case to check increment function with memstore flushing
   * @throws Exception
   */
  @Test
  public void testParallelIncrementWithMemStoreFlush() throws Exception {
    byte[] family = Incrementer.family;
    this.region = initHRegion(tableName, method, CONF, family);
    final HRegion region = this.region;
    final AtomicBoolean incrementDone = new AtomicBoolean(false);
    Runnable flusher = new Runnable() {
      @Override
      public void run() {
        while (!incrementDone.get()) {
          try {
            region.flush(true);
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }
    };

    // after all increment finished, the row will increment to 20*100 = 2000
    int threadNum = 20;
    int incCounter = 100;
    long expected = threadNum * incCounter;
    Thread[] incrementers = new Thread[threadNum];
    Thread flushThread = new Thread(flusher);
    for (int i = 0; i < threadNum; i++) {
      incrementers[i] = new Thread(new Incrementer(this.region, incCounter));
      incrementers[i].start();
    }
    flushThread.start();
    for (int i = 0; i < threadNum; i++) {
      incrementers[i].join();
    }

    incrementDone.set(true);
    flushThread.join();

    Get get = new Get(Incrementer.incRow);
    get.addColumn(Incrementer.family, Incrementer.qualifier);
    get.setMaxVersions(1);
    Result res = this.region.get(get);
    List<Cell> kvs = res.getColumnCells(Incrementer.family, Incrementer.qualifier);

    // we just got the latest version
    assertEquals(kvs.size(), 1);
    Cell kv = kvs.get(0);
    assertEquals(expected, Bytes.toLong(kv.getValueArray(), kv.getValueOffset()));
    this.region = null;
  }

  /**
   * TestCase for append
   */
  private static class Appender implements Runnable {
    private HRegion region;
    private final static byte[] appendRow = Bytes.toBytes("appendRow");
    private final static byte[] family = Bytes.toBytes("family");
    private final static byte[] qualifier = Bytes.toBytes("qualifier");
    private final static byte[] CHAR = Bytes.toBytes("a");
    private int appendCounter;

    public Appender(HRegion region, int appendCounter) {
      this.region = region;
      this.appendCounter = appendCounter;
    }

    @Override
    public void run() {
      int count = 0;
      while (count < appendCounter) {
        Append app = new Append(appendRow);
        app.add(family, qualifier, CHAR);
        count++;
        try {
          region.append(app);
        } catch (IOException e) {
          LOG.info("Count=" + count + ", max=" + appendCounter + ", " + e);
          break;
        }
      }
    }
  }

  /**
   * Test case to check append function with memstore flushing
   * @throws Exception
   */
  @Test
  public void testParallelAppendWithMemStoreFlush() throws Exception {
    byte[] family = Appender.family;
    this.region = initHRegion(tableName, method, CONF, family);
    final HRegion region = this.region;
    final AtomicBoolean appendDone = new AtomicBoolean(false);
    Runnable flusher = new Runnable() {
      @Override
      public void run() {
        while (!appendDone.get()) {
          try {
            region.flush(true);
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }
    };

    // After all append finished, the value will append to threadNum *
    // appendCounter Appender.CHAR
    int threadNum = 20;
    int appendCounter = 100;
    byte[] expected = new byte[threadNum * appendCounter];
    for (int i = 0; i < threadNum * appendCounter; i++) {
      System.arraycopy(Appender.CHAR, 0, expected, i, 1);
    }
    Thread[] appenders = new Thread[threadNum];
    Thread flushThread = new Thread(flusher);
    for (int i = 0; i < threadNum; i++) {
      appenders[i] = new Thread(new Appender(this.region, appendCounter));
      appenders[i].start();
    }
    flushThread.start();
    for (int i = 0; i < threadNum; i++) {
      appenders[i].join();
    }

    appendDone.set(true);
    flushThread.join();

    Get get = new Get(Appender.appendRow);
    get.addColumn(Appender.family, Appender.qualifier);
    get.setMaxVersions(1);
    Result res = this.region.get(get);
    List<Cell> kvs = res.getColumnCells(Appender.family, Appender.qualifier);

    // we just got the latest version
    assertEquals(kvs.size(), 1);
    Cell kv = kvs.get(0);
    byte[] appendResult = new byte[kv.getValueLength()];
    System.arraycopy(kv.getValueArray(), kv.getValueOffset(), appendResult, 0, kv.getValueLength());
    assertArrayEquals(expected, appendResult);
    this.region = null;
  }

  /**
   * Test case to check put function with memstore flushing for same row, same ts
   * @throws Exception
   */
  @Test
  public void testPutWithMemStoreFlush() throws Exception {
    byte[] family = Bytes.toBytes("family");
    ;
    byte[] qualifier = Bytes.toBytes("qualifier");
    byte[] row = Bytes.toBytes("putRow");
    byte[] value = null;
    this.region = initHRegion(tableName, method, CONF, family);
    Put put = null;
    Get get = null;
    List<Cell> kvs = null;
    Result res = null;

    put = new Put(row);
    value = Bytes.toBytes("value0");
    put.add(family, qualifier, 1234567l, value);
    region.put(put);
    get = new Get(row);
    get.addColumn(family, qualifier);
    get.setMaxVersions();
    res = this.region.get(get);
    kvs = res.getColumnCells(family, qualifier);
    assertEquals(1, kvs.size());
    assertArrayEquals(Bytes.toBytes("value0"), CellUtil.cloneValue(kvs.get(0)));

    region.flush(true);
    get = new Get(row);
    get.addColumn(family, qualifier);
    get.setMaxVersions();
    res = this.region.get(get);
    kvs = res.getColumnCells(family, qualifier);
    assertEquals(1, kvs.size());
    assertArrayEquals(Bytes.toBytes("value0"), CellUtil.cloneValue(kvs.get(0)));

    put = new Put(row);
    value = Bytes.toBytes("value1");
    put.add(family, qualifier, 1234567l, value);
    region.put(put);
    get = new Get(row);
    get.addColumn(family, qualifier);
    get.setMaxVersions();
    res = this.region.get(get);
    kvs = res.getColumnCells(family, qualifier);
    assertEquals(1, kvs.size());
    assertArrayEquals(Bytes.toBytes("value1"), CellUtil.cloneValue(kvs.get(0)));

    region.flush(true);
    get = new Get(row);
    get.addColumn(family, qualifier);
    get.setMaxVersions();
    res = this.region.get(get);
    kvs = res.getColumnCells(family, qualifier);
    assertEquals(1, kvs.size());
    assertArrayEquals(Bytes.toBytes("value1"), CellUtil.cloneValue(kvs.get(0)));
  }

  @Test
  public void testDurability() throws Exception {
    String method = "testDurability";
    // there are 5 x 5 cases:
    // table durability(SYNC,FSYNC,ASYC,SKIP,USE_DEFAULT) x mutation
    // durability(SYNC,FSYNC,ASYC,SKIP,USE_DEFAULT)

    // expected cases for append and sync wal
    durabilityTest(method, Durability.SYNC_WAL, Durability.SYNC_WAL, 0, true, true, false);
    durabilityTest(method, Durability.SYNC_WAL, Durability.FSYNC_WAL, 0, true, true, false);
    durabilityTest(method, Durability.SYNC_WAL, Durability.USE_DEFAULT, 0, true, true, false);

    durabilityTest(method, Durability.FSYNC_WAL, Durability.SYNC_WAL, 0, true, true, false);
    durabilityTest(method, Durability.FSYNC_WAL, Durability.FSYNC_WAL, 0, true, true, false);
    durabilityTest(method, Durability.FSYNC_WAL, Durability.USE_DEFAULT, 0, true, true, false);

    durabilityTest(method, Durability.ASYNC_WAL, Durability.SYNC_WAL, 0, true, true, false);
    durabilityTest(method, Durability.ASYNC_WAL, Durability.FSYNC_WAL, 0, true, true, false);

    durabilityTest(method, Durability.SKIP_WAL, Durability.SYNC_WAL, 0, true, true, false);
    durabilityTest(method, Durability.SKIP_WAL, Durability.FSYNC_WAL, 0, true, true, false);

    durabilityTest(method, Durability.USE_DEFAULT, Durability.SYNC_WAL, 0, true, true, false);
    durabilityTest(method, Durability.USE_DEFAULT, Durability.FSYNC_WAL, 0, true, true, false);
    durabilityTest(method, Durability.USE_DEFAULT, Durability.USE_DEFAULT, 0, true, true, false);

    // expected cases for async wal
    durabilityTest(method, Durability.SYNC_WAL, Durability.ASYNC_WAL, 0, true, false, false);
    durabilityTest(method, Durability.FSYNC_WAL, Durability.ASYNC_WAL, 0, true, false, false);
    durabilityTest(method, Durability.ASYNC_WAL, Durability.ASYNC_WAL, 0, true, false, false);
    durabilityTest(method, Durability.SKIP_WAL, Durability.ASYNC_WAL, 0, true, false, false);
    durabilityTest(method, Durability.USE_DEFAULT, Durability.ASYNC_WAL, 0, true, false, false);
    durabilityTest(method, Durability.ASYNC_WAL, Durability.USE_DEFAULT, 0, true, false, false);

    durabilityTest(method, Durability.SYNC_WAL, Durability.ASYNC_WAL, 5000, true, false, true);
    durabilityTest(method, Durability.FSYNC_WAL, Durability.ASYNC_WAL, 5000, true, false, true);
    durabilityTest(method, Durability.ASYNC_WAL, Durability.ASYNC_WAL, 5000, true, false, true);
    durabilityTest(method, Durability.SKIP_WAL, Durability.ASYNC_WAL, 5000, true, false, true);
    durabilityTest(method, Durability.USE_DEFAULT, Durability.ASYNC_WAL, 5000, true, false, true);
    durabilityTest(method, Durability.ASYNC_WAL, Durability.USE_DEFAULT, 5000, true, false, true);

    // expect skip wal cases
    durabilityTest(method, Durability.SYNC_WAL, Durability.SKIP_WAL, 0, true, false, false);
    durabilityTest(method, Durability.FSYNC_WAL, Durability.SKIP_WAL, 0, true, false, false);
    durabilityTest(method, Durability.ASYNC_WAL, Durability.SKIP_WAL, 0, true, false, false);
    durabilityTest(method, Durability.SKIP_WAL, Durability.SKIP_WAL, 0, true, false, false);
    durabilityTest(method, Durability.USE_DEFAULT, Durability.SKIP_WAL, 0, true, false, false);
    durabilityTest(method, Durability.SKIP_WAL, Durability.USE_DEFAULT, 0, true, false, false);

  }

  @SuppressWarnings("unchecked")
  private void durabilityTest(String method, Durability tableDurability,
      Durability mutationDurability, long timeout, boolean expectAppend, final boolean expectSync,
      final boolean expectSyncFromLogSyncer) throws Exception {
    Configuration conf = HBaseConfiguration.create(CONF);
    method = method + "_" + tableDurability.name() + "_" + mutationDurability.name();
    TableName tableName = TableName.valueOf(method);
    byte[] family = Bytes.toBytes("family");
    Path logDir = new Path(new Path(dir + method), "log");
    final Configuration walConf = new Configuration(conf);
    FSUtils.setRootDir(walConf, logDir);
    final WALFactory wals = new WALFactory(walConf, null, UUID.randomUUID().toString());
    final WAL wal = spy(wals.getWAL(tableName.getName()));
    this.region = initHRegion(tableName.getName(), HConstants.EMPTY_START_ROW,
        HConstants.EMPTY_END_ROW, method, conf, false, tableDurability, wal,
        new byte[][] { family });

    Put put = new Put(Bytes.toBytes("r1"));
    put.add(family, Bytes.toBytes("q1"), Bytes.toBytes("v1"));
    put.setDurability(mutationDurability);
    region.put(put);

    //verify append called or not
    verify(wal, expectAppend ? times(1) : never())
      .append((HTableDescriptor)any(), (HRegionInfo)any(), (WALKey)any(),
          (WALEdit)any(), (AtomicLong)any(), Mockito.anyBoolean(), (List<Cell>)any());

    // verify sync called or not
    if (expectSync || expectSyncFromLogSyncer) {
      TEST_UTIL.waitFor(timeout, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          try {
            if (expectSync) {
              verify(wal, times(1)).sync(anyLong()); // Hregion calls this one
            } else if (expectSyncFromLogSyncer) {
              verify(wal, times(1)).sync(); // wal syncer calls this one
            }
          } catch (Throwable ignore) {
          }
          return true;
        }
      });
    } else {
      //verify(wal, never()).sync(anyLong());
      verify(wal, never()).sync();
    }

    HBaseTestingUtility.closeRegionAndWAL(this.region);
    this.region = null;
  }

  @Test
  public void testRegionReplicaSecondary() throws IOException {
    // create a primary region, load some data and flush
    // create a secondary region, and do a get against that
    Path rootDir = new Path(dir + "testRegionReplicaSecondary");
    FSUtils.setRootDir(TEST_UTIL.getConfiguration(), rootDir);

    byte[][] families = new byte[][] {
        Bytes.toBytes("cf1"), Bytes.toBytes("cf2"), Bytes.toBytes("cf3")
    };
    byte[] cq = Bytes.toBytes("cq");
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("testRegionReplicaSecondary"));
    for (byte[] family : families) {
      htd.addFamily(new HColumnDescriptor(family));
    }

    long time = System.currentTimeMillis();
    HRegionInfo primaryHri = new HRegionInfo(htd.getTableName(),
      HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW,
      false, time, 0);
    HRegionInfo secondaryHri = new HRegionInfo(htd.getTableName(),
      HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW,
      false, time, 1);

    HRegion primaryRegion = null, secondaryRegion = null;

    try {
      primaryRegion = HBaseTestingUtility.createRegionAndWAL(primaryHri,
          rootDir, TEST_UTIL.getConfiguration(), htd);

      // load some data
      putData(primaryRegion, 0, 1000, cq, families);

      // flush region
      primaryRegion.flush(true);

      // open secondary region
      secondaryRegion = HRegion.openHRegion(rootDir, secondaryHri, htd, null, CONF);

      verifyData(secondaryRegion, 0, 1000, cq, families);
    } finally {
      if (primaryRegion != null) {
        HBaseTestingUtility.closeRegionAndWAL(primaryRegion);
      }
      if (secondaryRegion != null) {
        HBaseTestingUtility.closeRegionAndWAL(secondaryRegion);
      }
    }
  }

  @Test
  public void testRegionReplicaSecondaryIsReadOnly() throws IOException {
    // create a primary region, load some data and flush
    // create a secondary region, and do a put against that
    Path rootDir = new Path(dir + "testRegionReplicaSecondary");
    FSUtils.setRootDir(TEST_UTIL.getConfiguration(), rootDir);

    byte[][] families = new byte[][] {
        Bytes.toBytes("cf1"), Bytes.toBytes("cf2"), Bytes.toBytes("cf3")
    };
    byte[] cq = Bytes.toBytes("cq");
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("testRegionReplicaSecondary"));
    for (byte[] family : families) {
      htd.addFamily(new HColumnDescriptor(family));
    }

    long time = System.currentTimeMillis();
    HRegionInfo primaryHri = new HRegionInfo(htd.getTableName(),
      HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW,
      false, time, 0);
    HRegionInfo secondaryHri = new HRegionInfo(htd.getTableName(),
      HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW,
      false, time, 1);

    HRegion primaryRegion = null, secondaryRegion = null;

    try {
      primaryRegion = HBaseTestingUtility.createRegionAndWAL(primaryHri,
          rootDir, TEST_UTIL.getConfiguration(), htd);

      // load some data
      putData(primaryRegion, 0, 1000, cq, families);

      // flush region
      primaryRegion.flush(true);

      // open secondary region
      secondaryRegion = HRegion.openHRegion(rootDir, secondaryHri, htd, null, CONF);

      try {
        putData(secondaryRegion, 0, 1000, cq, families);
        fail("Should have thrown exception");
      } catch (IOException ex) {
        // expected
      }
    } finally {
      if (primaryRegion != null) {
        HBaseTestingUtility.closeRegionAndWAL(primaryRegion);
      }
      if (secondaryRegion != null) {
        HBaseTestingUtility.closeRegionAndWAL(secondaryRegion);
      }
    }
  }

  static WALFactory createWALFactory(Configuration conf, Path rootDir) throws IOException {
    Configuration confForWAL = new Configuration(conf);
    confForWAL.set(HConstants.HBASE_DIR, rootDir.toString());
    return new WALFactory(confForWAL,
        Collections.<WALActionsListener>singletonList(new MetricsWAL()),
        "hregion-" + RandomStringUtils.randomNumeric(8));
  }

  @Test
  public void testCompactionFromPrimary() throws IOException {
    Path rootDir = new Path(dir + "testRegionReplicaSecondary");
    FSUtils.setRootDir(TEST_UTIL.getConfiguration(), rootDir);

    byte[][] families = new byte[][] {
        Bytes.toBytes("cf1"), Bytes.toBytes("cf2"), Bytes.toBytes("cf3")
    };
    byte[] cq = Bytes.toBytes("cq");
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("testRegionReplicaSecondary"));
    for (byte[] family : families) {
      htd.addFamily(new HColumnDescriptor(family));
    }

    long time = System.currentTimeMillis();
    HRegionInfo primaryHri = new HRegionInfo(htd.getTableName(),
      HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW,
      false, time, 0);
    HRegionInfo secondaryHri = new HRegionInfo(htd.getTableName(),
      HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW,
      false, time, 1);

    HRegion primaryRegion = null, secondaryRegion = null;

    try {
      primaryRegion = HBaseTestingUtility.createRegionAndWAL(primaryHri,
          rootDir, TEST_UTIL.getConfiguration(), htd);

      // load some data
      putData(primaryRegion, 0, 1000, cq, families);

      // flush region
      primaryRegion.flush(true);

      // open secondary region
      secondaryRegion = HRegion.openHRegion(rootDir, secondaryHri, htd, null, CONF);

      // move the file of the primary region to the archive, simulating a compaction
      Collection<StoreFile> storeFiles = primaryRegion.getStore(families[0]).getStorefiles();
      primaryRegion.getRegionFileSystem().removeStoreFiles(Bytes.toString(families[0]), storeFiles);
      Collection<StoreFileInfo> storeFileInfos = primaryRegion.getRegionFileSystem().getStoreFiles(families[0]);
      Assert.assertTrue(storeFileInfos == null || storeFileInfos.size() == 0);

      verifyData(secondaryRegion, 0, 1000, cq, families);
    } finally {
      if (primaryRegion != null) {
        HBaseTestingUtility.closeRegionAndWAL(primaryRegion);
      }
      if (secondaryRegion != null) {
        HBaseTestingUtility.closeRegionAndWAL(secondaryRegion);
      }
    }
  }

  private void putData(int startRow, int numRows, byte[] qf, byte[]... families) throws IOException {
    putData(this.region, startRow, numRows, qf, families);
  }

  private void putData(HRegion region,
      int startRow, int numRows, byte[] qf, byte[]... families) throws IOException {
    putData(region, Durability.SKIP_WAL, startRow, numRows, qf, families);
  }

  static void putData(HRegion region, Durability durability,
      int startRow, int numRows, byte[] qf, byte[]... families) throws IOException {
    for (int i = startRow; i < startRow + numRows; i++) {
      Put put = new Put(Bytes.toBytes("" + i));
      put.setDurability(durability);
      for (byte[] family : families) {
        put.add(family, qf, null);
      }
      region.put(put);
    }
  }

  static void verifyData(HRegion newReg, int startRow, int numRows, byte[] qf, byte[]... families)
      throws IOException {
    for (int i = startRow; i < startRow + numRows; i++) {
      byte[] row = Bytes.toBytes("" + i);
      Get get = new Get(row);
      for (byte[] family : families) {
        get.addColumn(family, qf);
      }
      Result result = newReg.get(get);
      Cell[] raw = result.rawCells();
      assertEquals(families.length, result.size());
      for (int j = 0; j < families.length; j++) {
        assertTrue(CellUtil.matchingRow(raw[j], row));
        assertTrue(CellUtil.matchingFamily(raw[j], families[j]));
        assertTrue(CellUtil.matchingQualifier(raw[j], qf));
      }
    }
  }

  static void assertGet(final HRegion r, final byte[] family, final byte[] k) throws IOException {
    // Now I have k, get values out and assert they are as expected.
    Get get = new Get(k).addFamily(family).setMaxVersions();
    Cell[] results = r.get(get).rawCells();
    for (int j = 0; j < results.length; j++) {
      byte[] tmp = CellUtil.cloneValue(results[j]);
      // Row should be equal to value every time.
      assertTrue(Bytes.equals(k, tmp));
    }
  }

  /*
   * Assert first value in the passed region is <code>firstValue</code>.
   *
   * @param r
   *
   * @param fs
   *
   * @param firstValue
   *
   * @throws IOException
   */
  private void assertScan(final HRegion r, final byte[] fs, final byte[] firstValue)
      throws IOException {
    byte[][] families = { fs };
    Scan scan = new Scan();
    for (int i = 0; i < families.length; i++)
      scan.addFamily(families[i]);
    InternalScanner s = r.getScanner(scan);
    try {
      List<Cell> curVals = new ArrayList<Cell>();
      boolean first = true;
      OUTER_LOOP: while (NextState.hasMoreValues(s.next(curVals))) {
        for (Cell kv : curVals) {
          byte[] val = CellUtil.cloneValue(kv);
          byte[] curval = val;
          if (first) {
            first = false;
            assertTrue(Bytes.compareTo(curval, firstValue) == 0);
          } else {
            // Not asserting anything. Might as well break.
            break OUTER_LOOP;
          }
        }
      }
    } finally {
      s.close();
    }
  }

  /**
   * Test that we get the expected flush results back
   * @throws IOException
   */
  @Test
  public void testFlushResult() throws IOException {
    String method = name.getMethodName();
    byte[] tableName = Bytes.toBytes(method);
    byte[] family = Bytes.toBytes("family");

    this.region = initHRegion(tableName, method, family);

    // empty memstore, flush doesn't run
    HRegion.FlushResult fr = region.flush(true);
    assertFalse(fr.isFlushSucceeded());
    assertFalse(fr.isCompactionNeeded());

    // Flush enough files to get up to the threshold, doesn't need compactions
    for (int i = 0; i < 2; i++) {
      Put put = new Put(tableName).add(family, family, tableName);
      region.put(put);
      fr = region.flush(true);
      assertTrue(fr.isFlushSucceeded());
      assertFalse(fr.isCompactionNeeded());
    }

    // Two flushes after the threshold, compactions are needed
    for (int i = 0; i < 2; i++) {
      Put put = new Put(tableName).add(family, family, tableName);
      region.put(put);
      fr = region.flush(true);
      assertTrue(fr.isFlushSucceeded());
      assertTrue(fr.isCompactionNeeded());
    }
  }

  private Configuration initSplit() {
    // Always compact if there is more than one store file.
    CONF.setInt("hbase.hstore.compactionThreshold", 2);

    // Make lease timeout longer, lease checks less frequent
    CONF.setInt("hbase.master.lease.thread.wakefrequency", 5 * 1000);

    CONF.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 10 * 1000);

    // Increase the amount of time between client retries
    CONF.setLong("hbase.client.pause", 15 * 1000);

    // This size should make it so we always split using the addContent
    // below. After adding all data, the first region is 1.3M
    CONF.setLong(HConstants.HREGION_MAX_FILESIZE, 1024 * 128);
    return CONF;
  }

  /**
   * @param tableName
   * @param callingMethod
   * @param conf
   * @param families
   * @throws IOException
   * @return A region on which you must call
   *         {@link HBaseTestingUtility#closeRegionAndWAL(HRegion)} when done.
   */
  private static HRegion initHRegion(TableName tableName, String callingMethod, Configuration conf,
      byte[]... families) throws IOException {
    return initHRegion(tableName.getName(), null, null, callingMethod, conf, false, families);
  }

  /**
   * @param tableName
   * @param callingMethod
   * @param conf
   * @param families
   * @throws IOException
   * @return A region on which you must call
   *         {@link HBaseTestingUtility#closeRegionAndWAL(HRegion)} when done.
   */
  private static HRegion initHRegion(byte[] tableName, String callingMethod, Configuration conf,
      byte[]... families) throws IOException {
    return initHRegion(tableName, null, null, callingMethod, conf, false, families);
  }

  /**
   * @param tableName
   * @param callingMethod
   * @param conf
   * @param isReadOnly
   * @param families
   * @throws IOException
   * @return A region on which you must call
   *         {@link HBaseTestingUtility#closeRegionAndWAL(HRegion)} when done.
   */
  private static HRegion initHRegion(byte[] tableName, String callingMethod, Configuration conf,
      boolean isReadOnly, byte[]... families) throws IOException {
    return initHRegion(tableName, null, null, callingMethod, conf, isReadOnly, families);
  }

  public static HRegion initHRegion(byte[] tableName, byte[] startKey, byte[] stopKey,
      String callingMethod, Configuration conf, boolean isReadOnly, byte[]... families)
      throws IOException {
    Path logDir = TEST_UTIL.getDataTestDirOnTestFS(callingMethod + ".log");
    HRegionInfo hri = new HRegionInfo(TableName.valueOf(tableName), startKey, stopKey);
    final WAL wal = HBaseTestingUtility.createWal(conf, logDir, hri);
    return initHRegion(tableName, startKey, stopKey, callingMethod, conf, isReadOnly,
        Durability.SYNC_WAL, wal, families);
  }

  /**
   * @param tableName
   * @param startKey
   * @param stopKey
   * @param callingMethod
   * @param conf
   * @param isReadOnly
   * @param families
   * @throws IOException
   * @return A region on which you must call
   *         {@link HBaseTestingUtility#closeRegionAndWAL(HRegion)} when done.
   */
  public static HRegion initHRegion(byte[] tableName, byte[] startKey, byte[] stopKey,
      String callingMethod, Configuration conf, boolean isReadOnly, Durability durability,
      WAL wal, byte[]... families) throws IOException {
    return TEST_UTIL.createLocalHRegion(tableName, startKey, stopKey, callingMethod, conf,
        isReadOnly, durability, wal, families);
  }

  /**
   * Assert that the passed in Cell has expected contents for the specified row,
   * column & timestamp.
   */
  private void checkOneCell(Cell kv, byte[] cf, int rowIdx, int colIdx, long ts) {
    String ctx = "rowIdx=" + rowIdx + "; colIdx=" + colIdx + "; ts=" + ts;
    assertEquals("Row mismatch which checking: " + ctx, "row:" + rowIdx,
        Bytes.toString(CellUtil.cloneRow(kv)));
    assertEquals("ColumnFamily mismatch while checking: " + ctx, Bytes.toString(cf),
        Bytes.toString(CellUtil.cloneFamily(kv)));
    assertEquals("Column qualifier mismatch while checking: " + ctx, "column:" + colIdx,
        Bytes.toString(CellUtil.cloneQualifier(kv)));
    assertEquals("Timestamp mismatch while checking: " + ctx, ts, kv.getTimestamp());
    assertEquals("Value mismatch while checking: " + ctx, "value-version-" + ts,
        Bytes.toString(CellUtil.cloneValue(kv)));
  }

  @Test (timeout=60000)
  public void testReverseScanner_FromMemStore_SingleCF_Normal()
      throws IOException {
    byte[] rowC = Bytes.toBytes("rowC");
    byte[] rowA = Bytes.toBytes("rowA");
    byte[] rowB = Bytes.toBytes("rowB");
    byte[] cf = Bytes.toBytes("CF");
    byte[][] families = { cf };
    byte[] col = Bytes.toBytes("C");
    long ts = 1;
    String method = this.getName();
    this.region = initHRegion(tableName, method, families);
    try {
      KeyValue kv1 = new KeyValue(rowC, cf, col, ts, KeyValue.Type.Put, null);
      KeyValue kv11 = new KeyValue(rowC, cf, col, ts + 1, KeyValue.Type.Put,
          null);
      KeyValue kv2 = new KeyValue(rowA, cf, col, ts, KeyValue.Type.Put, null);
      KeyValue kv3 = new KeyValue(rowB, cf, col, ts, KeyValue.Type.Put, null);
      Put put = null;
      put = new Put(rowC);
      put.add(kv1);
      put.add(kv11);
      region.put(put);
      put = new Put(rowA);
      put.add(kv2);
      region.put(put);
      put = new Put(rowB);
      put.add(kv3);
      region.put(put);

      Scan scan = new Scan(rowC);
      scan.setMaxVersions(5);
      scan.setReversed(true);
      InternalScanner scanner = region.getScanner(scan);
      List<Cell> currRow = new ArrayList<Cell>();
      boolean hasNext = NextState.hasMoreValues(scanner.next(currRow));
      assertEquals(2, currRow.size());
      assertTrue(Bytes.equals(currRow.get(0).getRow(), rowC));
      assertTrue(hasNext);
      currRow.clear();
      hasNext = NextState.hasMoreValues(scanner.next(currRow));
      assertEquals(1, currRow.size());
      assertTrue(Bytes.equals(currRow.get(0).getRow(), rowB));
      assertTrue(hasNext);
      currRow.clear();
      hasNext = NextState.hasMoreValues(scanner.next(currRow));
      assertEquals(1, currRow.size());
      assertTrue(Bytes.equals(currRow.get(0).getRow(), rowA));
      assertFalse(hasNext);
      scanner.close();
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  @Test (timeout=60000)
  public void testReverseScanner_FromMemStore_SingleCF_LargerKey()
      throws IOException {
    byte[] rowC = Bytes.toBytes("rowC");
    byte[] rowA = Bytes.toBytes("rowA");
    byte[] rowB = Bytes.toBytes("rowB");
    byte[] rowD = Bytes.toBytes("rowD");
    byte[] cf = Bytes.toBytes("CF");
    byte[][] families = { cf };
    byte[] col = Bytes.toBytes("C");
    long ts = 1;
    String method = this.getName();
    this.region = initHRegion(tableName, method, families);
    try {
      KeyValue kv1 = new KeyValue(rowC, cf, col, ts, KeyValue.Type.Put, null);
      KeyValue kv11 = new KeyValue(rowC, cf, col, ts + 1, KeyValue.Type.Put,
          null);
      KeyValue kv2 = new KeyValue(rowA, cf, col, ts, KeyValue.Type.Put, null);
      KeyValue kv3 = new KeyValue(rowB, cf, col, ts, KeyValue.Type.Put, null);
      Put put = null;
      put = new Put(rowC);
      put.add(kv1);
      put.add(kv11);
      region.put(put);
      put = new Put(rowA);
      put.add(kv2);
      region.put(put);
      put = new Put(rowB);
      put.add(kv3);
      region.put(put);

      Scan scan = new Scan(rowD);
      List<Cell> currRow = new ArrayList<Cell>();
      scan.setReversed(true);
      scan.setMaxVersions(5);
      InternalScanner scanner = region.getScanner(scan);
      boolean hasNext = NextState.hasMoreValues(scanner.next(currRow));
      assertEquals(2, currRow.size());
      assertTrue(Bytes.equals(currRow.get(0).getRow(), rowC));
      assertTrue(hasNext);
      currRow.clear();
      hasNext = NextState.hasMoreValues(scanner.next(currRow));
      assertEquals(1, currRow.size());
      assertTrue(Bytes.equals(currRow.get(0).getRow(), rowB));
      assertTrue(hasNext);
      currRow.clear();
      hasNext = NextState.hasMoreValues(scanner.next(currRow));
      assertEquals(1, currRow.size());
      assertTrue(Bytes.equals(currRow.get(0).getRow(), rowA));
      assertFalse(hasNext);
      scanner.close();
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  @Test (timeout=60000)
  public void testReverseScanner_FromMemStore_SingleCF_FullScan()
      throws IOException {
    byte[] rowC = Bytes.toBytes("rowC");
    byte[] rowA = Bytes.toBytes("rowA");
    byte[] rowB = Bytes.toBytes("rowB");
    byte[] cf = Bytes.toBytes("CF");
    byte[][] families = { cf };
    byte[] col = Bytes.toBytes("C");
    long ts = 1;
    String method = this.getName();
    this.region = initHRegion(tableName, method, families);
    try {
      KeyValue kv1 = new KeyValue(rowC, cf, col, ts, KeyValue.Type.Put, null);
      KeyValue kv11 = new KeyValue(rowC, cf, col, ts + 1, KeyValue.Type.Put,
          null);
      KeyValue kv2 = new KeyValue(rowA, cf, col, ts, KeyValue.Type.Put, null);
      KeyValue kv3 = new KeyValue(rowB, cf, col, ts, KeyValue.Type.Put, null);
      Put put = null;
      put = new Put(rowC);
      put.add(kv1);
      put.add(kv11);
      region.put(put);
      put = new Put(rowA);
      put.add(kv2);
      region.put(put);
      put = new Put(rowB);
      put.add(kv3);
      region.put(put);
      Scan scan = new Scan();
      List<Cell> currRow = new ArrayList<Cell>();
      scan.setReversed(true);
      InternalScanner scanner = region.getScanner(scan);
      boolean hasNext = NextState.hasMoreValues(scanner.next(currRow));
      assertEquals(1, currRow.size());
      assertTrue(Bytes.equals(currRow.get(0).getRow(), rowC));
      assertTrue(hasNext);
      currRow.clear();
      hasNext = NextState.hasMoreValues(scanner.next(currRow));
      assertEquals(1, currRow.size());
      assertTrue(Bytes.equals(currRow.get(0).getRow(), rowB));
      assertTrue(hasNext);
      currRow.clear();
      hasNext = NextState.hasMoreValues(scanner.next(currRow));
      assertEquals(1, currRow.size());
      assertTrue(Bytes.equals(currRow.get(0).getRow(), rowA));
      assertFalse(hasNext);
      scanner.close();
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  @Test (timeout=60000)
  public void testReverseScanner_moreRowsMayExistAfter() throws IOException {
    // case for "INCLUDE_AND_SEEK_NEXT_ROW & SEEK_NEXT_ROW" endless loop
    byte[] rowA = Bytes.toBytes("rowA");
    byte[] rowB = Bytes.toBytes("rowB");
    byte[] rowC = Bytes.toBytes("rowC");
    byte[] rowD = Bytes.toBytes("rowD");
    byte[] rowE = Bytes.toBytes("rowE");
    byte[] cf = Bytes.toBytes("CF");
    byte[][] families = { cf };
    byte[] col1 = Bytes.toBytes("col1");
    byte[] col2 = Bytes.toBytes("col2");
    long ts = 1;
    String method = this.getName();
    this.region = initHRegion(tableName, method, families);
    try {
      KeyValue kv1 = new KeyValue(rowA, cf, col1, ts, KeyValue.Type.Put, null);
      KeyValue kv2 = new KeyValue(rowB, cf, col1, ts, KeyValue.Type.Put, null);
      KeyValue kv3 = new KeyValue(rowC, cf, col1, ts, KeyValue.Type.Put, null);
      KeyValue kv4_1 = new KeyValue(rowD, cf, col1, ts, KeyValue.Type.Put, null);
      KeyValue kv4_2 = new KeyValue(rowD, cf, col2, ts, KeyValue.Type.Put, null);
      KeyValue kv5 = new KeyValue(rowE, cf, col1, ts, KeyValue.Type.Put, null);
      Put put = null;
      put = new Put(rowA);
      put.add(kv1);
      region.put(put);
      put = new Put(rowB);
      put.add(kv2);
      region.put(put);
      put = new Put(rowC);
      put.add(kv3);
      region.put(put);
      put = new Put(rowD);
      put.add(kv4_1);
      region.put(put);
      put = new Put(rowD);
      put.add(kv4_2);
      region.put(put);
      put = new Put(rowE);
      put.add(kv5);
      region.put(put);
      region.flush(true);
      Scan scan = new Scan(rowD, rowA);
      scan.addColumn(families[0], col1);
      scan.setReversed(true);
      List<Cell> currRow = new ArrayList<Cell>();
      InternalScanner scanner = region.getScanner(scan);
      boolean hasNext = NextState.hasMoreValues(scanner.next(currRow));
      assertEquals(1, currRow.size());
      assertTrue(Bytes.equals(currRow.get(0).getRow(), rowD));
      assertTrue(hasNext);
      currRow.clear();
      hasNext = NextState.hasMoreValues(scanner.next(currRow));
      assertEquals(1, currRow.size());
      assertTrue(Bytes.equals(currRow.get(0).getRow(), rowC));
      assertTrue(hasNext);
      currRow.clear();
      hasNext = NextState.hasMoreValues(scanner.next(currRow));
      assertEquals(1, currRow.size());
      assertTrue(Bytes.equals(currRow.get(0).getRow(), rowB));
      assertFalse(hasNext);
      scanner.close();

      scan = new Scan(rowD, rowA);
      scan.addColumn(families[0], col2);
      scan.setReversed(true);
      currRow.clear();
      scanner = region.getScanner(scan);
      hasNext = NextState.hasMoreValues(scanner.next(currRow));
      assertEquals(1, currRow.size());
      assertTrue(Bytes.equals(currRow.get(0).getRow(), rowD));
      scanner.close();
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  @Test (timeout=60000)
  public void testReverseScanner_smaller_blocksize() throws IOException {
    // case to ensure no conflict with HFile index optimization
    byte[] rowA = Bytes.toBytes("rowA");
    byte[] rowB = Bytes.toBytes("rowB");
    byte[] rowC = Bytes.toBytes("rowC");
    byte[] rowD = Bytes.toBytes("rowD");
    byte[] rowE = Bytes.toBytes("rowE");
    byte[] cf = Bytes.toBytes("CF");
    byte[][] families = { cf };
    byte[] col1 = Bytes.toBytes("col1");
    byte[] col2 = Bytes.toBytes("col2");
    long ts = 1;
    String method = this.getName();
    HBaseConfiguration config = new HBaseConfiguration();
    config.setInt("test.block.size", 1);
    this.region = initHRegion(tableName, method, config, families);
    try {
      KeyValue kv1 = new KeyValue(rowA, cf, col1, ts, KeyValue.Type.Put, null);
      KeyValue kv2 = new KeyValue(rowB, cf, col1, ts, KeyValue.Type.Put, null);
      KeyValue kv3 = new KeyValue(rowC, cf, col1, ts, KeyValue.Type.Put, null);
      KeyValue kv4_1 = new KeyValue(rowD, cf, col1, ts, KeyValue.Type.Put, null);
      KeyValue kv4_2 = new KeyValue(rowD, cf, col2, ts, KeyValue.Type.Put, null);
      KeyValue kv5 = new KeyValue(rowE, cf, col1, ts, KeyValue.Type.Put, null);
      Put put = null;
      put = new Put(rowA);
      put.add(kv1);
      region.put(put);
      put = new Put(rowB);
      put.add(kv2);
      region.put(put);
      put = new Put(rowC);
      put.add(kv3);
      region.put(put);
      put = new Put(rowD);
      put.add(kv4_1);
      region.put(put);
      put = new Put(rowD);
      put.add(kv4_2);
      region.put(put);
      put = new Put(rowE);
      put.add(kv5);
      region.put(put);
      region.flush(true);
      Scan scan = new Scan(rowD, rowA);
      scan.addColumn(families[0], col1);
      scan.setReversed(true);
      List<Cell> currRow = new ArrayList<Cell>();
      InternalScanner scanner = region.getScanner(scan);
      boolean hasNext = NextState.hasMoreValues(scanner.next(currRow));
      assertEquals(1, currRow.size());
      assertTrue(Bytes.equals(currRow.get(0).getRow(), rowD));
      assertTrue(hasNext);
      currRow.clear();
      hasNext = NextState.hasMoreValues(scanner.next(currRow));
      assertEquals(1, currRow.size());
      assertTrue(Bytes.equals(currRow.get(0).getRow(), rowC));
      assertTrue(hasNext);
      currRow.clear();
      hasNext = NextState.hasMoreValues(scanner.next(currRow));
      assertEquals(1, currRow.size());
      assertTrue(Bytes.equals(currRow.get(0).getRow(), rowB));
      assertFalse(hasNext);
      scanner.close();

      scan = new Scan(rowD, rowA);
      scan.addColumn(families[0], col2);
      scan.setReversed(true);
      currRow.clear();
      scanner = region.getScanner(scan);
      hasNext = NextState.hasMoreValues(scanner.next(currRow));
      assertEquals(1, currRow.size());
      assertTrue(Bytes.equals(currRow.get(0).getRow(), rowD));
      scanner.close();
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  @Test (timeout=60000)
  public void testReverseScanner_FromMemStoreAndHFiles_MultiCFs1()
      throws IOException {
    byte[] row0 = Bytes.toBytes("row0"); // 1 kv
    byte[] row1 = Bytes.toBytes("row1"); // 2 kv
    byte[] row2 = Bytes.toBytes("row2"); // 4 kv
    byte[] row3 = Bytes.toBytes("row3"); // 2 kv
    byte[] row4 = Bytes.toBytes("row4"); // 5 kv
    byte[] row5 = Bytes.toBytes("row5"); // 2 kv
    byte[] cf1 = Bytes.toBytes("CF1");
    byte[] cf2 = Bytes.toBytes("CF2");
    byte[] cf3 = Bytes.toBytes("CF3");
    byte[][] families = { cf1, cf2, cf3 };
    byte[] col = Bytes.toBytes("C");
    long ts = 1;
    String method = this.getName();
    HBaseConfiguration conf = new HBaseConfiguration();
    // disable compactions in this test.
    conf.setInt("hbase.hstore.compactionThreshold", 10000);
    this.region = initHRegion(tableName, method, conf, families);
    try {
      // kv naming style: kv(row number) totalKvCountInThisRow seq no
      KeyValue kv0_1_1 = new KeyValue(row0, cf1, col, ts, KeyValue.Type.Put,
          null);
      KeyValue kv1_2_1 = new KeyValue(row1, cf2, col, ts, KeyValue.Type.Put,
          null);
      KeyValue kv1_2_2 = new KeyValue(row1, cf1, col, ts + 1,
          KeyValue.Type.Put, null);
      KeyValue kv2_4_1 = new KeyValue(row2, cf2, col, ts, KeyValue.Type.Put,
          null);
      KeyValue kv2_4_2 = new KeyValue(row2, cf1, col, ts, KeyValue.Type.Put,
          null);
      KeyValue kv2_4_3 = new KeyValue(row2, cf3, col, ts, KeyValue.Type.Put,
          null);
      KeyValue kv2_4_4 = new KeyValue(row2, cf1, col, ts + 4,
          KeyValue.Type.Put, null);
      KeyValue kv3_2_1 = new KeyValue(row3, cf2, col, ts, KeyValue.Type.Put,
          null);
      KeyValue kv3_2_2 = new KeyValue(row3, cf1, col, ts + 4,
          KeyValue.Type.Put, null);
      KeyValue kv4_5_1 = new KeyValue(row4, cf1, col, ts, KeyValue.Type.Put,
          null);
      KeyValue kv4_5_2 = new KeyValue(row4, cf3, col, ts, KeyValue.Type.Put,
          null);
      KeyValue kv4_5_3 = new KeyValue(row4, cf3, col, ts + 5,
          KeyValue.Type.Put, null);
      KeyValue kv4_5_4 = new KeyValue(row4, cf2, col, ts, KeyValue.Type.Put,
          null);
      KeyValue kv4_5_5 = new KeyValue(row4, cf1, col, ts + 3,
          KeyValue.Type.Put, null);
      KeyValue kv5_2_1 = new KeyValue(row5, cf2, col, ts, KeyValue.Type.Put,
          null);
      KeyValue kv5_2_2 = new KeyValue(row5, cf3, col, ts, KeyValue.Type.Put,
          null);
      // hfiles(cf1/cf2) :"row1"(1 kv) / "row2"(1 kv) / "row4"(2 kv)
      Put put = null;
      put = new Put(row1);
      put.add(kv1_2_1);
      region.put(put);
      put = new Put(row2);
      put.add(kv2_4_1);
      region.put(put);
      put = new Put(row4);
      put.add(kv4_5_4);
      put.add(kv4_5_5);
      region.put(put);
      region.flush(true);
      // hfiles(cf1/cf3) : "row1" (1 kvs) / "row2" (1 kv) / "row4" (2 kv)
      put = new Put(row4);
      put.add(kv4_5_1);
      put.add(kv4_5_3);
      region.put(put);
      put = new Put(row1);
      put.add(kv1_2_2);
      region.put(put);
      put = new Put(row2);
      put.add(kv2_4_4);
      region.put(put);
      region.flush(true);
      // hfiles(cf1/cf3) : "row2"(2 kv) / "row3"(1 kvs) / "row4" (1 kv)
      put = new Put(row4);
      put.add(kv4_5_2);
      region.put(put);
      put = new Put(row2);
      put.add(kv2_4_2);
      put.add(kv2_4_3);
      region.put(put);
      put = new Put(row3);
      put.add(kv3_2_2);
      region.put(put);
      region.flush(true);
      // memstore(cf1/cf2/cf3) : "row0" (1 kvs) / "row3" ( 1 kv) / "row5" (max)
      // ( 2 kv)
      put = new Put(row0);
      put.add(kv0_1_1);
      region.put(put);
      put = new Put(row3);
      put.add(kv3_2_1);
      region.put(put);
      put = new Put(row5);
      put.add(kv5_2_1);
      put.add(kv5_2_2);
      region.put(put);
      // scan range = ["row4", min), skip the max "row5"
      Scan scan = new Scan(row4);
      scan.setMaxVersions(5);
      scan.setBatch(3);
      scan.setReversed(true);
      InternalScanner scanner = region.getScanner(scan);
      List<Cell> currRow = new ArrayList<Cell>();
      boolean hasNext = false;
      // 1. scan out "row4" (5 kvs), "row5" can't be scanned out since not
      // included in scan range
      // "row4" takes 2 next() calls since batch=3
      hasNext = NextState.hasMoreValues(scanner.next(currRow));
      assertEquals(3, currRow.size());
      assertTrue(Bytes.equals(currRow.get(0).getRow(), row4));
      assertTrue(hasNext);
      currRow.clear();
      hasNext = NextState.hasMoreValues(scanner.next(currRow));
      assertEquals(2, currRow.size());
      assertTrue(Bytes.equals(currRow.get(0).getRow(), row4));
      assertTrue(hasNext);
      // 2. scan out "row3" (2 kv)
      currRow.clear();
      hasNext = NextState.hasMoreValues(scanner.next(currRow));
      assertEquals(2, currRow.size());
      assertTrue(Bytes.equals(currRow.get(0).getRow(), row3));
      assertTrue(hasNext);
      // 3. scan out "row2" (4 kvs)
      // "row2" takes 2 next() calls since batch=3
      currRow.clear();
      hasNext = NextState.hasMoreValues(scanner.next(currRow));
      assertEquals(3, currRow.size());
      assertTrue(Bytes.equals(currRow.get(0).getRow(), row2));
      assertTrue(hasNext);
      currRow.clear();
      hasNext = NextState.hasMoreValues(scanner.next(currRow));
      assertEquals(1, currRow.size());
      assertTrue(Bytes.equals(currRow.get(0).getRow(), row2));
      assertTrue(hasNext);
      // 4. scan out "row1" (2 kv)
      currRow.clear();
      hasNext = NextState.hasMoreValues(scanner.next(currRow));
      assertEquals(2, currRow.size());
      assertTrue(Bytes.equals(currRow.get(0).getRow(), row1));
      assertTrue(hasNext);
      // 5. scan out "row0" (1 kv)
      currRow.clear();
      hasNext = NextState.hasMoreValues(scanner.next(currRow));
      assertEquals(1, currRow.size());
      assertTrue(Bytes.equals(currRow.get(0).getRow(), row0));
      assertFalse(hasNext);

      scanner.close();
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  @Test (timeout=60000)
  public void testReverseScanner_FromMemStoreAndHFiles_MultiCFs2()
      throws IOException {
    byte[] row1 = Bytes.toBytes("row1");
    byte[] row2 = Bytes.toBytes("row2");
    byte[] row3 = Bytes.toBytes("row3");
    byte[] row4 = Bytes.toBytes("row4");
    byte[] cf1 = Bytes.toBytes("CF1");
    byte[] cf2 = Bytes.toBytes("CF2");
    byte[] cf3 = Bytes.toBytes("CF3");
    byte[] cf4 = Bytes.toBytes("CF4");
    byte[][] families = { cf1, cf2, cf3, cf4 };
    byte[] col = Bytes.toBytes("C");
    long ts = 1;
    String method = this.getName();
    HBaseConfiguration conf = new HBaseConfiguration();
    // disable compactions in this test.
    conf.setInt("hbase.hstore.compactionThreshold", 10000);
    this.region = initHRegion(tableName, method, conf, families);
    try {
      KeyValue kv1 = new KeyValue(row1, cf1, col, ts, KeyValue.Type.Put, null);
      KeyValue kv2 = new KeyValue(row2, cf2, col, ts, KeyValue.Type.Put, null);
      KeyValue kv3 = new KeyValue(row3, cf3, col, ts, KeyValue.Type.Put, null);
      KeyValue kv4 = new KeyValue(row4, cf4, col, ts, KeyValue.Type.Put, null);
      // storefile1
      Put put = new Put(row1);
      put.add(kv1);
      region.put(put);
      region.flush(true);
      // storefile2
      put = new Put(row2);
      put.add(kv2);
      region.put(put);
      region.flush(true);
      // storefile3
      put = new Put(row3);
      put.add(kv3);
      region.put(put);
      region.flush(true);
      // memstore
      put = new Put(row4);
      put.add(kv4);
      region.put(put);
      // scan range = ["row4", min)
      Scan scan = new Scan(row4);
      scan.setReversed(true);
      scan.setBatch(10);
      InternalScanner scanner = region.getScanner(scan);
      List<Cell> currRow = new ArrayList<Cell>();
      boolean hasNext = NextState.hasMoreValues(scanner.next(currRow));
      assertEquals(1, currRow.size());
      assertTrue(Bytes.equals(currRow.get(0).getRow(), row4));
      assertTrue(hasNext);
      currRow.clear();
      hasNext = NextState.hasMoreValues(scanner.next(currRow));
      assertEquals(1, currRow.size());
      assertTrue(Bytes.equals(currRow.get(0).getRow(), row3));
      assertTrue(hasNext);
      currRow.clear();
      hasNext = NextState.hasMoreValues(scanner.next(currRow));
      assertEquals(1, currRow.size());
      assertTrue(Bytes.equals(currRow.get(0).getRow(), row2));
      assertTrue(hasNext);
      currRow.clear();
      hasNext = NextState.hasMoreValues(scanner.next(currRow));
      assertEquals(1, currRow.size());
      assertTrue(Bytes.equals(currRow.get(0).getRow(), row1));
      assertFalse(hasNext);
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  @Test (timeout=60000)
  public void testSplitRegionWithReverseScan() throws IOException {
    byte [] tableName = Bytes.toBytes("testSplitRegionWithReverseScan");
    byte [] qualifier = Bytes.toBytes("qualifier");
    Configuration hc = initSplit();
    int numRows = 3;
    byte [][] families = {fam1};

    //Setting up region
    String method = this.getName();
    this.region = initHRegion(tableName, method, hc, families);

    //Put data in region
    int startRow = 100;
    putData(startRow, numRows, qualifier, families);
    int splitRow = startRow + numRows;
    putData(splitRow, numRows, qualifier, families);
    int endRow = splitRow + numRows;
    region.flush(true);

    HRegion [] regions = null;
    try {
      regions = splitRegion(region, Bytes.toBytes("" + splitRow));
      //Opening the regions returned.
      for (int i = 0; i < regions.length; i++) {
        regions[i] = HRegion.openHRegion(regions[i], null);
      }
      //Verifying that the region has been split
      assertEquals(2, regions.length);

      //Verifying that all data is still there and that data is in the right
      //place
      verifyData(regions[0], startRow, numRows, qualifier, families);
      verifyData(regions[1], splitRow, numRows, qualifier, families);

      //fire the reverse scan1:  top range, and larger than the last row
      Scan scan = new Scan(Bytes.toBytes(String.valueOf(startRow + 10 * numRows)));
      scan.setReversed(true);
      InternalScanner scanner = regions[1].getScanner(scan);
      List<Cell> currRow = new ArrayList<Cell>();
      boolean more = false;
      int verify = startRow + 2 * numRows - 1;
      do {
        more = NextState.hasMoreValues(scanner.next(currRow));
        assertEquals(Bytes.toString(currRow.get(0).getRow()), verify + "");
        verify--;
        currRow.clear();
      } while(more);
      assertEquals(verify, startRow + numRows - 1);
      scanner.close();
      //fire the reverse scan2:  top range, and equals to the last row
      scan = new Scan(Bytes.toBytes(String.valueOf(startRow + 2 * numRows - 1)));
      scan.setReversed(true);
      scanner = regions[1].getScanner(scan);
      verify = startRow + 2 * numRows - 1;
      do {
        more = NextState.hasMoreValues(scanner.next(currRow));
        assertEquals(Bytes.toString(currRow.get(0).getRow()), verify + "");
        verify--;
        currRow.clear();
      } while(more);
      assertEquals(verify, startRow + numRows - 1);
      scanner.close();
      //fire the reverse scan3:  bottom range, and larger than the last row
      scan = new Scan(Bytes.toBytes(String.valueOf(startRow + numRows)));
      scan.setReversed(true);
      scanner = regions[0].getScanner(scan);
      verify = startRow + numRows - 1;
      do {
        more = NextState.hasMoreValues(scanner.next(currRow));
        assertEquals(Bytes.toString(currRow.get(0).getRow()), verify + "");
        verify--;
        currRow.clear();
      } while(more);
      assertEquals(verify, 99);
      scanner.close();
      //fire the reverse scan4:  bottom range, and equals to the last row
      scan = new Scan(Bytes.toBytes(String.valueOf(startRow + numRows - 1)));
      scan.setReversed(true);
      scanner = regions[0].getScanner(scan);
      verify = startRow + numRows - 1;
      do {
        more = NextState.hasMoreValues(scanner.next(currRow));
        assertEquals(Bytes.toString(currRow.get(0).getRow()), verify + "");
        verify--;
        currRow.clear();
      } while(more);
      assertEquals(verify, startRow - 1);
      scanner.close();
    } finally {
      this.region.close();
      this.region = null;
    }
  }

  @Test
  public void testWriteRequestsCounter() throws IOException {
    byte[] fam = Bytes.toBytes("info");
    byte[][] families = { fam };
    this.region = initHRegion(tableName, method, CONF, families);

    Assert.assertEquals(0L, region.getWriteRequestsCount());

    Put put = new Put(row);
    put.add(fam, fam, fam);

    Assert.assertEquals(0L, region.getWriteRequestsCount());
    region.put(put);
    Assert.assertEquals(1L, region.getWriteRequestsCount());
    region.put(put);
    Assert.assertEquals(2L, region.getWriteRequestsCount());
    region.put(put);
    Assert.assertEquals(3L, region.getWriteRequestsCount());

    region.delete(new Delete(row));
    Assert.assertEquals(4L, region.getWriteRequestsCount());

    HBaseTestingUtility.closeRegionAndWAL(this.region);
    this.region = null;
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testOpenRegionWrittenToWAL() throws Exception {
    final ServerName serverName = ServerName.valueOf("testOpenRegionWrittenToWAL", 100, 42);
    final RegionServerServices rss = spy(TEST_UTIL.createMockRegionServerService(serverName));

    HTableDescriptor htd
        = new HTableDescriptor(TableName.valueOf("testOpenRegionWrittenToWAL"));
    htd.addFamily(new HColumnDescriptor(fam1));
    htd.addFamily(new HColumnDescriptor(fam2));

    HRegionInfo hri = new HRegionInfo(htd.getTableName(),
      HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY);

    // open the region w/o rss and wal and flush some files
    HRegion region =
         HBaseTestingUtility.createRegionAndWAL(hri, TEST_UTIL.getDataTestDir(), TEST_UTIL
             .getConfiguration(), htd);
    assertNotNull(region);

    // create a file in fam1 for the region before opening in OpenRegionHandler
    region.put(new Put(Bytes.toBytes("a")).add(fam1, fam1, fam1));
    region.flush(true);
    HBaseTestingUtility.closeRegionAndWAL(region);

    ArgumentCaptor<WALEdit> editCaptor = ArgumentCaptor.forClass(WALEdit.class);

    // capture append() calls
    WAL wal = mock(WAL.class);
    when(rss.getWAL((HRegionInfo) any())).thenReturn(wal);

    try {
      region = HRegion.openHRegion(hri, htd, rss.getWAL(hri),
        TEST_UTIL.getConfiguration(), rss, null);

      verify(wal, times(1)).append((HTableDescriptor)any(), (HRegionInfo)any(), (WALKey)any()
        , editCaptor.capture(), (AtomicLong)any(), anyBoolean(), (List<Cell>)any());

      WALEdit edit = editCaptor.getValue();
      assertNotNull(edit);
      assertNotNull(edit.getCells());
      assertEquals(1, edit.getCells().size());
      RegionEventDescriptor desc = WALEdit.getRegionEventDescriptor(edit.getCells().get(0));
      assertNotNull(desc);

      LOG.info("RegionEventDescriptor from WAL: " + desc);

      assertEquals(RegionEventDescriptor.EventType.REGION_OPEN, desc.getEventType());
      assertTrue(Bytes.equals(desc.getTableName().toByteArray(), htd.getName()));
      assertTrue(Bytes.equals(desc.getEncodedRegionName().toByteArray(),
        hri.getEncodedNameAsBytes()));
      assertTrue(desc.getLogSequenceNumber() > 0);
      assertEquals(serverName, ProtobufUtil.toServerName(desc.getServer()));
      assertEquals(2, desc.getStoresCount());

      StoreDescriptor store = desc.getStores(0);
      assertTrue(Bytes.equals(store.getFamilyName().toByteArray(), fam1));
      assertEquals(store.getStoreHomeDir(), Bytes.toString(fam1));
      assertEquals(1, store.getStoreFileCount()); // 1store file
      assertFalse(store.getStoreFile(0).contains("/")); // ensure path is relative

      store = desc.getStores(1);
      assertTrue(Bytes.equals(store.getFamilyName().toByteArray(), fam2));
      assertEquals(store.getStoreHomeDir(), Bytes.toString(fam2));
      assertEquals(0, store.getStoreFileCount()); // no store files

    } finally {
      HBaseTestingUtility.closeRegionAndWAL(region);
    }
  }

  // Helper for test testOpenRegionWrittenToWALForLogReplay
  static class HRegionWithSeqId extends HRegion {
    public HRegionWithSeqId(final Path tableDir, final WAL wal, final FileSystem fs,
        final Configuration confParam, final HRegionInfo regionInfo,
        final HTableDescriptor htd, final RegionServerServices rsServices) {
      super(tableDir, wal, fs, confParam, regionInfo, htd, rsServices);
    }
    @Override
    protected long getNextSequenceId(WAL wal) throws IOException {
      return 42;
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testOpenRegionWrittenToWALForLogReplay() throws Exception {
    // similar to the above test but with distributed log replay
    final ServerName serverName = ServerName.valueOf("testOpenRegionWrittenToWALForLogReplay",
      100, 42);
    final RegionServerServices rss = spy(TEST_UTIL.createMockRegionServerService(serverName));

    HTableDescriptor htd
        = new HTableDescriptor(TableName.valueOf("testOpenRegionWrittenToWALForLogReplay"));
    htd.addFamily(new HColumnDescriptor(fam1));
    htd.addFamily(new HColumnDescriptor(fam2));

    HRegionInfo hri = new HRegionInfo(htd.getTableName(),
      HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY);

    // open the region w/o rss and wal and flush some files
    HRegion region =
         HBaseTestingUtility.createRegionAndWAL(hri, TEST_UTIL.getDataTestDir(), TEST_UTIL
             .getConfiguration(), htd);
    assertNotNull(region);

    // create a file in fam1 for the region before opening in OpenRegionHandler
    region.put(new Put(Bytes.toBytes("a")).add(fam1, fam1, fam1));
    region.flush(true);
    HBaseTestingUtility.closeRegionAndWAL(region);

    ArgumentCaptor<WALEdit> editCaptor = ArgumentCaptor.forClass(WALEdit.class);

    // capture append() calls
    WAL wal = mock(WAL.class);
    when(rss.getWAL((HRegionInfo) any())).thenReturn(wal);

    // add the region to recovering regions
    HashMap<String, Region> recoveringRegions = Maps.newHashMap();
    recoveringRegions.put(region.getRegionInfo().getEncodedName(), null);
    when(rss.getRecoveringRegions()).thenReturn(recoveringRegions);

    try {
      Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
      conf.set(HConstants.REGION_IMPL, HRegionWithSeqId.class.getName());
      region = HRegion.openHRegion(hri, htd, rss.getWAL(hri),
        conf, rss, null);

      // verify that we have not appended region open event to WAL because this region is still
      // recovering
      verify(wal, times(0)).append((HTableDescriptor)any(), (HRegionInfo)any(), (WALKey)any()
        , editCaptor.capture(), (AtomicLong)any(), anyBoolean(), (List<Cell>)any());

      // not put the region out of recovering state
      new FinishRegionRecoveringHandler(rss, region.getRegionInfo().getEncodedName(), "/foo")
        .prepare().process();

      // now we should have put the entry
      verify(wal, times(1)).append((HTableDescriptor)any(), (HRegionInfo)any(), (WALKey)any()
        , editCaptor.capture(), (AtomicLong)any(), anyBoolean(), (List<Cell>)any());

      WALEdit edit = editCaptor.getValue();
      assertNotNull(edit);
      assertNotNull(edit.getCells());
      assertEquals(1, edit.getCells().size());
      RegionEventDescriptor desc = WALEdit.getRegionEventDescriptor(edit.getCells().get(0));
      assertNotNull(desc);

      LOG.info("RegionEventDescriptor from WAL: " + desc);

      assertEquals(RegionEventDescriptor.EventType.REGION_OPEN, desc.getEventType());
      assertTrue(Bytes.equals(desc.getTableName().toByteArray(), htd.getName()));
      assertTrue(Bytes.equals(desc.getEncodedRegionName().toByteArray(),
        hri.getEncodedNameAsBytes()));
      assertTrue(desc.getLogSequenceNumber() > 0);
      assertEquals(serverName, ProtobufUtil.toServerName(desc.getServer()));
      assertEquals(2, desc.getStoresCount());

      StoreDescriptor store = desc.getStores(0);
      assertTrue(Bytes.equals(store.getFamilyName().toByteArray(), fam1));
      assertEquals(store.getStoreHomeDir(), Bytes.toString(fam1));
      assertEquals(1, store.getStoreFileCount()); // 1store file
      assertFalse(store.getStoreFile(0).contains("/")); // ensure path is relative

      store = desc.getStores(1);
      assertTrue(Bytes.equals(store.getFamilyName().toByteArray(), fam2));
      assertEquals(store.getStoreHomeDir(), Bytes.toString(fam2));
      assertEquals(0, store.getStoreFileCount()); // no store files

    } finally {
      HBaseTestingUtility.closeRegionAndWAL(region);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCloseRegionWrittenToWAL() throws Exception {
    final ServerName serverName = ServerName.valueOf("testCloseRegionWrittenToWAL", 100, 42);
    final RegionServerServices rss = spy(TEST_UTIL.createMockRegionServerService(serverName));

    HTableDescriptor htd
    = new HTableDescriptor(TableName.valueOf("testOpenRegionWrittenToWAL"));
    htd.addFamily(new HColumnDescriptor(fam1));
    htd.addFamily(new HColumnDescriptor(fam2));

    HRegionInfo hri = new HRegionInfo(htd.getTableName(),
      HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY);

    ArgumentCaptor<WALEdit> editCaptor = ArgumentCaptor.forClass(WALEdit.class);

    // capture append() calls
    WAL wal = mock(WAL.class);
    when(rss.getWAL((HRegionInfo) any())).thenReturn(wal);

    // open a region first so that it can be closed later
    region = HRegion.openHRegion(hri, htd, rss.getWAL(hri),
      TEST_UTIL.getConfiguration(), rss, null);

    // close the region
    region.close(false);

    // 2 times, one for region open, the other close region
    verify(wal, times(2)).append((HTableDescriptor)any(), (HRegionInfo)any(), (WALKey)any(),
      editCaptor.capture(), (AtomicLong)any(), anyBoolean(), (List<Cell>)any());

    WALEdit edit = editCaptor.getAllValues().get(1);
    assertNotNull(edit);
    assertNotNull(edit.getCells());
    assertEquals(1, edit.getCells().size());
    RegionEventDescriptor desc = WALEdit.getRegionEventDescriptor(edit.getCells().get(0));
    assertNotNull(desc);

    LOG.info("RegionEventDescriptor from WAL: " + desc);

    assertEquals(RegionEventDescriptor.EventType.REGION_CLOSE, desc.getEventType());
    assertTrue(Bytes.equals(desc.getTableName().toByteArray(), htd.getName()));
    assertTrue(Bytes.equals(desc.getEncodedRegionName().toByteArray(),
      hri.getEncodedNameAsBytes()));
    assertTrue(desc.getLogSequenceNumber() > 0);
    assertEquals(serverName, ProtobufUtil.toServerName(desc.getServer()));
    assertEquals(2, desc.getStoresCount());

    StoreDescriptor store = desc.getStores(0);
    assertTrue(Bytes.equals(store.getFamilyName().toByteArray(), fam1));
    assertEquals(store.getStoreHomeDir(), Bytes.toString(fam1));
    assertEquals(0, store.getStoreFileCount()); // no store files

    store = desc.getStores(1);
    assertTrue(Bytes.equals(store.getFamilyName().toByteArray(), fam2));
    assertEquals(store.getStoreHomeDir(), Bytes.toString(fam2));
    assertEquals(0, store.getStoreFileCount()); // no store files
  }

  /**
   * Test RegionTooBusyException thrown when region is busy
   */
  @Test (timeout=24000)
  public void testRegionTooBusy() throws IOException {
    String method = "testRegionTooBusy";
    byte[] tableName = Bytes.toBytes(method);
    byte[] family = Bytes.toBytes("family");
    long defaultBusyWaitDuration = CONF.getLong("hbase.busy.wait.duration",
      HRegion.DEFAULT_BUSY_WAIT_DURATION);
    CONF.setLong("hbase.busy.wait.duration", 1000);
    region = initHRegion(tableName, method, CONF, family);
    final AtomicBoolean stopped = new AtomicBoolean(true);
    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          region.lock.writeLock().lock();
          stopped.set(false);
          while (!stopped.get()) {
            Thread.sleep(100);
          }
        } catch (InterruptedException ie) {
        } finally {
          region.lock.writeLock().unlock();
        }
      }
    });
    t.start();
    Get get = new Get(row);
    try {
      while (stopped.get()) {
        Thread.sleep(100);
      }
      region.get(get);
      fail("Should throw RegionTooBusyException");
    } catch (InterruptedException ie) {
      fail("test interrupted");
    } catch (RegionTooBusyException e) {
      // Good, expected
    } finally {
      stopped.set(true);
      try {
        t.join();
      } catch (Throwable e) {
      }

      HBaseTestingUtility.closeRegionAndWAL(region);
      region = null;
      CONF.setLong("hbase.busy.wait.duration", defaultBusyWaitDuration);
    }
  }

  @Test
  public void testCellTTLs() throws IOException {
    IncrementingEnvironmentEdge edge = new IncrementingEnvironmentEdge();
    EnvironmentEdgeManager.injectEdge(edge);

    final byte[] row = Bytes.toBytes("testRow");
    final byte[] q1 = Bytes.toBytes("q1");
    final byte[] q2 = Bytes.toBytes("q2");
    final byte[] q3 = Bytes.toBytes("q3");
    final byte[] q4 = Bytes.toBytes("q4");

    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("testCellTTLs"));
    HColumnDescriptor hcd = new HColumnDescriptor(fam1);
    hcd.setTimeToLive(10); // 10 seconds
    htd.addFamily(hcd);

    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    conf.setInt(HFile.FORMAT_VERSION_KEY, HFile.MIN_FORMAT_VERSION_WITH_TAGS);

    HRegion region = HBaseTestingUtility.createRegionAndWAL(new HRegionInfo(htd.getTableName(),
            HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY),
        TEST_UTIL.getDataTestDir(), conf, htd);
    assertNotNull(region);
    try {
      long now = EnvironmentEdgeManager.currentTime();
      // Add a cell that will expire in 5 seconds via cell TTL
      region.put(new Put(row).add(new KeyValue(row, fam1, q1, now,
        HConstants.EMPTY_BYTE_ARRAY, new Tag[] {
          // TTL tags specify ts in milliseconds
          new Tag(TagType.TTL_TAG_TYPE, Bytes.toBytes(5000L)) } )));
      // Add a cell that will expire after 10 seconds via family setting
      region.put(new Put(row).add(fam1, q2, now, HConstants.EMPTY_BYTE_ARRAY));
      // Add a cell that will expire in 15 seconds via cell TTL
      region.put(new Put(row).add(new KeyValue(row, fam1, q3, now + 10000 - 1,
        HConstants.EMPTY_BYTE_ARRAY, new Tag[] {
          // TTL tags specify ts in milliseconds
          new Tag(TagType.TTL_TAG_TYPE, Bytes.toBytes(5000L)) } )));
      // Add a cell that will expire in 20 seconds via family setting
      region.put(new Put(row).add(fam1, q4, now + 10000 - 1, HConstants.EMPTY_BYTE_ARRAY));

      // Flush so we are sure store scanning gets this right
      region.flush(true);

      // A query at time T+0 should return all cells
      Result r = region.get(new Get(row));
      assertNotNull(r.getValue(fam1, q1));
      assertNotNull(r.getValue(fam1, q2));
      assertNotNull(r.getValue(fam1, q3));
      assertNotNull(r.getValue(fam1, q4));

      // Increment time to T+5 seconds
      edge.incrementTime(5000);

      r = region.get(new Get(row));
      assertNull(r.getValue(fam1, q1));
      assertNotNull(r.getValue(fam1, q2));
      assertNotNull(r.getValue(fam1, q3));
      assertNotNull(r.getValue(fam1, q4));

      // Increment time to T+10 seconds
      edge.incrementTime(5000);

      r = region.get(new Get(row));
      assertNull(r.getValue(fam1, q1));
      assertNull(r.getValue(fam1, q2));
      assertNotNull(r.getValue(fam1, q3));
      assertNotNull(r.getValue(fam1, q4));

      // Increment time to T+15 seconds
      edge.incrementTime(5000);

      r = region.get(new Get(row));
      assertNull(r.getValue(fam1, q1));
      assertNull(r.getValue(fam1, q2));
      assertNull(r.getValue(fam1, q3));
      assertNotNull(r.getValue(fam1, q4));

      // Increment time to T+20 seconds
      edge.incrementTime(10000);

      r = region.get(new Get(row));
      assertNull(r.getValue(fam1, q1));
      assertNull(r.getValue(fam1, q2));
      assertNull(r.getValue(fam1, q3));
      assertNull(r.getValue(fam1, q4));

      // Fun with disappearing increments

      // Start at 1
      region.put(new Put(row).add(fam1, q1, Bytes.toBytes(1L)));
      r = region.get(new Get(row));
      byte[] val = r.getValue(fam1, q1);
      assertNotNull(val);
      assertEquals(Bytes.toLong(val), 1L);

      // Increment with a TTL of 5 seconds
      Increment incr = new Increment(row).addColumn(fam1, q1, 1L);
      incr.setTTL(5000);
      region.increment(incr); // 2

      // New value should be 2
      r = region.get(new Get(row));
      val = r.getValue(fam1, q1);
      assertNotNull(val);
      assertEquals(Bytes.toLong(val), 2L);

      // Increment time to T+25 seconds
      edge.incrementTime(5000);

      // Value should be back to 1
      r = region.get(new Get(row));
      val = r.getValue(fam1, q1);
      assertNotNull(val);
      assertEquals(Bytes.toLong(val), 1L);

      // Increment time to T+30 seconds
      edge.incrementTime(5000);

      // Original value written at T+20 should be gone now via family TTL
      r = region.get(new Get(row));
      assertNull(r.getValue(fam1, q1));

    } finally {
      HBaseTestingUtility.closeRegionAndWAL(region);
    }
  }

  static HRegion initHRegion(byte[] tableName, String callingMethod,
      byte[]... families) throws IOException {
    return initHRegion(tableName, callingMethod, HBaseConfiguration.create(),
        families);
  }
}
