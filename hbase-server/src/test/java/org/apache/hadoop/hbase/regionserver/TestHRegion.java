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

import static org.apache.hadoop.hbase.HBaseTestingUtil.COLUMNS;
import static org.apache.hadoop.hbase.HBaseTestingUtil.fam1;
import static org.apache.hadoop.hbase.HBaseTestingUtil.fam2;
import static org.apache.hadoop.hbase.HBaseTestingUtil.fam3;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.math.BigDecimal;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ArrayBackedTag;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Cell.Type;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.DroppedSnapshotException;
import org.apache.hadoop.hbase.ExtendedCellBuilderFactory;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HConstants.OperationStatusCode;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MultithreadedTestUtil;
import org.apache.hadoop.hbase.MultithreadedTestUtil.RepeatingTestThread;
import org.apache.hadoop.hbase.MultithreadedTestUtil.TestThread;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.StartTestingClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.CheckAndMutateResult;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.MetaTableMetrics;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.exceptions.FailedSanityCheckException;
import org.apache.hadoop.hbase.filter.BigDecimalComparator;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.NullComparator;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.regionserver.Region.Operation;
import org.apache.hadoop.hbase.regionserver.Region.RowLock;
import org.apache.hadoop.hbase.regionserver.TestHStore.FaultyFileSystem;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequestImpl;
import org.apache.hadoop.hbase.regionserver.wal.FSHLog;
import org.apache.hadoop.hbase.regionserver.wal.MetricsWALSource;
import org.apache.hadoop.hbase.regionserver.wal.WALUtil;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationObserver;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.VerySlowRegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.hadoop.hbase.util.IncrementingEnvironmentEdge;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.FaultyFSLog;
import org.apache.hadoop.hbase.wal.NettyAsyncFSWALConfigHelper;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.hadoop.hbase.wal.WALProvider;
import org.apache.hadoop.hbase.wal.WALProvider.Writer;
import org.apache.hadoop.hbase.wal.WALSplitUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.CompactionDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.FlushDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.FlushDescriptor.FlushAction;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.FlushDescriptor.StoreFlushDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.RegionEventDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.StoreDescriptor;

/**
 * Basic stand-alone testing of HRegion.  No clusters!
 *
 * A lot of the meta information for an HRegion now lives inside other HRegions
 * or in the HBaseMaster, so only basic testing is possible.
 */
@Category({VerySlowRegionServerTests.class, LargeTests.class})
@SuppressWarnings("deprecation")
public class TestHRegion {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHRegion.class);

  // Do not spin up clusters in here. If you need to spin up a cluster, do it
  // over in TestHRegionOnCluster.
  private static final Logger LOG = LoggerFactory.getLogger(TestHRegion.class);
  @Rule
  public TestName name = new TestName();
  @Rule public final ExpectedException thrown = ExpectedException.none();

  private static final String COLUMN_FAMILY = "MyCF";
  private static final byte [] COLUMN_FAMILY_BYTES = Bytes.toBytes(COLUMN_FAMILY);
  private static final EventLoopGroup GROUP = new NioEventLoopGroup();

  HRegion region = null;
  // Do not run unit tests in parallel (? Why not?  It don't work?  Why not?  St.Ack)
  protected static HBaseTestingUtil TEST_UTIL;
  public static Configuration CONF ;
  private String dir;
  private final int MAX_VERSIONS = 2;

  // Test names
  protected TableName tableName;
  protected String method;
  protected final byte[] qual = Bytes.toBytes("qual");
  protected final byte[] qual1 = Bytes.toBytes("qual1");
  protected final byte[] qual2 = Bytes.toBytes("qual2");
  protected final byte[] qual3 = Bytes.toBytes("qual3");
  protected final byte[] value = Bytes.toBytes("value");
  protected final byte[] value1 = Bytes.toBytes("value1");
  protected final byte[] value2 = Bytes.toBytes("value2");
  protected final byte[] row = Bytes.toBytes("rowA");
  protected final byte[] row2 = Bytes.toBytes("rowB");

  protected final MetricsAssertHelper metricsAssertHelper = CompatibilitySingletonFactory
      .getInstance(MetricsAssertHelper.class);

  @Before
  public void setup() throws IOException {
    TEST_UTIL = new HBaseTestingUtil();
    CONF = TEST_UTIL.getConfiguration();
    NettyAsyncFSWALConfigHelper.setEventLoopConfig(CONF, GROUP, NioSocketChannel.class);
    dir = TEST_UTIL.getDataTestDir("TestHRegion").toString();
    method = name.getMethodName();
    tableName = TableName.valueOf(method);
    CONF.set(CompactingMemStore.IN_MEMORY_FLUSH_THRESHOLD_FACTOR_KEY, String.valueOf(0.09));
  }

  @After
  public void tearDown() throws IOException {
    // Region may have been closed, but it is still no harm if we close it again here using HTU.
    HBaseTestingUtil.closeRegionAndWAL(region);
    EnvironmentEdgeManagerTestHelper.reset();
    LOG.info("Cleaning test directory: " + TEST_UTIL.getDataTestDir());
    TEST_UTIL.cleanupTestDir();
  }

  /**
   * Test that I can use the max flushed sequence id after the close.
   * @throws IOException
   */
  @Test
  public void testSequenceId() throws IOException {
    region = initHRegion(tableName, method, CONF, COLUMN_FAMILY_BYTES);
    assertEquals(HConstants.NO_SEQNUM, region.getMaxFlushedSeqId());
    // Weird. This returns 0 if no store files or no edits. Afraid to change it.
    assertEquals(0, (long)region.getMaxStoreSeqId().get(COLUMN_FAMILY_BYTES));
    HBaseTestingUtil.closeRegionAndWAL(this.region);
    assertEquals(HConstants.NO_SEQNUM, region.getMaxFlushedSeqId());
    assertEquals(0, (long)region.getMaxStoreSeqId().get(COLUMN_FAMILY_BYTES));
    // Open region again.
    region = initHRegion(tableName, method, CONF, COLUMN_FAMILY_BYTES);
    byte [] value = Bytes.toBytes(method);
    // Make a random put against our cf.
    Put put = new Put(value);
    put.addColumn(COLUMN_FAMILY_BYTES, null, value);
    region.put(put);
    // No flush yet so init numbers should still be in place.
    assertEquals(HConstants.NO_SEQNUM, region.getMaxFlushedSeqId());
    assertEquals(0, (long)region.getMaxStoreSeqId().get(COLUMN_FAMILY_BYTES));
    region.flush(true);
    long max = region.getMaxFlushedSeqId();
    HBaseTestingUtil.closeRegionAndWAL(this.region);
    assertEquals(max, region.getMaxFlushedSeqId());
    this.region = null;
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
  @Test
  public void testCloseCarryingSnapshot() throws IOException {
    region = initHRegion(tableName, method, CONF, COLUMN_FAMILY_BYTES);
    HStore store = region.getStore(COLUMN_FAMILY_BYTES);
    // Get some random bytes.
    byte [] value = Bytes.toBytes(method);
    // Make a random put against our cf.
    Put put = new Put(value);
    put.addColumn(COLUMN_FAMILY_BYTES, null, value);
    // First put something in current memstore, which will be in snapshot after flusher.prepare()
    region.put(put);
    StoreFlushContext storeFlushCtx = store.createFlushContext(12345, FlushLifeCycleTracker.DUMMY);
    storeFlushCtx.prepare();
    // Second put something in current memstore
    put.addColumn(COLUMN_FAMILY_BYTES, Bytes.toBytes("abc"), value);
    region.put(put);
    // Close with something in memstore and something in the snapshot.  Make sure all is cleared.
    HBaseTestingUtil.closeRegionAndWAL(region);
    assertEquals(0, region.getMemStoreDataSize());
    region = null;
  }

  /*
   * This test is for verifying memstore snapshot size is correctly updated in case of rollback
   * See HBASE-10845
   */
  @Test
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
      protected void doSync(long txid, boolean forceSync) throws IOException {
        storeFlushCtx.prepare();
        super.doSync(txid, forceSync);
      }
    }

    FileSystem fs = FileSystem.get(CONF);
    Path rootDir = new Path(dir + "testMemstoreSnapshotSize");
    MyFaultyFSLog faultyLog = new MyFaultyFSLog(fs, rootDir, "testMemstoreSnapshotSize", CONF);
    faultyLog.init();
    region = initHRegion(tableName, null, null, CONF, false, Durability.SYNC_WAL, faultyLog,
        COLUMN_FAMILY_BYTES);

    HStore store = region.getStore(COLUMN_FAMILY_BYTES);
    // Get some random bytes.
    byte [] value = Bytes.toBytes(method);
    faultyLog.setStoreFlushCtx(store.createFlushContext(12345, FlushLifeCycleTracker.DUMMY));

    Put put = new Put(value);
    put.addColumn(COLUMN_FAMILY_BYTES, Bytes.toBytes("abc"), value);
    faultyLog.setFailureType(FaultyFSLog.FailureType.SYNC);
    boolean threwIOE = false;
    try {
      region.put(put);
    } catch (IOException ioe) {
      threwIOE = true;
    } finally {
      assertTrue("The regionserver should have thrown an exception", threwIOE);
    }
    MemStoreSize mss = store.getFlushableSize();
    assertTrue("flushable size should be zero, but it is " + mss,
        mss.getDataSize() == 0);
  }

  /**
   * Create a WAL outside of the usual helper in
   * {@link HBaseTestingUtil#createWal(Configuration, Path, RegionInfo)} because that method
   * doesn't play nicely with FaultyFileSystem. Call this method before overriding
   * {@code fs.file.impl}.
   * @param callingMethod a unique component for the path, probably the name of the test method.
   */
  private static WAL createWALCompatibleWithFaultyFileSystem(String callingMethod,
      Configuration conf, TableName tableName) throws IOException {
    final Path logDir = TEST_UTIL.getDataTestDirOnTestFS(callingMethod + ".log");
    final Configuration walConf = new Configuration(conf);
    CommonFSUtils.setRootDir(walConf, logDir);
    return new WALFactory(walConf, callingMethod)
        .getWAL(RegionInfoBuilder.newBuilder(tableName).build());
  }

  @Test
  public void testMemstoreSizeAccountingWithFailedPostBatchMutate() throws IOException {
    String testName = "testMemstoreSizeAccountingWithFailedPostBatchMutate";
    FileSystem fs = FileSystem.get(CONF);
    Path rootDir = new Path(dir + testName);
    FSHLog hLog = new FSHLog(fs, rootDir, testName, CONF);
    hLog.init();
    region = initHRegion(tableName, null, null, CONF, false, Durability.SYNC_WAL, hLog,
      COLUMN_FAMILY_BYTES);
    HStore store = region.getStore(COLUMN_FAMILY_BYTES);
    assertEquals(0, region.getMemStoreDataSize());

    // Put one value
    byte [] value = Bytes.toBytes(method);
    Put put = new Put(value);
    put.addColumn(COLUMN_FAMILY_BYTES, Bytes.toBytes("abc"), value);
    region.put(put);
    long onePutSize = region.getMemStoreDataSize();
    assertTrue(onePutSize > 0);

    RegionCoprocessorHost mockedCPHost = Mockito.mock(RegionCoprocessorHost.class);
    doThrow(new IOException())
       .when(mockedCPHost).postBatchMutate(Mockito.<MiniBatchOperationInProgress<Mutation>>any());
    region.setCoprocessorHost(mockedCPHost);

    put = new Put(value);
    put.addColumn(COLUMN_FAMILY_BYTES, Bytes.toBytes("dfg"), value);
    try {
      region.put(put);
      fail("Should have failed with IOException");
    } catch (IOException expected) {
    }
    long expectedSize = onePutSize * 2;
    assertEquals("memstoreSize should be incremented",
        expectedSize, region.getMemStoreDataSize());
    assertEquals("flushable size should be incremented",
        expectedSize, store.getFlushableSize().getDataSize());

    region.setCoprocessorHost(null);
  }

  /**
   * A test case of HBASE-21041
   * @throws Exception Exception
   */
  @Test
  public void testFlushAndMemstoreSizeCounting() throws Exception {
    byte[] family = Bytes.toBytes("family");
    this.region = initHRegion(tableName, method, CONF, family);
    final WALFactory wals = new WALFactory(CONF, method);
    try {
      for (byte[] row : HBaseTestingUtil.ROWS) {
        Put put = new Put(row);
        put.addColumn(family, family, row);
        region.put(put);
      }
      region.flush(true);
      // After flush, data size should be zero
      assertEquals(0, region.getMemStoreDataSize());
      // After flush, a new active mutable segment is created, so the heap size
      // should equal to MutableSegment.DEEP_OVERHEAD
      assertEquals(MutableSegment.DEEP_OVERHEAD, region.getMemStoreHeapSize());
      // After flush, offheap should be zero
      assertEquals(0, region.getMemStoreOffHeapSize());
    } finally {
      HBaseTestingUtil.closeRegionAndWAL(this.region);
      this.region = null;
      wals.close();
    }
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
  @Test
  public void testFlushSizeAccounting() throws Exception {
    final Configuration conf = HBaseConfiguration.create(CONF);
    final WAL wal = createWALCompatibleWithFaultyFileSystem(method, conf, tableName);
    // Only retry once.
    conf.setInt("hbase.hstore.flush.retries.number", 1);
    final User user =
      User.createUserForTesting(conf, method, new String[]{"foo"});
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
          region = initHRegion(tableName, null, null, CONF, false, Durability.SYNC_WAL, wal,
              COLUMN_FAMILY_BYTES);
          long size = region.getMemStoreDataSize();
          Assert.assertEquals(0, size);
          // Put one item into memstore.  Measure the size of one item in memstore.
          Put p1 = new Put(row);
          p1.add(new KeyValue(row, COLUMN_FAMILY_BYTES, qual1, 1, (byte[]) null));
          region.put(p1);
          final long sizeOfOnePut = region.getMemStoreDataSize();
          // Fail a flush which means the current memstore will hang out as memstore 'snapshot'.
          try {
            LOG.info("Flushing");
            region.flush(true);
            Assert.fail("Didn't bubble up IOE!");
          } catch (DroppedSnapshotException dse) {
            // What we are expecting
            region.closing.set(false); // this is needed for the rest of the test to work
          }
          // Make it so all writes succeed from here on out
          ffs.fault.set(false);
          // Check sizes.  Should still be the one entry.
          Assert.assertEquals(sizeOfOnePut, region.getMemStoreDataSize());
          // Now add two entries so that on this next flush that fails, we can see if we
          // subtract the right amount, the snapshot size only.
          Put p2 = new Put(row);
          p2.add(new KeyValue(row, COLUMN_FAMILY_BYTES, qual2, 2, (byte[])null));
          p2.add(new KeyValue(row, COLUMN_FAMILY_BYTES, qual3, 3, (byte[])null));
          region.put(p2);
          long expectedSize = sizeOfOnePut * 3;
          Assert.assertEquals(expectedSize, region.getMemStoreDataSize());
          // Do a successful flush.  It will clear the snapshot only.  Thats how flushes work.
          // If already a snapshot, we clear it else we move the memstore to be snapshot and flush
          // it
          region.flush(true);
          // Make sure our memory accounting is right.
          Assert.assertEquals(sizeOfOnePut * 2, region.getMemStoreDataSize());
        } finally {
          HBaseTestingUtil.closeRegionAndWAL(region);
        }
        return null;
      }
    });
    FileSystem.closeAllForUGI(user.getUGI());
  }

  @Test
  public void testCloseWithFailingFlush() throws Exception {
    final Configuration conf = HBaseConfiguration.create(CONF);
    final WAL wal = createWALCompatibleWithFaultyFileSystem(method, conf, tableName);
    // Only retry once.
    conf.setInt("hbase.hstore.flush.retries.number", 1);
    final User user =
      User.createUserForTesting(conf, this.method, new String[]{"foo"});
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
          region = initHRegion(tableName, null, null, CONF, false,
              Durability.SYNC_WAL, wal, COLUMN_FAMILY_BYTES);
          long size = region.getMemStoreDataSize();
          Assert.assertEquals(0, size);
          // Put one item into memstore.  Measure the size of one item in memstore.
          Put p1 = new Put(row);
          p1.add(new KeyValue(row, COLUMN_FAMILY_BYTES, qual1, 1, (byte[])null));
          region.put(p1);
          // Manufacture an outstanding snapshot -- fake a failed flush by doing prepare step only.
          HStore store = region.getStore(COLUMN_FAMILY_BYTES);
          StoreFlushContext storeFlushCtx =
              store.createFlushContext(12345, FlushLifeCycleTracker.DUMMY);
          storeFlushCtx.prepare();
          // Now add two entries to the foreground memstore.
          Put p2 = new Put(row);
          p2.add(new KeyValue(row, COLUMN_FAMILY_BYTES, qual2, 2, (byte[])null));
          p2.add(new KeyValue(row, COLUMN_FAMILY_BYTES, qual3, 3, (byte[])null));
          region.put(p2);
          // Now try close on top of a failing flush.
          HBaseTestingUtil.closeRegionAndWAL(region);
          region = null;
          fail();
        } catch (DroppedSnapshotException dse) {
          // Expected
          LOG.info("Expected DroppedSnapshotException");
        } finally {
          // Make it so all writes succeed from here on out so can close clean
          ffs.fault.set(false);
          HBaseTestingUtil.closeRegionAndWAL(region);
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
    put.addColumn(family, Bytes.toBytes("q1"), Bytes.toBytes("v1"));
    region.put(put);
    region.flush(true);

    Scan scan = new Scan();
    scan.readVersions(3);
    // open the first scanner
    RegionScanner scanner1 = region.getScanner(scan);

    Delete delete = new Delete(Bytes.toBytes("r1"));
    region.delete(delete);
    region.flush(true);

    // open the second scanner
    RegionScanner scanner2 = region.getScanner(scan);

    List<Cell> results = new ArrayList<>();

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
    put.addColumn(family, Bytes.toBytes("q1"), Bytes.toBytes("v1"));
    region.put(put);
    put = new Put(Bytes.toBytes("r2"));
    put.addColumn(family, Bytes.toBytes("q1"), Bytes.toBytes("v1"));
    region.put(put);
    region.flush(true);

    Scan scan = new Scan();
    scan.readVersions(3);
    // open the first scanner
    RegionScanner scanner1 = region.getScanner(scan);

    System.out.println("Smallest read point:" + region.getSmallestReadPoint());

    region.compact(true);

    scanner1.reseek(Bytes.toBytes("r2"));
    List<Cell> results = new ArrayList<>();
    scanner1.next(results);
    Cell keyValue = results.get(0);
    Assert.assertTrue(Bytes.compareTo(CellUtil.cloneRow(keyValue), Bytes.toBytes("r2")) == 0);
    scanner1.close();
  }

  @Test
  public void testArchiveRecoveredEditsReplay() throws Exception {
    byte[] family = Bytes.toBytes("family");
    this.region = initHRegion(tableName, method, CONF, family);
    final WALFactory wals = new WALFactory(CONF, method);
    try {
      Path regiondir = region.getRegionFileSystem().getRegionDir();
      FileSystem fs = region.getRegionFileSystem().getFileSystem();
      byte[] regionName = region.getRegionInfo().getEncodedNameAsBytes();

      Path recoveredEditsDir = WALSplitUtil.getRegionDirRecoveredEditsDir(regiondir);

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
        writer.append(new WAL.Entry(new WALKeyImpl(regionName, tableName, i, time,
          HConstants.DEFAULT_CLUSTER_ID), edit));

        writer.close();
      }
      MonitoredTask status = TaskMonitor.get().createStatus(method);
      Map<byte[], Long> maxSeqIdInStores = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      for (HStore store : region.getStores()) {
        maxSeqIdInStores.put(Bytes.toBytes(store.getColumnFamilyName()), minSeqId - 1);
      }
      CONF.set("hbase.region.archive.recovered.edits", "true");
      CONF.set(CommonFSUtils.HBASE_WAL_DIR, "/custom_wal_dir");
      long seqId = region.replayRecoveredEditsIfAny(maxSeqIdInStores, null, status);
      assertEquals(maxSeqId, seqId);
      region.getMVCC().advanceTo(seqId);
      String fakeFamilyName = recoveredEditsDir.getName();
      Path rootDir = new Path(CONF.get(HConstants.HBASE_DIR));
      Path storeArchiveDir = HFileArchiveUtil.getStoreArchivePathForRootDir(rootDir,
        region.getRegionInfo(), Bytes.toBytes(fakeFamilyName));
      FileStatus[] list = TEST_UTIL.getTestFileSystem().listStatus(storeArchiveDir);
      assertEquals(6, list.length);
    } finally {
      CONF.set("hbase.region.archive.recovered.edits", "false");
      CONF.set(CommonFSUtils.HBASE_WAL_DIR, "");
      HBaseTestingUtil.closeRegionAndWAL(this.region);
      this.region = null;
      wals.close();
    }
  }

  @Test
  public void testSkipRecoveredEditsReplay() throws Exception {
    byte[] family = Bytes.toBytes("family");
    this.region = initHRegion(tableName, method, CONF, family);
    final WALFactory wals = new WALFactory(CONF, method);
    try {
      Path regiondir = region.getRegionFileSystem().getRegionDir();
      FileSystem fs = region.getRegionFileSystem().getFileSystem();
      byte[] regionName = region.getRegionInfo().getEncodedNameAsBytes();

      Path recoveredEditsDir = WALSplitUtil.getRegionDirRecoveredEditsDir(regiondir);

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
        writer.append(new WAL.Entry(new WALKeyImpl(regionName, tableName, i, time,
            HConstants.DEFAULT_CLUSTER_ID), edit));

        writer.close();
      }
      MonitoredTask status = TaskMonitor.get().createStatus(method);
      Map<byte[], Long> maxSeqIdInStores = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      for (HStore store : region.getStores()) {
        maxSeqIdInStores.put(Bytes.toBytes(store.getColumnFamilyName()), minSeqId - 1);
      }
      long seqId = region.replayRecoveredEditsIfAny(maxSeqIdInStores, null, status);
      assertEquals(maxSeqId, seqId);
      region.getMVCC().advanceTo(seqId);
      Get get = new Get(row);
      Result result = region.get(get);
      for (long i = minSeqId; i <= maxSeqId; i += 10) {
        List<Cell> kvs = result.getColumnCells(family, Bytes.toBytes(i));
        assertEquals(1, kvs.size());
        assertArrayEquals(Bytes.toBytes(i), CellUtil.cloneValue(kvs.get(0)));
      }
    } finally {
      HBaseTestingUtil.closeRegionAndWAL(this.region);
      this.region = null;
      wals.close();
    }
  }

  @Test
  public void testSkipRecoveredEditsReplaySomeIgnored() throws Exception {
    byte[] family = Bytes.toBytes("family");
    this.region = initHRegion(tableName, method, CONF, family);
    final WALFactory wals = new WALFactory(CONF, method);
    try {
      Path regiondir = region.getRegionFileSystem().getRegionDir();
      FileSystem fs = region.getRegionFileSystem().getFileSystem();
      byte[] regionName = region.getRegionInfo().getEncodedNameAsBytes();

      Path recoveredEditsDir = WALSplitUtil.getRegionDirRecoveredEditsDir(regiondir);

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
        writer.append(new WAL.Entry(new WALKeyImpl(regionName, tableName, i, time,
            HConstants.DEFAULT_CLUSTER_ID), edit));

        writer.close();
      }
      long recoverSeqId = 1030;
      MonitoredTask status = TaskMonitor.get().createStatus(method);
      Map<byte[], Long> maxSeqIdInStores = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      for (HStore store : region.getStores()) {
        maxSeqIdInStores.put(Bytes.toBytes(store.getColumnFamilyName()), recoverSeqId - 1);
      }
      long seqId = region.replayRecoveredEditsIfAny(maxSeqIdInStores, null, status);
      assertEquals(maxSeqId, seqId);
      region.getMVCC().advanceTo(seqId);
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
      HBaseTestingUtil.closeRegionAndWAL(this.region);
      this.region = null;
      wals.close();
    }
  }

  @Test
  public void testSkipRecoveredEditsReplayAllIgnored() throws Exception {
    byte[] family = Bytes.toBytes("family");
    this.region = initHRegion(tableName, method, CONF, family);
    Path regiondir = region.getRegionFileSystem().getRegionDir();
    FileSystem fs = region.getRegionFileSystem().getFileSystem();

    Path recoveredEditsDir = WALSplitUtil.getRegionDirRecoveredEditsDir(regiondir);
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

    Map<byte[], Long> maxSeqIdInStores = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (HStore store : region.getStores()) {
      maxSeqIdInStores.put(Bytes.toBytes(store.getColumnFamilyName()), minSeqId);
    }
    long seqId = region.replayRecoveredEditsIfAny(maxSeqIdInStores, null, null);
    assertEquals(minSeqId, seqId);
  }

  @Test
  public void testSkipRecoveredEditsReplayTheLastFileIgnored() throws Exception {
    byte[] family = Bytes.toBytes("family");
    this.region = initHRegion(tableName, method, CONF, family);
    final WALFactory wals = new WALFactory(CONF, method);
    try {
      Path regiondir = region.getRegionFileSystem().getRegionDir();
      FileSystem fs = region.getRegionFileSystem().getFileSystem();
      byte[] regionName = region.getRegionInfo().getEncodedNameAsBytes();
      byte[][] columns = region.getTableDescriptor().getColumnFamilyNames().toArray(new byte[0][]);

      assertEquals(0, region.getStoreFileList(columns).size());

      Path recoveredEditsDir = WALSplitUtil.getRegionDirRecoveredEditsDir(regiondir);

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
        writer.append(new WAL.Entry(new WALKeyImpl(regionName, tableName, i, time,
            HConstants.DEFAULT_CLUSTER_ID), edit));
        writer.close();
      }

      long recoverSeqId = 1030;
      Map<byte[], Long> maxSeqIdInStores = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      MonitoredTask status = TaskMonitor.get().createStatus(method);
      for (HStore store : region.getStores()) {
        maxSeqIdInStores.put(Bytes.toBytes(store.getColumnFamilyName()), recoverSeqId - 1);
      }
      long seqId = region.replayRecoveredEditsIfAny(maxSeqIdInStores, null, status);
      assertEquals(maxSeqId, seqId);

      // assert that the files are flushed
      assertEquals(1, region.getStoreFileList(columns).size());

    } finally {
      HBaseTestingUtil.closeRegionAndWAL(this.region);
      this.region = null;
      wals.close();
    }
  }

  @Test
  public void testRecoveredEditsReplayCompaction() throws Exception {
    testRecoveredEditsReplayCompaction(false);
    testRecoveredEditsReplayCompaction(true);
  }

  public void testRecoveredEditsReplayCompaction(boolean mismatchedRegionName) throws Exception {
    CONF.setClass(HConstants.REGION_IMPL, HRegionForTesting.class, Region.class);
    byte[] family = Bytes.toBytes("family");
    this.region = initHRegion(tableName, method, CONF, family);
    final WALFactory wals = new WALFactory(CONF, method);
    try {
      Path regiondir = region.getRegionFileSystem().getRegionDir();
      FileSystem fs = region.getRegionFileSystem().getFileSystem();
      byte[] regionName = region.getRegionInfo().getEncodedNameAsBytes();

      long maxSeqId = 3;
      long minSeqId = 0;

      for (long i = minSeqId; i < maxSeqId; i++) {
        Put put = new Put(Bytes.toBytes(i));
        put.addColumn(family, Bytes.toBytes(i), Bytes.toBytes(i));
        region.put(put);
        region.flush(true);
      }

      // this will create a region with 3 files
      assertEquals(3, region.getStore(family).getStorefilesCount());
      List<Path> storeFiles = new ArrayList<>(3);
      for (HStoreFile sf : region.getStore(family).getStorefiles()) {
        storeFiles.add(sf.getPath());
      }

      // disable compaction completion
      CONF.setBoolean("hbase.hstore.compaction.complete", false);
      region.compactStores();

      // ensure that nothing changed
      assertEquals(3, region.getStore(family).getStorefilesCount());

      // now find the compacted file, and manually add it to the recovered edits
      Path tmpDir = new Path(region.getRegionFileSystem().getTempDir(), Bytes.toString(family));
      FileStatus[] files = CommonFSUtils.listStatus(fs, tmpDir);
      String errorMsg = "Expected to find 1 file in the region temp directory "
          + "from the compaction, could not find any";
      assertNotNull(errorMsg, files);
      assertEquals(errorMsg, 1, files.length);
      // move the file inside region dir
      Path newFile = region.getRegionFileSystem().commitStoreFile(Bytes.toString(family),
          files[0].getPath());

      byte[] encodedNameAsBytes = this.region.getRegionInfo().getEncodedNameAsBytes();
      byte[] fakeEncodedNameAsBytes = new byte [encodedNameAsBytes.length];
      for (int i=0; i < encodedNameAsBytes.length; i++) {
        // Mix the byte array to have a new encodedName
        fakeEncodedNameAsBytes[i] = (byte) (encodedNameAsBytes[i] + 1);
      }

      CompactionDescriptor compactionDescriptor = ProtobufUtil.toCompactionDescriptor(this.region
        .getRegionInfo(), mismatchedRegionName ? fakeEncodedNameAsBytes : null, family,
            storeFiles, Lists.newArrayList(newFile),
            region.getRegionFileSystem().getStoreDir(Bytes.toString(family)));

      WALUtil.writeCompactionMarker(region.getWAL(), this.region.getReplicationScope(),
          this.region.getRegionInfo(), compactionDescriptor, region.getMVCC(), null);

      Path recoveredEditsDir = WALSplitUtil.getRegionDirRecoveredEditsDir(regiondir);

      Path recoveredEdits = new Path(recoveredEditsDir, String.format("%019d", 1000));
      fs.create(recoveredEdits);
      WALProvider.Writer writer = wals.createRecoveredEditsWriter(fs, recoveredEdits);

      long time = System.nanoTime();

      writer.append(new WAL.Entry(new WALKeyImpl(regionName, tableName, 10, time,
          HConstants.DEFAULT_CLUSTER_ID), WALEdit.createCompaction(region.getRegionInfo(),
          compactionDescriptor)));
      writer.close();

      // close the region now, and reopen again
      region.getTableDescriptor();
      region.getRegionInfo();
      HBaseTestingUtil.closeRegionAndWAL(this.region);
      try {
        region = HRegion.openHRegion(region, null);
      } catch (WrongRegionException wre) {
        fail("Matching encoded region name should not have produced WrongRegionException");
      }

      // now check whether we have only one store file, the compacted one
      Collection<HStoreFile> sfs = region.getStore(family).getStorefiles();
      for (HStoreFile sf : sfs) {
        LOG.info(Objects.toString(sf.getPath()));
      }
      if (!mismatchedRegionName) {
        assertEquals(1, region.getStore(family).getStorefilesCount());
      }
      files = CommonFSUtils.listStatus(fs, tmpDir);
      assertTrue("Expected to find 0 files inside " + tmpDir, files == null || files.length == 0);

      for (long i = minSeqId; i < maxSeqId; i++) {
        Get get = new Get(Bytes.toBytes(i));
        Result result = region.get(get);
        byte[] value = result.getValue(family, Bytes.toBytes(i));
        assertArrayEquals(Bytes.toBytes(i), value);
      }
    } finally {
      HBaseTestingUtil.closeRegionAndWAL(this.region);
      this.region = null;
      wals.close();
      CONF.setClass(HConstants.REGION_IMPL, HRegion.class, Region.class);
    }
  }

  @Test
  public void testFlushMarkers() throws Exception {
    // tests that flush markers are written to WAL and handled at recovered edits
    byte[] family = Bytes.toBytes("family");
    Path logDir = TEST_UTIL.getDataTestDirOnTestFS(method + ".log");
    final Configuration walConf = new Configuration(TEST_UTIL.getConfiguration());
    CommonFSUtils.setRootDir(walConf, logDir);
    final WALFactory wals = new WALFactory(walConf, method);
    final WAL wal = wals.getWAL(RegionInfoBuilder.newBuilder(tableName).build());

    this.region = initHRegion(tableName, HConstants.EMPTY_START_ROW,
      HConstants.EMPTY_END_ROW, CONF, false, Durability.USE_DEFAULT, wal, family);
    try {
      Path regiondir = region.getRegionFileSystem().getRegionDir();
      FileSystem fs = region.getRegionFileSystem().getFileSystem();
      byte[] regionName = region.getRegionInfo().getEncodedNameAsBytes();

      long maxSeqId = 3;
      long minSeqId = 0;

      for (long i = minSeqId; i < maxSeqId; i++) {
        Put put = new Put(Bytes.toBytes(i));
        put.addColumn(family, Bytes.toBytes(i), Bytes.toBytes(i));
        region.put(put);
        region.flush(true);
      }

      // this will create a region with 3 files from flush
      assertEquals(3, region.getStore(family).getStorefilesCount());
      List<String> storeFiles = new ArrayList<>(3);
      for (HStoreFile sf : region.getStore(family).getStorefiles()) {
        storeFiles.add(sf.getPath().getName());
      }

      // now verify that the flush markers are written
      wal.shutdown();
      WAL.Reader reader = WALFactory.createReader(fs, AbstractFSWALProvider.getCurrentFileName(wal),
        TEST_UTIL.getConfiguration());
      try {
        List<WAL.Entry> flushDescriptors = new ArrayList<>();
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

        Path recoveredEditsDir = WALSplitUtil.getRegionDirRecoveredEditsDir(regiondir);

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
      HBaseTestingUtil.closeRegionAndWAL(this.region);
      region = HRegion.openHRegion(region, null);

      // now check whether we have can read back the data from region
      for (long i = minSeqId; i < maxSeqId; i++) {
        Get get = new Get(Bytes.toBytes(i));
        Result result = region.get(get);
        byte[] value = result.getValue(family, Bytes.toBytes(i));
        assertArrayEquals(Bytes.toBytes(i), value);
      }
    } finally {
      HBaseTestingUtil.closeRegionAndWAL(this.region);
      this.region = null;
      wals.close();
    }
  }

  static class IsFlushWALMarker implements ArgumentMatcher<WALEdit> {
    volatile FlushAction[] actions;
    public IsFlushWALMarker(FlushAction... actions) {
      this.actions = actions;
    }
    @Override
    public boolean matches(WALEdit edit) {
      List<Cell> cells = edit.getCells();
      if (cells.isEmpty()) {
        return false;
      }
      if (WALEdit.isMetaEditFamily(cells.get(0))) {
        FlushDescriptor desc;
        try {
          desc = WALEdit.getFlushDescriptor(cells.get(0));
        } catch (IOException e) {
          LOG.warn(e.toString(), e);
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
  public void testFlushMarkersWALFail() throws Exception {
    // test the cases where the WAL append for flush markers fail.
    byte[] family = Bytes.toBytes("family");

    // spy an actual WAL implementation to throw exception (was not able to mock)
    Path logDir = TEST_UTIL.getDataTestDirOnTestFS(method + "log");

    final Configuration walConf = new Configuration(TEST_UTIL.getConfiguration());
    CommonFSUtils.setRootDir(walConf, logDir);
    // Make up a WAL that we can manipulate at append time.
    class FailAppendFlushMarkerWAL extends FSHLog {
      volatile FlushAction [] flushActions = null;

      public FailAppendFlushMarkerWAL(FileSystem fs, Path root, String logDir, Configuration conf)
      throws IOException {
        super(fs, root, logDir, conf);
      }

      @Override
      protected Writer createWriterInstance(Path path) throws IOException {
        final Writer w = super.createWriterInstance(path);
        return new Writer() {
          @Override
          public void close() throws IOException {
            w.close();
          }

          @Override
          public void sync(boolean forceSync) throws IOException {
            w.sync(forceSync);
          }

          @Override
          public void append(Entry entry) throws IOException {
            List<Cell> cells = entry.getEdit().getCells();
            if (WALEdit.isMetaEditFamily(cells.get(0))) {
              FlushDescriptor desc = WALEdit.getFlushDescriptor(cells.get(0));
              if (desc != null) {
                for (FlushAction flushAction: flushActions) {
                  if (desc.getAction().equals(flushAction)) {
                    throw new IOException("Failed to append flush marker! " + flushAction);
                  }
                }
              }
            }
            w.append(entry);
          }

          @Override
          public long getLength() {
            return w.getLength();
          }

          @Override
          public long getSyncedLength() {
            return w.getSyncedLength();
          }
        };
      }
    }
    FailAppendFlushMarkerWAL wal = new FailAppendFlushMarkerWAL(FileSystem.get(walConf),
      CommonFSUtils.getRootDir(walConf), method, walConf);
    wal.init();
    this.region = initHRegion(tableName, HConstants.EMPTY_START_ROW,
      HConstants.EMPTY_END_ROW, CONF, false, Durability.USE_DEFAULT, wal, family);
    int i = 0;
    Put put = new Put(Bytes.toBytes(i));
    put.setDurability(Durability.SKIP_WAL); // have to skip mocked wal
    put.addColumn(family, Bytes.toBytes(i), Bytes.toBytes(i));
    region.put(put);

    // 1. Test case where START_FLUSH throws exception
    wal.flushActions = new FlushAction [] {FlushAction.START_FLUSH};

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
    // The WAL is hosed now. It has two edits appended. We cannot roll the log without it
    // throwing a DroppedSnapshotException to force an abort. Just clean up the mess.
    region.close(true);
    wal.close();

    // 2. Test case where START_FLUSH succeeds but COMMIT_FLUSH will throw exception
    wal.flushActions = new FlushAction[] { FlushAction.COMMIT_FLUSH };
    wal = new FailAppendFlushMarkerWAL(FileSystem.get(walConf), CommonFSUtils.getRootDir(walConf),
      method, walConf);
    wal.init();
    this.region = initHRegion(tableName, HConstants.EMPTY_START_ROW,
      HConstants.EMPTY_END_ROW, CONF, false, Durability.USE_DEFAULT, wal, family);
    region.put(put);
    // 3. Test case where ABORT_FLUSH will throw exception.
    // Even if ABORT_FLUSH throws exception, we should not fail with IOE, but continue with
    // DroppedSnapshotException. Below COMMIT_FLUSH will cause flush to abort
    wal.flushActions = new FlushAction [] {FlushAction.COMMIT_FLUSH, FlushAction.ABORT_FLUSH};

    try {
      region.flush(true);
      fail("This should have thrown exception");
    } catch (DroppedSnapshotException expected) {
      // we expect this exception, since we were able to write the snapshot, but failed to
      // write the flush marker to WAL
    } catch (IOException unexpected) {
      throw unexpected;
    }
  }

  @Test
  public void testGetWhileRegionClose() throws IOException {
    Configuration hc = initSplit();
    int numRows = 100;
    byte[][] families = { fam1, fam2, fam3 };

    // Setting up region
    this.region = initHRegion(tableName, method, hc, families);
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
        HBaseTestingUtil.closeRegionAndWAL(this.region);
        this.region = null;
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

    GetTillDoneOrException(final int i, final byte[] r, final AtomicBoolean d,
        final AtomicInteger c) {
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
    final TableName tableName = TableName.valueOf(name.getMethodName());
    byte[][] FAMILIES = new byte[][] { Bytes.toBytes("trans-blob"), Bytes.toBytes("trans-type"),
        Bytes.toBytes("trans-date"), Bytes.toBytes("trans-tags"), Bytes.toBytes("trans-group") };
    this.region = initHRegion(tableName, method, CONF, FAMILIES);
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
  }

  @Test
  public void testAppendWithReadOnlyTable() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    this.region = initHRegion(tableName, method, CONF, true, Bytes.toBytes("somefamily"));
    boolean exceptionCaught = false;
    Append append = new Append(Bytes.toBytes("somerow"));
    append.setDurability(Durability.SKIP_WAL);
    append.addColumn(Bytes.toBytes("somefamily"), Bytes.toBytes("somequalifier"),
        Bytes.toBytes("somevalue"));
    try {
      region.append(append);
    } catch (IOException e) {
      exceptionCaught = true;
    }
    assertTrue(exceptionCaught == true);
  }

  @Test
  public void testIncrWithReadOnlyTable() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    this.region = initHRegion(tableName, method, CONF, true, Bytes.toBytes("somefamily"));
    boolean exceptionCaught = false;
    Increment inc = new Increment(Bytes.toBytes("somerow"));
    inc.setDurability(Durability.SKIP_WAL);
    inc.addColumn(Bytes.toBytes("somefamily"), Bytes.toBytes("somequalifier"), 1L);
    try {
      region.increment(inc);
    } catch (IOException e) {
      exceptionCaught = true;
    }
    assertTrue(exceptionCaught == true);
  }

  private void deleteColumns(HRegion r, String value, String keyPrefix) throws IOException {
    InternalScanner scanner = buildScanner(keyPrefix, value, r);
    int count = 0;
    boolean more = false;
    List<Cell> results = new ArrayList<>();
    do {
      more = scanner.next(results);
      if (results != null && !results.isEmpty())
        count++;
      else
        break;
      Delete delete = new Delete(CellUtil.cloneRow(results.get(0)));
      delete.addColumn(Bytes.toBytes("trans-tags"), Bytes.toBytes("qual2"));
      r.delete(delete);
      results.clear();
    } while (more);
    assertEquals("Did not perform correct number of deletes", 3, count);
  }

  private int getNumberOfRows(String keyPrefix, String value, HRegion r) throws Exception {
    InternalScanner resultScanner = buildScanner(keyPrefix, value, r);
    int numberOfResults = 0;
    List<Cell> results = new ArrayList<>();
    boolean more = false;
    do {
      more = resultScanner.next(results);
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
        Bytes.toBytes("qual2"), CompareOperator.EQUAL, Bytes.toBytes(value));
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
      put.addColumn(Bytes.toBytes("trans-blob"), null, Bytes.toBytes("value for blob"));
      put.addColumn(Bytes.toBytes("trans-type"), null, Bytes.toBytes("statement"));
      put.addColumn(Bytes.toBytes("trans-date"), null, Bytes.toBytes("20090921010101999"));
      put.addColumn(Bytes.toBytes("trans-tags"), Bytes.toBytes("qual2"), Bytes.toBytes(value));
      put.addColumn(Bytes.toBytes("trans-group"), null, Bytes.toBytes("adhocTransactionGroupId"));
      r.put(put);
    }
  }

  @Test
  public void testFamilyWithAndWithoutColon() throws Exception {
    byte[] cf = Bytes.toBytes(COLUMN_FAMILY);
    this.region = initHRegion(tableName, method, CONF, cf);
    Put p = new Put(tableName.toBytes());
    byte[] cfwithcolon = Bytes.toBytes(COLUMN_FAMILY + ":");
    p.addColumn(cfwithcolon, cfwithcolon, cfwithcolon);
    boolean exception = false;
    try {
      this.region.put(p);
    } catch (NoSuchColumnFamilyException e) {
      exception = true;
    }
    assertTrue(exception);
  }

  @Test
  public void testBatchPut_whileNoRowLocksHeld() throws IOException {
    final Put[] puts = new Put[10];
    MetricsWALSource source = CompatibilitySingletonFactory.getInstance(MetricsWALSource.class);
    long syncs = prepareRegionForBachPut(puts, source, false);

    OperationStatus[] codes = this.region.batchMutate(puts);
    assertEquals(10, codes.length);
    for (int i = 0; i < 10; i++) {
      assertEquals(OperationStatusCode.SUCCESS, codes[i].getOperationStatusCode());
    }
    metricsAssertHelper.assertCounter("syncTimeNumOps", syncs + 1, source);

    LOG.info("Next a batch put with one invalid family");
    puts[5].addColumn(Bytes.toBytes("BAD_CF"), qual, value);
    codes = this.region.batchMutate(puts);
    assertEquals(10, codes.length);
    for (int i = 0; i < 10; i++) {
      assertEquals((i == 5) ? OperationStatusCode.BAD_FAMILY : OperationStatusCode.SUCCESS,
          codes[i].getOperationStatusCode());
    }

    metricsAssertHelper.assertCounter("syncTimeNumOps", syncs + 2, source);
  }

  @Test
  public void testBatchPut_whileMultipleRowLocksHeld() throws Exception {
    final Put[] puts = new Put[10];
    MetricsWALSource source = CompatibilitySingletonFactory.getInstance(MetricsWALSource.class);
    long syncs = prepareRegionForBachPut(puts, source, false);

    puts[5].addColumn(Bytes.toBytes("BAD_CF"), qual, value);

    LOG.info("batchPut will have to break into four batches to avoid row locks");
    RowLock rowLock1 = region.getRowLock(Bytes.toBytes("row_2"));
    RowLock rowLock2 = region.getRowLock(Bytes.toBytes("row_1"));
    RowLock rowLock3 = region.getRowLock(Bytes.toBytes("row_3"));
    RowLock rowLock4 = region.getRowLock(Bytes.toBytes("row_3"), true);

    MultithreadedTestUtil.TestContext ctx = new MultithreadedTestUtil.TestContext(CONF);
    final AtomicReference<OperationStatus[]> retFromThread = new AtomicReference<>();
    final CountDownLatch startingPuts = new CountDownLatch(1);
    final CountDownLatch startingClose = new CountDownLatch(1);
    TestThread putter = new TestThread(ctx) {
      @Override
      public void doWork() throws IOException {
        startingPuts.countDown();
        retFromThread.set(region.batchMutate(puts));
      }
    };
    LOG.info("...starting put thread while holding locks");
    ctx.addThread(putter);
    ctx.startThreads();

    // Now attempt to close the region from another thread.  Prior to HBASE-12565
    // this would cause the in-progress batchMutate operation to to fail with
    // exception because it use to release and re-acquire the close-guard lock
    // between batches.  Caller then didn't get status indicating which writes succeeded.
    // We now expect this thread to block until the batchMutate call finishes.
    Thread regionCloseThread = new TestThread(ctx) {
      @Override
      public void doWork() {
        try {
          startingPuts.await();
          // Give some time for the batch mutate to get in.
          // We don't want to race with the mutate
          Thread.sleep(10);
          startingClose.countDown();
          HBaseTestingUtil.closeRegionAndWAL(region);
          region = null;
        } catch (IOException e) {
          throw new RuntimeException(e);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    };
    regionCloseThread.start();

    startingClose.await();
    startingPuts.await();
    Thread.sleep(100);
    LOG.info("...releasing row lock 1, which should let put thread continue");
    rowLock1.release();
    rowLock2.release();
    rowLock3.release();
    waitForCounter(source, "syncTimeNumOps", syncs + 1);

    LOG.info("...joining on put thread");
    ctx.stop();
    regionCloseThread.join();

    OperationStatus[] codes = retFromThread.get();
    for (int i = 0; i < codes.length; i++) {
      assertEquals((i == 5) ? OperationStatusCode.BAD_FAMILY : OperationStatusCode.SUCCESS,
          codes[i].getOperationStatusCode());
    }
    rowLock4.release();
  }

  private void waitForCounter(MetricsWALSource source, String metricName, long expectedCount)
      throws InterruptedException {
    long startWait = EnvironmentEdgeManager.currentTime();
    long currentCount;
    while ((currentCount = metricsAssertHelper.getCounter(metricName, source)) < expectedCount) {
      Thread.sleep(100);
      if (EnvironmentEdgeManager.currentTime() - startWait > 10000) {
        fail(String.format("Timed out waiting for '%s' >= '%s', currentCount=%s", metricName,
            expectedCount, currentCount));
      }
    }
  }

  @Test
  public void testAtomicBatchPut() throws IOException {
    final Put[] puts = new Put[10];
    MetricsWALSource source = CompatibilitySingletonFactory.getInstance(MetricsWALSource.class);
    long syncs = prepareRegionForBachPut(puts, source, false);

    // 1. Straight forward case, should succeed
    OperationStatus[] codes = this.region.batchMutate(puts, true);
    assertEquals(10, codes.length);
    for (int i = 0; i < 10; i++) {
      assertEquals(OperationStatusCode.SUCCESS, codes[i].getOperationStatusCode());
    }
    metricsAssertHelper.assertCounter("syncTimeNumOps", syncs + 1, source);

    // 2. Failed to get lock
    RowLock lock = region.getRowLock(Bytes.toBytes("row_" + 3));
    // Method {@link HRegion#getRowLock(byte[])} is reentrant. As 'row_3' is locked in this
    // thread, need to run {@link HRegion#batchMutate(HRegion.BatchOperation)} in different thread
    MultithreadedTestUtil.TestContext ctx = new MultithreadedTestUtil.TestContext(CONF);
    final AtomicReference<IOException> retFromThread = new AtomicReference<>();
    final CountDownLatch finishedPuts = new CountDownLatch(1);
    TestThread putter = new TestThread(ctx) {
      @Override
      public void doWork() throws IOException {
        try {
          region.batchMutate(puts, true);
        } catch (IOException ioe) {
          LOG.error("test failed!", ioe);
          retFromThread.set(ioe);
        }
        finishedPuts.countDown();
      }
    };
    LOG.info("...starting put thread while holding locks");
    ctx.addThread(putter);
    ctx.startThreads();
    LOG.info("...waiting for batch puts while holding locks");
    try {
      finishedPuts.await();
    } catch (InterruptedException e) {
      LOG.error("Interrupted!", e);
    } finally {
      if (lock != null) {
        lock.release();
      }
    }
    assertNotNull(retFromThread.get());
    metricsAssertHelper.assertCounter("syncTimeNumOps", syncs + 1, source);

    // 3. Exception thrown in validation
    LOG.info("Next a batch put with one invalid family");
    puts[5].addColumn(Bytes.toBytes("BAD_CF"), qual, value);
    thrown.expect(NoSuchColumnFamilyException.class);
    this.region.batchMutate(puts, true);
  }

  @Test
  public void testBatchPutWithTsSlop() throws Exception {
    // add data with a timestamp that is too recent for range. Ensure assert
    CONF.setInt("hbase.hregion.keyvalue.timestamp.slop.millisecs", 1000);
    final Put[] puts = new Put[10];
    MetricsWALSource source = CompatibilitySingletonFactory.getInstance(MetricsWALSource.class);

    long syncs = prepareRegionForBachPut(puts, source, true);

    OperationStatus[] codes = this.region.batchMutate(puts);
    assertEquals(10, codes.length);
    for (int i = 0; i < 10; i++) {
      assertEquals(OperationStatusCode.SANITY_CHECK_FAILURE, codes[i].getOperationStatusCode());
    }
    metricsAssertHelper.assertCounter("syncTimeNumOps", syncs, source);
  }

  /**
   * @return syncs initial syncTimeNumOps
   */
  private long prepareRegionForBachPut(final Put[] puts, final MetricsWALSource source,
      boolean slop) throws IOException {
    this.region = initHRegion(tableName, method, CONF, COLUMN_FAMILY_BYTES);

    LOG.info("First a batch put with all valid puts");
    for (int i = 0; i < puts.length; i++) {
      puts[i] = slop ? new Put(Bytes.toBytes("row_" + i), Long.MAX_VALUE - 100) :
          new Put(Bytes.toBytes("row_" + i));
      puts[i].addColumn(COLUMN_FAMILY_BYTES, qual, value);
    }

    long syncs = metricsAssertHelper.getCounter("syncTimeNumOps", source);
    metricsAssertHelper.assertCounter("syncTimeNumOps", syncs, source);
    return syncs;
  }

  // ////////////////////////////////////////////////////////////////////////////
  // checkAndMutate tests
  // ////////////////////////////////////////////////////////////////////////////
  @Test
  @Deprecated
  public void testCheckAndMutate_WithEmptyRowValue() throws IOException {
    byte[] row1 = Bytes.toBytes("row1");
    byte[] fam1 = Bytes.toBytes("fam1");
    byte[] qf1 = Bytes.toBytes("qualifier");
    byte[] emptyVal = new byte[] {};
    byte[] val1 = Bytes.toBytes("value1");
    byte[] val2 = Bytes.toBytes("value2");

    // Setting up region
    this.region = initHRegion(tableName, method, CONF, fam1);
    // Putting empty data in key
    Put put = new Put(row1);
    put.addColumn(fam1, qf1, emptyVal);

    // checkAndPut with empty value
    boolean res = region.checkAndMutate(row1, fam1, qf1, CompareOperator.EQUAL,
        new BinaryComparator(emptyVal), put);
    assertTrue(res);

    // Putting data in key
    put = new Put(row1);
    put.addColumn(fam1, qf1, val1);

    // checkAndPut with correct value
    res = region.checkAndMutate(row1, fam1, qf1, CompareOperator.EQUAL,
        new BinaryComparator(emptyVal), put);
    assertTrue(res);

    // not empty anymore
    res = region.checkAndMutate(row1, fam1, qf1, CompareOperator.EQUAL,
        new BinaryComparator(emptyVal), put);
    assertFalse(res);

    Delete delete = new Delete(row1);
    delete.addColumn(fam1, qf1);
    res = region.checkAndMutate(row1, fam1, qf1, CompareOperator.EQUAL,
        new BinaryComparator(emptyVal), delete);
    assertFalse(res);

    put = new Put(row1);
    put.addColumn(fam1, qf1, val2);
    // checkAndPut with correct value
    res = region.checkAndMutate(row1, fam1, qf1, CompareOperator.EQUAL,
        new BinaryComparator(val1), put);
    assertTrue(res);

    // checkAndDelete with correct value
    delete = new Delete(row1);
    delete.addColumn(fam1, qf1);
    delete.addColumn(fam1, qf1);
    res = region.checkAndMutate(row1, fam1, qf1, CompareOperator.EQUAL,
        new BinaryComparator(val2), delete);
    assertTrue(res);

    delete = new Delete(row1);
    res = region.checkAndMutate(row1, fam1, qf1, CompareOperator.EQUAL,
        new BinaryComparator(emptyVal), delete);
    assertTrue(res);

    // checkAndPut looking for a null value
    put = new Put(row1);
    put.addColumn(fam1, qf1, val1);

    res = region
        .checkAndMutate(row1, fam1, qf1, CompareOperator.EQUAL, new NullComparator(), put);
    assertTrue(res);
  }

  @Test
  @Deprecated
  public void testCheckAndMutate_WithWrongValue() throws IOException {
    byte[] row1 = Bytes.toBytes("row1");
    byte[] fam1 = Bytes.toBytes("fam1");
    byte[] qf1 = Bytes.toBytes("qualifier");
    byte[] val1 = Bytes.toBytes("value1");
    byte[] val2 = Bytes.toBytes("value2");
    BigDecimal bd1 = new BigDecimal(Double.MAX_VALUE);
    BigDecimal bd2 = new BigDecimal(Double.MIN_VALUE);

    // Setting up region
    this.region = initHRegion(tableName, method, CONF, fam1);
    // Putting data in key
    Put put = new Put(row1);
    put.addColumn(fam1, qf1, val1);
    region.put(put);

    // checkAndPut with wrong value
    boolean res = region.checkAndMutate(row1, fam1, qf1, CompareOperator.EQUAL,
        new BinaryComparator(val2), put);
    assertEquals(false, res);

    // checkAndDelete with wrong value
    Delete delete = new Delete(row1);
    delete.addFamily(fam1);
    res = region.checkAndMutate(row1, fam1, qf1, CompareOperator.EQUAL,
        new BinaryComparator(val2), put);
    assertEquals(false, res);

    // Putting data in key
    put = new Put(row1);
    put.addColumn(fam1, qf1, Bytes.toBytes(bd1));
    region.put(put);

    // checkAndPut with wrong value
    res =
        region.checkAndMutate(row1, fam1, qf1, CompareOperator.EQUAL,
            new BigDecimalComparator(bd2), put);
    assertEquals(false, res);

    // checkAndDelete with wrong value
    delete = new Delete(row1);
    delete.addFamily(fam1);
    res =
        region.checkAndMutate(row1, fam1, qf1, CompareOperator.EQUAL,
            new BigDecimalComparator(bd2), put);
    assertEquals(false, res);
  }

  @Test
  @Deprecated
  public void testCheckAndMutate_WithCorrectValue() throws IOException {
    byte[] row1 = Bytes.toBytes("row1");
    byte[] fam1 = Bytes.toBytes("fam1");
    byte[] qf1 = Bytes.toBytes("qualifier");
    byte[] val1 = Bytes.toBytes("value1");
    BigDecimal bd1 = new BigDecimal(Double.MIN_VALUE);

    // Setting up region
    this.region = initHRegion(tableName, method, CONF, fam1);
    // Putting data in key
    long now = EnvironmentEdgeManager.currentTime();
    Put put = new Put(row1);
    put.addColumn(fam1, qf1, now, val1);
    region.put(put);

    // checkAndPut with correct value
    boolean res = region.checkAndMutate(row1, fam1, qf1, CompareOperator.EQUAL,
        new BinaryComparator(val1), put);
    assertEquals("First", true, res);

    // checkAndDelete with correct value
    Delete delete = new Delete(row1, now + 1);
    delete.addColumn(fam1, qf1);
    res = region.checkAndMutate(row1, fam1, qf1, CompareOperator.EQUAL, new BinaryComparator(val1),
        delete);
    assertEquals("Delete", true, res);

    // Putting data in key
    put = new Put(row1);
    put.addColumn(fam1, qf1, now + 2, Bytes.toBytes(bd1));
    region.put(put);

    // checkAndPut with correct value
    res =
        region.checkAndMutate(row1, fam1, qf1, CompareOperator.EQUAL, new BigDecimalComparator(
            bd1), put);
    assertEquals("Second put", true, res);

    // checkAndDelete with correct value
    delete = new Delete(row1, now + 3);
    delete.addColumn(fam1, qf1);
    res =
        region.checkAndMutate(row1, fam1, qf1, CompareOperator.EQUAL, new BigDecimalComparator(
            bd1), delete);
    assertEquals("Second delete", true, res);
  }

  @Test
  @Deprecated
  public void testCheckAndMutate_WithNonEqualCompareOp() throws IOException {
    byte[] row1 = Bytes.toBytes("row1");
    byte[] fam1 = Bytes.toBytes("fam1");
    byte[] qf1 = Bytes.toBytes("qualifier");
    byte[] val1 = Bytes.toBytes("value1");
    byte[] val2 = Bytes.toBytes("value2");
    byte[] val3 = Bytes.toBytes("value3");
    byte[] val4 = Bytes.toBytes("value4");

    // Setting up region
    this.region = initHRegion(tableName, method, CONF, fam1);
    // Putting val3 in key
    Put put = new Put(row1);
    put.addColumn(fam1, qf1, val3);
    region.put(put);

    // Test CompareOp.LESS: original = val3, compare with val3, fail
    boolean res = region.checkAndMutate(row1, fam1, qf1, CompareOperator.LESS,
        new BinaryComparator(val3), put);
    assertEquals(false, res);

    // Test CompareOp.LESS: original = val3, compare with val4, fail
    res = region.checkAndMutate(row1, fam1, qf1, CompareOperator.LESS,
        new BinaryComparator(val4), put);
    assertEquals(false, res);

    // Test CompareOp.LESS: original = val3, compare with val2,
    // succeed (now value = val2)
    put = new Put(row1);
    put.addColumn(fam1, qf1, val2);
    res = region.checkAndMutate(row1, fam1, qf1, CompareOperator.LESS,
        new BinaryComparator(val2), put);
    assertEquals(true, res);

    // Test CompareOp.LESS_OR_EQUAL: original = val2, compare with val3, fail
    res = region.checkAndMutate(row1, fam1, qf1, CompareOperator.LESS_OR_EQUAL,
        new BinaryComparator(val3), put);
    assertEquals(false, res);

    // Test CompareOp.LESS_OR_EQUAL: original = val2, compare with val2,
    // succeed (value still = val2)
    res = region.checkAndMutate(row1, fam1, qf1, CompareOperator.LESS_OR_EQUAL,
        new BinaryComparator(val2), put);
    assertEquals(true, res);

    // Test CompareOp.LESS_OR_EQUAL: original = val2, compare with val1,
    // succeed (now value = val3)
    put = new Put(row1);
    put.addColumn(fam1, qf1, val3);
    res = region.checkAndMutate(row1, fam1, qf1, CompareOperator.LESS_OR_EQUAL,
        new BinaryComparator(val1), put);
    assertEquals(true, res);

    // Test CompareOp.GREATER: original = val3, compare with val3, fail
    res = region.checkAndMutate(row1, fam1, qf1, CompareOperator.GREATER,
        new BinaryComparator(val3), put);
    assertEquals(false, res);

    // Test CompareOp.GREATER: original = val3, compare with val2, fail
    res = region.checkAndMutate(row1, fam1, qf1, CompareOperator.GREATER,
        new BinaryComparator(val2), put);
    assertEquals(false, res);

    // Test CompareOp.GREATER: original = val3, compare with val4,
    // succeed (now value = val2)
    put = new Put(row1);
    put.addColumn(fam1, qf1, val2);
    res = region.checkAndMutate(row1, fam1, qf1, CompareOperator.GREATER,
        new BinaryComparator(val4), put);
    assertEquals(true, res);

    // Test CompareOp.GREATER_OR_EQUAL: original = val2, compare with val1, fail
    res = region.checkAndMutate(row1, fam1, qf1, CompareOperator.GREATER_OR_EQUAL,
        new BinaryComparator(val1), put);
    assertEquals(false, res);

    // Test CompareOp.GREATER_OR_EQUAL: original = val2, compare with val2,
    // succeed (value still = val2)
    res = region.checkAndMutate(row1, fam1, qf1, CompareOperator.GREATER_OR_EQUAL,
        new BinaryComparator(val2), put);
    assertEquals(true, res);

    // Test CompareOp.GREATER_OR_EQUAL: original = val2, compare with val3, succeed
    res = region.checkAndMutate(row1, fam1, qf1, CompareOperator.GREATER_OR_EQUAL,
        new BinaryComparator(val3), put);
    assertEquals(true, res);
  }

  @Test
  @Deprecated
  public void testCheckAndPut_ThatPutWasWritten() throws IOException {
    byte[] row1 = Bytes.toBytes("row1");
    byte[] fam1 = Bytes.toBytes("fam1");
    byte[] fam2 = Bytes.toBytes("fam2");
    byte[] qf1 = Bytes.toBytes("qualifier");
    byte[] val1 = Bytes.toBytes("value1");
    byte[] val2 = Bytes.toBytes("value2");

    byte[][] families = { fam1, fam2 };

    // Setting up region
    this.region = initHRegion(tableName, method, CONF, families);
    // Putting data in the key to check
    Put put = new Put(row1);
    put.addColumn(fam1, qf1, val1);
    region.put(put);

    // Creating put to add
    long ts = EnvironmentEdgeManager.currentTime();
    KeyValue kv = new KeyValue(row1, fam2, qf1, ts, KeyValue.Type.Put, val2);
    put = new Put(row1);
    put.add(kv);

    // checkAndPut with wrong value
    boolean res = region.checkAndMutate(row1, fam1, qf1, CompareOperator.EQUAL,
        new BinaryComparator(val1), put);
    assertEquals(true, res);

    Get get = new Get(row1);
    get.addColumn(fam2, qf1);
    Cell[] actual = region.get(get).rawCells();

    Cell[] expected = { kv };

    assertEquals(expected.length, actual.length);
    for (int i = 0; i < actual.length; i++) {
      assertEquals(expected[i], actual[i]);
    }
  }

  @Test
  @Deprecated
  public void testCheckAndPut_wrongRowInPut() throws IOException {
    this.region = initHRegion(tableName, method, CONF, COLUMNS);
    Put put = new Put(row2);
    put.addColumn(fam1, qual1, value1);
    try {
      region.checkAndMutate(row, fam1, qual1, CompareOperator.EQUAL,
          new BinaryComparator(value2), put);
      fail();
    } catch (org.apache.hadoop.hbase.DoNotRetryIOException expected) {
      // expected exception.
    }
  }

  @Test
  @Deprecated
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
    this.region = initHRegion(tableName, method, CONF, families);
    // Put content
    Put put = new Put(row1);
    put.addColumn(fam1, qf1, val1);
    region.put(put);
    Threads.sleep(2);

    put = new Put(row1);
    put.addColumn(fam1, qf1, val2);
    put.addColumn(fam2, qf1, val3);
    put.addColumn(fam2, qf2, val2);
    put.addColumn(fam2, qf3, val1);
    put.addColumn(fam1, qf3, val1);
    region.put(put);

    LOG.info("get={}", region.get(new Get(row1).addColumn(fam1, qf1)).toString());

    // Multi-column delete
    Delete delete = new Delete(row1);
    delete.addColumn(fam1, qf1);
    delete.addColumn(fam2, qf1);
    delete.addColumn(fam1, qf3);
    boolean res = region.checkAndMutate(row1, fam1, qf1, CompareOperator.EQUAL,
        new BinaryComparator(val2), delete);
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
    delete.addFamily(fam2);
    res = region.checkAndMutate(row1, fam2, qf1, CompareOperator.EQUAL,
        new BinaryComparator(emptyVal), delete);
    assertEquals(true, res);

    get = new Get(row1);
    r = region.get(get);
    assertEquals(1, r.size());
    assertArrayEquals(val1, r.getValue(fam1, qf1));

    // Row delete
    delete = new Delete(row1);
    res = region.checkAndMutate(row1, fam1, qf1, CompareOperator.EQUAL, new BinaryComparator(val1),
        delete);
    assertEquals(true, res);
    get = new Get(row1);
    r = region.get(get);
    assertEquals(0, r.size());
  }

  @Test
  @Deprecated
  public void testCheckAndMutate_WithFilters() throws Throwable {
    final byte[] FAMILY = Bytes.toBytes("fam");

    // Setting up region
    this.region = initHRegion(tableName, method, CONF, FAMILY);

    // Put one row
    Put put = new Put(row);
    put.addColumn(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("a"));
    put.addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("b"));
    put.addColumn(FAMILY, Bytes.toBytes("C"), Bytes.toBytes("c"));
    region.put(put);

    // Put with success
    boolean ok = region.checkAndMutate(row,
      new FilterList(
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"), CompareOperator.EQUAL,
          Bytes.toBytes("a")),
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("B"), CompareOperator.EQUAL,
          Bytes.toBytes("b"))
      ),
      new Put(row).addColumn(FAMILY, Bytes.toBytes("D"), Bytes.toBytes("d")));
    assertTrue(ok);

    Result result = region.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("D")));
    assertEquals("d", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("D"))));

    // Put with failure
    ok = region.checkAndMutate(row,
      new FilterList(
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"), CompareOperator.EQUAL,
          Bytes.toBytes("a")),
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("B"), CompareOperator.EQUAL,
          Bytes.toBytes("c"))
      ),
      new Put(row).addColumn(FAMILY, Bytes.toBytes("E"), Bytes.toBytes("e")));
    assertFalse(ok);

    assertTrue(region.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("E"))).isEmpty());

    // Delete with success
    ok = region.checkAndMutate(row,
      new FilterList(
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"), CompareOperator.EQUAL,
          Bytes.toBytes("a")),
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("B"), CompareOperator.EQUAL,
          Bytes.toBytes("b"))
      ),
      new Delete(row).addColumns(FAMILY, Bytes.toBytes("D")));
    assertTrue(ok);

    assertTrue(region.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("D"))).isEmpty());

    // Mutate with success
    ok = region.checkAndRowMutate(row,
      new FilterList(
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"), CompareOperator.EQUAL,
          Bytes.toBytes("a")),
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("B"), CompareOperator.EQUAL,
          Bytes.toBytes("b"))
      ),
      new RowMutations(row)
        .add((Mutation) new Put(row)
          .addColumn(FAMILY, Bytes.toBytes("E"), Bytes.toBytes("e")))
        .add((Mutation) new Delete(row).addColumns(FAMILY, Bytes.toBytes("A"))));
    assertTrue(ok);

    result = region.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("E")));
    assertEquals("e", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("E"))));

    assertTrue(region.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("A"))).isEmpty());
  }

  @Test
  @Deprecated
  public void testCheckAndMutate_WithFiltersAndTimeRange() throws Throwable {
    final byte[] FAMILY = Bytes.toBytes("fam");

    // Setting up region
    this.region = initHRegion(tableName, method, CONF, FAMILY);

    // Put with specifying the timestamp
    region.put(new Put(row).addColumn(FAMILY, Bytes.toBytes("A"), 100, Bytes.toBytes("a")));

    // Put with success
    boolean ok = region.checkAndMutate(row,
      new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"), CompareOperator.EQUAL,
        Bytes.toBytes("a")),
      TimeRange.between(0, 101),
      new Put(row).addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("b")));
    assertTrue(ok);

    Result result = region.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("B")));
    assertEquals("b", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("B"))));

    // Put with failure
    ok = region.checkAndMutate(row,
      new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"), CompareOperator.EQUAL,
        Bytes.toBytes("a")),
      TimeRange.between(0, 100),
      new Put(row).addColumn(FAMILY, Bytes.toBytes("C"), Bytes.toBytes("c")));
    assertFalse(ok);

    assertTrue(region.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("C"))).isEmpty());

    // Mutate with success
    ok = region.checkAndRowMutate(row,
      new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"), CompareOperator.EQUAL,
        Bytes.toBytes("a")),
      TimeRange.between(0, 101),
      new RowMutations(row)
        .add((Mutation) new Put(row)
          .addColumn(FAMILY, Bytes.toBytes("D"), Bytes.toBytes("d")))
        .add((Mutation) new Delete(row).addColumns(FAMILY, Bytes.toBytes("A"))));
    assertTrue(ok);

    result = region.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("D")));
    assertEquals("d", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("D"))));

    assertTrue(region.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("A"))).isEmpty());
  }

  @Test
  @Deprecated
  public void testCheckAndMutate_wrongMutationType() throws Throwable {
    // Setting up region
    this.region = initHRegion(tableName, method, CONF, fam1);

    try {
      region.checkAndMutate(row, fam1, qual1, CompareOperator.EQUAL, new BinaryComparator(value1),
        new Increment(row).addColumn(fam1, qual1, 1));
      fail("should throw DoNotRetryIOException");
    } catch (DoNotRetryIOException e) {
      assertEquals("Unsupported mutate type: INCREMENT", e.getMessage());
    }

    try {
      region.checkAndMutate(row,
        new SingleColumnValueFilter(fam1, qual1, CompareOperator.EQUAL, value1),
        new Increment(row).addColumn(fam1, qual1, 1));
      fail("should throw DoNotRetryIOException");
    } catch (DoNotRetryIOException e) {
      assertEquals("Unsupported mutate type: INCREMENT", e.getMessage());
    }
  }

  @Test
  @Deprecated
  public void testCheckAndMutate_wrongRow() throws Throwable {
    final byte[] wrongRow = Bytes.toBytes("wrongRow");

    // Setting up region
    this.region = initHRegion(tableName, method, CONF, fam1);

    try {
      region.checkAndMutate(row, fam1, qual1, CompareOperator.EQUAL, new BinaryComparator(value1),
        new Put(wrongRow).addColumn(fam1, qual1, value1));
      fail("should throw DoNotRetryIOException");
    } catch (DoNotRetryIOException e) {
      assertEquals("The row of the action <wrongRow> doesn't match the original one <rowA>",
        e.getMessage());
    }

    try {
      region.checkAndMutate(row,
        new SingleColumnValueFilter(fam1, qual1, CompareOperator.EQUAL, value1),
        new Put(wrongRow).addColumn(fam1, qual1, value1));
      fail("should throw DoNotRetryIOException");
    } catch (DoNotRetryIOException e) {
      assertEquals("The row of the action <wrongRow> doesn't match the original one <rowA>",
        e.getMessage());
    }

    try {
      region.checkAndRowMutate(row, fam1, qual1, CompareOperator.EQUAL,
        new BinaryComparator(value1),
        new RowMutations(wrongRow)
          .add((Mutation) new Put(wrongRow)
            .addColumn(fam1, qual1, value1))
          .add((Mutation) new Delete(wrongRow).addColumns(fam1, qual2)));
      fail("should throw DoNotRetryIOException");
    } catch (DoNotRetryIOException e) {
      assertEquals("The row of the action <wrongRow> doesn't match the original one <rowA>",
        e.getMessage());
    }

    try {
      region.checkAndRowMutate(row,
        new SingleColumnValueFilter(fam1, qual1, CompareOperator.EQUAL, value1),
        new RowMutations(wrongRow)
          .add((Mutation) new Put(wrongRow)
            .addColumn(fam1, qual1, value1))
          .add((Mutation) new Delete(wrongRow).addColumns(fam1, qual2)));
      fail("should throw DoNotRetryIOException");
    } catch (DoNotRetryIOException e) {
      assertEquals("The row of the action <wrongRow> doesn't match the original one <rowA>",
        e.getMessage());
    }
  }

  @Test
  public void testCheckAndMutateWithEmptyRowValue() throws IOException {
    byte[] row1 = Bytes.toBytes("row1");
    byte[] fam1 = Bytes.toBytes("fam1");
    byte[] qf1 = Bytes.toBytes("qualifier");
    byte[] emptyVal = new byte[] {};
    byte[] val1 = Bytes.toBytes("value1");
    byte[] val2 = Bytes.toBytes("value2");

    // Setting up region
    this.region = initHRegion(tableName, method, CONF, fam1);
    // Putting empty data in key
    Put put = new Put(row1);
    put.addColumn(fam1, qf1, emptyVal);

    // checkAndPut with empty value
    CheckAndMutateResult res = region.checkAndMutate(CheckAndMutate.newBuilder(row1)
      .ifMatches(fam1, qf1, CompareOperator.EQUAL, emptyVal).build(put));
    assertTrue(res.isSuccess());
    assertNull(res.getResult());

    // Putting data in key
    put = new Put(row1);
    put.addColumn(fam1, qf1, val1);

    // checkAndPut with correct value
    res = region.checkAndMutate(CheckAndMutate.newBuilder(row1)
      .ifMatches(fam1, qf1, CompareOperator.EQUAL, emptyVal).build(put));
    assertTrue(res.isSuccess());
    assertNull(res.getResult());

    // not empty anymore
    res = region.checkAndMutate(CheckAndMutate.newBuilder(row1)
      .ifMatches(fam1, qf1, CompareOperator.EQUAL, emptyVal).build(put));
    assertFalse(res.isSuccess());
    assertNull(res.getResult());

    Delete delete = new Delete(row1);
    delete.addColumn(fam1, qf1);
    res = region.checkAndMutate(CheckAndMutate.newBuilder(row1)
      .ifMatches(fam1, qf1, CompareOperator.EQUAL, emptyVal).build(delete));
    assertFalse(res.isSuccess());
    assertNull(res.getResult());

    put = new Put(row1);
    put.addColumn(fam1, qf1, val2);
    // checkAndPut with correct value
    res = region.checkAndMutate(CheckAndMutate.newBuilder(row1)
      .ifMatches(fam1, qf1, CompareOperator.EQUAL, val1).build(put));
    assertTrue(res.isSuccess());
    assertNull(res.getResult());

    // checkAndDelete with correct value
    delete = new Delete(row1);
    delete.addColumn(fam1, qf1);
    delete.addColumn(fam1, qf1);
    res = region.checkAndMutate(CheckAndMutate.newBuilder(row1)
      .ifMatches(fam1, qf1, CompareOperator.EQUAL, val2).build(delete));
    assertTrue(res.isSuccess());
    assertNull(res.getResult());

    delete = new Delete(row1);
    res = region.checkAndMutate(CheckAndMutate.newBuilder(row1)
      .ifMatches(fam1, qf1, CompareOperator.EQUAL, emptyVal).build(delete));
    assertTrue(res.isSuccess());
    assertNull(res.getResult());

    // checkAndPut looking for a null value
    put = new Put(row1);
    put.addColumn(fam1, qf1, val1);

    res = region.checkAndMutate(CheckAndMutate.newBuilder(row1).ifNotExists(fam1, qf1)
      .build(put));
    assertTrue(res.isSuccess());
    assertNull(res.getResult());
  }

  @Test
  public void testCheckAndMutateWithWrongValue() throws IOException {
    byte[] row1 = Bytes.toBytes("row1");
    byte[] fam1 = Bytes.toBytes("fam1");
    byte[] qf1 = Bytes.toBytes("qualifier");
    byte[] val1 = Bytes.toBytes("value1");
    byte[] val2 = Bytes.toBytes("value2");
    BigDecimal bd1 = new BigDecimal(Double.MAX_VALUE);
    BigDecimal bd2 = new BigDecimal(Double.MIN_VALUE);

    // Setting up region
    this.region = initHRegion(tableName, method, CONF, fam1);
    // Putting data in key
    Put put = new Put(row1);
    put.addColumn(fam1, qf1, val1);
    region.put(put);

    // checkAndPut with wrong value
    CheckAndMutateResult res = region.checkAndMutate(CheckAndMutate.newBuilder(row1)
      .ifMatches(fam1, qf1, CompareOperator.EQUAL, val2).build(put));
    assertFalse(res.isSuccess());
    assertNull(res.getResult());

    // checkAndDelete with wrong value
    Delete delete = new Delete(row1);
    delete.addFamily(fam1);
    res = region.checkAndMutate(CheckAndMutate.newBuilder(row1)
      .ifMatches(fam1, qf1, CompareOperator.EQUAL, val2).build(put));
    assertFalse(res.isSuccess());
    assertNull(res.getResult());

    // Putting data in key
    put = new Put(row1);
    put.addColumn(fam1, qf1, Bytes.toBytes(bd1));
    region.put(put);

    // checkAndPut with wrong value
    res = region.checkAndMutate(CheckAndMutate.newBuilder(row1)
      .ifMatches(fam1, qf1, CompareOperator.EQUAL, Bytes.toBytes(bd2)).build(put));
    assertFalse(res.isSuccess());
    assertNull(res.getResult());

    // checkAndDelete with wrong value
    delete = new Delete(row1);
    delete.addFamily(fam1);
    res = region.checkAndMutate(CheckAndMutate.newBuilder(row1)
      .ifMatches(fam1, qf1, CompareOperator.EQUAL, Bytes.toBytes(bd2)).build(delete));
    assertFalse(res.isSuccess());
    assertNull(res.getResult());
  }

  @Test
  public void testCheckAndMutateWithCorrectValue() throws IOException {
    byte[] row1 = Bytes.toBytes("row1");
    byte[] fam1 = Bytes.toBytes("fam1");
    byte[] qf1 = Bytes.toBytes("qualifier");
    byte[] val1 = Bytes.toBytes("value1");
    BigDecimal bd1 = new BigDecimal(Double.MIN_VALUE);

    // Setting up region
    this.region = initHRegion(tableName, method, CONF, fam1);
    // Putting data in key
    long now = EnvironmentEdgeManager.currentTime();
    Put put = new Put(row1);
    put.addColumn(fam1, qf1, now, val1);
    region.put(put);

    // checkAndPut with correct value
    CheckAndMutateResult res = region.checkAndMutate(CheckAndMutate.newBuilder(row1)
      .ifMatches(fam1, qf1, CompareOperator.EQUAL, val1).build(put));
    assertTrue("First", res.isSuccess());

    // checkAndDelete with correct value
    Delete delete = new Delete(row1, now + 1);
    delete.addColumn(fam1, qf1);
    res = region.checkAndMutate(CheckAndMutate.newBuilder(row1)
      .ifMatches(fam1, qf1, CompareOperator.EQUAL, val1).build(delete));
    assertTrue("Delete", res.isSuccess());
    assertNull(res.getResult());

    // Putting data in key
    put = new Put(row1);
    put.addColumn(fam1, qf1, now + 2, Bytes.toBytes(bd1));
    region.put(put);

    // checkAndPut with correct value
    res = region.checkAndMutate(CheckAndMutate.newBuilder(row1)
        .ifMatches(fam1, qf1, CompareOperator.EQUAL, Bytes.toBytes(bd1)).build(put));
    assertTrue("Second put", res.isSuccess());
    assertNull(res.getResult());

    // checkAndDelete with correct value
    delete = new Delete(row1, now + 3);
    delete.addColumn(fam1, qf1);
    res = region.checkAndMutate(CheckAndMutate.newBuilder(row1)
        .ifMatches(fam1, qf1, CompareOperator.EQUAL, Bytes.toBytes(bd1)).build(delete));
    assertTrue("Second delete", res.isSuccess());
    assertNull(res.getResult());
  }

  @Test
  public void testCheckAndMutateWithNonEqualCompareOp() throws IOException {
    byte[] row1 = Bytes.toBytes("row1");
    byte[] fam1 = Bytes.toBytes("fam1");
    byte[] qf1 = Bytes.toBytes("qualifier");
    byte[] val1 = Bytes.toBytes("value1");
    byte[] val2 = Bytes.toBytes("value2");
    byte[] val3 = Bytes.toBytes("value3");
    byte[] val4 = Bytes.toBytes("value4");

    // Setting up region
    this.region = initHRegion(tableName, method, CONF, fam1);
    // Putting val3 in key
    Put put = new Put(row1);
    put.addColumn(fam1, qf1, val3);
    region.put(put);

    // Test CompareOp.LESS: original = val3, compare with val3, fail
    CheckAndMutateResult res = region.checkAndMutate(CheckAndMutate.newBuilder(row1)
      .ifMatches(fam1, qf1, CompareOperator.LESS, val3).build(put));
    assertFalse(res.isSuccess());
    assertNull(res.getResult());

    // Test CompareOp.LESS: original = val3, compare with val4, fail
    res = region.checkAndMutate(CheckAndMutate.newBuilder(row1)
      .ifMatches(fam1, qf1, CompareOperator.LESS, val4).build(put));
    assertFalse(res.isSuccess());
    assertNull(res.getResult());

    // Test CompareOp.LESS: original = val3, compare with val2,
    // succeed (now value = val2)
    put = new Put(row1);
    put.addColumn(fam1, qf1, val2);
    res = region.checkAndMutate(CheckAndMutate.newBuilder(row1)
      .ifMatches(fam1, qf1, CompareOperator.LESS, val2).build(put));
    assertTrue(res.isSuccess());
    assertNull(res.getResult());

    // Test CompareOp.LESS_OR_EQUAL: original = val2, compare with val3, fail
    res = region.checkAndMutate(CheckAndMutate.newBuilder(row1)
      .ifMatches(fam1, qf1, CompareOperator.LESS_OR_EQUAL, val3).build(put));
    assertFalse(res.isSuccess());
    assertNull(res.getResult());

    // Test CompareOp.LESS_OR_EQUAL: original = val2, compare with val2,
    // succeed (value still = val2)
    res = region.checkAndMutate(CheckAndMutate.newBuilder(row1)
      .ifMatches(fam1, qf1, CompareOperator.LESS_OR_EQUAL, val2).build(put));
    assertTrue(res.isSuccess());
    assertNull(res.getResult());

    // Test CompareOp.LESS_OR_EQUAL: original = val2, compare with val1,
    // succeed (now value = val3)
    put = new Put(row1);
    put.addColumn(fam1, qf1, val3);
    res = region.checkAndMutate(CheckAndMutate.newBuilder(row1)
      .ifMatches(fam1, qf1, CompareOperator.LESS_OR_EQUAL, val1).build(put));
    assertTrue(res.isSuccess());
    assertNull(res.getResult());

    // Test CompareOp.GREATER: original = val3, compare with val3, fail
    res = region.checkAndMutate(CheckAndMutate.newBuilder(row1)
      .ifMatches(fam1, qf1, CompareOperator.GREATER, val3).build(put));
    assertFalse(res.isSuccess());
    assertNull(res.getResult());

    // Test CompareOp.GREATER: original = val3, compare with val2, fail
    res = region.checkAndMutate(CheckAndMutate.newBuilder(row1)
      .ifMatches(fam1, qf1, CompareOperator.GREATER, val2).build(put));
    assertFalse(res.isSuccess());
    assertNull(res.getResult());

    // Test CompareOp.GREATER: original = val3, compare with val4,
    // succeed (now value = val2)
    put = new Put(row1);
    put.addColumn(fam1, qf1, val2);
    res = region.checkAndMutate(CheckAndMutate.newBuilder(row1)
      .ifMatches(fam1, qf1, CompareOperator.GREATER, val4).build(put));
    assertTrue(res.isSuccess());
    assertNull(res.getResult());

    // Test CompareOp.GREATER_OR_EQUAL: original = val2, compare with val1, fail
    res = region.checkAndMutate(CheckAndMutate.newBuilder(row1)
      .ifMatches(fam1, qf1, CompareOperator.GREATER_OR_EQUAL, val1).build(put));
    assertFalse(res.isSuccess());
    assertNull(res.getResult());

    // Test CompareOp.GREATER_OR_EQUAL: original = val2, compare with val2,
    // succeed (value still = val2)
    res = region.checkAndMutate(CheckAndMutate.newBuilder(row1)
      .ifMatches(fam1, qf1, CompareOperator.GREATER_OR_EQUAL, val2).build(put));
    assertTrue(res.isSuccess());
    assertNull(res.getResult());

    // Test CompareOp.GREATER_OR_EQUAL: original = val2, compare with val3, succeed
    res = region.checkAndMutate(CheckAndMutate.newBuilder(row1)
      .ifMatches(fam1, qf1, CompareOperator.GREATER_OR_EQUAL, val3).build(put));
    assertTrue(res.isSuccess());
    assertNull(res.getResult());
  }

  @Test
  public void testCheckAndPutThatPutWasWritten() throws IOException {
    byte[] row1 = Bytes.toBytes("row1");
    byte[] fam1 = Bytes.toBytes("fam1");
    byte[] fam2 = Bytes.toBytes("fam2");
    byte[] qf1 = Bytes.toBytes("qualifier");
    byte[] val1 = Bytes.toBytes("value1");
    byte[] val2 = Bytes.toBytes("value2");

    byte[][] families = { fam1, fam2 };

    // Setting up region
    this.region = initHRegion(tableName, method, CONF, families);
    // Putting data in the key to check
    Put put = new Put(row1);
    put.addColumn(fam1, qf1, val1);
    region.put(put);

    // Creating put to add
    long ts = EnvironmentEdgeManager.currentTime();
    KeyValue kv = new KeyValue(row1, fam2, qf1, ts, KeyValue.Type.Put, val2);
    put = new Put(row1);
    put.add(kv);

    // checkAndPut with wrong value
    CheckAndMutateResult res = region.checkAndMutate(CheckAndMutate.newBuilder(row1)
      .ifMatches(fam1, qf1, CompareOperator.EQUAL, val1).build(put));
    assertTrue(res.isSuccess());
    assertNull(res.getResult());

    Get get = new Get(row1);
    get.addColumn(fam2, qf1);
    Cell[] actual = region.get(get).rawCells();

    Cell[] expected = { kv };

    assertEquals(expected.length, actual.length);
    for (int i = 0; i < actual.length; i++) {
      assertEquals(expected[i], actual[i]);
    }
  }

  @Test
  public void testCheckAndDeleteThatDeleteWasWritten() throws IOException {
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
    this.region = initHRegion(tableName, method, CONF, families);
    // Put content
    Put put = new Put(row1);
    put.addColumn(fam1, qf1, val1);
    region.put(put);
    Threads.sleep(2);

    put = new Put(row1);
    put.addColumn(fam1, qf1, val2);
    put.addColumn(fam2, qf1, val3);
    put.addColumn(fam2, qf2, val2);
    put.addColumn(fam2, qf3, val1);
    put.addColumn(fam1, qf3, val1);
    region.put(put);

    LOG.info("get={}", region.get(new Get(row1).addColumn(fam1, qf1)).toString());

    // Multi-column delete
    Delete delete = new Delete(row1);
    delete.addColumn(fam1, qf1);
    delete.addColumn(fam2, qf1);
    delete.addColumn(fam1, qf3);
    CheckAndMutateResult res = region.checkAndMutate(CheckAndMutate.newBuilder(row1)
      .ifMatches(fam1, qf1, CompareOperator.EQUAL, val2).build(delete));
    assertTrue(res.isSuccess());
    assertNull(res.getResult());

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
    delete.addFamily(fam2);
    res = region.checkAndMutate(CheckAndMutate.newBuilder(row1)
      .ifMatches(fam2, qf1, CompareOperator.EQUAL, emptyVal).build(delete));
    assertTrue(res.isSuccess());
    assertNull(res.getResult());

    get = new Get(row1);
    r = region.get(get);
    assertEquals(1, r.size());
    assertArrayEquals(val1, r.getValue(fam1, qf1));

    // Row delete
    delete = new Delete(row1);
    res = region.checkAndMutate(CheckAndMutate.newBuilder(row1)
      .ifMatches(fam1, qf1, CompareOperator.EQUAL, val1).build(delete));
    assertTrue(res.isSuccess());
    assertNull(res.getResult());

    get = new Get(row1);
    r = region.get(get);
    assertEquals(0, r.size());
  }

  @Test
  public void testCheckAndMutateWithFilters() throws Throwable {
    final byte[] FAMILY = Bytes.toBytes("fam");

    // Setting up region
    this.region = initHRegion(tableName, method, CONF, FAMILY);

    // Put one row
    Put put = new Put(row);
    put.addColumn(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("a"));
    put.addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("b"));
    put.addColumn(FAMILY, Bytes.toBytes("C"), Bytes.toBytes("c"));
    region.put(put);

    // Put with success
    CheckAndMutateResult res = region.checkAndMutate(CheckAndMutate.newBuilder(row)
      .ifMatches(new FilterList(
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"), CompareOperator.EQUAL,
          Bytes.toBytes("a")),
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("B"), CompareOperator.EQUAL,
          Bytes.toBytes("b"))))
      .build(new Put(row).addColumn(FAMILY, Bytes.toBytes("D"), Bytes.toBytes("d"))));
    assertTrue(res.isSuccess());
    assertNull(res.getResult());

    Result result = region.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("D")));
    assertEquals("d", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("D"))));

    // Put with failure
    res = region.checkAndMutate(CheckAndMutate.newBuilder(row)
      .ifMatches(new FilterList(
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"), CompareOperator.EQUAL,
          Bytes.toBytes("a")),
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("B"), CompareOperator.EQUAL,
          Bytes.toBytes("c"))))
      .build(new Put(row).addColumn(FAMILY, Bytes.toBytes("E"), Bytes.toBytes("e"))));
    assertFalse(res.isSuccess());
    assertNull(res.getResult());

    assertTrue(region.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("E"))).isEmpty());

    // Delete with success
    res = region.checkAndMutate(CheckAndMutate.newBuilder(row)
      .ifMatches(new FilterList(
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"), CompareOperator.EQUAL,
          Bytes.toBytes("a")),
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("B"), CompareOperator.EQUAL,
          Bytes.toBytes("b"))))
      .build(new Delete(row).addColumns(FAMILY, Bytes.toBytes("D"))));
    assertTrue(res.isSuccess());
    assertNull(res.getResult());

    assertTrue(region.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("D"))).isEmpty());

    // Mutate with success
    res = region.checkAndMutate(CheckAndMutate.newBuilder(row)
      .ifMatches(new FilterList(
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"), CompareOperator.EQUAL,
          Bytes.toBytes("a")),
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("B"), CompareOperator.EQUAL,
          Bytes.toBytes("b"))))
      .build(new RowMutations(row)
        .add((Mutation) new Put(row)
          .addColumn(FAMILY, Bytes.toBytes("E"), Bytes.toBytes("e")))
        .add((Mutation) new Delete(row).addColumns(FAMILY, Bytes.toBytes("A")))));
    assertTrue(res.isSuccess());
    assertNull(res.getResult());

    result = region.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("E")));
    assertEquals("e", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("E"))));

    assertTrue(region.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("A"))).isEmpty());
  }

  @Test
  public void testCheckAndMutateWithFiltersAndTimeRange() throws Throwable {
    final byte[] FAMILY = Bytes.toBytes("fam");

    // Setting up region
    this.region = initHRegion(tableName, method, CONF, FAMILY);

    // Put with specifying the timestamp
    region.put(new Put(row).addColumn(FAMILY, Bytes.toBytes("A"), 100, Bytes.toBytes("a")));

    // Put with success
    CheckAndMutateResult res = region.checkAndMutate(CheckAndMutate.newBuilder(row)
      .ifMatches(new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"), CompareOperator.EQUAL,
        Bytes.toBytes("a")))
      .timeRange(TimeRange.between(0, 101))
      .build(new Put(row).addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("b"))));
    assertTrue(res.isSuccess());
    assertNull(res.getResult());

    Result result = region.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("B")));
    assertEquals("b", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("B"))));

    // Put with failure
    res = region.checkAndMutate(CheckAndMutate.newBuilder(row)
      .ifMatches(new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"), CompareOperator.EQUAL,
        Bytes.toBytes("a")))
      .timeRange(TimeRange.between(0, 100))
      .build(new Put(row).addColumn(FAMILY, Bytes.toBytes("C"), Bytes.toBytes("c"))));
    assertFalse(res.isSuccess());
    assertNull(res.getResult());

    assertTrue(region.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("C"))).isEmpty());

    // RowMutations with success
    res = region.checkAndMutate(CheckAndMutate.newBuilder(row)
      .ifMatches(new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"), CompareOperator.EQUAL,
        Bytes.toBytes("a")))
      .timeRange(TimeRange.between(0, 101))
      .build(new RowMutations(row)
        .add((Mutation) new Put(row)
          .addColumn(FAMILY, Bytes.toBytes("D"), Bytes.toBytes("d")))
        .add((Mutation) new Delete(row).addColumns(FAMILY, Bytes.toBytes("A")))));
    assertTrue(res.isSuccess());
    assertNull(res.getResult());

    result = region.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("D")));
    assertEquals("d", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("D"))));

    assertTrue(region.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("A"))).isEmpty());
  }

  @Test
  public void testCheckAndIncrement() throws Throwable {
    final byte[] FAMILY = Bytes.toBytes("fam");

    // Setting up region
    this.region = initHRegion(tableName, method, CONF, FAMILY);

    region.put(new Put(row).addColumn(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("a")));

    // CheckAndIncrement with correct value
    CheckAndMutateResult res = region.checkAndMutate(CheckAndMutate.newBuilder(row)
        .ifEquals(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("a"))
        .build(new Increment(row).addColumn(FAMILY, Bytes.toBytes("B"), 1)));
    assertTrue(res.isSuccess());
    assertEquals(1, Bytes.toLong(res.getResult().getValue(FAMILY, Bytes.toBytes("B"))));

    Result result = region.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("B")));
    assertEquals(1, Bytes.toLong(result.getValue(FAMILY, Bytes.toBytes("B"))));

    // CheckAndIncrement with wrong value
    res = region.checkAndMutate(CheckAndMutate.newBuilder(row)
      .ifEquals(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("b"))
      .build(new Increment(row).addColumn(FAMILY, Bytes.toBytes("B"), 1)));
    assertFalse(res.isSuccess());
    assertNull(res.getResult());

    result = region.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("B")));
    assertEquals(1, Bytes.toLong(result.getValue(FAMILY, Bytes.toBytes("B"))));

    region.put(new Put(row).addColumn(FAMILY, Bytes.toBytes("C"), Bytes.toBytes("c")));

    // CheckAndIncrement with a filter and correct value
    res = region.checkAndMutate(CheckAndMutate.newBuilder(row)
      .ifMatches(new FilterList(
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"), CompareOperator.EQUAL,
          Bytes.toBytes("a")),
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("C"), CompareOperator.EQUAL,
          Bytes.toBytes("c"))))
      .build(new Increment(row).addColumn(FAMILY, Bytes.toBytes("B"), 2)));
    assertTrue(res.isSuccess());
    assertEquals(3, Bytes.toLong(res.getResult().getValue(FAMILY, Bytes.toBytes("B"))));

    result = region.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("B")));
    assertEquals(3, Bytes.toLong(result.getValue(FAMILY, Bytes.toBytes("B"))));

    // CheckAndIncrement with a filter and correct value
    res = region.checkAndMutate(CheckAndMutate.newBuilder(row)
      .ifMatches(new FilterList(
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"), CompareOperator.EQUAL,
          Bytes.toBytes("b")),
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("C"), CompareOperator.EQUAL,
          Bytes.toBytes("d"))))
      .build(new Increment(row).addColumn(FAMILY, Bytes.toBytes("B"), 2)));
    assertFalse(res.isSuccess());
    assertNull(res.getResult());

    result = region.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("B")));
    assertEquals(3, Bytes.toLong(result.getValue(FAMILY, Bytes.toBytes("B"))));
  }

  @Test
  public void testCheckAndAppend() throws Throwable {
    final byte[] FAMILY = Bytes.toBytes("fam");

    // Setting up region
    this.region = initHRegion(tableName, method, CONF, FAMILY);

    region.put(new Put(row).addColumn(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("a")));

    // CheckAndAppend with correct value
    CheckAndMutateResult res =
      region.checkAndMutate(CheckAndMutate.newBuilder(row)
        .ifEquals(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("a"))
        .build(new Append(row).addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("b"))));
    assertTrue(res.isSuccess());
    assertEquals("b", Bytes.toString(res.getResult().getValue(FAMILY, Bytes.toBytes("B"))));

    Result result = region.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("B")));
    assertEquals("b", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("B"))));

    // CheckAndAppend with wrong value
    res = region.checkAndMutate(CheckAndMutate.newBuilder(row)
      .ifEquals(FAMILY, Bytes.toBytes("A"), Bytes.toBytes("b"))
      .build(new Append(row).addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("b"))));
    assertFalse(res.isSuccess());
    assertNull(res.getResult());

    result = region.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("B")));
    assertEquals("b", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("B"))));

    region.put(new Put(row).addColumn(FAMILY, Bytes.toBytes("C"), Bytes.toBytes("c")));

    // CheckAndAppend with a filter and correct value
    res = region.checkAndMutate(CheckAndMutate.newBuilder(row)
      .ifMatches(new FilterList(
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"), CompareOperator.EQUAL,
          Bytes.toBytes("a")),
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("C"), CompareOperator.EQUAL,
          Bytes.toBytes("c"))))
      .build(new Append(row).addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("bb"))));
    assertTrue(res.isSuccess());
    assertEquals("bbb", Bytes.toString(res.getResult().getValue(FAMILY, Bytes.toBytes("B"))));

    result = region.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("B")));
    assertEquals("bbb", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("B"))));

    // CheckAndAppend with a filter and wrong value
    res = region.checkAndMutate(CheckAndMutate.newBuilder(row)
      .ifMatches(new FilterList(
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("A"), CompareOperator.EQUAL,
          Bytes.toBytes("b")),
        new SingleColumnValueFilter(FAMILY, Bytes.toBytes("C"), CompareOperator.EQUAL,
          Bytes.toBytes("d"))))
      .build(new Append(row).addColumn(FAMILY, Bytes.toBytes("B"), Bytes.toBytes("bb"))));
    assertFalse(res.isSuccess());
    assertNull(res.getResult());

    result = region.get(new Get(row).addColumn(FAMILY, Bytes.toBytes("B")));
    assertEquals("bbb", Bytes.toString(result.getValue(FAMILY, Bytes.toBytes("B"))));
  }

  @Test
  public void testCheckAndIncrementAndAppend() throws Throwable {
    // Setting up region
    this.region = initHRegion(tableName, method, CONF, fam1);

    // CheckAndMutate with Increment and Append
    CheckAndMutate checkAndMutate = CheckAndMutate.newBuilder(row)
      .ifNotExists(fam1, qual)
      .build(new RowMutations(row)
        .add((Mutation) new Increment(row).addColumn(fam1, qual1, 1L))
        .add((Mutation) new Append(row).addColumn(fam1, qual2, Bytes.toBytes("a")))
      );

    CheckAndMutateResult result = region.checkAndMutate(checkAndMutate);
    assertTrue(result.isSuccess());
    assertEquals(1L, Bytes.toLong(result.getResult().getValue(fam1, qual1)));
    assertEquals("a", Bytes.toString(result.getResult().getValue(fam1, qual2)));

    Result r = region.get(new Get(row));
    assertEquals(1L, Bytes.toLong(r.getValue(fam1, qual1)));
    assertEquals("a", Bytes.toString(r.getValue(fam1, qual2)));

    // Set return results to false
    checkAndMutate = CheckAndMutate.newBuilder(row)
      .ifNotExists(fam1, qual)
      .build(new RowMutations(row)
        .add((Mutation) new Increment(row).addColumn(fam1, qual1, 1L).setReturnResults(false))
        .add((Mutation) new Append(row).addColumn(fam1, qual2, Bytes.toBytes("a"))
          .setReturnResults(false))
      );

    result = region.checkAndMutate(checkAndMutate);
    assertTrue(result.isSuccess());
    assertNull(result.getResult().getValue(fam1, qual1));
    assertNull(result.getResult().getValue(fam1, qual2));

    r = region.get(new Get(row));
    assertEquals(2L, Bytes.toLong(r.getValue(fam1, qual1)));
    assertEquals("aa", Bytes.toString(r.getValue(fam1, qual2)));

    checkAndMutate = CheckAndMutate.newBuilder(row)
      .ifNotExists(fam1, qual)
      .build(new RowMutations(row)
        .add((Mutation) new Increment(row).addColumn(fam1, qual1, 1L))
        .add((Mutation) new Append(row).addColumn(fam1, qual2, Bytes.toBytes("a"))
          .setReturnResults(false))
      );

    result = region.checkAndMutate(checkAndMutate);
    assertTrue(result.isSuccess());
    assertEquals(3L, Bytes.toLong(result.getResult().getValue(fam1, qual1)));
    assertNull(result.getResult().getValue(fam1, qual2));

    r = region.get(new Get(row));
    assertEquals(3L, Bytes.toLong(r.getValue(fam1, qual1)));
    assertEquals("aaa", Bytes.toString(r.getValue(fam1, qual2)));
  }

  @Test
  public void testCheckAndRowMutations() throws Throwable {
    final byte[] row = Bytes.toBytes("row");
    final byte[] q1 = Bytes.toBytes("q1");
    final byte[] q2 = Bytes.toBytes("q2");
    final byte[] q3 = Bytes.toBytes("q3");
    final byte[] q4 = Bytes.toBytes("q4");
    final String v1 = "v1";

    region = initHRegion(tableName, method, CONF, fam1);

    // Initial values
    region.batchMutate(new Mutation[] {
      new Put(row).addColumn(fam1, q2, Bytes.toBytes("toBeDeleted")),
      new Put(row).addColumn(fam1, q3, Bytes.toBytes(5L)),
      new Put(row).addColumn(fam1, q4, Bytes.toBytes("a")),
    });

    // Do CheckAndRowMutations
    CheckAndMutate checkAndMutate = CheckAndMutate.newBuilder(row)
      .ifNotExists(fam1, q1)
      .build(new RowMutations(row).add(Arrays.asList(
        new Put(row).addColumn(fam1, q1, Bytes.toBytes(v1)),
        new Delete(row).addColumns(fam1, q2),
        new Increment(row).addColumn(fam1, q3, 1),
        new Append(row).addColumn(fam1, q4, Bytes.toBytes("b"))))
      );

    CheckAndMutateResult result = region.checkAndMutate(checkAndMutate);
    assertTrue(result.isSuccess());
    assertEquals(6L, Bytes.toLong(result.getResult().getValue(fam1, q3)));
    assertEquals("ab", Bytes.toString(result.getResult().getValue(fam1, q4)));

    // Verify the value
    Result r = region.get(new Get(row));
    assertEquals(v1, Bytes.toString(r.getValue(fam1, q1)));
    assertNull(r.getValue(fam1, q2));
    assertEquals(6L, Bytes.toLong(r.getValue(fam1, q3)));
    assertEquals("ab", Bytes.toString(r.getValue(fam1, q4)));

    // Do CheckAndRowMutations again
    checkAndMutate = CheckAndMutate.newBuilder(row)
      .ifNotExists(fam1, q1)
      .build(new RowMutations(row).add(Arrays.asList(
        new Delete(row).addColumns(fam1, q1),
        new Put(row).addColumn(fam1, q2, Bytes.toBytes(v1)),
        new Increment(row).addColumn(fam1, q3, 1),
        new Append(row).addColumn(fam1, q4, Bytes.toBytes("b"))))
      );

    result = region.checkAndMutate(checkAndMutate);
    assertFalse(result.isSuccess());
    assertNull(result.getResult());

    // Verify the value
    r = region.get(new Get(row));
    assertEquals(v1, Bytes.toString(r.getValue(fam1, q1)));
    assertNull(r.getValue(fam1, q2));
    assertEquals(6L, Bytes.toLong(r.getValue(fam1, q3)));
    assertEquals("ab", Bytes.toString(r.getValue(fam1, q4)));
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
    put.addColumn(fam1, qual, 1, value);
    put.addColumn(fam1, qual, 2, value);

    this.region = initHRegion(tableName, method, CONF, fam1);
    region.put(put);

    // We do support deleting more than 1 'latest' version
    Delete delete = new Delete(row1);
    delete.addColumn(fam1, qual);
    delete.addColumn(fam1, qual);
    region.delete(delete);

    Get get = new Get(row1);
    get.addFamily(fam1);
    Result r = region.get(get);
    assertEquals(0, r.size());
  }

  @Test
  public void testDelete_CheckFamily() throws IOException {
    byte[] row1 = Bytes.toBytes("row1");
    byte[] fam1 = Bytes.toBytes("fam1");
    byte[] fam2 = Bytes.toBytes("fam2");
    byte[] fam3 = Bytes.toBytes("fam3");
    byte[] fam4 = Bytes.toBytes("fam4");

    // Setting up region
    this.region = initHRegion(tableName, method, CONF, fam1, fam2, fam3);
    List<Cell> kvs = new ArrayList<>();
    kvs.add(new KeyValue(row1, fam4, null, null));

    byte[] forUnitTestsOnly = Bytes.toBytes("ForUnitTestsOnly");

    // testing existing family
    NavigableMap<byte[], List<Cell>> deleteMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    deleteMap.put(fam2, kvs);
    region.delete(new Delete(forUnitTestsOnly, HConstants.LATEST_TIMESTAMP, deleteMap));

    // testing non existing family
    NavigableMap<byte[], List<Cell>> deleteMap2 = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    deleteMap2.put(fam4, kvs);
    assertThrows("Family " + Bytes.toString(fam4) + " does exist",
      NoSuchColumnFamilyException.class,
      () -> region.delete(new Delete(forUnitTestsOnly, HConstants.LATEST_TIMESTAMP, deleteMap2)));
  }

  @Test
  public void testDelete_mixed() throws IOException, InterruptedException {
    byte[] fam = Bytes.toBytes("info");
    byte[][] families = { fam };
    this.region = initHRegion(tableName, method, CONF, families);
    EnvironmentEdgeManagerTestHelper.injectEdge(new IncrementingEnvironmentEdge());

    byte[] row = Bytes.toBytes("table_name");
    // column names
    byte[] serverinfo = Bytes.toBytes("serverinfo");
    byte[] splitA = Bytes.toBytes("splitA");
    byte[] splitB = Bytes.toBytes("splitB");

    // add some data:
    Put put = new Put(row);
    put.addColumn(fam, splitA, Bytes.toBytes("reference_A"));
    region.put(put);

    put = new Put(row);
    put.addColumn(fam, splitB, Bytes.toBytes("reference_B"));
    region.put(put);

    put = new Put(row);
    put.addColumn(fam, serverinfo, Bytes.toBytes("ip_address"));
    region.put(put);

    // ok now delete a split:
    Delete delete = new Delete(row);
    delete.addColumns(fam, splitA);
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
    put.addColumn(fam, splitA, Bytes.toBytes("reference_A"));
    region.put(put);
    get = new Get(row);
    result = region.get(get);
    assertEquals(3, result.size());

    // Now delete all... then test I can add stuff back
    delete = new Delete(row);
    region.delete(delete);
    assertEquals(0, region.get(get).size());

    region.put(new Put(row).addColumn(fam, splitA, Bytes.toBytes("reference_A")));
    result = region.get(get);
    assertEquals(1, result.size());
  }

  @Test
  public void testDeleteRowWithFutureTs() throws IOException {
    byte[] fam = Bytes.toBytes("info");
    byte[][] families = { fam };
    this.region = initHRegion(tableName, method, CONF, families);
    byte[] row = Bytes.toBytes("table_name");
    // column names
    byte[] serverinfo = Bytes.toBytes("serverinfo");

    // add data in the far future
    Put put = new Put(row);
    put.addColumn(fam, serverinfo, HConstants.LATEST_TIMESTAMP - 5, Bytes.toBytes("value"));
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
  }

  /**
   * Tests that the special LATEST_TIMESTAMP option for puts gets replaced by
   * the actual timestamp
   */
  @Test
  public void testPutWithLatestTS() throws IOException {
    byte[] fam = Bytes.toBytes("info");
    byte[][] families = { fam };
    this.region = initHRegion(tableName, method, CONF, families);
    byte[] row = Bytes.toBytes("row1");
    // column names
    byte[] qual = Bytes.toBytes("qual");

    // add data with LATEST_TIMESTAMP, put without WAL
    Put put = new Put(row);
    put.addColumn(fam, qual, HConstants.LATEST_TIMESTAMP, Bytes.toBytes("value"));
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
    put.addColumn(fam, qual, HConstants.LATEST_TIMESTAMP, Bytes.toBytes("value"));
    region.put(put);

    // Make sure it shows up with an actual timestamp
    get = new Get(row).addColumn(fam, qual);
    result = region.get(get);
    assertEquals(1, result.size());
    kv = result.rawCells()[0];
    LOG.info("Got: " + kv);
    assertTrue("LATEST_TIMESTAMP was not replaced with real timestamp",
        kv.getTimestamp() != HConstants.LATEST_TIMESTAMP);
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

    // add data with a timestamp that is too recent for range. Ensure assert
    CONF.setInt("hbase.hregion.keyvalue.timestamp.slop.millisecs", 1000);
    this.region = initHRegion(tableName, method, CONF, families);
    boolean caughtExcep = false;
    try {
      // no TS specified == use latest. should not error
      region.put(new Put(row).addColumn(fam, Bytes.toBytes("qual"), Bytes.toBytes("value")));
      // TS out of range. should error
      region.put(new Put(row).addColumn(fam, Bytes.toBytes("qual"),
        EnvironmentEdgeManager.currentTime() + 2000, Bytes.toBytes("value")));
      fail("Expected IOE for TS out of configured timerange");
    } catch (FailedSanityCheckException ioe) {
      LOG.debug("Received expected exception", ioe);
      caughtExcep = true;
    }
    assertTrue("Should catch FailedSanityCheckException", caughtExcep);
  }

  @Test
  public void testScanner_DeleteOneFamilyNotAnother() throws IOException {
    byte[] fam1 = Bytes.toBytes("columnA");
    byte[] fam2 = Bytes.toBytes("columnB");
    this.region = initHRegion(tableName, method, CONF, fam1, fam2);
    byte[] rowA = Bytes.toBytes("rowA");
    byte[] rowB = Bytes.toBytes("rowB");

    byte[] value = Bytes.toBytes("value");

    Delete delete = new Delete(rowA);
    delete.addFamily(fam1);

    region.delete(delete);

    // now create data.
    Put put = new Put(rowA);
    put.addColumn(fam2, null, value);
    region.put(put);

    put = new Put(rowB);
    put.addColumn(fam1, null, value);
    put.addColumn(fam2, null, value);
    region.put(put);

    Scan scan = new Scan();
    scan.addFamily(fam1).addFamily(fam2);
    InternalScanner s = region.getScanner(scan);
    List<Cell> results = new ArrayList<>();
    s.next(results);
    assertTrue(CellUtil.matchingRows(results.get(0), rowA));

    results.clear();
    s.next(results);
    assertTrue(CellUtil.matchingRows(results.get(0), rowB));
  }

  @Test
  public void testDataInMemoryWithoutWAL() throws IOException {
    FileSystem fs = FileSystem.get(CONF);
    Path rootDir = new Path(dir + "testDataInMemoryWithoutWAL");
    FSHLog hLog = new FSHLog(fs, rootDir, "testDataInMemoryWithoutWAL", CONF);
    hLog.init();
    // This chunk creation is done throughout the code base. Do we want to move it into core?
    // It is missing from this test. W/o it we NPE.
    region = initHRegion(tableName, null, null, CONF, false, Durability.SYNC_WAL, hLog,
        COLUMN_FAMILY_BYTES);

    Cell originalCell = ExtendedCellBuilderFactory.create(CellBuilderType.DEEP_COPY)
      .setRow(row)
      .setFamily(COLUMN_FAMILY_BYTES)
      .setQualifier(qual1)
      .setTimestamp(EnvironmentEdgeManager.currentTime())
      .setType(KeyValue.Type.Put.getCode())
      .setValue(value1)
      .build();
    final long originalSize = originalCell.getSerializedSize();

    Cell addCell = ExtendedCellBuilderFactory.create(CellBuilderType.DEEP_COPY)
      .setRow(row)
      .setFamily(COLUMN_FAMILY_BYTES)
      .setQualifier(qual1)
      .setTimestamp(EnvironmentEdgeManager.currentTime())
      .setType(KeyValue.Type.Put.getCode())
      .setValue(Bytes.toBytes("xxxxxxxxxx"))
      .build();
    final long addSize = addCell.getSerializedSize();

    LOG.info("originalSize:" + originalSize
      + ", addSize:" + addSize);
    // start test. We expect that the addPut's durability will be replaced
    // by originalPut's durability.

    // case 1:
    testDataInMemoryWithoutWAL(region,
            new Put(row).add(originalCell).setDurability(Durability.SKIP_WAL),
            new Put(row).add(addCell).setDurability(Durability.SKIP_WAL),
            originalSize + addSize);

    // case 2:
    testDataInMemoryWithoutWAL(region,
            new Put(row).add(originalCell).setDurability(Durability.SKIP_WAL),
            new Put(row).add(addCell).setDurability(Durability.SYNC_WAL),
            originalSize + addSize);

    // case 3:
    testDataInMemoryWithoutWAL(region,
            new Put(row).add(originalCell).setDurability(Durability.SYNC_WAL),
            new Put(row).add(addCell).setDurability(Durability.SKIP_WAL),
            0);

    // case 4:
    testDataInMemoryWithoutWAL(region,
            new Put(row).add(originalCell).setDurability(Durability.SYNC_WAL),
            new Put(row).add(addCell).setDurability(Durability.SYNC_WAL),
            0);
  }

  private static void testDataInMemoryWithoutWAL(HRegion region, Put originalPut,
          final Put addPut, long delta) throws IOException {
    final long initSize = region.getDataInMemoryWithoutWAL();
    // save normalCPHost and replaced by mockedCPHost
    RegionCoprocessorHost normalCPHost = region.getCoprocessorHost();
    RegionCoprocessorHost mockedCPHost = Mockito.mock(RegionCoprocessorHost.class);
    // Because the preBatchMutate returns void, we can't do usual Mockito when...then form. Must
    // do below format (from Mockito doc).
    Mockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        MiniBatchOperationInProgress<Mutation> mb = invocation.getArgument(0);
        mb.addOperationsFromCP(0, new Mutation[]{addPut});
        return null;
      }
    }).when(mockedCPHost).preBatchMutate(Mockito.isA(MiniBatchOperationInProgress.class));
    ColumnFamilyDescriptorBuilder builder = ColumnFamilyDescriptorBuilder.
        newBuilder(COLUMN_FAMILY_BYTES);
    ScanInfo info = new ScanInfo(CONF, builder.build(), Long.MAX_VALUE,
        Long.MAX_VALUE, region.getCellComparator());
    Mockito.when(mockedCPHost.preFlushScannerOpen(Mockito.any(HStore.class),
        Mockito.any())).thenReturn(info);
    Mockito.when(mockedCPHost.preFlush(Mockito.any(), Mockito.any(StoreScanner.class),
        Mockito.any())).thenAnswer(i -> i.getArgument(1));
    region.setCoprocessorHost(mockedCPHost);

    region.put(originalPut);
    region.setCoprocessorHost(normalCPHost);
    final long finalSize = region.getDataInMemoryWithoutWAL();
    assertEquals("finalSize:" + finalSize + ", initSize:"
      + initSize + ", delta:" + delta,finalSize, initSize + delta);
  }

  @Test
  public void testDeleteColumns_PostInsert() throws IOException, InterruptedException {
    Delete delete = new Delete(row);
    delete.addColumns(fam1, qual1);
    doTestDelete_AndPostInsert(delete);
  }

  @Test
  public void testaddFamily_PostInsert() throws IOException, InterruptedException {
    Delete delete = new Delete(row);
    delete.addFamily(fam1);
    doTestDelete_AndPostInsert(delete);
  }

  public void doTestDelete_AndPostInsert(Delete delete) throws IOException, InterruptedException {
    this.region = initHRegion(tableName, method, CONF, fam1);
    EnvironmentEdgeManagerTestHelper.injectEdge(new IncrementingEnvironmentEdge());
    Put put = new Put(row);
    put.addColumn(fam1, qual1, value1);
    region.put(put);

    // now delete the value:
    region.delete(delete);

    // ok put data:
    put = new Put(row);
    put.addColumn(fam1, qual1, value2);
    region.put(put);

    // ok get:
    Get get = new Get(row);
    get.addColumn(fam1, qual1);

    Result r = region.get(get);
    assertEquals(1, r.size());
    assertArrayEquals(value2, r.getValue(fam1, qual1));

    // next:
    Scan scan = new Scan().withStartRow(row);
    scan.addColumn(fam1, qual1);
    InternalScanner s = region.getScanner(scan);

    List<Cell> results = new ArrayList<>();
    assertEquals(false, s.next(results));
    assertEquals(1, results.size());
    Cell kv = results.get(0);

    assertArrayEquals(value2, CellUtil.cloneValue(kv));
    assertArrayEquals(fam1, CellUtil.cloneFamily(kv));
    assertArrayEquals(qual1, CellUtil.cloneQualifier(kv));
    assertArrayEquals(row, CellUtil.cloneRow(kv));
  }

  @Test
  public void testDelete_CheckTimestampUpdated() throws IOException {
    byte[] row1 = Bytes.toBytes("row1");
    byte[] col1 = Bytes.toBytes("col1");
    byte[] col2 = Bytes.toBytes("col2");
    byte[] col3 = Bytes.toBytes("col3");

    byte[] forUnitTestsOnly = Bytes.toBytes("ForUnitTestsOnly");

    // Setting up region
    this.region = initHRegion(tableName, method, CONF, fam1);
    // Building checkerList
    List<Cell> kvs = new ArrayList<>();
    kvs.add(new KeyValue(row1, fam1, col1, null));
    kvs.add(new KeyValue(row1, fam1, col2, null));
    kvs.add(new KeyValue(row1, fam1, col3, null));

    NavigableMap<byte[], List<Cell>> deleteMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    deleteMap.put(fam1, kvs);
    region.delete(new Delete(forUnitTestsOnly, HConstants.LATEST_TIMESTAMP, deleteMap));

    // extract the key values out the memstore:
    // This is kinda hacky, but better than nothing...
    long now = EnvironmentEdgeManager.currentTime();
    AbstractMemStore memstore = (AbstractMemStore) region.getStore(fam1).memstore;
    Cell firstCell = memstore.getActive().first();
    assertTrue(firstCell.getTimestamp() <= now);
    now = firstCell.getTimestamp();
    for (Cell cell : memstore.getActive().getCellSet()) {
      assertTrue(cell.getTimestamp() <= now);
      now = cell.getTimestamp();
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
    this.region = initHRegion(tableName, method, CONF, fam1);
    Get get = new Get(row1);
    get.addColumn(fam2, col1);

    // Test
    try {
      region.get(get);
      fail("Expecting DoNotRetryIOException in get but did not get any");
    } catch (org.apache.hadoop.hbase.DoNotRetryIOException e) {
      LOG.info("Got expected DoNotRetryIOException successfully");
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
    this.region = initHRegion(tableName, method, CONF, fam1);
    // Add to memstore
    Put put = new Put(row1);
    put.addColumn(fam1, col1, null);
    put.addColumn(fam1, col2, null);
    put.addColumn(fam1, col3, null);
    put.addColumn(fam1, col4, null);
    put.addColumn(fam1, col5, null);
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
      assertTrue(CellUtil.matchingRows(expected[i], res.rawCells()[i]));
      assertTrue(CellUtil.matchingFamily(expected[i], res.rawCells()[i]));
      assertTrue(CellUtil.matchingQualifier(expected[i], res.rawCells()[i]));
    }

    // Test using a filter on a Get
    Get g = new Get(row1);
    final int count = 2;
    g.setFilter(new ColumnCountGetFilter(count));
    res = region.get(g);
    assertEquals(count, res.size());
  }

  @Test
  public void testGet_Empty() throws IOException {
    byte[] row = Bytes.toBytes("row");
    byte[] fam = Bytes.toBytes("fam");

    this.region = initHRegion(tableName, method, CONF, fam);
    Get get = new Get(row);
    get.addFamily(fam);
    Result r = region.get(get);

    assertTrue(r.isEmpty());
  }

  @Test
  public void testGetWithFilter() throws IOException, InterruptedException {
    byte[] row1 = Bytes.toBytes("row1");
    byte[] fam1 = Bytes.toBytes("fam1");
    byte[] col1 = Bytes.toBytes("col1");
    byte[] value1 = Bytes.toBytes("value1");
    byte[] value2 = Bytes.toBytes("value2");

    final int maxVersions = 3;
    TableDescriptor tableDescriptor =
      TableDescriptorBuilder.newBuilder(TableName.valueOf("testFilterAndColumnTracker"))
        .setColumnFamily(
          ColumnFamilyDescriptorBuilder.newBuilder(fam1).setMaxVersions(maxVersions).build())
        .build();
    ChunkCreator.initialize(MemStoreLAB.CHUNK_SIZE_DEFAULT, false, 0, 0,
      0, null, MemStoreLAB.INDEX_CHUNK_SIZE_PERCENTAGE_DEFAULT);
    RegionInfo info = RegionInfoBuilder.newBuilder(tableDescriptor.getTableName()).build();
    Path logDir = TEST_UTIL.getDataTestDirOnTestFS(method + ".log");
    final WAL wal = HBaseTestingUtil.createWal(TEST_UTIL.getConfiguration(), logDir, info);
    this.region = TEST_UTIL.createLocalHRegion(info, CONF, tableDescriptor, wal);

    // Put 4 version to memstore
    long ts = 0;
    Put put = new Put(row1, ts);
    put.addColumn(fam1, col1, value1);
    region.put(put);
    put = new Put(row1, ts + 1);
    put.addColumn(fam1, col1, Bytes.toBytes("filter1"));
    region.put(put);
    put = new Put(row1, ts + 2);
    put.addColumn(fam1, col1, Bytes.toBytes("filter2"));
    region.put(put);
    put = new Put(row1, ts + 3);
    put.addColumn(fam1, col1, value2);
    region.put(put);

    Get get = new Get(row1);
    get.readAllVersions();
    Result res = region.get(get);
    // Get 3 versions, the oldest version has gone from user view
    assertEquals(maxVersions, res.size());

    get.setFilter(new ValueFilter(CompareOperator.EQUAL, new SubstringComparator("value")));
    res = region.get(get);
    // When use value filter, the oldest version should still gone from user view and it
    // should only return one key vaule
    assertEquals(1, res.size());
    assertTrue(CellUtil.matchingValue(new KeyValue(row1, fam1, col1, value2), res.rawCells()[0]));
    assertEquals(ts + 3, res.rawCells()[0].getTimestamp());

    region.flush(true);
    region.compact(true);
    Thread.sleep(1000);
    res = region.get(get);
    // After flush and compact, the result should be consistent with previous result
    assertEquals(1, res.size());
    assertTrue(CellUtil.matchingValue(new KeyValue(row1, fam1, col1, value2), res.rawCells()[0]));
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
    this.region = initHRegion(tableName, method, CONF, families);
    Scan scan = new Scan();
    scan.addFamily(fam1);
    scan.addFamily(fam2);
    try {
      region.getScanner(scan);
    } catch (Exception e) {
      assertTrue("Families could not be found in Region", false);
    }
  }

  @Test
  public void testGetScanner_WithNotOkFamilies() throws IOException {
    byte[] fam1 = Bytes.toBytes("fam1");
    byte[] fam2 = Bytes.toBytes("fam2");

    byte[][] families = { fam1 };

    // Setting up region
    this.region = initHRegion(tableName, method, CONF, families);
    Scan scan = new Scan();
    scan.addFamily(fam2);
    boolean ok = false;
    try {
      region.getScanner(scan);
    } catch (Exception e) {
      ok = true;
    }
    assertTrue("Families could not be found in Region", ok);
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
    this.region = initHRegion(tableName, method, CONF, families);
    // Putting data in Region
    Put put = new Put(row1);
    put.addColumn(fam1, null, null);
    put.addColumn(fam2, null, null);
    put.addColumn(fam3, null, null);
    put.addColumn(fam4, null, null);
    region.put(put);

    Scan scan = null;
    RegionScannerImpl is = null;

    // Testing to see how many scanners that is produced by getScanner,
    // starting
    // with known number, 2 - current = 1
    scan = new Scan();
    scan.addFamily(fam2);
    scan.addFamily(fam4);
    is = region.getScanner(scan);
    assertEquals(1, is.storeHeap.getHeap().size());

    scan = new Scan();
    is = region.getScanner(scan);
    assertEquals(families.length - 1, is.storeHeap.getHeap().size());
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
    try {
      this.region = initHRegion(tableName, method, CONF, families);
    } catch (IOException e) {
      e.printStackTrace();
      fail("Got IOException during initHRegion, " + e.getMessage());
    }
    region.closed.set(true);
    try {
      region.getScanner(null);
      fail("Expected to get an exception during getScanner on a region that is closed");
    } catch (NotServingRegionException e) {
      // this is the correct exception that is expected
    } catch (IOException e) {
      fail("Got wrong type of exception - should be a NotServingRegionException, " +
          "but was an IOException: "
          + e.getMessage());
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
    long ts = EnvironmentEdgeManager.currentTime();

    // Setting up region
    this.region = initHRegion(tableName, method, CONF, families);
    // Putting data in Region
    Put put = null;
    put = new Put(row1);
    put.addColumn(fam1, (byte[]) null, ts, null);
    put.addColumn(fam2, (byte[]) null, ts, null);
    put.addColumn(fam3, (byte[]) null, ts, null);
    put.addColumn(fam4, (byte[]) null, ts, null);
    region.put(put);

    put = new Put(row2);
    put.addColumn(fam1, (byte[]) null, ts, null);
    put.addColumn(fam2, (byte[]) null, ts, null);
    put.addColumn(fam3, (byte[]) null, ts, null);
    put.addColumn(fam4, (byte[]) null, ts, null);
    region.put(put);

    Scan scan = new Scan();
    scan.addFamily(fam2);
    scan.addFamily(fam4);
    InternalScanner is = region.getScanner(scan);

    List<Cell> res = null;

    // Result 1
    List<Cell> expected1 = new ArrayList<>();
    expected1.add(new KeyValue(row1, fam2, null, ts, KeyValue.Type.Put, null));
    expected1.add(new KeyValue(row1, fam4, null, ts, KeyValue.Type.Put, null));

    res = new ArrayList<>();
    is.next(res);
    for (int i = 0; i < res.size(); i++) {
      assertTrue(PrivateCellUtil.equalsIgnoreMvccVersion(expected1.get(i), res.get(i)));
    }

    // Result 2
    List<Cell> expected2 = new ArrayList<>();
    expected2.add(new KeyValue(row2, fam2, null, ts, KeyValue.Type.Put, null));
    expected2.add(new KeyValue(row2, fam4, null, ts, KeyValue.Type.Put, null));

    res = new ArrayList<>();
    is.next(res);
    for (int i = 0; i < res.size(); i++) {
      assertTrue(PrivateCellUtil.equalsIgnoreMvccVersion(expected2.get(i), res.get(i)));
    }
  }

  @Test
  public void testScanner_ExplicitColumns_FromMemStore_EnforceVersions() throws IOException {
    byte[] row1 = Bytes.toBytes("row1");
    byte[] qf1 = Bytes.toBytes("qualifier1");
    byte[] qf2 = Bytes.toBytes("qualifier2");
    byte[] fam1 = Bytes.toBytes("fam1");
    byte[][] families = { fam1 };

    long ts1 = EnvironmentEdgeManager.currentTime();
    long ts2 = ts1 + 1;
    long ts3 = ts1 + 2;

    // Setting up region
    this.region = initHRegion(tableName, method, CONF, families);
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
    List<Cell> expected = new ArrayList<>();
    expected.add(kv13);
    expected.add(kv12);

    Scan scan = new Scan().withStartRow(row1);
    scan.addColumn(fam1, qf1);
    scan.readVersions(MAX_VERSIONS);
    List<Cell> actual = new ArrayList<>();
    InternalScanner scanner = region.getScanner(scan);

    boolean hasNext = scanner.next(actual);
    assertEquals(false, hasNext);

    // Verify result
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i), actual.get(i));
    }
  }

  @Test
  public void testScanner_ExplicitColumns_FromFilesOnly_EnforceVersions() throws IOException {
    byte[] row1 = Bytes.toBytes("row1");
    byte[] qf1 = Bytes.toBytes("qualifier1");
    byte[] qf2 = Bytes.toBytes("qualifier2");
    byte[] fam1 = Bytes.toBytes("fam1");
    byte[][] families = { fam1 };

    long ts1 = 1;
    long ts2 = ts1 + 1;
    long ts3 = ts1 + 2;

    // Setting up region
    this.region = initHRegion(tableName, method, CONF, families);
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
    List<Cell> expected = new ArrayList<>();
    expected.add(kv13);
    expected.add(kv12);
    expected.add(kv23);
    expected.add(kv22);

    Scan scan = new Scan().withStartRow(row1);
    scan.addColumn(fam1, qf1);
    scan.addColumn(fam1, qf2);
    scan.readVersions(MAX_VERSIONS);
    List<Cell> actual = new ArrayList<>();
    InternalScanner scanner = region.getScanner(scan);

    boolean hasNext = scanner.next(actual);
    assertEquals(false, hasNext);

    // Verify result
    for (int i = 0; i < expected.size(); i++) {
      assertTrue(PrivateCellUtil.equalsIgnoreMvccVersion(expected.get(i), actual.get(i)));
    }
  }

  @Test
  public void testScanner_ExplicitColumns_FromMemStoreAndFiles_EnforceVersions() throws
      IOException {
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
    this.region = initHRegion(tableName, method, CONF, families);
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
    List<Cell> expected = new ArrayList<>();
    expected.add(kv14);
    expected.add(kv13);
    expected.add(kv12);
    expected.add(kv24);
    expected.add(kv23);
    expected.add(kv22);

    Scan scan = new Scan().withStartRow(row1);
    scan.addColumn(fam1, qf1);
    scan.addColumn(fam1, qf2);
    int versions = 3;
    scan.readVersions(versions);
    List<Cell> actual = new ArrayList<>();
    InternalScanner scanner = region.getScanner(scan);

    boolean hasNext = scanner.next(actual);
    assertEquals(false, hasNext);

    // Verify result
    for (int i = 0; i < expected.size(); i++) {
      assertTrue(PrivateCellUtil.equalsIgnoreMvccVersion(expected.get(i), actual.get(i)));
    }
  }

  @Test
  public void testScanner_Wildcard_FromMemStore_EnforceVersions() throws IOException {
    byte[] row1 = Bytes.toBytes("row1");
    byte[] qf1 = Bytes.toBytes("qualifier1");
    byte[] qf2 = Bytes.toBytes("qualifier2");
    byte[] fam1 = Bytes.toBytes("fam1");
    byte[][] families = { fam1 };

    long ts1 = EnvironmentEdgeManager.currentTime();
    long ts2 = ts1 + 1;
    long ts3 = ts1 + 2;

    // Setting up region
    this.region = initHRegion(tableName, method, CONF, families);
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
    List<Cell> expected = new ArrayList<>();
    expected.add(kv13);
    expected.add(kv12);
    expected.add(kv23);
    expected.add(kv22);

    Scan scan = new Scan().withStartRow(row1);
    scan.addFamily(fam1);
    scan.readVersions(MAX_VERSIONS);
    List<Cell> actual = new ArrayList<>();
    InternalScanner scanner = region.getScanner(scan);

    boolean hasNext = scanner.next(actual);
    assertEquals(false, hasNext);

    // Verify result
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i), actual.get(i));
    }
  }

  @Test
  public void testScanner_Wildcard_FromFilesOnly_EnforceVersions() throws IOException {
    byte[] row1 = Bytes.toBytes("row1");
    byte[] qf1 = Bytes.toBytes("qualifier1");
    byte[] qf2 = Bytes.toBytes("qualifier2");
    byte[] fam1 = Bytes.toBytes("fam1");

    long ts1 = 1;
    long ts2 = ts1 + 1;
    long ts3 = ts1 + 2;

    // Setting up region
    this.region = initHRegion(tableName, method, CONF, fam1);
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
    List<Cell> expected = new ArrayList<>();
    expected.add(kv13);
    expected.add(kv12);
    expected.add(kv23);
    expected.add(kv22);

    Scan scan = new Scan().withStartRow(row1);
    scan.addFamily(fam1);
    scan.readVersions(MAX_VERSIONS);
    List<Cell> actual = new ArrayList<>();
    InternalScanner scanner = region.getScanner(scan);

    boolean hasNext = scanner.next(actual);
    assertEquals(false, hasNext);

    // Verify result
    for (int i = 0; i < expected.size(); i++) {
      assertTrue(PrivateCellUtil.equalsIgnoreMvccVersion(expected.get(i), actual.get(i)));
    }
  }

  @Test
  public void testScanner_StopRow1542() throws IOException {
    byte[] family = Bytes.toBytes("testFamily");
    this.region = initHRegion(tableName, method, CONF, family);
    byte[] row1 = Bytes.toBytes("row111");
    byte[] row2 = Bytes.toBytes("row222");
    byte[] row3 = Bytes.toBytes("row333");
    byte[] row4 = Bytes.toBytes("row444");
    byte[] row5 = Bytes.toBytes("row555");

    byte[] col1 = Bytes.toBytes("Pub111");
    byte[] col2 = Bytes.toBytes("Pub222");

    Put put = new Put(row1);
    put.addColumn(family, col1, Bytes.toBytes(10L));
    region.put(put);

    put = new Put(row2);
    put.addColumn(family, col1, Bytes.toBytes(15L));
    region.put(put);

    put = new Put(row3);
    put.addColumn(family, col2, Bytes.toBytes(20L));
    region.put(put);

    put = new Put(row4);
    put.addColumn(family, col2, Bytes.toBytes(30L));
    region.put(put);

    put = new Put(row5);
    put.addColumn(family, col1, Bytes.toBytes(40L));
    region.put(put);

    Scan scan = new Scan().withStartRow(row3).withStopRow(row4);
    scan.readAllVersions();
    scan.addColumn(family, col1);
    InternalScanner s = region.getScanner(scan);

    List<Cell> results = new ArrayList<>();
    assertEquals(false, s.next(results));
    assertEquals(0, results.size());
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
    this.region = initHRegion(tableName, method, CONF, fam1);
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
    List<KeyValue> expected = new ArrayList<>();
    expected.add(kv14);
    expected.add(kv13);
    expected.add(kv12);
    expected.add(kv24);
    expected.add(kv23);
    expected.add(kv22);

    Scan scan = new Scan().withStartRow(row1);
    int versions = 3;
    scan.readVersions(versions);
    List<Cell> actual = new ArrayList<>();
    InternalScanner scanner = region.getScanner(scan);

    boolean hasNext = scanner.next(actual);
    assertEquals(false, hasNext);

    // Verify result
    for (int i = 0; i < expected.size(); i++) {
      assertTrue(PrivateCellUtil.equalsIgnoreMvccVersion(expected.get(i), actual.get(i)));
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
    this.region = initHRegion(tableName, method, CONF, cf_essential, cf_joined, cf_alpha);
    byte[] row1 = Bytes.toBytes("row1");
    byte[] row2 = Bytes.toBytes("row2");
    byte[] row3 = Bytes.toBytes("row3");

    byte[] col_normal = Bytes.toBytes("d");
    byte[] col_alpha = Bytes.toBytes("a");

    byte[] filtered_val = Bytes.toBytes(3);

    Put put = new Put(row1);
    put.addColumn(cf_essential, col_normal, Bytes.toBytes(1));
    put.addColumn(cf_joined, col_alpha, Bytes.toBytes(1));
    region.put(put);

    put = new Put(row2);
    put.addColumn(cf_essential, col_alpha, Bytes.toBytes(2));
    put.addColumn(cf_joined, col_normal, Bytes.toBytes(2));
    put.addColumn(cf_alpha, col_alpha, Bytes.toBytes(2));
    region.put(put);

    put = new Put(row3);
    put.addColumn(cf_essential, col_normal, filtered_val);
    put.addColumn(cf_joined, col_normal, filtered_val);
    region.put(put);

    // Check two things:
    // 1. result list contains expected values
    // 2. result list is sorted properly

    Scan scan = new Scan();
    Filter filter = new SingleColumnValueExcludeFilter(cf_essential, col_normal,
            CompareOperator.NOT_EQUAL, filtered_val);
    scan.setFilter(filter);
    scan.setLoadColumnFamiliesOnDemand(true);
    InternalScanner s = region.getScanner(scan);

    List<Cell> results = new ArrayList<>();
    assertTrue(s.next(results));
    assertEquals(1, results.size());
    results.clear();

    assertTrue(s.next(results));
    assertEquals(3, results.size());
    assertTrue("orderCheck", CellUtil.matchingFamily(results.get(0), cf_alpha));
    assertTrue("orderCheck", CellUtil.matchingFamily(results.get(1), cf_essential));
    assertTrue("orderCheck", CellUtil.matchingFamily(results.get(2), cf_joined));
    results.clear();

    assertFalse(s.next(results));
    assertEquals(0, results.size());
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

    this.region = initHRegion(tableName, method, CONF, cf_first, cf_second);
    final byte[] col_a = Bytes.toBytes("a");
    final byte[] col_b = Bytes.toBytes("b");

    Put put;

    for (int i = 0; i < 10; i++) {
      put = new Put(Bytes.toBytes("r" + Integer.toString(i)));
      put.addColumn(cf_first, col_a, Bytes.toBytes(i));
      if (i < 5) {
        put.addColumn(cf_first, col_b, Bytes.toBytes(i));
        put.addColumn(cf_second, col_a, Bytes.toBytes(i));
        put.addColumn(cf_second, col_b, Bytes.toBytes(i));
      }
      region.put(put);
    }

    Scan scan = new Scan();
    scan.setLoadColumnFamiliesOnDemand(true);
    Filter bogusFilter = new FilterBase() {
      @Override
      public ReturnCode filterCell(final Cell ignored) throws IOException {
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

    List<Cell> results = new ArrayList<>();
    int index = 0;
    ScannerContext scannerContext = ScannerContext.newBuilder().setBatchLimit(3).build();
    while (true) {
      boolean more = s.next(results, scannerContext);
      if ((index >> 1) < 5) {
        if (index % 2 == 0) {
          assertEquals(3, results.size());
        } else {
          assertEquals(1, results.size());
        }
      } else {
        assertEquals(1, results.size());
      }
      results.clear();
      index++;
      if (!more) {
        break;
      }
    }
  }

  @Test
  public void testScannerOperationId() throws IOException {
    region = initHRegion(tableName, method, CONF, COLUMN_FAMILY_BYTES);
    Scan scan = new Scan();
    RegionScanner scanner = region.getScanner(scan);
    assertNull(scanner.getOperationId());
    scanner.close();

    String operationId = "test_operation_id_0101";
    scan = new Scan().setId(operationId);
    scanner = region.getScanner(scan);
    assertEquals(operationId, scanner.getOperationId());
    scanner.close();

    HBaseTestingUtil.closeRegionAndWAL(this.region);
  }

  /**
   * Write an HFile block full with Cells whose qualifier that are identical between
   * 0 and Short.MAX_VALUE. See HBASE-13329.
   * @throws Exception
   */
  @Test
  public void testLongQualifier() throws Exception {
    byte[] family = Bytes.toBytes("family");
    this.region = initHRegion(tableName, method, CONF, family);
    byte[] q = new byte[Short.MAX_VALUE+2];
    Arrays.fill(q, 0, q.length-1, (byte)42);
    for (byte i=0; i<10; i++) {
      Put p = new Put(Bytes.toBytes("row"));
      // qualifiers that differ past Short.MAX_VALUE
      q[q.length-1]=i;
      p.addColumn(family, q, q);
      region.put(p);
    }
    region.flush(false);
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

    this.region = initHRegion(tableName, method, CONF, family);
    FlushThread flushThread = new FlushThread();
    try {
      flushThread.start();

      Scan scan = new Scan();
      scan.addFamily(family);
      scan.setFilter(new SingleColumnValueFilter(family, qual1, CompareOperator.EQUAL,
          new BinaryComparator(Bytes.toBytes(5L))));

      int expectedCount = 0;
      List<Cell> res = new ArrayList<>();

      boolean toggle = true;
      for (long i = 0; i < numRows; i++) {
        Put put = new Put(Bytes.toBytes(i));
        put.setDurability(Durability.SKIP_WAL);
        put.addColumn(family, qual1, Bytes.toBytes(i % 10));
        region.put(put);

        if (i != 0 && i % compactInterval == 0) {
          LOG.debug("iteration = " + i+ " ts=" + EnvironmentEdgeManager.currentTime());
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
          while (scanner.next(res))
            ;
          if (!toggle) {
            flushThread.flush();
          }
          assertEquals("toggle="+toggle+"i=" + i + " ts=" + EnvironmentEdgeManager.currentTime(),
              expectedCount, res.size());
          toggle = !toggle;
        }
      }

    } finally {
      try {
        flushThread.done();
        flushThread.join();
        flushThread.checkNoError();
      } catch (InterruptedException ie) {
        LOG.warn("Caught exception when joining with flushThread", ie);
      }
      HBaseTestingUtil.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  protected class FlushThread extends Thread {
    private volatile boolean done;
    private Throwable error = null;

    FlushThread() {
      super("FlushThread");
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
            LOG.error("Error while flushing cache", e);
            error = e;
          }
          break;
        } catch (Throwable t) {
          LOG.error("Uncaught exception", t);
          throw t;
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
   * So can be overridden in subclasses.
   */
  int getNumQualifiersForTestWritesWhileScanning() {
    return 100;
  }

  /**
   * So can be overridden in subclasses.
   */
  int getTestCountForTestWritesWhileScanning() {
    return 100;
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
    int testCount = getTestCountForTestWritesWhileScanning();
    int numRows = 1;
    int numFamilies = 10;
    int numQualifiers = getNumQualifiersForTestWritesWhileScanning();
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

    this.region = initHRegion(tableName, method, CONF, families);
    FlushThread flushThread = new FlushThread();
    PutThread putThread = new PutThread(numRows, families, qualifiers);
    try {
      putThread.start();
      putThread.waitForFirstPut();

      flushThread.start();

      Scan scan = new Scan().withStartRow(Bytes.toBytes("row0"))
        .withStopRow(Bytes.toBytes("row1"));

      int expectedCount = numFamilies * numQualifiers;
      List<Cell> res = new ArrayList<>();

      long prevTimestamp = 0L;
      for (int i = 0; i < testCount; i++) {

        if (i != 0 && i % compactInterval == 0) {
          region.compact(true);
          for (HStore store : region.getStores()) {
            store.closeAndArchiveCompactedFiles();
          }
        }

        if (i != 0 && i % flushInterval == 0) {
          flushThread.flush();
        }

        boolean previousEmpty = res.isEmpty();
        res.clear();
        try (InternalScanner scanner = region.getScanner(scan)) {
          boolean moreRows;
          do {
            moreRows = scanner.next(res);
          } while (moreRows);
        }
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

    } finally {
      try {
        flushThread.done();
        flushThread.join();
        flushThread.checkNoError();

        putThread.join();
        putThread.checkNoError();
      } catch (InterruptedException ie) {
        LOG.warn("Caught exception when joining with flushThread", ie);
      }

      try {
        HBaseTestingUtil.closeRegionAndWAL(this.region);
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
      super("PutThread");
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
                put.addColumn(family, qualifier, numPutsFinished, value);
              }
            }
            region.put(put);
            numPutsFinished++;
            if (numPutsFinished > 0 && numPutsFinished % 47 == 0) {
              LOG.debug("put iteration = {}", numPutsFinished);
              Delete delete = new Delete(row, (long) numPutsFinished - 30);
              region.delete(delete);
            }
            numPutsFinished++;
          }
        } catch (InterruptedIOException e) {
          // This is fine. It means we are done, or didn't get the lock on time
          LOG.info("Interrupted", e);
        } catch (IOException e) {
          LOG.error("Error while putting records", e);
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

        @Override
        public void doAnAction() throws Exception {
          region.flush(true);
          // Compact regularly to avoid creating too many files and exceeding
          // the ulimit.
          region.compact(false);
          for (HStore store : region.getStores()) {
            store.closeAndArchiveCompactedFiles();
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
                    + "(memStoreTS:" + previousKV.getSequenceId() + ")" + ", New KV: " + kv
                    + "(memStoreTS:" + kv.getSequenceId() + ")");
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
      HBaseTestingUtil.closeRegionAndWAL(this.region);
      this.region = null;
    }
  }

  @Test
  public void testHolesInMeta() throws Exception {
    byte[] family = Bytes.toBytes("family");
    this.region = initHRegion(tableName, Bytes.toBytes("x"), Bytes.toBytes("z"), method, CONF,
        false, family);
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
  }

  @Test
  public void testIndexesScanWithOneDeletedRow() throws IOException {
    byte[] family = Bytes.toBytes("family");

    // Setting up region
    this.region = initHRegion(tableName, method, CONF, family);
    Put put = new Put(Bytes.toBytes(1L));
    put.addColumn(family, qual1, 1L, Bytes.toBytes(1L));
    region.put(put);

    region.flush(true);

    Delete delete = new Delete(Bytes.toBytes(1L), 1L);
    region.delete(delete);

    put = new Put(Bytes.toBytes(2L));
    put.addColumn(family, qual1, 2L, Bytes.toBytes(2L));
    region.put(put);

    Scan idxScan = new Scan();
    idxScan.addFamily(family);
    idxScan.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ALL, Arrays.<Filter> asList(
        new SingleColumnValueFilter(family, qual1, CompareOperator.GREATER_OR_EQUAL,
            new BinaryComparator(Bytes.toBytes(0L))), new SingleColumnValueFilter(family, qual1,
                    CompareOperator.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes(3L))))));
    InternalScanner scanner = region.getScanner(idxScan);
    List<Cell> res = new ArrayList<>();

    while (scanner.next(res)) {
      // Ignore res value.
    }
    assertEquals(1L, res.size());
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
    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(fam1)
        .setMaxVersions(Integer.MAX_VALUE).setBloomFilterType(BloomType.ROWCOL).build())
      .build();
    RegionInfo info = RegionInfoBuilder.newBuilder(tableDescriptor.getTableName()).build();
    this.region = TEST_UTIL.createLocalHRegion(info, tableDescriptor);
    int num_unique_rows = 10;
    int duplicate_multiplier = 2;
    int num_storefiles = 4;

    int version = 0;
    for (int f = 0; f < num_storefiles; f++) {
      for (int i = 0; i < duplicate_multiplier; i++) {
        for (int j = 0; j < num_unique_rows; j++) {
          Put put = new Put(Bytes.toBytes("row" + j));
          put.setDurability(Durability.SKIP_WAL);
          long ts = version++;
          put.addColumn(fam1, qf1, ts, val1);
          region.put(put);
        }
      }
      region.flush(true);
    }
    // before compaction
    HStore store = region.getStore(fam1);
    Collection<HStoreFile> storeFiles = store.getStorefiles();
    for (HStoreFile storefile : storeFiles) {
      StoreFileReader reader = storefile.getReader();
      reader.loadFileInfo();
      reader.loadBloomfilter();
      assertEquals(num_unique_rows * duplicate_multiplier, reader.getEntries());
      assertEquals(num_unique_rows, reader.getFilterEntries());
    }

    region.compact(true);

    // after compaction
    storeFiles = store.getStorefiles();
    for (HStoreFile storefile : storeFiles) {
      StoreFileReader reader = storefile.getReader();
      reader.loadFileInfo();
      reader.loadBloomfilter();
      assertEquals(num_unique_rows * duplicate_multiplier * num_storefiles, reader.getEntries());
      assertEquals(num_unique_rows, reader.getFilterEntries());
    }
  }

  @Test
  public void testAllColumnsWithBloomFilter() throws IOException {
    byte[] TABLE = Bytes.toBytes(name.getMethodName());
    byte[] FAMILY = Bytes.toBytes("family");

    // Create table
    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(TABLE))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(FAMILY)
        .setMaxVersions(Integer.MAX_VALUE).setBloomFilterType(BloomType.ROWCOL).build())
      .build();
    RegionInfo info = RegionInfoBuilder.newBuilder(tableDescriptor.getTableName()).build();
    this.region = TEST_UTIL.createLocalHRegion(info, tableDescriptor);
    // For row:0, col:0: insert versions 1 through 5.
    byte[] row = Bytes.toBytes("row:" + 0);
    byte[] column = Bytes.toBytes("column:" + 0);
    Put put = new Put(row);
    put.setDurability(Durability.SKIP_WAL);
    for (long idx = 1; idx <= 4; idx++) {
      put.addColumn(FAMILY, column, idx, Bytes.toBytes("value-version-" + idx));
    }
    region.put(put);

    // Flush
    region.flush(true);

    // Get rows
    Get get = new Get(row);
    get.readAllVersions();
    Cell[] kvs = region.get(get).rawCells();

    // Check if rows are correct
    assertEquals(4, kvs.length);
    checkOneCell(kvs[0], FAMILY, 0, 0, 4);
    checkOneCell(kvs[1], FAMILY, 0, 0, 3);
    checkOneCell(kvs[2], FAMILY, 0, 0, 2);
    checkOneCell(kvs[3], FAMILY, 0, 0, 1);
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
    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(familyName)
        .setMaxVersions(Integer.MAX_VALUE).setBloomFilterType(BloomType.ROWCOL).build())
      .build();
    RegionInfo info = RegionInfoBuilder.newBuilder(tableDescriptor.getTableName()).build();
    this.region = TEST_UTIL.createLocalHRegion(info, tableDescriptor);
    // Insert some data
    byte[] row = Bytes.toBytes("row1");
    byte[] col = Bytes.toBytes("col1");

    Put put = new Put(row);
    put.addColumn(familyName, col, 1, Bytes.toBytes("SomeRandomValue"));
    region.put(put);
    region.flush(true);

    Delete del = new Delete(row);
    region.delete(del);
    region.flush(true);

    // Get remaining rows (should have none)
    Get get = new Get(row);
    get.addColumn(familyName, col);

    Cell[] keyValues = region.get(get).rawCells();
    assertEquals(0, keyValues.length);
  }

  @Test
  public void testgetHDFSBlocksDistribution() throws Exception {
    HBaseTestingUtil htu = new HBaseTestingUtil();
    // Why do we set the block size in this test?  If we set it smaller than the kvs, then we'll
    // break up the file in to more pieces that can be distributed across the three nodes and we
    // won't be able to have the condition this test asserts; that at least one node has
    // a copy of all replicas -- if small block size, then blocks are spread evenly across the
    // the three nodes.  hfilev3 with tags seems to put us over the block size.  St.Ack.
    // final int DEFAULT_BLOCK_SIZE = 1024;
    // htu.getConfiguration().setLong("dfs.blocksize", DEFAULT_BLOCK_SIZE);
    htu.getConfiguration().setInt("dfs.replication", 2);

    // set up a cluster with 3 nodes
    SingleProcessHBaseCluster cluster = null;
    String dataNodeHosts[] = new String[] { "host1", "host2", "host3" };
    int regionServersCount = 3;

    try {
      StartTestingClusterOption option = StartTestingClusterOption.builder()
          .numRegionServers(regionServersCount).dataNodeHosts(dataNodeHosts).build();
      cluster = htu.startMiniCluster(option);
      byte[][] families = { fam1, fam2 };
      Table ht = htu.createTable(tableName, families);

      // Setting up region
      byte row[] = Bytes.toBytes("row1");
      byte col[] = Bytes.toBytes("col1");

      Put put = new Put(row);
      put.addColumn(fam1, col, 1, Bytes.toBytes("test1"));
      put.addColumn(fam2, col, 1, Bytes.toBytes("test2"));
      ht.put(put);

      HRegion firstRegion = htu.getHBaseCluster().getRegions(tableName).get(0);
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
          htu.getConfiguration(), firstRegion.getTableDescriptor(), firstRegion.getRegionInfo());
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
    RegionInfo info;
    try {
      FileSystem fs = Mockito.mock(FileSystem.class);
      Mockito.when(fs.exists((Path) Mockito.anyObject())).thenThrow(new IOException());
      TableDescriptorBuilder tableDescriptorBuilder =
        TableDescriptorBuilder.newBuilder(tableName);
      ColumnFamilyDescriptor columnFamilyDescriptor =
        ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cf")).build();
      tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
      info = RegionInfoBuilder.newBuilder(tableName).build();
      Path path = new Path(dir + "testStatusSettingToAbortIfAnyExceptionDuringRegionInitilization");
      region = HRegion.newHRegion(path, null, fs, CONF, info,
        tableDescriptorBuilder.build(), null);
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
    }
  }

  /**
   * Verifies that the .regioninfo file is written on region creation and that
   * is recreated if missing during region opening.
   */
  @Test
  public void testRegionInfoFileCreation() throws IOException {
    Path rootDir = new Path(dir + "testRegionInfoFileCreation");

    TableDescriptorBuilder tableDescriptorBuilder =
      TableDescriptorBuilder.newBuilder(TableName.valueOf(name.getMethodName()));
    ColumnFamilyDescriptor columnFamilyDescriptor =
      ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cf")).build();
    tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
    TableDescriptor tableDescriptor = tableDescriptorBuilder.build();

    RegionInfo hri = RegionInfoBuilder.newBuilder(tableDescriptor.getTableName()).build();

    // Create a region and skip the initialization (like CreateTableHandler)
    region = HBaseTestingUtil.createRegionAndWAL(hri, rootDir, CONF,
      tableDescriptor, false);
    Path regionDir = region.getRegionFileSystem().getRegionDir();
    FileSystem fs = region.getRegionFileSystem().getFileSystem();
    HBaseTestingUtil.closeRegionAndWAL(region);

    Path regionInfoFile = new Path(regionDir, HRegionFileSystem.REGION_INFO_FILE);

    // Verify that the .regioninfo file is present
    assertTrue(HRegionFileSystem.REGION_INFO_FILE + " should be present in the region dir",
        fs.exists(regionInfoFile));

    // Try to open the region
    region = HRegion.openHRegion(rootDir, hri, tableDescriptor, null, CONF);
    assertEquals(regionDir, region.getRegionFileSystem().getRegionDir());
    HBaseTestingUtil.closeRegionAndWAL(region);

    // Verify that the .regioninfo file is still there
    assertTrue(HRegionFileSystem.REGION_INFO_FILE + " should be present in the region dir",
        fs.exists(regionInfoFile));

    // Remove the .regioninfo file and verify is recreated on region open
    fs.delete(regionInfoFile, true);
    assertFalse(HRegionFileSystem.REGION_INFO_FILE + " should be removed from the region dir",
        fs.exists(regionInfoFile));

    region = HRegion.openHRegion(rootDir, hri, tableDescriptor, null, CONF);
//    region = TEST_UTIL.openHRegion(hri, htd);
    assertEquals(regionDir, region.getRegionFileSystem().getRegionDir());
    HBaseTestingUtil.closeRegionAndWAL(region);

    // Verify that the .regioninfo file is still there
    assertTrue(HRegionFileSystem.REGION_INFO_FILE + " should be present in the region dir",
        fs.exists(new Path(regionDir, HRegionFileSystem.REGION_INFO_FILE)));

    region = null;
  }

  /**
   * TestCase for increment
   */
  private static class Incrementer implements Runnable {
    private HRegion region;
    private final static byte[] incRow = Bytes.toBytes("incRow");
    private final static byte[] family = Bytes.toBytes("family");
    private final static byte[] qualifier = Bytes.toBytes("qualifier");
    private final static long ONE = 1L;
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
    long expected = (long) threadNum * incCounter;
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
    get.readVersions(1);
    Result res = this.region.get(get);
    List<Cell> kvs = res.getColumnCells(Incrementer.family, Incrementer.qualifier);

    // we just got the latest version
    assertEquals(1, kvs.size());
    Cell kv = kvs.get(0);
    assertEquals(expected, Bytes.toLong(kv.getValueArray(), kv.getValueOffset()));
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
        app.addColumn(family, qualifier, CHAR);
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
    get.readVersions(1);
    Result res = this.region.get(get);
    List<Cell> kvs = res.getColumnCells(Appender.family, Appender.qualifier);

    // we just got the latest version
    assertEquals(1, kvs.size());
    Cell kv = kvs.get(0);
    byte[] appendResult = new byte[kv.getValueLength()];
    System.arraycopy(kv.getValueArray(), kv.getValueOffset(), appendResult, 0, kv.getValueLength());
    assertArrayEquals(expected, appendResult);
  }

  /**
   * Test case to check put function with memstore flushing for same row, same ts
   * @throws Exception
   */
  @Test
  public void testPutWithMemStoreFlush() throws Exception {
    byte[] family = Bytes.toBytes("family");
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
    put.addColumn(family, qualifier, 1234567L, value);
    region.put(put);
    get = new Get(row);
    get.addColumn(family, qualifier);
    get.readAllVersions();
    res = this.region.get(get);
    kvs = res.getColumnCells(family, qualifier);
    assertEquals(1, kvs.size());
    assertArrayEquals(Bytes.toBytes("value0"), CellUtil.cloneValue(kvs.get(0)));

    region.flush(true);
    get = new Get(row);
    get.addColumn(family, qualifier);
    get.readAllVersions();
    res = this.region.get(get);
    kvs = res.getColumnCells(family, qualifier);
    assertEquals(1, kvs.size());
    assertArrayEquals(Bytes.toBytes("value0"), CellUtil.cloneValue(kvs.get(0)));

    put = new Put(row);
    value = Bytes.toBytes("value1");
    put.addColumn(family, qualifier, 1234567L, value);
    region.put(put);
    get = new Get(row);
    get.addColumn(family, qualifier);
    get.readAllVersions();
    res = this.region.get(get);
    kvs = res.getColumnCells(family, qualifier);
    assertEquals(1, kvs.size());
    assertArrayEquals(Bytes.toBytes("value1"), CellUtil.cloneValue(kvs.get(0)));

    region.flush(true);
    get = new Get(row);
    get.addColumn(family, qualifier);
    get.readAllVersions();
    res = this.region.get(get);
    kvs = res.getColumnCells(family, qualifier);
    assertEquals(1, kvs.size());
    assertArrayEquals(Bytes.toBytes("value1"), CellUtil.cloneValue(kvs.get(0)));
  }

  @Test
  public void testDurability() throws Exception {
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
    durabilityTest(method, Durability.SYNC_WAL, Durability.SKIP_WAL, 0, false, false, false);
    durabilityTest(method, Durability.FSYNC_WAL, Durability.SKIP_WAL, 0, false, false, false);
    durabilityTest(method, Durability.ASYNC_WAL, Durability.SKIP_WAL, 0, false, false, false);
    durabilityTest(method, Durability.SKIP_WAL, Durability.SKIP_WAL, 0, false, false, false);
    durabilityTest(method, Durability.USE_DEFAULT, Durability.SKIP_WAL, 0, false, false, false);
    durabilityTest(method, Durability.SKIP_WAL, Durability.USE_DEFAULT, 0, false, false, false);

  }

  private void durabilityTest(String method, Durability tableDurability,
      Durability mutationDurability, long timeout, boolean expectAppend, final boolean expectSync,
      final boolean expectSyncFromLogSyncer) throws Exception {
    Configuration conf = HBaseConfiguration.create(CONF);
    method = method + "_" + tableDurability.name() + "_" + mutationDurability.name();
    byte[] family = Bytes.toBytes("family");
    Path logDir = new Path(new Path(dir + method), "log");
    final Configuration walConf = new Configuration(conf);
    CommonFSUtils.setRootDir(walConf, logDir);
    // XXX: The spied AsyncFSWAL can not work properly because of a Mockito defect that can not
    // deal with classes which have a field of an inner class. See discussions in HBASE-15536.
    walConf.set(WALFactory.WAL_PROVIDER, "filesystem");
    final WALFactory wals = new WALFactory(walConf, HBaseTestingUtil.getRandomUUID().toString());
    final WAL wal = spy(wals.getWAL(RegionInfoBuilder.newBuilder(tableName).build()));
    this.region = initHRegion(tableName, HConstants.EMPTY_START_ROW,
        HConstants.EMPTY_END_ROW, CONF, false, tableDurability, wal,
        new byte[][] { family });

    Put put = new Put(Bytes.toBytes("r1"));
    put.addColumn(family, Bytes.toBytes("q1"), Bytes.toBytes("v1"));
    put.setDurability(mutationDurability);
    region.put(put);

    // verify append called or not
    verify(wal, expectAppend ? times(1) : never()).appendData((RegionInfo) any(),
      (WALKeyImpl) any(), (WALEdit) any());

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

    HBaseTestingUtil.closeRegionAndWAL(this.region);
    wals.close();
    this.region = null;
  }

  @Test
  public void testRegionReplicaSecondary() throws IOException {
    // create a primary region, load some data and flush
    // create a secondary region, and do a get against that
    Path rootDir = new Path(dir + name.getMethodName());
    CommonFSUtils.setRootDir(TEST_UTIL.getConfiguration(), rootDir);

    byte[][] families = new byte[][] {
        Bytes.toBytes("cf1"), Bytes.toBytes("cf2"), Bytes.toBytes("cf3")
    };
    byte[] cq = Bytes.toBytes("cq");
    TableDescriptorBuilder builder =
      TableDescriptorBuilder.newBuilder(
        TableName.valueOf(name.getMethodName()));
    for (byte[] family : families) {
      builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(family));
    }
    TableDescriptor tableDescriptor = builder.build();
    long time = EnvironmentEdgeManager.currentTime();
    RegionInfo primaryHri = RegionInfoBuilder.newBuilder(tableDescriptor.getTableName())
      .setRegionId(time).setReplicaId(0).build();
    RegionInfo secondaryHri = RegionInfoBuilder.newBuilder(tableDescriptor.getTableName())
      .setRegionId(time).setReplicaId(1).build();

    HRegion primaryRegion = null, secondaryRegion = null;

    try {
      primaryRegion = HBaseTestingUtil.createRegionAndWAL(primaryHri,
          rootDir, TEST_UTIL.getConfiguration(), tableDescriptor);

      // load some data
      putData(primaryRegion, 0, 1000, cq, families);

      // flush region
      primaryRegion.flush(true);

      // open secondary region
      secondaryRegion = HRegion.openHRegion(rootDir, secondaryHri, tableDescriptor, null, CONF);

      verifyData(secondaryRegion, 0, 1000, cq, families);
    } finally {
      if (primaryRegion != null) {
        HBaseTestingUtil.closeRegionAndWAL(primaryRegion);
      }
      if (secondaryRegion != null) {
        HBaseTestingUtil.closeRegionAndWAL(secondaryRegion);
      }
    }
  }

  @Test
  public void testRegionReplicaSecondaryIsReadOnly() throws IOException {
    // create a primary region, load some data and flush
    // create a secondary region, and do a put against that
    Path rootDir = new Path(dir + name.getMethodName());
    CommonFSUtils.setRootDir(TEST_UTIL.getConfiguration(), rootDir);

    byte[][] families = new byte[][] {
        Bytes.toBytes("cf1"), Bytes.toBytes("cf2"), Bytes.toBytes("cf3")
    };
    byte[] cq = Bytes.toBytes("cq");
    TableDescriptorBuilder builder =
      TableDescriptorBuilder.newBuilder(TableName.valueOf(name.getMethodName()));
    for (byte[] family : families) {
      builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(family));
    }
    TableDescriptor tableDescriptor = builder.build();
    long time = EnvironmentEdgeManager.currentTime();
    RegionInfo primaryHri = RegionInfoBuilder.newBuilder(tableDescriptor.getTableName())
      .setRegionId(time).setReplicaId(0).build();
    RegionInfo secondaryHri = RegionInfoBuilder.newBuilder(tableDescriptor.getTableName())
      .setRegionId(time).setReplicaId(1).build();

    HRegion primaryRegion = null, secondaryRegion = null;

    try {
      primaryRegion = HBaseTestingUtil.createRegionAndWAL(primaryHri,
          rootDir, TEST_UTIL.getConfiguration(), tableDescriptor);

      // load some data
      putData(primaryRegion, 0, 1000, cq, families);

      // flush region
      primaryRegion.flush(true);

      // open secondary region
      secondaryRegion = HRegion.openHRegion(rootDir, secondaryHri, tableDescriptor, null, CONF);

      try {
        putData(secondaryRegion, 0, 1000, cq, families);
        fail("Should have thrown exception");
      } catch (IOException ex) {
        // expected
      }
    } finally {
      if (primaryRegion != null) {
        HBaseTestingUtil.closeRegionAndWAL(primaryRegion);
      }
      if (secondaryRegion != null) {
        HBaseTestingUtil.closeRegionAndWAL(secondaryRegion);
      }
    }
  }

  static WALFactory createWALFactory(Configuration conf, Path rootDir) throws IOException {
    Configuration confForWAL = new Configuration(conf);
    confForWAL.set(HConstants.HBASE_DIR, rootDir.toString());
    return new WALFactory(confForWAL, "hregion-" + RandomStringUtils.randomNumeric(8));
  }

  @Test
  public void testCompactionFromPrimary() throws IOException {
    Path rootDir = new Path(dir + name.getMethodName());
    CommonFSUtils.setRootDir(TEST_UTIL.getConfiguration(), rootDir);

    byte[][] families = new byte[][] {
        Bytes.toBytes("cf1"), Bytes.toBytes("cf2"), Bytes.toBytes("cf3")
    };
    byte[] cq = Bytes.toBytes("cq");
    TableDescriptorBuilder builder =
      TableDescriptorBuilder.newBuilder(TableName.valueOf(name.getMethodName()));
    for (byte[] family : families) {
      builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(family));
    }
    TableDescriptor tableDescriptor = builder.build();
    long time = EnvironmentEdgeManager.currentTime();
    RegionInfo primaryHri = RegionInfoBuilder.newBuilder(tableDescriptor.getTableName())
      .setRegionId(time).setReplicaId(0).build();
    RegionInfo secondaryHri = RegionInfoBuilder.newBuilder(tableDescriptor.getTableName())
      .setRegionId(time).setReplicaId(1).build();

    HRegion primaryRegion = null, secondaryRegion = null;

    try {
      primaryRegion = HBaseTestingUtil.createRegionAndWAL(primaryHri,
          rootDir, TEST_UTIL.getConfiguration(), tableDescriptor);

      // load some data
      putData(primaryRegion, 0, 1000, cq, families);

      // flush region
      primaryRegion.flush(true);

      // open secondary region
      secondaryRegion = HRegion.openHRegion(rootDir, secondaryHri, tableDescriptor, null, CONF);

      // move the file of the primary region to the archive, simulating a compaction
      Collection<HStoreFile> storeFiles = primaryRegion.getStore(families[0]).getStorefiles();
      primaryRegion.getRegionFileSystem().removeStoreFiles(Bytes.toString(families[0]), storeFiles);
      Collection<StoreFileInfo> storeFileInfos = primaryRegion.getRegionFileSystem()
          .getStoreFiles(Bytes.toString(families[0]));
      Assert.assertTrue(storeFileInfos == null || storeFileInfos.isEmpty());

      verifyData(secondaryRegion, 0, 1000, cq, families);
    } finally {
      if (primaryRegion != null) {
        HBaseTestingUtil.closeRegionAndWAL(primaryRegion);
      }
      if (secondaryRegion != null) {
        HBaseTestingUtil.closeRegionAndWAL(secondaryRegion);
      }
    }
  }

  private void putData(int startRow, int numRows, byte[] qf, byte[]... families) throws
      IOException {
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
        put.addColumn(family, qf, null);
      }
      region.put(put);
      LOG.info(put.toString());
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
        assertTrue(CellUtil.matchingRows(raw[j], row));
        assertTrue(CellUtil.matchingFamily(raw[j], families[j]));
        assertTrue(CellUtil.matchingQualifier(raw[j], qf));
      }
    }
  }

  static void assertGet(final HRegion r, final byte[] family, final byte[] k) throws IOException {
    // Now I have k, get values out and assert they are as expected.
    Get get = new Get(k).addFamily(family).readAllVersions();
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
  protected void assertScan(final HRegion r, final byte[] fs, final byte[] firstValue)
      throws IOException {
    byte[][] families = { fs };
    Scan scan = new Scan();
    for (int i = 0; i < families.length; i++)
      scan.addFamily(families[i]);
    InternalScanner s = r.getScanner(scan);
    try {
      List<Cell> curVals = new ArrayList<>();
      boolean first = true;
      OUTER_LOOP: while (s.next(curVals)) {
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
   */
  @Test
  public void testFlushResult() throws IOException {
    byte[] family = Bytes.toBytes("family");

    this.region = initHRegion(tableName, method, family);

    // empty memstore, flush doesn't run
    HRegion.FlushResult fr = region.flush(true);
    assertFalse(fr.isFlushSucceeded());
    assertFalse(fr.isCompactionNeeded());

    // Flush enough files to get up to the threshold, doesn't need compactions
    for (int i = 0; i < 2; i++) {
      Put put = new Put(tableName.toBytes()).addColumn(family, family, tableName.toBytes());
      region.put(put);
      fr = region.flush(true);
      assertTrue(fr.isFlushSucceeded());
      assertFalse(fr.isCompactionNeeded());
    }

    // Two flushes after the threshold, compactions are needed
    for (int i = 0; i < 2; i++) {
      Put put = new Put(tableName.toBytes()).addColumn(family, family, tableName.toBytes());
      region.put(put);
      fr = region.flush(true);
      assertTrue(fr.isFlushSucceeded());
      assertTrue(fr.isCompactionNeeded());
    }
  }

  protected Configuration initSplit() {
    // Always compact if there is more than one store file.
    CONF.setInt("hbase.hstore.compactionThreshold", 2);

    CONF.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 10 * 1000);

    // Increase the amount of time between client retries
    CONF.setLong("hbase.client.pause", 15 * 1000);

    // This size should make it so we always split using the addContent
    // below. After adding all data, the first region is 1.3M
    CONF.setLong(HConstants.HREGION_MAX_FILESIZE, 1024 * 128);
    return CONF;
  }

  /**
   * @return A region on which you must call
   *         {@link HBaseTestingUtil#closeRegionAndWAL(HRegion)} when done.
   */
  protected HRegion initHRegion(TableName tableName, String callingMethod, Configuration conf,
      byte[]... families) throws IOException {
    return initHRegion(tableName, callingMethod, conf, false, families);
  }

  /**
   * @return A region on which you must call
   *         {@link HBaseTestingUtil#closeRegionAndWAL(HRegion)} when done.
   */
  protected HRegion initHRegion(TableName tableName, String callingMethod, Configuration conf,
      boolean isReadOnly, byte[]... families) throws IOException {
    return initHRegion(tableName, null, null, callingMethod, conf, isReadOnly, families);
  }

  protected HRegion initHRegion(TableName tableName, byte[] startKey, byte[] stopKey,
    String callingMethod, Configuration conf, boolean isReadOnly, byte[]... families)
    throws IOException {
    Path logDir = TEST_UTIL.getDataTestDirOnTestFS(callingMethod + ".log");
    RegionInfo hri =
      RegionInfoBuilder.newBuilder(tableName).setStartKey(startKey).setEndKey(stopKey).build();
    final WAL wal = HBaseTestingUtil.createWal(conf, logDir, hri);
    return initHRegion(tableName, startKey, stopKey, conf, isReadOnly, Durability.SYNC_WAL, wal,
      families);
  }

  /**
   * @return A region on which you must call
   *         {@link HBaseTestingUtil#closeRegionAndWAL(HRegion)} when done.
   */
  public HRegion initHRegion(TableName tableName, byte[] startKey, byte[] stopKey,
      Configuration conf, boolean isReadOnly, Durability durability, WAL wal,
      byte[]... families) throws IOException {
    ChunkCreator.initialize(MemStoreLAB.CHUNK_SIZE_DEFAULT, false, 0, 0,
      0, null, MemStoreLAB.INDEX_CHUNK_SIZE_PERCENTAGE_DEFAULT);
    return TEST_UTIL.createLocalHRegion(tableName, startKey, stopKey,
        conf, isReadOnly, durability, wal, families);
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

  @Test
  public void testReverseScanner_FromMemStore_SingleCF_Normal()
      throws IOException {
    byte[] rowC = Bytes.toBytes("rowC");
    byte[] rowA = Bytes.toBytes("rowA");
    byte[] rowB = Bytes.toBytes("rowB");
    byte[] cf = Bytes.toBytes("CF");
    byte[][] families = { cf };
    byte[] col = Bytes.toBytes("C");
    long ts = 1;
    this.region = initHRegion(tableName, method, families);
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

    Scan scan = new Scan().withStartRow(rowC);
    scan.readVersions(5);
    scan.setReversed(true);
    InternalScanner scanner = region.getScanner(scan);
    List<Cell> currRow = new ArrayList<>();
    boolean hasNext = scanner.next(currRow);
    assertEquals(2, currRow.size());
    assertTrue(Bytes.equals(currRow.get(0).getRowArray(), currRow.get(0).getRowOffset(), currRow
        .get(0).getRowLength(), rowC, 0, rowC.length));
    assertTrue(hasNext);
    currRow.clear();
    hasNext = scanner.next(currRow);
    assertEquals(1, currRow.size());
    assertTrue(Bytes.equals(currRow.get(0).getRowArray(), currRow.get(0).getRowOffset(), currRow
        .get(0).getRowLength(), rowB, 0, rowB.length));
    assertTrue(hasNext);
    currRow.clear();
    hasNext = scanner.next(currRow);
    assertEquals(1, currRow.size());
    assertTrue(Bytes.equals(currRow.get(0).getRowArray(), currRow.get(0).getRowOffset(), currRow
        .get(0).getRowLength(), rowA, 0, rowA.length));
    assertFalse(hasNext);
    scanner.close();
  }

  @Test
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
    this.region = initHRegion(tableName, method, families);
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

    Scan scan = new Scan().withStartRow(rowD);
    List<Cell> currRow = new ArrayList<>();
    scan.setReversed(true);
    scan.readVersions(5);
    InternalScanner scanner = region.getScanner(scan);
    boolean hasNext = scanner.next(currRow);
    assertEquals(2, currRow.size());
    assertTrue(Bytes.equals(currRow.get(0).getRowArray(), currRow.get(0).getRowOffset(), currRow
        .get(0).getRowLength(), rowC, 0, rowC.length));
    assertTrue(hasNext);
    currRow.clear();
    hasNext = scanner.next(currRow);
    assertEquals(1, currRow.size());
    assertTrue(Bytes.equals(currRow.get(0).getRowArray(), currRow.get(0).getRowOffset(), currRow
        .get(0).getRowLength(), rowB, 0, rowB.length));
    assertTrue(hasNext);
    currRow.clear();
    hasNext = scanner.next(currRow);
    assertEquals(1, currRow.size());
    assertTrue(Bytes.equals(currRow.get(0).getRowArray(), currRow.get(0).getRowOffset(), currRow
        .get(0).getRowLength(), rowA, 0, rowA.length));
    assertFalse(hasNext);
    scanner.close();
  }

  @Test
  public void testReverseScanner_FromMemStore_SingleCF_FullScan()
      throws IOException {
    byte[] rowC = Bytes.toBytes("rowC");
    byte[] rowA = Bytes.toBytes("rowA");
    byte[] rowB = Bytes.toBytes("rowB");
    byte[] cf = Bytes.toBytes("CF");
    byte[][] families = { cf };
    byte[] col = Bytes.toBytes("C");
    long ts = 1;
    this.region = initHRegion(tableName, method, families);
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
    List<Cell> currRow = new ArrayList<>();
    scan.setReversed(true);
    InternalScanner scanner = region.getScanner(scan);
    boolean hasNext = scanner.next(currRow);
    assertEquals(1, currRow.size());
    assertTrue(Bytes.equals(currRow.get(0).getRowArray(), currRow.get(0).getRowOffset(), currRow
        .get(0).getRowLength(), rowC, 0, rowC.length));
    assertTrue(hasNext);
    currRow.clear();
    hasNext = scanner.next(currRow);
    assertEquals(1, currRow.size());
    assertTrue(Bytes.equals(currRow.get(0).getRowArray(), currRow.get(0).getRowOffset(), currRow
        .get(0).getRowLength(), rowB, 0, rowB.length));
    assertTrue(hasNext);
    currRow.clear();
    hasNext = scanner.next(currRow);
    assertEquals(1, currRow.size());
    assertTrue(Bytes.equals(currRow.get(0).getRowArray(), currRow.get(0).getRowOffset(), currRow
        .get(0).getRowLength(), rowA, 0, rowA.length));
    assertFalse(hasNext);
    scanner.close();
  }

  @Test
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
    this.region = initHRegion(tableName, method, families);
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
    Scan scan = new Scan().withStartRow(rowD).withStopRow(rowA);
    scan.addColumn(families[0], col1);
    scan.setReversed(true);
    List<Cell> currRow = new ArrayList<>();
    InternalScanner scanner = region.getScanner(scan);
    boolean hasNext = scanner.next(currRow);
    assertEquals(1, currRow.size());
    assertTrue(Bytes.equals(currRow.get(0).getRowArray(), currRow.get(0).getRowOffset(), currRow
        .get(0).getRowLength(), rowD, 0, rowD.length));
    assertTrue(hasNext);
    currRow.clear();
    hasNext = scanner.next(currRow);
    assertEquals(1, currRow.size());
    assertTrue(Bytes.equals(currRow.get(0).getRowArray(), currRow.get(0).getRowOffset(), currRow
        .get(0).getRowLength(), rowC, 0, rowC.length));
    assertTrue(hasNext);
    currRow.clear();
    hasNext = scanner.next(currRow);
    assertEquals(1, currRow.size());
    assertTrue(Bytes.equals(currRow.get(0).getRowArray(), currRow.get(0).getRowOffset(), currRow
        .get(0).getRowLength(), rowB, 0, rowB.length));
    assertFalse(hasNext);
    scanner.close();

    scan = new Scan().withStartRow(rowD).withStopRow(rowA);
    scan.addColumn(families[0], col2);
    scan.setReversed(true);
    currRow.clear();
    scanner = region.getScanner(scan);
    hasNext = scanner.next(currRow);
    assertEquals(1, currRow.size());
    assertTrue(Bytes.equals(currRow.get(0).getRowArray(), currRow.get(0).getRowOffset(), currRow
        .get(0).getRowLength(), rowD, 0, rowD.length));
    scanner.close();
  }

  @Test
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
    Configuration conf = new Configuration(CONF);
    conf.setInt("test.block.size", 1);
    this.region = initHRegion(tableName, method, conf, families);
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
    Scan scan = new Scan().withStartRow(rowD).withStopRow(rowA);
    scan.addColumn(families[0], col1);
    scan.setReversed(true);
    List<Cell> currRow = new ArrayList<>();
    InternalScanner scanner = region.getScanner(scan);
    boolean hasNext = scanner.next(currRow);
    assertEquals(1, currRow.size());
    assertTrue(Bytes.equals(currRow.get(0).getRowArray(), currRow.get(0).getRowOffset(), currRow
        .get(0).getRowLength(), rowD, 0, rowD.length));
    assertTrue(hasNext);
    currRow.clear();
    hasNext = scanner.next(currRow);
    assertEquals(1, currRow.size());
    assertTrue(Bytes.equals(currRow.get(0).getRowArray(), currRow.get(0).getRowOffset(), currRow
        .get(0).getRowLength(), rowC, 0, rowC.length));
    assertTrue(hasNext);
    currRow.clear();
    hasNext = scanner.next(currRow);
    assertEquals(1, currRow.size());
    assertTrue(Bytes.equals(currRow.get(0).getRowArray(), currRow.get(0).getRowOffset(), currRow
        .get(0).getRowLength(), rowB, 0, rowB.length));
    assertFalse(hasNext);
    scanner.close();

    scan = new Scan().withStartRow(rowD).withStopRow(rowA);
    scan.addColumn(families[0], col2);
    scan.setReversed(true);
    currRow.clear();
    scanner = region.getScanner(scan);
    hasNext = scanner.next(currRow);
    assertEquals(1, currRow.size());
    assertTrue(Bytes.equals(currRow.get(0).getRowArray(), currRow.get(0).getRowOffset(), currRow
        .get(0).getRowLength(), rowD, 0, rowD.length));
    scanner.close();
  }

  @Test
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
    Configuration conf = new Configuration(CONF);
    // disable compactions in this test.
    conf.setInt("hbase.hstore.compactionThreshold", 10000);
    this.region = initHRegion(tableName, method, conf, families);
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
    Scan scan = new Scan().withStartRow(row4);
    scan.readVersions(5);
    scan.setBatch(3);
    scan.setReversed(true);
    InternalScanner scanner = region.getScanner(scan);
    List<Cell> currRow = new ArrayList<>();
    boolean hasNext = false;
    // 1. scan out "row4" (5 kvs), "row5" can't be scanned out since not
    // included in scan range
    // "row4" takes 2 next() calls since batch=3
    hasNext = scanner.next(currRow);
    assertEquals(3, currRow.size());
    assertTrue(Bytes.equals(currRow.get(0).getRowArray(), currRow.get(0).getRowOffset(), currRow
        .get(0).getRowLength(), row4, 0, row4.length));
    assertTrue(hasNext);
    currRow.clear();
    hasNext = scanner.next(currRow);
    assertEquals(2, currRow.size());
    assertTrue(Bytes.equals(currRow.get(0).getRowArray(), currRow.get(0).getRowOffset(),
        currRow.get(0).getRowLength(), row4, 0,
      row4.length));
    assertTrue(hasNext);
    // 2. scan out "row3" (2 kv)
    currRow.clear();
    hasNext = scanner.next(currRow);
    assertEquals(2, currRow.size());
    assertTrue(Bytes.equals(currRow.get(0).getRowArray(), currRow.get(0).getRowOffset(), currRow
        .get(0).getRowLength(), row3, 0, row3.length));
    assertTrue(hasNext);
    // 3. scan out "row2" (4 kvs)
    // "row2" takes 2 next() calls since batch=3
    currRow.clear();
    hasNext = scanner.next(currRow);
    assertEquals(3, currRow.size());
    assertTrue(Bytes.equals(currRow.get(0).getRowArray(), currRow.get(0).getRowOffset(), currRow
        .get(0).getRowLength(), row2, 0, row2.length));
    assertTrue(hasNext);
    currRow.clear();
    hasNext = scanner.next(currRow);
    assertEquals(1, currRow.size());
    assertTrue(Bytes.equals(currRow.get(0).getRowArray(), currRow.get(0).getRowOffset(), currRow
        .get(0).getRowLength(), row2, 0, row2.length));
    assertTrue(hasNext);
    // 4. scan out "row1" (2 kv)
    currRow.clear();
    hasNext = scanner.next(currRow);
    assertEquals(2, currRow.size());
    assertTrue(Bytes.equals(currRow.get(0).getRowArray(), currRow.get(0).getRowOffset(), currRow
        .get(0).getRowLength(), row1, 0, row1.length));
    assertTrue(hasNext);
    // 5. scan out "row0" (1 kv)
    currRow.clear();
    hasNext = scanner.next(currRow);
    assertEquals(1, currRow.size());
    assertTrue(Bytes.equals(currRow.get(0).getRowArray(), currRow.get(0).getRowOffset(), currRow
        .get(0).getRowLength(), row0, 0, row0.length));
    assertFalse(hasNext);

    scanner.close();
  }

  @Test
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
    Configuration conf = new Configuration(CONF);
    // disable compactions in this test.
    conf.setInt("hbase.hstore.compactionThreshold", 10000);
    this.region = initHRegion(tableName, method, conf, families);
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
    Scan scan = new Scan().withStartRow(row4);
    scan.setReversed(true);
    scan.setBatch(10);
    InternalScanner scanner = region.getScanner(scan);
    List<Cell> currRow = new ArrayList<>();
    boolean hasNext = scanner.next(currRow);
    assertEquals(1, currRow.size());
    assertTrue(Bytes.equals(currRow.get(0).getRowArray(), currRow.get(0).getRowOffset(), currRow
        .get(0).getRowLength(), row4, 0, row4.length));
    assertTrue(hasNext);
    currRow.clear();
    hasNext = scanner.next(currRow);
    assertEquals(1, currRow.size());
    assertTrue(Bytes.equals(currRow.get(0).getRowArray(), currRow.get(0).getRowOffset(), currRow
        .get(0).getRowLength(), row3, 0, row3.length));
    assertTrue(hasNext);
    currRow.clear();
    hasNext = scanner.next(currRow);
    assertEquals(1, currRow.size());
    assertTrue(Bytes.equals(currRow.get(0).getRowArray(), currRow.get(0).getRowOffset(), currRow
        .get(0).getRowLength(), row2, 0, row2.length));
    assertTrue(hasNext);
    currRow.clear();
    hasNext = scanner.next(currRow);
    assertEquals(1, currRow.size());
    assertTrue(Bytes.equals(currRow.get(0).getRowArray(), currRow.get(0).getRowOffset(), currRow
        .get(0).getRowLength(), row1, 0, row1.length));
    assertFalse(hasNext);
  }

  /**
   * Test for HBASE-14497: Reverse Scan threw StackOverflow caused by readPt checking
   */
  @Test
  public void testReverseScanner_StackOverflow() throws IOException {
    byte[] cf1 = Bytes.toBytes("CF1");
    byte[][] families = {cf1};
    byte[] col = Bytes.toBytes("C");
    Configuration conf = new Configuration(CONF);
    this.region = initHRegion(tableName, method, conf, families);
    // setup with one storefile and one memstore, to create scanner and get an earlier readPt
    Put put = new Put(Bytes.toBytes("19998"));
    put.addColumn(cf1, col, Bytes.toBytes("val"));
    region.put(put);
    region.flushcache(true, true, FlushLifeCycleTracker.DUMMY);
    Put put2 = new Put(Bytes.toBytes("19997"));
    put2.addColumn(cf1, col, Bytes.toBytes("val"));
    region.put(put2);

    Scan scan = new Scan().withStartRow(Bytes.toBytes("19998"));
    scan.setReversed(true);
    InternalScanner scanner = region.getScanner(scan);

    // create one storefile contains many rows will be skipped
    // to check StoreFileScanner.seekToPreviousRow
    for (int i = 10000; i < 20000; i++) {
      Put p = new Put(Bytes.toBytes(""+i));
      p.addColumn(cf1, col, Bytes.toBytes("" + i));
      region.put(p);
    }
    region.flushcache(true, true, FlushLifeCycleTracker.DUMMY);

    // create one memstore contains many rows will be skipped
    // to check MemStoreScanner.seekToPreviousRow
    for (int i = 10000; i < 20000; i++) {
      Put p = new Put(Bytes.toBytes(""+i));
      p.addColumn(cf1, col, Bytes.toBytes("" + i));
      region.put(p);
    }

    List<Cell> currRow = new ArrayList<>();
    boolean hasNext;
    do {
      hasNext = scanner.next(currRow);
    } while (hasNext);
    assertEquals(2, currRow.size());
    assertEquals("19998", Bytes.toString(currRow.get(0).getRowArray(),
      currRow.get(0).getRowOffset(), currRow.get(0).getRowLength()));
    assertEquals("19997", Bytes.toString(currRow.get(1).getRowArray(),
      currRow.get(1).getRowOffset(), currRow.get(1).getRowLength()));
  }

  @Test
  public void testReverseScanShouldNotScanMemstoreIfReadPtLesser() throws Exception {
    byte[] cf1 = Bytes.toBytes("CF1");
    byte[][] families = { cf1 };
    byte[] col = Bytes.toBytes("C");
    this.region = initHRegion(tableName, method, CONF, families);
    // setup with one storefile and one memstore, to create scanner and get an earlier readPt
    Put put = new Put(Bytes.toBytes("19996"));
    put.addColumn(cf1, col, Bytes.toBytes("val"));
    region.put(put);
    Put put2 = new Put(Bytes.toBytes("19995"));
    put2.addColumn(cf1, col, Bytes.toBytes("val"));
    region.put(put2);
    // create a reverse scan
    Scan scan = new Scan().withStartRow(Bytes.toBytes("19996"));
    scan.setReversed(true);
    RegionScannerImpl scanner = region.getScanner(scan);

    // flush the cache. This will reset the store scanner
    region.flushcache(true, true, FlushLifeCycleTracker.DUMMY);

    // create one memstore contains many rows will be skipped
    // to check MemStoreScanner.seekToPreviousRow
    for (int i = 10000; i < 20000; i++) {
      Put p = new Put(Bytes.toBytes("" + i));
      p.addColumn(cf1, col, Bytes.toBytes("" + i));
      region.put(p);
    }
    List<Cell> currRow = new ArrayList<>();
    boolean hasNext;
    boolean assertDone = false;
    do {
      hasNext = scanner.next(currRow);
      // With HBASE-15871, after the scanner is reset the memstore scanner should not be
      // added here
      if (!assertDone) {
        StoreScanner current =
            (StoreScanner) (scanner.storeHeap).getCurrentForTesting();
        List<KeyValueScanner> scanners = current.getAllScannersForTesting();
        assertEquals("There should be only one scanner the store file scanner", 1,
          scanners.size());
        assertDone = true;
      }
    } while (hasNext);
    assertEquals(2, currRow.size());
    assertEquals("19996", Bytes.toString(currRow.get(0).getRowArray(),
      currRow.get(0).getRowOffset(), currRow.get(0).getRowLength()));
    assertEquals("19995", Bytes.toString(currRow.get(1).getRowArray(),
      currRow.get(1).getRowOffset(), currRow.get(1).getRowLength()));
  }

  @Test
  public void testReverseScanWhenPutCellsAfterOpenReverseScan() throws Exception {
    byte[] cf1 = Bytes.toBytes("CF1");
    byte[][] families = { cf1 };
    byte[] col = Bytes.toBytes("C");

    this.region = initHRegion(tableName, method, CONF, families);

    Put put = new Put(Bytes.toBytes("199996"));
    put.addColumn(cf1, col, Bytes.toBytes("val"));
    region.put(put);
    Put put2 = new Put(Bytes.toBytes("199995"));
    put2.addColumn(cf1, col, Bytes.toBytes("val"));
    region.put(put2);

    // Create a reverse scan
    Scan scan = new Scan().withStartRow(Bytes.toBytes("199996"));
    scan.setReversed(true);
    RegionScannerImpl scanner = region.getScanner(scan);

    // Put a lot of cells that have sequenceIDs grater than the readPt of the reverse scan
    for (int i = 100000; i < 200000; i++) {
      Put p = new Put(Bytes.toBytes("" + i));
      p.addColumn(cf1, col, Bytes.toBytes("" + i));
      region.put(p);
    }
    List<Cell> currRow = new ArrayList<>();
    boolean hasNext;
    do {
      hasNext = scanner.next(currRow);
    } while (hasNext);

    assertEquals(2, currRow.size());
    assertEquals("199996", Bytes.toString(currRow.get(0).getRowArray(),
      currRow.get(0).getRowOffset(), currRow.get(0).getRowLength()));
    assertEquals("199995", Bytes.toString(currRow.get(1).getRowArray(),
      currRow.get(1).getRowOffset(), currRow.get(1).getRowLength()));
  }

  @Test
  public void testWriteRequestsCounter() throws IOException {
    byte[] fam = Bytes.toBytes("info");
    byte[][] families = { fam };
    this.region = initHRegion(tableName, method, CONF, families);

    Assert.assertEquals(0L, region.getWriteRequestsCount());

    Put put = new Put(row);
    put.addColumn(fam, fam, fam);

    Assert.assertEquals(0L, region.getWriteRequestsCount());
    region.put(put);
    Assert.assertEquals(1L, region.getWriteRequestsCount());
    region.put(put);
    Assert.assertEquals(2L, region.getWriteRequestsCount());
    region.put(put);
    Assert.assertEquals(3L, region.getWriteRequestsCount());

    region.delete(new Delete(row));
    Assert.assertEquals(4L, region.getWriteRequestsCount());
  }

  @Test
  public void testOpenRegionWrittenToWAL() throws Exception {
    final ServerName serverName = ServerName.valueOf(name.getMethodName(), 100, 42);
    final RegionServerServices rss = spy(TEST_UTIL.createMockRegionServerService(serverName));

    TableDescriptor htd = TableDescriptorBuilder.newBuilder(TableName.valueOf(name.getMethodName()))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(fam1))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(fam2)).build();
    RegionInfo hri = RegionInfoBuilder.newBuilder(htd.getTableName()).build();

    // open the region w/o rss and wal and flush some files
    region =
         HBaseTestingUtil.createRegionAndWAL(hri, TEST_UTIL.getDataTestDir(), TEST_UTIL
             .getConfiguration(), htd);
    assertNotNull(region);

    // create a file in fam1 for the region before opening in OpenRegionHandler
    region.put(new Put(Bytes.toBytes("a")).addColumn(fam1, fam1, fam1));
    region.flush(true);
    HBaseTestingUtil.closeRegionAndWAL(region);

    ArgumentCaptor<WALEdit> editCaptor = ArgumentCaptor.forClass(WALEdit.class);

    // capture append() calls
    WAL wal = mockWAL();
    when(rss.getWAL(any(RegionInfo.class))).thenReturn(wal);

    region = HRegion.openHRegion(hri, htd, rss.getWAL(hri),
      TEST_UTIL.getConfiguration(), rss, null);

    verify(wal, times(1)).appendMarker(any(RegionInfo.class), any(WALKeyImpl.class),
      editCaptor.capture());

    WALEdit edit = editCaptor.getValue();
    assertNotNull(edit);
    assertNotNull(edit.getCells());
    assertEquals(1, edit.getCells().size());
    RegionEventDescriptor desc = WALEdit.getRegionEventDescriptor(edit.getCells().get(0));
    assertNotNull(desc);

    LOG.info("RegionEventDescriptor from WAL: " + desc);

    assertEquals(RegionEventDescriptor.EventType.REGION_OPEN, desc.getEventType());
    assertTrue(Bytes.equals(desc.getTableName().toByteArray(), htd.getTableName().toBytes()));
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
  }

  // Helper for test testOpenRegionWrittenToWALForLogReplay
  static class HRegionWithSeqId extends HRegion {
    public HRegionWithSeqId(final Path tableDir, final WAL wal, final FileSystem fs,
        final Configuration confParam, final RegionInfo regionInfo,
        final TableDescriptor htd, final RegionServerServices rsServices) {
      super(tableDir, wal, fs, confParam, regionInfo, htd, rsServices);
    }
    @Override
    protected long getNextSequenceId(WAL wal) throws IOException {
      return 42;
    }
  }

  @Test
  public void testFlushedFileWithNoTags() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(fam1)).build();
    RegionInfo info = RegionInfoBuilder.newBuilder(tableName).build();
    Path path = TEST_UTIL.getDataTestDir(getClass().getSimpleName());
    region = HBaseTestingUtil.createRegionAndWAL(info, path,
      TEST_UTIL.getConfiguration(), tableDescriptor);
    Put put = new Put(Bytes.toBytes("a-b-0-0"));
    put.addColumn(fam1, qual1, Bytes.toBytes("c1-value"));
    region.put(put);
    region.flush(true);
    HStore store = region.getStore(fam1);
    Collection<HStoreFile> storefiles = store.getStorefiles();
    for (HStoreFile sf : storefiles) {
      assertFalse("Tags should not be present "
          ,sf.getReader().getHFileReader().getFileContext().isIncludesTags());
    }
  }

  /**
   * Utility method to setup a WAL mock.
   * <p/>
   * Needs to do the bit where we close latch on the WALKeyImpl on append else test hangs.
   * @return a mock WAL
   */
  private WAL mockWAL() throws IOException {
    WAL wal = mock(WAL.class);
    when(wal.appendData(any(RegionInfo.class), any(WALKeyImpl.class), any(WALEdit.class)))
      .thenAnswer(new Answer<Long>() {
        @Override
        public Long answer(InvocationOnMock invocation) throws Throwable {
          WALKeyImpl key = invocation.getArgument(1);
          MultiVersionConcurrencyControl.WriteEntry we = key.getMvcc().begin();
          key.setWriteEntry(we);
          return 1L;
        }
      });
    when(wal.appendMarker(any(RegionInfo.class), any(WALKeyImpl.class), any(WALEdit.class))).
        thenAnswer(new Answer<Long>() {
          @Override
          public Long answer(InvocationOnMock invocation) throws Throwable {
            WALKeyImpl key = invocation.getArgument(1);
            MultiVersionConcurrencyControl.WriteEntry we = key.getMvcc().begin();
            key.setWriteEntry(we);
            return 1L;
          }
        });
    return wal;
  }

  @Test
  public void testCloseRegionWrittenToWAL() throws Exception {
    Path rootDir = new Path(dir + name.getMethodName());
    CommonFSUtils.setRootDir(TEST_UTIL.getConfiguration(), rootDir);

    final ServerName serverName = ServerName.valueOf("testCloseRegionWrittenToWAL", 100, 42);
    final RegionServerServices rss = spy(TEST_UTIL.createMockRegionServerService(serverName));

    TableDescriptor htd = TableDescriptorBuilder.newBuilder(TableName.valueOf(name.getMethodName()))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(fam1))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(fam2)).build();
    RegionInfo hri = RegionInfoBuilder.newBuilder(htd.getTableName()).build();

    ArgumentCaptor<WALEdit> editCaptor = ArgumentCaptor.forClass(WALEdit.class);

    // capture append() calls
    WAL wal = mockWAL();
    when(rss.getWAL(any(RegionInfo.class))).thenReturn(wal);


    // create and then open a region first so that it can be closed later
    region = HRegion.createHRegion(hri, rootDir, TEST_UTIL.getConfiguration(), htd, rss.getWAL(hri));
    region = HRegion.openHRegion(hri, htd, rss.getWAL(hri),
      TEST_UTIL.getConfiguration(), rss, null);

    // close the region
    region.close(false);

    // 2 times, one for region open, the other close region
    verify(wal, times(2)).appendMarker(any(RegionInfo.class),
        (WALKeyImpl) any(WALKeyImpl.class), editCaptor.capture());

    WALEdit edit = editCaptor.getAllValues().get(1);
    assertNotNull(edit);
    assertNotNull(edit.getCells());
    assertEquals(1, edit.getCells().size());
    RegionEventDescriptor desc = WALEdit.getRegionEventDescriptor(edit.getCells().get(0));
    assertNotNull(desc);

    LOG.info("RegionEventDescriptor from WAL: " + desc);

    assertEquals(RegionEventDescriptor.EventType.REGION_CLOSE, desc.getEventType());
    assertTrue(Bytes.equals(desc.getTableName().toByteArray(), htd.getTableName().toBytes()));
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
  @Test
  public void testRegionTooBusy() throws IOException {
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

      HBaseTestingUtil.closeRegionAndWAL(region);
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

    // 10 seconds
    TableDescriptor tableDescriptor =
      TableDescriptorBuilder.newBuilder(TableName.valueOf(name.getMethodName()))
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(fam1).setTimeToLive(10).build())
        .build();

    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    conf.setInt(HFile.FORMAT_VERSION_KEY, HFile.MIN_FORMAT_VERSION_WITH_TAGS);

    region = HBaseTestingUtil.createRegionAndWAL(
      RegionInfoBuilder.newBuilder(tableDescriptor.getTableName()).build(),
      TEST_UTIL.getDataTestDir(), conf, tableDescriptor);
    assertNotNull(region);
    long now = EnvironmentEdgeManager.currentTime();
    // Add a cell that will expire in 5 seconds via cell TTL
    region.put(new Put(row).add(new KeyValue(row, fam1, q1, now,
      HConstants.EMPTY_BYTE_ARRAY, new ArrayBackedTag[] {
        // TTL tags specify ts in milliseconds
        new ArrayBackedTag(TagType.TTL_TAG_TYPE, Bytes.toBytes(5000L)) })));
    // Add a cell that will expire after 10 seconds via family setting
    region.put(new Put(row).addColumn(fam1, q2, now, HConstants.EMPTY_BYTE_ARRAY));
    // Add a cell that will expire in 15 seconds via cell TTL
    region.put(new Put(row).add(new KeyValue(row, fam1, q3, now + 10000 - 1,
      HConstants.EMPTY_BYTE_ARRAY, new ArrayBackedTag[] {
        // TTL tags specify ts in milliseconds
        new ArrayBackedTag(TagType.TTL_TAG_TYPE, Bytes.toBytes(5000L)) })));
    // Add a cell that will expire in 20 seconds via family setting
    region.put(new Put(row).addColumn(fam1, q4, now + 10000 - 1, HConstants.EMPTY_BYTE_ARRAY));

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
    region.put(new Put(row).addColumn(fam1, q1, Bytes.toBytes(1L)));
    r = region.get(new Get(row));
    byte[] val = r.getValue(fam1, q1);
    assertNotNull(val);
    assertEquals(1L, Bytes.toLong(val));

    // Increment with a TTL of 5 seconds
    Increment incr = new Increment(row).addColumn(fam1, q1, 1L);
    incr.setTTL(5000);
    region.increment(incr); // 2

    // New value should be 2
    r = region.get(new Get(row));
    val = r.getValue(fam1, q1);
    assertNotNull(val);
    assertEquals(2L, Bytes.toLong(val));

    // Increment time to T+25 seconds
    edge.incrementTime(5000);

    // Value should be back to 1
    r = region.get(new Get(row));
    val = r.getValue(fam1, q1);
    assertNotNull(val);
    assertEquals(1L, Bytes.toLong(val));

    // Increment time to T+30 seconds
    edge.incrementTime(5000);

    // Original value written at T+20 should be gone now via family TTL
    r = region.get(new Get(row));
    assertNull(r.getValue(fam1, q1));
  }

  @Test
  public void testTTLsUsingSmallHeartBeatCells() throws IOException {
    IncrementingEnvironmentEdge edge = new IncrementingEnvironmentEdge();
    EnvironmentEdgeManager.injectEdge(edge);

    final byte[] row = Bytes.toBytes("testRow");
    final byte[] q1 = Bytes.toBytes("q1");
    final byte[] q2 = Bytes.toBytes("q2");
    final byte[] q3 = Bytes.toBytes("q3");
    final byte[] q4 = Bytes.toBytes("q4");
    final byte[] q5 = Bytes.toBytes("q5");
    final byte[] q6 = Bytes.toBytes("q6");
    final byte[] q7 = Bytes.toBytes("q7");
    final byte[] q8 = Bytes.toBytes("q8");

    // 10 seconds
    int ttlSecs = 10;
    TableDescriptor tableDescriptor =
      TableDescriptorBuilder.newBuilder(TableName.valueOf(name.getMethodName())).setColumnFamily(
        ColumnFamilyDescriptorBuilder.newBuilder(fam1).setTimeToLive(ttlSecs).build()).build();

    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    conf.setInt(HFile.FORMAT_VERSION_KEY, HFile.MIN_FORMAT_VERSION_WITH_TAGS);
    // using small heart beat cells
    conf.setLong(StoreScanner.HBASE_CELLS_SCANNED_PER_HEARTBEAT_CHECK, 2);

    region = HBaseTestingUtil
      .createRegionAndWAL(RegionInfoBuilder.newBuilder(tableDescriptor.getTableName()).build(),
        TEST_UTIL.getDataTestDir(), conf, tableDescriptor);
    assertNotNull(region);
    long now = EnvironmentEdgeManager.currentTime();
    // Add a cell that will expire in 5 seconds via cell TTL
    region.put(new Put(row).addColumn(fam1, q1, now, HConstants.EMPTY_BYTE_ARRAY));
    region.put(new Put(row).addColumn(fam1, q2, now, HConstants.EMPTY_BYTE_ARRAY));
    region.put(new Put(row).addColumn(fam1, q3, now, HConstants.EMPTY_BYTE_ARRAY));
    // Add a cell that will expire after 10 seconds via family setting
    region
      .put(new Put(row).addColumn(fam1, q4, now + ttlSecs * 1000 + 1, HConstants.EMPTY_BYTE_ARRAY));
    region
      .put(new Put(row).addColumn(fam1, q5, now + ttlSecs * 1000 + 1, HConstants.EMPTY_BYTE_ARRAY));

    region.put(new Put(row).addColumn(fam1, q6, now, HConstants.EMPTY_BYTE_ARRAY));
    region.put(new Put(row).addColumn(fam1, q7, now, HConstants.EMPTY_BYTE_ARRAY));
    region
      .put(new Put(row).addColumn(fam1, q8, now + ttlSecs * 1000 + 1, HConstants.EMPTY_BYTE_ARRAY));

    // Flush so we are sure store scanning gets this right
    region.flush(true);

    // A query at time T+0 should return all cells
    checkScan(8);

    // Increment time to T+ttlSecs seconds
    edge.incrementTime(ttlSecs * 1000);
    checkScan(3);
  }

  private void checkScan(int expectCellSize) throws IOException{
    Scan s = new Scan().withStartRow(row);
    ScannerContext.Builder contextBuilder = ScannerContext.newBuilder(true);
    ScannerContext scannerContext = contextBuilder.build();
    RegionScanner scanner = region.getScanner(s);
    List<Cell> kvs = new ArrayList<>();
    scanner.next(kvs, scannerContext);
    assertEquals(expectCellSize, kvs.size());
    scanner.close();
  }

  @Test
  public void testIncrementTimestampsAreMonotonic() throws IOException {
    region = initHRegion(tableName, method, CONF, fam1);
    ManualEnvironmentEdge edge = new ManualEnvironmentEdge();
    EnvironmentEdgeManager.injectEdge(edge);

    edge.setValue(10);
    Increment inc = new Increment(row);
    inc.setDurability(Durability.SKIP_WAL);
    inc.addColumn(fam1, qual1, 1L);
    region.increment(inc);

    Result result = region.get(new Get(row));
    Cell c = result.getColumnLatestCell(fam1, qual1);
    assertNotNull(c);
    assertEquals(10L, c.getTimestamp());

    edge.setValue(1); // clock goes back
    region.increment(inc);
    result = region.get(new Get(row));
    c = result.getColumnLatestCell(fam1, qual1);
    assertEquals(11L, c.getTimestamp());
    assertEquals(2L, Bytes.toLong(c.getValueArray(), c.getValueOffset(), c.getValueLength()));
  }

  @Test
  public void testAppendTimestampsAreMonotonic() throws IOException {
    region = initHRegion(tableName, method, CONF, fam1);
    ManualEnvironmentEdge edge = new ManualEnvironmentEdge();
    EnvironmentEdgeManager.injectEdge(edge);

    edge.setValue(10);
    Append a = new Append(row);
    a.setDurability(Durability.SKIP_WAL);
    a.addColumn(fam1, qual1, qual1);
    region.append(a);

    Result result = region.get(new Get(row));
    Cell c = result.getColumnLatestCell(fam1, qual1);
    assertNotNull(c);
    assertEquals(10L, c.getTimestamp());

    edge.setValue(1); // clock goes back
    region.append(a);
    result = region.get(new Get(row));
    c = result.getColumnLatestCell(fam1, qual1);
    assertEquals(11L, c.getTimestamp());

    byte[] expected = new byte[qual1.length*2];
    System.arraycopy(qual1, 0, expected, 0, qual1.length);
    System.arraycopy(qual1, 0, expected, qual1.length, qual1.length);

    assertTrue(Bytes.equals(c.getValueArray(), c.getValueOffset(), c.getValueLength(),
      expected, 0, expected.length));
  }

  @Test
  public void testCheckAndMutateTimestampsAreMonotonic() throws IOException {
    region = initHRegion(tableName, method, CONF, fam1);
    ManualEnvironmentEdge edge = new ManualEnvironmentEdge();
    EnvironmentEdgeManager.injectEdge(edge);

    edge.setValue(10);
    Put p = new Put(row);
    p.setDurability(Durability.SKIP_WAL);
    p.addColumn(fam1, qual1, qual1);
    region.put(p);

    Result result = region.get(new Get(row));
    Cell c = result.getColumnLatestCell(fam1, qual1);
    assertNotNull(c);
    assertEquals(10L, c.getTimestamp());

    edge.setValue(1); // clock goes back
    p = new Put(row);
    p.setDurability(Durability.SKIP_WAL);
    p.addColumn(fam1, qual1, qual2);
    region.checkAndMutate(row, fam1, qual1, CompareOperator.EQUAL, new BinaryComparator(qual1), p);
    result = region.get(new Get(row));
    c = result.getColumnLatestCell(fam1, qual1);
    assertEquals(10L, c.getTimestamp());

    assertTrue(Bytes.equals(c.getValueArray(), c.getValueOffset(), c.getValueLength(),
      qual2, 0, qual2.length));
  }

  @Test
  public void testBatchMutateWithWrongRegionException() throws Exception {
    final byte[] a = Bytes.toBytes("a");
    final byte[] b = Bytes.toBytes("b");
    final byte[] c = Bytes.toBytes("c"); // exclusive

    int prevLockTimeout = CONF.getInt("hbase.rowlock.wait.duration", 30000);
    CONF.setInt("hbase.rowlock.wait.duration", 1000);
    region = initHRegion(tableName, a, c, method, CONF, false, fam1);

    Mutation[] mutations = new Mutation[] {
        new Put(a)
            .add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
              .setRow(a)
              .setFamily(fam1)
              .setTimestamp(HConstants.LATEST_TIMESTAMP)
              .setType(Cell.Type.Put)
              .build()),
        // this is outside the region boundary
        new Put(c).add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
              .setRow(c)
              .setFamily(fam1)
              .setTimestamp(HConstants.LATEST_TIMESTAMP)
              .setType(Type.Put)
              .build()),
        new Put(b).add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
              .setRow(b)
              .setFamily(fam1)
              .setTimestamp(HConstants.LATEST_TIMESTAMP)
              .setType(Cell.Type.Put)
              .build())
    };

    OperationStatus[] status = region.batchMutate(mutations);
    assertEquals(OperationStatusCode.SUCCESS, status[0].getOperationStatusCode());
    assertEquals(OperationStatusCode.SANITY_CHECK_FAILURE, status[1].getOperationStatusCode());
    assertEquals(OperationStatusCode.SUCCESS, status[2].getOperationStatusCode());


    // test with a row lock held for a long time
    final CountDownLatch obtainedRowLock = new CountDownLatch(1);
    ExecutorService exec = Executors.newFixedThreadPool(2);
    Future<Void> f1 = exec.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        LOG.info("Acquiring row lock");
        RowLock rl = region.getRowLock(b);
        obtainedRowLock.countDown();
        LOG.info("Waiting for 5 seconds before releasing lock");
        Threads.sleep(5000);
        LOG.info("Releasing row lock");
        rl.release();
        return null;
      }
    });
    obtainedRowLock.await(30, TimeUnit.SECONDS);

    Future<Void> f2 = exec.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        Mutation[] mutations = new Mutation[] {
            new Put(a).add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
                .setRow(a)
                .setFamily(fam1)
                .setTimestamp(HConstants.LATEST_TIMESTAMP)
                .setType(Cell.Type.Put)
                .build()),
            new Put(b).add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
                .setRow(b)
                .setFamily(fam1)
                .setTimestamp(HConstants.LATEST_TIMESTAMP)
                .setType(Cell.Type.Put)
                .build()),
        };

        // this will wait for the row lock, and it will eventually succeed
        OperationStatus[] status = region.batchMutate(mutations);
        assertEquals(OperationStatusCode.SUCCESS, status[0].getOperationStatusCode());
        assertEquals(OperationStatusCode.SUCCESS, status[1].getOperationStatusCode());
        return null;
      }
    });

    f1.get();
    f2.get();

    CONF.setInt("hbase.rowlock.wait.duration", prevLockTimeout);
  }

  @Test
  public void testBatchMutateWithZeroRowLockWait() throws Exception {
    final byte[] a = Bytes.toBytes("a");
    final byte[] b = Bytes.toBytes("b");
    final byte[] c = Bytes.toBytes("c"); // exclusive

    Configuration conf = new Configuration(CONF);
    conf.setInt("hbase.rowlock.wait.duration", 0);
    final RegionInfo hri =
      RegionInfoBuilder.newBuilder(tableName).setStartKey(a).setEndKey(c).build();
    final TableDescriptor htd = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(fam1)).build();
    region = HRegion.createHRegion(hri, TEST_UTIL.getDataTestDir(), conf, htd,
      HBaseTestingUtil.createWal(conf, TEST_UTIL.getDataTestDirOnTestFS(method + ".log"), hri));

    Mutation[] mutations = new Mutation[] {
        new Put(a)
            .add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
              .setRow(a)
              .setFamily(fam1)
              .setTimestamp(HConstants.LATEST_TIMESTAMP)
              .setType(Cell.Type.Put)
              .build()),
        new Put(b).add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
              .setRow(b)
              .setFamily(fam1)
              .setTimestamp(HConstants.LATEST_TIMESTAMP)
              .setType(Cell.Type.Put)
              .build())
    };

    OperationStatus[] status = region.batchMutate(mutations);
    assertEquals(OperationStatusCode.SUCCESS, status[0].getOperationStatusCode());
    assertEquals(OperationStatusCode.SUCCESS, status[1].getOperationStatusCode());


    // test with a row lock held for a long time
    final CountDownLatch obtainedRowLock = new CountDownLatch(1);
    ExecutorService exec = Executors.newFixedThreadPool(2);
    Future<Void> f1 = exec.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        LOG.info("Acquiring row lock");
        RowLock rl = region.getRowLock(b);
        obtainedRowLock.countDown();
        LOG.info("Waiting for 5 seconds before releasing lock");
        Threads.sleep(5000);
        LOG.info("Releasing row lock");
        rl.release();
        return null;
      }
    });
    obtainedRowLock.await(30, TimeUnit.SECONDS);

    Future<Void> f2 = exec.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        Mutation[] mutations = new Mutation[] {
            new Put(a).add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
                .setRow(a)
                .setFamily(fam1)
                .setTimestamp(HConstants.LATEST_TIMESTAMP)
                .setType(Cell.Type.Put)
                .build()),
            new Put(b).add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
                .setRow(b)
                .setFamily(fam1)
                .setTimestamp(HConstants.LATEST_TIMESTAMP)
                .setType(Cell.Type.Put)
                .build()),
        };
        // when handling row b we are going to spin on the failure to get the row lock
        // until the lock above is released, but we will still succeed so long as that
        // takes less time then the test time out.
        OperationStatus[] status = region.batchMutate(mutations);
        assertEquals(OperationStatusCode.SUCCESS, status[0].getOperationStatusCode());
        assertEquals(OperationStatusCode.SUCCESS, status[1].getOperationStatusCode());
        return null;
      }
    });

    f1.get();
    f2.get();
  }

  @Test
  public void testCheckAndRowMutateTimestampsAreMonotonic() throws IOException {
    region = initHRegion(tableName, method, CONF, fam1);
    ManualEnvironmentEdge edge = new ManualEnvironmentEdge();
    EnvironmentEdgeManager.injectEdge(edge);

    edge.setValue(10);
    Put p = new Put(row);
    p.setDurability(Durability.SKIP_WAL);
    p.addColumn(fam1, qual1, qual1);
    region.put(p);

    Result result = region.get(new Get(row));
    Cell c = result.getColumnLatestCell(fam1, qual1);
    assertNotNull(c);
    assertEquals(10L, c.getTimestamp());

    edge.setValue(1); // clock goes back
    p = new Put(row);
    p.setDurability(Durability.SKIP_WAL);
    p.addColumn(fam1, qual1, qual2);
    RowMutations rm = new RowMutations(row);
    rm.add(p);
    assertTrue(region.checkAndRowMutate(row, fam1, qual1, CompareOperator.EQUAL,
        new BinaryComparator(qual1), rm));
    result = region.get(new Get(row));
    c = result.getColumnLatestCell(fam1, qual1);
    assertEquals(10L, c.getTimestamp());
    LOG.info("c value " +
      Bytes.toStringBinary(c.getValueArray(), c.getValueOffset(), c.getValueLength()));

    assertTrue(Bytes.equals(c.getValueArray(), c.getValueOffset(), c.getValueLength(),
      qual2, 0, qual2.length));
  }

  HRegion initHRegion(TableName tableName, String callingMethod,
      byte[]... families) throws IOException {
    return initHRegion(tableName, callingMethod, HBaseConfiguration.create(),
        families);
  }

  /**
   * HBASE-16429 Make sure no stuck if roll writer when ring buffer is filled with appends
   * @throws IOException if IO error occurred during test
   */
  @Test
  public void testWritesWhileRollWriter() throws IOException {
    int testCount = 10;
    int numRows = 1024;
    int numFamilies = 2;
    int numQualifiers = 2;
    final byte[][] families = new byte[numFamilies][];
    for (int i = 0; i < numFamilies; i++) {
      families[i] = Bytes.toBytes("family" + i);
    }
    final byte[][] qualifiers = new byte[numQualifiers][];
    for (int i = 0; i < numQualifiers; i++) {
      qualifiers[i] = Bytes.toBytes("qual" + i);
    }

    CONF.setInt("hbase.regionserver.wal.disruptor.event.count", 2);
    this.region = initHRegion(tableName, method, CONF, families);
    try {
      List<Thread> threads = new ArrayList<>();
      for (int i = 0; i < numRows; i++) {
        final int count = i;
        Thread t = new Thread(new Runnable() {

          @Override
          public void run() {
            byte[] row = Bytes.toBytes("row" + count);
            Put put = new Put(row);
            put.setDurability(Durability.SYNC_WAL);
            byte[] value = Bytes.toBytes(String.valueOf(count));
            for (byte[] family : families) {
              for (byte[] qualifier : qualifiers) {
                put.addColumn(family, qualifier, count, value);
              }
            }
            try {
              region.put(put);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        });
        threads.add(t);
      }
      for (Thread t : threads) {
        t.start();
      }

      for (int i = 0; i < testCount; i++) {
        region.getWAL().rollWriter();
        Thread.yield();
      }
    } finally {
      try {
        HBaseTestingUtil.closeRegionAndWAL(this.region);
        CONF.setInt("hbase.regionserver.wal.disruptor.event.count", 16 * 1024);
      } catch (DroppedSnapshotException dse) {
        // We could get this on way out because we interrupt the background flusher and it could
        // fail anywhere causing a DSE over in the background flusher... only it is not properly
        // dealt with so could still be memory hanging out when we get to here -- memory we can't
        // flush because the accounting is 'off' since original DSE.
      }
      this.region = null;
    }
  }

  @Test
  public void testMutateRow() throws Exception {
    final byte[] row = Bytes.toBytes("row");
    final byte[] q1 = Bytes.toBytes("q1");
    final byte[] q2 = Bytes.toBytes("q2");
    final byte[] q3 = Bytes.toBytes("q3");
    final byte[] q4 = Bytes.toBytes("q4");
    final String v1 = "v1";

    region = initHRegion(tableName, method, CONF, fam1);

    // Initial values
    region.batchMutate(new Mutation[] {
      new Put(row).addColumn(fam1, q2, Bytes.toBytes("toBeDeleted")),
      new Put(row).addColumn(fam1, q3, Bytes.toBytes(5L)),
      new Put(row).addColumn(fam1, q4, Bytes.toBytes("a")),
    });

    // Do mutateRow
    Result result = region.mutateRow(new RowMutations(row).add(Arrays.asList(
      new Put(row).addColumn(fam1, q1, Bytes.toBytes(v1)),
      new Delete(row).addColumns(fam1, q2),
      new Increment(row).addColumn(fam1, q3, 1),
      new Append(row).addColumn(fam1, q4, Bytes.toBytes("b")))));

    assertNotNull(result);
    assertEquals(6L, Bytes.toLong(result.getValue(fam1, q3)));
    assertEquals("ab", Bytes.toString(result.getValue(fam1, q4)));

    // Verify the value
    result = region.get(new Get(row));
    assertEquals(v1, Bytes.toString(result.getValue(fam1, q1)));
    assertNull(result.getValue(fam1, q2));
    assertEquals(6L, Bytes.toLong(result.getValue(fam1, q3)));
    assertEquals("ab", Bytes.toString(result.getValue(fam1, q4)));
  }

  @Test
  public void testMutateRowInParallel() throws Exception {
    final int numReaderThreads = 100;
    final CountDownLatch latch = new CountDownLatch(numReaderThreads);

    final byte[] row = Bytes.toBytes("row");
    final byte[] q1 = Bytes.toBytes("q1");
    final byte[] q2 = Bytes.toBytes("q2");
    final byte[] q3 = Bytes.toBytes("q3");
    final byte[] q4 = Bytes.toBytes("q4");
    final String v1 = "v1";
    final String v2 = "v2";

    // We need to ensure the timestamp of the delete operation is more than the previous one
    final AtomicLong deleteTimestamp = new AtomicLong();

    region = initHRegion(tableName, method, CONF, fam1);

    // Initial values
    region.batchMutate(new Mutation[] {
      new Put(row).addColumn(fam1, q1, Bytes.toBytes(v1))
        .addColumn(fam1, q2, deleteTimestamp.getAndIncrement(), Bytes.toBytes(v2))
        .addColumn(fam1, q3, Bytes.toBytes(1L))
        .addColumn(fam1, q4, Bytes.toBytes("a"))
    });

    final AtomicReference<AssertionError> assertionError = new AtomicReference<>();

    // Writer thread
    Thread writerThread = new Thread(() -> {
      try {
        while (true) {
          // If all the reader threads finish, then stop the writer thread
          if (latch.await(0, TimeUnit.MILLISECONDS)) {
            return;
          }

          // Execute the mutations. This should be done atomically
          region.mutateRow(new RowMutations(row).add(Arrays.asList(
            new Put(row).addColumn(fam1, q1, Bytes.toBytes(v2)),
            new Delete(row).addColumns(fam1, q2, deleteTimestamp.getAndIncrement()),
            new Increment(row).addColumn(fam1, q3, 1L),
            new Append(row).addColumn(fam1, q4, Bytes.toBytes("b")))));

          // We need to ensure the timestamps of the Increment/Append operations are more than the
          // previous ones
          Result result = region.get(new Get(row).addColumn(fam1, q3).addColumn(fam1, q4));
          long tsIncrement = result.getColumnLatestCell(fam1, q3).getTimestamp();
          long tsAppend = result.getColumnLatestCell(fam1, q4).getTimestamp();

          // Put the initial values
          region.batchMutate(new Mutation[] {
            new Put(row).addColumn(fam1, q1, Bytes.toBytes(v1))
              .addColumn(fam1, q2, deleteTimestamp.getAndIncrement(), Bytes.toBytes(v2))
              .addColumn(fam1, q3, tsIncrement + 1, Bytes.toBytes(1L))
              .addColumn(fam1, q4, tsAppend + 1, Bytes.toBytes("a"))
          });
        }
      } catch (Exception e) {
        assertionError.set(new AssertionError(e));
      }
    });
    writerThread.start();

    // Reader threads
    for (int i = 0; i < numReaderThreads; i++) {
      new Thread(() -> {
        try {
          for (int j = 0; j < 10000; j++) {
            // Verify the values
            Result result = region.get(new Get(row));

            // The values should be equals to either the initial values or the values after
            // executing the mutations
            String q1Value = Bytes.toString(result.getValue(fam1, q1));
            if (v1.equals(q1Value)) {
              assertEquals(v2, Bytes.toString(result.getValue(fam1, q2)));
              assertEquals(1L, Bytes.toLong(result.getValue(fam1, q3)));
              assertEquals("a", Bytes.toString(result.getValue(fam1, q4)));
            } else if (v2.equals(q1Value)) {
              assertNull(Bytes.toString(result.getValue(fam1, q2)));
              assertEquals(2L, Bytes.toLong(result.getValue(fam1, q3)));
              assertEquals("ab", Bytes.toString(result.getValue(fam1, q4)));
            } else {
              fail("the qualifier " + Bytes.toString(q1) + " should be " + v1 + " or " + v2 +
                ", but " + q1Value);
            }
          }
        } catch (Exception e) {
          assertionError.set(new AssertionError(e));
        } catch (AssertionError e) {
          assertionError.set(e);
        }

        latch.countDown();
      }).start();
    }

    writerThread.join();

    if (assertionError.get() != null) {
      throw assertionError.get();
    }
  }

  @Test
  public void testMutateRow_WriteRequestCount() throws Exception {
    byte[] row1 = Bytes.toBytes("row1");
    byte[] fam1 = Bytes.toBytes("fam1");
    byte[] qf1 = Bytes.toBytes("qualifier");
    byte[] val1 = Bytes.toBytes("value1");

    RowMutations rm = new RowMutations(row1);
    Put put = new Put(row1);
    put.addColumn(fam1, qf1, val1);
    rm.add(put);

    this.region = initHRegion(tableName, method, CONF, fam1);
    long wrcBeforeMutate = this.region.writeRequestsCount.longValue();
    this.region.mutateRow(rm);
    long wrcAfterMutate = this.region.writeRequestsCount.longValue();
    Assert.assertEquals(wrcBeforeMutate + rm.getMutations().size(), wrcAfterMutate);
  }

  @Test
  public void testBulkLoadReplicationEnabled() throws IOException {
    TEST_UTIL.getConfiguration().setBoolean(HConstants.REPLICATION_BULKLOAD_ENABLE_KEY, true);
    final ServerName serverName = ServerName.valueOf(name.getMethodName(), 100, 42);
    final RegionServerServices rss = spy(TEST_UTIL.createMockRegionServerService(serverName));

    TableDescriptor tableDescriptor =
      TableDescriptorBuilder.newBuilder(TableName.valueOf(name.getMethodName()))
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(fam1)).build();
    RegionInfo hri = RegionInfoBuilder.newBuilder(tableDescriptor.getTableName()).build();
    region = HRegion.openHRegion(hri, tableDescriptor, rss.getWAL(hri),
      TEST_UTIL.getConfiguration(), rss, null);

    assertTrue(region.conf.getBoolean(HConstants.REPLICATION_BULKLOAD_ENABLE_KEY, false));
    String plugins = region.conf.get(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, "");
    String replicationCoprocessorClass = ReplicationObserver.class.getCanonicalName();
    assertTrue(plugins.contains(replicationCoprocessorClass));
    assertTrue(region.getCoprocessorHost().
        getCoprocessors().contains(ReplicationObserver.class.getSimpleName()));
  }

  /**
   * The same as HRegion class, the only difference is that instantiateHStore will
   * create a different HStore - HStoreForTesting. [HBASE-8518]
   */
  public static class HRegionForTesting extends HRegion {

    public HRegionForTesting(final Path tableDir, final WAL wal, final FileSystem fs,
                             final Configuration confParam, final RegionInfo regionInfo,
                             final TableDescriptor htd, final RegionServerServices rsServices) {
      this(new HRegionFileSystem(confParam, fs, tableDir, regionInfo),
          wal, confParam, htd, rsServices);
    }

    public HRegionForTesting(HRegionFileSystem fs, WAL wal,
                             Configuration confParam, TableDescriptor htd,
                             RegionServerServices rsServices) {
      super(fs, wal, confParam, htd, rsServices);
    }

    /**
     * Create HStore instance.
     * @return If Mob is enabled, return HMobStore, otherwise return HStoreForTesting.
     */
    @Override
    protected HStore instantiateHStore(final ColumnFamilyDescriptor family, boolean warmup)
        throws IOException {
      if (family.isMobEnabled()) {
        if (HFile.getFormatVersion(this.conf) < HFile.MIN_FORMAT_VERSION_WITH_TAGS) {
          throw new IOException("A minimum HFile version of " + HFile.MIN_FORMAT_VERSION_WITH_TAGS +
              " is required for MOB feature. Consider setting " + HFile.FORMAT_VERSION_KEY +
              " accordingly.");
        }
        return new HMobStore(this, family, this.conf, warmup);
      }
      return new HStoreForTesting(this, family, this.conf, warmup);
    }
  }

  /**
   * HStoreForTesting is merely the same as HStore, the difference is in the doCompaction method
   * of HStoreForTesting there is a checkpoint "hbase.hstore.compaction.complete" which
   * doesn't let hstore compaction complete. In the former edition, this config is set in
   * HStore class inside compact method, though this is just for testing, otherwise it
   * doesn't do any help. In HBASE-8518, we try to get rid of all "hbase.hstore.compaction.complete"
   * config (except for testing code).
   */
  public static class HStoreForTesting extends HStore {

    protected HStoreForTesting(final HRegion region,
        final ColumnFamilyDescriptor family,
        final Configuration confParam, boolean warmup) throws IOException {
      super(region, family, confParam, warmup);
    }

    @Override
    protected List<HStoreFile> doCompaction(CompactionRequestImpl cr,
        Collection<HStoreFile> filesToCompact, User user, long compactionStartTime,
        List<Path> newFiles) throws IOException {
      // let compaction incomplete.
      if (!this.conf.getBoolean("hbase.hstore.compaction.complete", true)) {
        LOG.warn("hbase.hstore.compaction.complete is set to false");
        List<HStoreFile> sfs = new ArrayList<>(newFiles.size());
        final boolean evictOnClose =
            getCacheConfig() != null? getCacheConfig().shouldEvictOnClose(): true;
        for (Path newFile : newFiles) {
          // Create storefile around what we wrote with a reader on it.
          HStoreFile sf = storeEngine.createStoreFileAndReader(newFile);
          sf.closeStoreFile(evictOnClose);
          sfs.add(sf);
        }
        return sfs;
      }
      return super.doCompaction(cr, filesToCompact, user, compactionStartTime, newFiles);
    }
  }

  @Test
  public void testCloseNoInterrupt() throws Exception {
    byte[] cf1 = Bytes.toBytes("CF1");
    byte[][] families = { cf1 };
    final int SLEEP_TIME = 10 * 1000;

    Configuration conf = new Configuration(CONF);
    // Disable close thread interrupt and server abort behavior
    conf.setBoolean(HRegion.CLOSE_WAIT_ABORT, false);
    conf.setInt(HRegion.CLOSE_WAIT_INTERVAL, 1000);
    region = initHRegion(tableName, method, conf, families);

    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicBoolean holderInterrupted = new AtomicBoolean();
    Thread holder = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          LOG.info("Starting region operation holder");
          region.startRegionOperation(Operation.SCAN);
          latch.countDown();
          try {
            Thread.sleep(SLEEP_TIME);
          } catch (InterruptedException e) {
            LOG.info("Interrupted");
            holderInterrupted.set(true);
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        } finally {
          try {
            region.closeRegionOperation();
          } catch (IOException e) {
          }
          LOG.info("Stopped region operation holder");
        }
      }
    });

    holder.start();
    latch.await();
    region.close();
    region = null;
    holder.join();

    assertFalse("Region lock holder should not have been interrupted", holderInterrupted.get());
  }

  @Test
  public void testCloseInterrupt() throws Exception {
    byte[] cf1 = Bytes.toBytes("CF1");
    byte[][] families = { cf1 };
    final int SLEEP_TIME = 10 * 1000;

    Configuration conf = new Configuration(CONF);
    // Enable close thread interrupt and server abort behavior
    conf.setBoolean(HRegion.CLOSE_WAIT_ABORT, true);
    // Speed up the unit test, no need to wait default 10 seconds.
    conf.setInt(HRegion.CLOSE_WAIT_INTERVAL, 1000);
    region = initHRegion(tableName, method, conf, families);

    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicBoolean holderInterrupted = new AtomicBoolean();
    Thread holder = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          LOG.info("Starting region operation holder");
          region.startRegionOperation(Operation.SCAN);
          latch.countDown();
          try {
            Thread.sleep(SLEEP_TIME);
          } catch (InterruptedException e) {
            LOG.info("Interrupted");
            holderInterrupted.set(true);
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        } finally {
          try {
            region.closeRegionOperation();
          } catch (IOException e) {
          }
          LOG.info("Stopped region operation holder");
        }
      }
    });

    holder.start();
    latch.await();
    region.close();
    region = null;
    holder.join();

    assertTrue("Region lock holder was not interrupted", holderInterrupted.get());
  }

  @Test
  public void testCloseAbort() throws Exception {
    byte[] cf1 = Bytes.toBytes("CF1");
    byte[][] families = { cf1 };
    final int SLEEP_TIME = 10 * 1000;

    Configuration conf = new Configuration(CONF);
    // Enable close thread interrupt and server abort behavior.
    conf.setBoolean(HRegion.CLOSE_WAIT_ABORT, true);
    // Set the abort interval to a fraction of sleep time so we are guaranteed to be aborted.
    conf.setInt(HRegion.CLOSE_WAIT_TIME, SLEEP_TIME / 2);
    // Set the wait interval to a fraction of sleep time so we are guaranteed to be interrupted.
    conf.setInt(HRegion.CLOSE_WAIT_INTERVAL, SLEEP_TIME / 4);
    region = initHRegion(tableName, method, conf, families);
    RegionServerServices rsServices = mock(RegionServerServices.class);
    when(rsServices.getServerName()).thenReturn(ServerName.valueOf("localhost", 1000, 1000));
    region.rsServices = rsServices;

    final CountDownLatch latch = new CountDownLatch(1);
    Thread holder = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          LOG.info("Starting region operation holder");
          region.startRegionOperation(Operation.SCAN);
          latch.countDown();
          // Hold the lock for SLEEP_TIME seconds no matter how many times we are interrupted.
          int timeRemaining = SLEEP_TIME;
          while (timeRemaining > 0) {
            long start = EnvironmentEdgeManager.currentTime();
            try {
              Thread.sleep(timeRemaining);
            } catch (InterruptedException e) {
              LOG.info("Interrupted");
            }
            long end = EnvironmentEdgeManager.currentTime();
            timeRemaining -= end - start;
            if (timeRemaining < 0) {
              timeRemaining = 0;
            }
            if (timeRemaining > 0) {
              LOG.info("Sleeping again, remaining time " + timeRemaining + " ms");
            }
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        } finally {
          try {
            region.closeRegionOperation();
          } catch (IOException e) {
          }
          LOG.info("Stopped region operation holder");
        }
      }
    });

    holder.start();
    latch.await();
    try {
      region.close();
    } catch (IOException e) {
      LOG.info("Caught expected exception", e);
    }
    region = null;
    holder.join();

    // Verify the region tried to abort the server
    verify(rsServices, atLeast(1)).abort(anyString(),any());
  }

  @Test
  public void testInterruptProtection() throws Exception {
    byte[] cf1 = Bytes.toBytes("CF1");
    byte[][] families = { cf1 };
    final int SLEEP_TIME = 10 * 1000;

    Configuration conf = new Configuration(CONF);
    // Enable close thread interrupt and server abort behavior.
    conf.setBoolean(HRegion.CLOSE_WAIT_ABORT, true);
    conf.setInt(HRegion.CLOSE_WAIT_INTERVAL, 1000);
    region = initHRegion(tableName, method, conf, families);

    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicBoolean holderInterrupted = new AtomicBoolean();
    Thread holder = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          LOG.info("Starting region operation holder");
          region.startRegionOperation(Operation.SCAN);
          LOG.info("Protecting against interrupts");
          region.disableInterrupts();
          try {
            latch.countDown();
            try {
              Thread.sleep(SLEEP_TIME);
            } catch (InterruptedException e) {
              LOG.info("Interrupted");
              holderInterrupted.set(true);
            }
          } finally {
            region.enableInterrupts();
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        } finally {
          try {
            region.closeRegionOperation();
          } catch (IOException e) {
          }
          LOG.info("Stopped region operation holder");
        }
      }
    });

    holder.start();
    latch.await();
    region.close();
    region = null;
    holder.join();

    assertFalse("Region lock holder should not have been interrupted", holderInterrupted.get());
  }

  @Test
  public void testRegionOnCoprocessorsChange() throws IOException {
    byte[] cf1 = Bytes.toBytes("CF1");
    byte[][] families = { cf1 };

    Configuration conf = new Configuration(CONF);
    region = initHRegion(tableName, method, conf, families);
    assertNull(region.getCoprocessorHost());

    // set and verify the system coprocessors for region and user region
    Configuration newConf = new Configuration(conf);
    newConf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      MetaTableMetrics.class.getName());
    newConf.set(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY,
      NoOpRegionCoprocessor.class.getName());
    // trigger configuration change
    region.onConfigurationChange(newConf);
    assertTrue(region.getCoprocessorHost() != null);
    Set<String> coprocessors = region.getCoprocessorHost().getCoprocessors();
    assertTrue(coprocessors.size() == 2);
    assertTrue(region.getCoprocessorHost().getCoprocessors()
      .contains(MetaTableMetrics.class.getSimpleName()));
    assertTrue(region.getCoprocessorHost().getCoprocessors()
      .contains(NoOpRegionCoprocessor.class.getSimpleName()));

    // remove region coprocessor and keep only user region coprocessor
    newConf.unset(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY);
    region.onConfigurationChange(newConf);
    assertTrue(region.getCoprocessorHost() != null);
    coprocessors = region.getCoprocessorHost().getCoprocessors();
    assertTrue(coprocessors.size() == 1);
    assertTrue(region.getCoprocessorHost().getCoprocessors()
      .contains(NoOpRegionCoprocessor.class.getSimpleName()));
  }

  @Test
  public void testRegionOnCoprocessorsWithoutChange() throws IOException {
    byte[] cf1 = Bytes.toBytes("CF1");
    byte[][] families = { cf1 };

    Configuration conf = new Configuration(CONF);
    conf.set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      MetaTableMetrics.class.getCanonicalName());
    region = initHRegion(tableName, method, conf, families);
    // region service is null in unit test, we need to load the coprocessor once
    region.setCoprocessorHost(new RegionCoprocessorHost(region, null, conf));
    RegionCoprocessor regionCoprocessor = region.getCoprocessorHost()
      .findCoprocessor(MetaTableMetrics.class.getName());

    // simulate when other configuration may have changed and onConfigurationChange execute once
    region.onConfigurationChange(conf);
    RegionCoprocessor regionCoprocessorAfterOnConfigurationChange = region.getCoprocessorHost()
      .findCoprocessor(MetaTableMetrics.class.getName());
    assertEquals(regionCoprocessor, regionCoprocessorAfterOnConfigurationChange);
  }

  public static class NoOpRegionCoprocessor implements RegionCoprocessor, RegionObserver {
    // a empty region coprocessor class
  }
}
