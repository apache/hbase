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

import static org.apache.hadoop.hbase.regionserver.HStoreFile.BULKLOAD_TIME_KEY;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.MultithreadedTestUtil.RepeatingTestThread;
import org.apache.hadoop.hbase.MultithreadedTestUtil.TestContext;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClientServiceCallable;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RpcRetryingCaller;
import org.apache.hadoop.hbase.client.RpcRetryingCallerFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.wal.TestWALActionsListener;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.tool.BulkLoadHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.CompactRegionRequest;

/**
 * Tests bulk loading of HFiles and shows the atomicity or lack of atomicity of
 * the region server's bullkLoad functionality.
 */
@RunWith(Parameterized.class)
@Category({RegionServerTests.class, LargeTests.class})
public class TestHRegionServerBulkLoad {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHRegionServerBulkLoad.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestHRegionServerBulkLoad.class);
  protected static HBaseTestingUtility UTIL = new HBaseTestingUtility();
  protected final static Configuration conf = UTIL.getConfiguration();
  protected final static byte[] QUAL = Bytes.toBytes("qual");
  protected final static int NUM_CFS = 10;
  private int sleepDuration;
  public static int BLOCKSIZE = 64 * 1024;
  public static Algorithm COMPRESSION = Compression.Algorithm.NONE;

  protected final static byte[][] families = new byte[NUM_CFS][];
  static {
    for (int i = 0; i < NUM_CFS; i++) {
      families[i] = Bytes.toBytes(family(i));
    }
  }
  @Parameters
  public static final Collection<Object[]> parameters() {
    int[] sleepDurations = new int[] { 0, 30000 };
    List<Object[]> configurations = new ArrayList<>();
    for (int i : sleepDurations) {
      configurations.add(new Object[] { i });
    }
    return configurations;
  }

  public TestHRegionServerBulkLoad(int duration) {
    this.sleepDuration = duration;
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf.setInt("hbase.rpc.timeout", 10 * 1000);
  }

  /**
   * Create a rowkey compatible with
   * {@link #createHFile(FileSystem, Path, byte[], byte[], byte[], int)}.
   */
  public static byte[] rowkey(int i) {
    return Bytes.toBytes(String.format("row_%08d", i));
  }

  static String family(int i) {
    return String.format("family_%04d", i);
  }

  /**
   * Create an HFile with the given number of rows with a specified value.
   */
  public static void createHFile(FileSystem fs, Path path, byte[] family,
      byte[] qualifier, byte[] value, int numRows) throws IOException {
    HFileContext context = new HFileContextBuilder().withBlockSize(BLOCKSIZE)
                            .withCompression(COMPRESSION)
                            .build();
    HFile.Writer writer = HFile
        .getWriterFactory(conf, new CacheConfig(conf))
        .withPath(fs, path)
        .withFileContext(context)
        .create();
    long now = System.currentTimeMillis();
    try {
      // subtract 2 since iterateOnSplits doesn't include boundary keys
      for (int i = 0; i < numRows; i++) {
        KeyValue kv = new KeyValue(rowkey(i), family, qualifier, now, value);
        writer.append(kv);
      }
      writer.appendFileInfo(BULKLOAD_TIME_KEY, Bytes.toBytes(now));
    } finally {
      writer.close();
    }
  }

  /**
   * Thread that does full scans of the table looking for any partially
   * completed rows.
   *
   * Each iteration of this loads 10 hdfs files, which occupies 5 file open file
   * handles. So every 10 iterations (500 file handles) it does a region
   * compaction to reduce the number of open file handles.
   */
  public static class AtomicHFileLoader extends RepeatingTestThread {
    final AtomicLong numBulkLoads = new AtomicLong();
    final AtomicLong numCompactions = new AtomicLong();
    private TableName tableName;

    public AtomicHFileLoader(TableName tableName, TestContext ctx,
        byte targetFamilies[][]) throws IOException {
      super(ctx);
      this.tableName = tableName;
    }

    @Override
    public void doAnAction() throws Exception {
      long iteration = numBulkLoads.getAndIncrement();
      Path dir =  UTIL.getDataTestDirOnTestFS(String.format("bulkLoad_%08d",
          iteration));

      // create HFiles for different column families
      FileSystem fs = UTIL.getTestFileSystem();
      byte[] val = Bytes.toBytes(String.format("%010d", iteration));
      Map<byte[], List<Path>> family2Files = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      for (int i = 0; i < NUM_CFS; i++) {
        Path hfile = new Path(dir, family(i));
        byte[] fam = Bytes.toBytes(family(i));
        createHFile(fs, hfile, fam, QUAL, val, 1000);
        family2Files.put(fam, Collections.singletonList(hfile));
      }
      // bulk load HFiles
      BulkLoadHFiles.create(UTIL.getConfiguration()).bulkLoad(tableName, family2Files);
      // Periodically do compaction to reduce the number of open file handles.
      if (numBulkLoads.get() % 5 == 0) {
        RpcRetryingCallerFactory factory = new RpcRetryingCallerFactory(conf);
        RpcRetryingCaller<Void> caller = factory.<Void> newCaller();
        // 5 * 50 = 250 open file handles!
        ClientServiceCallable<Void> callable =
          new ClientServiceCallable<Void>(UTIL.getConnection(), tableName, Bytes.toBytes("aaa"),
            new RpcControllerFactory(UTIL.getConfiguration()).newController(),
            HConstants.PRIORITY_UNSET) {
            @Override
            protected Void rpcCall() throws Exception {
              LOG.debug(
                "compacting " + getLocation() + " for row " + Bytes.toStringBinary(getRow()));
              AdminProtos.AdminService.BlockingInterface server =
                ((ClusterConnection) UTIL.getConnection()).getAdmin(getLocation().getServerName());
              CompactRegionRequest request = RequestConverter.buildCompactRegionRequest(
                getLocation().getRegionInfo().getRegionName(), true, null);
              server.compactRegion(null, request);
              numCompactions.incrementAndGet();
              return null;
            }
          };
        caller.callWithRetries(callable, Integer.MAX_VALUE);
      }
    }
  }

  public static class MyObserver implements RegionCoprocessor, RegionObserver {
    static int sleepDuration;

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e, Store store,
        InternalScanner scanner, ScanType scanType, CompactionLifeCycleTracker tracker,
        CompactionRequest request)
        throws IOException {
      try {
        Thread.sleep(sleepDuration);
      } catch (InterruptedException ie) {
        IOException ioe = new InterruptedIOException();
        ioe.initCause(ie);
        throw ioe;
      }
      return scanner;
    }
  }

  /**
   * Thread that does full scans of the table looking for any partially
   * completed rows.
   */
  public static class AtomicScanReader extends RepeatingTestThread {
    byte targetFamilies[][];
    Table table;
    AtomicLong numScans = new AtomicLong();
    AtomicLong numRowsScanned = new AtomicLong();
    TableName TABLE_NAME;

    public AtomicScanReader(TableName TABLE_NAME, TestContext ctx,
        byte targetFamilies[][]) throws IOException {
      super(ctx);
      this.TABLE_NAME = TABLE_NAME;
      this.targetFamilies = targetFamilies;
      table = UTIL.getConnection().getTable(TABLE_NAME);
    }

    @Override
    public void doAnAction() throws Exception {
      Scan s = new Scan();
      for (byte[] family : targetFamilies) {
        s.addFamily(family);
      }
      ResultScanner scanner = table.getScanner(s);

      for (Result res : scanner) {
        byte[] lastRow = null, lastFam = null, lastQual = null;
        byte[] gotValue = null;
        for (byte[] family : targetFamilies) {
          byte qualifier[] = QUAL;
          byte thisValue[] = res.getValue(family, qualifier);
          if (gotValue != null && thisValue != null
              && !Bytes.equals(gotValue, thisValue)) {

            StringBuilder msg = new StringBuilder();
            msg.append("Failed on scan ").append(numScans)
                .append(" after scanning ").append(numRowsScanned)
                .append(" rows!\n");
            msg.append("Current  was " + Bytes.toString(res.getRow()) + "/"
                + Bytes.toString(family) + ":" + Bytes.toString(qualifier)
                + " = " + Bytes.toString(thisValue) + "\n");
            msg.append("Previous  was " + Bytes.toString(lastRow) + "/"
                + Bytes.toString(lastFam) + ":" + Bytes.toString(lastQual)
                + " = " + Bytes.toString(gotValue));
            throw new RuntimeException(msg.toString());
          }

          lastFam = family;
          lastQual = qualifier;
          lastRow = res.getRow();
          gotValue = thisValue;
        }
        numRowsScanned.getAndIncrement();
      }
      numScans.getAndIncrement();
    }
  }

  /**
   * Creates a table with given table name and specified number of column
   * families if the table does not already exist.
   */
  public void setupTable(TableName table, int cfs) throws IOException {
    try {
      LOG.info("Creating table " + table);
      HTableDescriptor htd = new HTableDescriptor(table);
      htd.addCoprocessor(MyObserver.class.getName());
      MyObserver.sleepDuration = this.sleepDuration;
      for (int i = 0; i < 10; i++) {
        htd.addFamily(new HColumnDescriptor(family(i)));
      }

      UTIL.getAdmin().createTable(htd);
    } catch (TableExistsException tee) {
      LOG.info("Table " + table + " already exists");
    }
  }

  /**
   * Atomic bulk load.
   */
  @Test
  public void testAtomicBulkLoad() throws Exception {
    TableName TABLE_NAME = TableName.valueOf("atomicBulkLoad");

    int millisToRun = 30000;
    int numScanners = 50;

    // Set createWALDir to true and use default values for other options.
    UTIL.startMiniCluster(StartMiniClusterOption.builder().createWALDir(true).build());
    try {
      WAL log = UTIL.getHBaseCluster().getRegionServer(0).getWAL(null);
      FindBulkHBaseListener listener = new FindBulkHBaseListener();
      log.registerWALActionsListener(listener);
      runAtomicBulkloadTest(TABLE_NAME, millisToRun, numScanners);
      assertThat(listener.isFound(), is(true));
    } finally {
      UTIL.shutdownMiniCluster();
    }
  }

  void runAtomicBulkloadTest(TableName tableName, int millisToRun, int numScanners)
      throws Exception {
    setupTable(tableName, 10);

    TestContext ctx = new TestContext(UTIL.getConfiguration());

    AtomicHFileLoader loader = new AtomicHFileLoader(tableName, ctx, null);
    ctx.addThread(loader);

    List<AtomicScanReader> scanners = Lists.newArrayList();
    for (int i = 0; i < numScanners; i++) {
      AtomicScanReader scanner = new AtomicScanReader(tableName, ctx, families);
      scanners.add(scanner);
      ctx.addThread(scanner);
    }

    ctx.startThreads();
    ctx.waitFor(millisToRun);
    ctx.stop();

    LOG.info("Loaders:");
    LOG.info("  loaded " + loader.numBulkLoads.get());
    LOG.info("  compations " + loader.numCompactions.get());

    LOG.info("Scanners:");
    for (AtomicScanReader scanner : scanners) {
      LOG.info("  scanned " + scanner.numScans.get());
      LOG.info("  verified " + scanner.numRowsScanned.get() + " rows");
    }
  }

  /**
   * Run test on an HBase instance for 5 minutes. This assumes that the table
   * under test only has a single region.
   */
  public static void main(String args[]) throws Exception {
    try {
      Configuration c = HBaseConfiguration.create();
      TestHRegionServerBulkLoad test = new TestHRegionServerBulkLoad(0);
      test.setConf(c);
      test.runAtomicBulkloadTest(TableName.valueOf("atomicTableTest"), 5 * 60 * 1000, 50);
    } finally {
      System.exit(0); // something hangs (believe it is lru threadpool)
    }
  }

  private void setConf(Configuration c) {
    UTIL = new HBaseTestingUtility(c);
  }

  static class FindBulkHBaseListener extends TestWALActionsListener.DummyWALActionsListener {
    private boolean found = false;

    @Override
    public void visitLogEntryBeforeWrite(WALKey logKey, WALEdit logEdit) {
      for (Cell cell : logEdit.getCells()) {
        KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
        for (Map.Entry entry : kv.toStringMap().entrySet()) {
          if (entry.getValue().equals(Bytes.toString(WALEdit.BULK_LOAD))) {
            found = true;
          }
        }
      }
    }

    public boolean isFound() {
      return found;
    }
  }
}


