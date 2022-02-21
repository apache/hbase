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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MultithreadedTestUtil.RepeatingTestThread;
import org.apache.hadoop.hbase.MultithreadedTestUtil.TestContext;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.AbstractHBaseTool;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;

/**
 * A test tool that uses multiple threads to read and write multifamily rows into a table, verifying
 * that reads never see partially-complete writes
 */
@InterfaceAudience.Private
public class AcidGuaranteesTestTool extends AbstractHBaseTool {

  private static final Logger LOG = LoggerFactory.getLogger(AcidGuaranteesTestTool.class);

  public static final TableName TABLE_NAME = TableName.valueOf("TestAcidGuarantees");
  public static final byte[] FAMILY_A = Bytes.toBytes("A");
  public static final byte[] FAMILY_B = Bytes.toBytes("B");
  public static final byte[] FAMILY_C = Bytes.toBytes("C");
  public static final byte[] QUALIFIER_NAME = Bytes.toBytes("data");

  public static final byte[][] FAMILIES = new byte[][] { FAMILY_A, FAMILY_B, FAMILY_C };

  public static int NUM_COLS_TO_CHECK = 50;

  private ExecutorService sharedPool;

  private long millisToRun;
  private int numWriters;
  private int numGetters;
  private int numScanners;
  private int numUniqueRows;
  private boolean crazyFlush;
  private boolean useMob;

  private ExecutorService createThreadPool() {
    int maxThreads = 256;
    int coreThreads = 128;

    long keepAliveTime = 60;
    BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>(
        maxThreads * HConstants.DEFAULT_HBASE_CLIENT_MAX_TOTAL_TASKS);

    ThreadPoolExecutor tpe =
      new ThreadPoolExecutor(coreThreads, maxThreads, keepAliveTime, TimeUnit.SECONDS, workQueue,
        new ThreadFactoryBuilder().setNameFormat(toString() + "-shared-pool-%d").setDaemon(true)
          .setUncaughtExceptionHandler(Threads.LOGGING_EXCEPTION_HANDLER).build());
    tpe.allowCoreThreadTimeOut(true);
    return tpe;
  }

  @Override
  protected void addOptions() {
    addOptWithArg("millis", "time limit in milliseconds");
    addOptWithArg("numWriters", "number of write threads");
    addOptWithArg("numGetters", "number of get threads");
    addOptWithArg("numScanners", "number of scan threads");
    addOptWithArg("numUniqueRows", "number of unique rows to test");
    addOptNoArg("crazyFlush",
      "if specified we will flush continuously otherwise will flush every minute");
    addOptNoArg("useMob", "if specified we will enable mob on the first column family");
  }

  @Override
  protected void processOptions(CommandLine cmd) {
    millisToRun = getOptionAsLong(cmd, "millis", 5000);
    numWriters = getOptionAsInt(cmd, "numWriters", 50);
    numGetters = getOptionAsInt(cmd, "numGetters", 2);
    numScanners = getOptionAsInt(cmd, "numScanners", 2);
    numUniqueRows = getOptionAsInt(cmd, "numUniqueRows", 3);
    crazyFlush = cmd.hasOption("crazyFlush");
    useMob = cmd.hasOption("useMob");
  }

  @Override
  protected int doWork() throws Exception {
    sharedPool = createThreadPool();
    try (Connection conn = ConnectionFactory.createConnection(getConf())) {
      runTestAtomicity(conn.getAdmin());
    } finally {
      sharedPool.shutdown();
    }
    return 0;
  }

  /**
   * Thread that does random full-row writes into a table.
   */
  public static class AtomicityWriter extends RepeatingTestThread {
    byte data[] = new byte[10];
    byte[][] targetRows;
    byte[][] targetFamilies;
    Connection connection;
    Table table;
    AtomicLong numWritten = new AtomicLong();

    public AtomicityWriter(TestContext ctx, byte[][] targetRows, byte[][] targetFamilies,
        ExecutorService pool) throws IOException {
      super(ctx);
      this.targetRows = targetRows;
      this.targetFamilies = targetFamilies;
      connection = ConnectionFactory.createConnection(ctx.getConf(), pool);
      table = connection.getTable(TABLE_NAME);
    }

    @Override
    public void doAnAction() throws Exception {
      // Pick a random row to write into
      byte[] targetRow = targetRows[ThreadLocalRandom.current().nextInt(targetRows.length)];
      Put p = new Put(targetRow);
      Bytes.random(data);
      for (byte[] family : targetFamilies) {
        for (int i = 0; i < NUM_COLS_TO_CHECK; i++) {
          byte qualifier[] = Bytes.toBytes("col" + i);
          p.addColumn(family, qualifier, data);
        }
      }
      table.put(p);
      numWritten.getAndIncrement();
    }

    @Override
    public void workDone() throws IOException {
      try {
        table.close();
      } finally {
        connection.close();
      }
    }
  }

  /**
   * Thread that does single-row reads in a table, looking for partially completed rows.
   */
  public static class AtomicGetReader extends RepeatingTestThread {
    byte[] targetRow;
    byte[][] targetFamilies;
    Connection connection;
    Table table;
    int numVerified = 0;
    AtomicLong numRead = new AtomicLong();

    public AtomicGetReader(TestContext ctx, byte[] targetRow, byte[][] targetFamilies,
        ExecutorService pool) throws IOException {
      super(ctx);
      this.targetRow = targetRow;
      this.targetFamilies = targetFamilies;
      connection = ConnectionFactory.createConnection(ctx.getConf(), pool);
      table = connection.getTable(TABLE_NAME);
    }

    @Override
    public void doAnAction() throws Exception {
      Get g = new Get(targetRow);
      Result res = table.get(g);
      byte[] gotValue = null;
      if (res.getRow() == null) {
        // Trying to verify but we didn't find the row - the writing
        // thread probably just hasn't started writing yet, so we can
        // ignore this action
        return;
      }

      for (byte[] family : targetFamilies) {
        for (int i = 0; i < NUM_COLS_TO_CHECK; i++) {
          byte qualifier[] = Bytes.toBytes("col" + i);
          byte thisValue[] = res.getValue(family, qualifier);
          if (gotValue != null && !Bytes.equals(gotValue, thisValue)) {
            gotFailure(gotValue, res);
          }
          numVerified++;
          gotValue = thisValue;
        }
      }
      numRead.getAndIncrement();
    }

    @Override
    public void workDone() throws IOException {
      try {
        table.close();
      } finally {
        connection.close();
      }
    }

    private void gotFailure(byte[] expected, Result res) {
      StringBuilder msg = new StringBuilder();
      msg.append("Failed after ").append(numVerified).append("!");
      msg.append("Expected=").append(Bytes.toStringBinary(expected));
      msg.append("Got:\n");
      for (Cell kv : res.listCells()) {
        msg.append(kv.toString());
        msg.append(" val= ");
        msg.append(Bytes.toStringBinary(CellUtil.cloneValue(kv)));
        msg.append("\n");
      }
      throw new RuntimeException(msg.toString());
    }
  }

  /**
   * Thread that does full scans of the table looking for any partially completed rows.
   */
  public static class AtomicScanReader extends RepeatingTestThread {
    byte[][] targetFamilies;
    Table table;
    Connection connection;
    AtomicLong numScans = new AtomicLong();
    AtomicLong numRowsScanned = new AtomicLong();

    public AtomicScanReader(TestContext ctx, byte[][] targetFamilies, ExecutorService pool)
        throws IOException {
      super(ctx);
      this.targetFamilies = targetFamilies;
      connection = ConnectionFactory.createConnection(ctx.getConf(), pool);
      table = connection.getTable(TABLE_NAME);
    }

    @Override
    public void doAnAction() throws Exception {
      Scan s = new Scan();
      for (byte[] family : targetFamilies) {
        s.addFamily(family);
      }
      ResultScanner scanner = table.getScanner(s);

      for (Result res : scanner) {
        byte[] gotValue = null;

        for (byte[] family : targetFamilies) {
          for (int i = 0; i < NUM_COLS_TO_CHECK; i++) {
            byte qualifier[] = Bytes.toBytes("col" + i);
            byte thisValue[] = res.getValue(family, qualifier);
            if (gotValue != null && !Bytes.equals(gotValue, thisValue)) {
              gotFailure(gotValue, res);
            }
            gotValue = thisValue;
          }
        }
        numRowsScanned.getAndIncrement();
      }
      numScans.getAndIncrement();
    }

    @Override
    public void workDone() throws IOException {
      try {
        table.close();
      } finally {
        connection.close();
      }
    }

    private void gotFailure(byte[] expected, Result res) {
      StringBuilder msg = new StringBuilder();
      msg.append("Failed after ").append(numRowsScanned).append("!");
      msg.append("Expected=").append(Bytes.toStringBinary(expected));
      msg.append("Got:\n");
      for (Cell kv : res.listCells()) {
        msg.append(kv.toString());
        msg.append(" val= ");
        msg.append(Bytes.toStringBinary(CellUtil.cloneValue(kv)));
        msg.append("\n");
      }
      throw new RuntimeException(msg.toString());
    }
  }

  private void createTableIfMissing(Admin admin, boolean useMob) throws IOException {
    if (!admin.tableExists(TABLE_NAME)) {
      TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(TABLE_NAME);
      Stream.of(FAMILIES).map(ColumnFamilyDescriptorBuilder::of)
          .forEachOrdered(builder::setColumnFamily);
      admin.createTable(builder.build());
    }
    ColumnFamilyDescriptor cfd = admin.getDescriptor(TABLE_NAME).getColumnFamilies()[0];
    if (cfd.isMobEnabled() != useMob) {
      admin.modifyColumnFamily(TABLE_NAME, ColumnFamilyDescriptorBuilder.newBuilder(cfd)
          .setMobEnabled(useMob).setMobThreshold(4).build());
    }
  }

  private void runTestAtomicity(Admin admin) throws Exception {
    createTableIfMissing(admin, useMob);
    TestContext ctx = new TestContext(conf);

    byte rows[][] = new byte[numUniqueRows][];
    for (int i = 0; i < numUniqueRows; i++) {
      rows[i] = Bytes.toBytes("test_row_" + i);
    }

    List<AtomicityWriter> writers = Lists.newArrayList();
    for (int i = 0; i < numWriters; i++) {
      AtomicityWriter writer = new AtomicityWriter(ctx, rows, FAMILIES, sharedPool);
      writers.add(writer);
      ctx.addThread(writer);
    }
    // Add a flusher
    ctx.addThread(new RepeatingTestThread(ctx) {
      @Override
      public void doAnAction() throws Exception {
        try {
          admin.flush(TABLE_NAME);
        } catch (IOException ioe) {
          LOG.warn("Ignoring exception while flushing: " + StringUtils.stringifyException(ioe));
        }
        // Flushing has been a source of ACID violations previously (see HBASE-2856), so ideally,
        // we would flush as often as possible. On a running cluster, this isn't practical:
        // (1) we will cause a lot of load due to all the flushing and compacting
        // (2) we cannot change the flushing/compacting related Configuration options to try to
        // alleviate this
        // (3) it is an unrealistic workload, since no one would actually flush that often.
        // Therefore, let's flush every minute to have more flushes than usual, but not overload
        // the running cluster.
        if (!crazyFlush) {
          Thread.sleep(60000);
        }
      }
    });

    List<AtomicGetReader> getters = Lists.newArrayList();
    for (int i = 0; i < numGetters; i++) {
      AtomicGetReader getter =
          new AtomicGetReader(ctx, rows[i % numUniqueRows], FAMILIES, sharedPool);
      getters.add(getter);
      ctx.addThread(getter);
    }

    List<AtomicScanReader> scanners = Lists.newArrayList();
    for (int i = 0; i < numScanners; i++) {
      AtomicScanReader scanner = new AtomicScanReader(ctx, FAMILIES, sharedPool);
      scanners.add(scanner);
      ctx.addThread(scanner);
    }

    ctx.startThreads();
    ctx.waitFor(millisToRun);
    ctx.stop();

    LOG.info("Finished test. Writers:");
    for (AtomicityWriter writer : writers) {
      LOG.info("  wrote " + writer.numWritten.get());
    }
    LOG.info("Readers:");
    for (AtomicGetReader reader : getters) {
      LOG.info("  read " + reader.numRead.get());
    }
    LOG.info("Scanners:");
    for (AtomicScanReader scanner : scanners) {
      LOG.info("  scanned " + scanner.numScans.get());
      LOG.info("  verified " + scanner.numRowsScanned.get() + " rows");
    }
  }

  public static void main(String[] args) {
    Configuration c = HBaseConfiguration.create();
    int status;
    try {
      AcidGuaranteesTestTool test = new AcidGuaranteesTestTool();
      status = ToolRunner.run(c, test, args);
    } catch (Exception e) {
      LOG.error("Exiting due to error", e);
      status = -1;
    }
    System.exit(status);
  }
}
