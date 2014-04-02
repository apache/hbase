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

package org.apache.hadoop.hbase.util.rpcbench;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.loadtest.ColumnFamilyProperties;
import org.apache.hadoop.hbase.loadtest.HBaseUtils;
import org.apache.hadoop.hbase.regionserver.metrics.PercentileMetric;
import org.apache.hadoop.hbase.util.AbstractHBaseTool;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Histogram;
import org.weakref.jmx.com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Tool that runs the benchmarks. This takes the name of a benchmark factory,
 * and performs a single put and does a lot of gets to retrieve that put.
 *
 * We can provide arguments like number of client threads that need to execute,
 * the number of rounds we need to repeat the experiment for, the length of the
 * payload and the number of operations we perform.
 */
public class HBaseRPCBenchmarkTool extends AbstractHBaseTool {

  private static final Log LOG = LogFactory.getLog(HBaseRPCBenchmarkTool.class);

  private static final long DEFAULT_REPORT_INTERVAL_MS = 10;
  private static final int DEFAULT_NUM_OPS = 200;
  private static final int DEFAULT_NUM_THREADS = 10;
  private static final int DEFAULT_ROW_LENGTH = 20;
  private static final String DEFAULT_CF = "cf";
  private static final int DEFAULT_QUAL_LENGTH = 10;
  private static final int DEFAULT_VALUE_LENGTH = 100;
  private static final String DEFAULT_TABLENAME = "RPCBenchmarkingTable";
  private static final int DEFAULT_ZK_PORT =
      HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT;
  private static final boolean DEFAULT_ASYNC_CALLS = false;

  /**
   * The following are the command line parameters which this tool takes.
   */
  private static final String OPT_CF = "cf";
  private static final String OPT_QUAL_LENGTH = "q";
  private static final String OPT_ROW_LENGTH = "r";
  private static final String OPT_TBL_NAME = "t";
  private static final String OPT_VALUE_LENGTH = "v";
  private static final String OPT_CLASS = "c";
  private static final String OPT_NUM_OPS = "ops";
  private static final String OPT_NUM_THREADS = "threads";
  private static final String OPT_REPORT_INTERVAL = "interval";
  private static final String OPT_ZK_QUORUM = "zk";
  private static final String OPT_ZK_PORT = "zkPort";
  private static final String OPT_GET_BATCH_SIZE = "gbatch";
  private static final String OPT_PUT_BATCH_SIZE = "pbatch";
  private static final String OPT_ASYNC_CALLS = "async";

  /**
   * These are values that we get from the command line and
   * a few other internal state variables.
   */
  private Configuration conf;
  // Initializing a histogram with minimum of 0 seconds and maximum of 1 second.
  private final Histogram histogram = new Histogram(100, 0,
      1*1000*1000*1000);
  private Class<?> factoryCls;
  private byte[] tblName;
  private String zkQuorum;
  private int zkPort;
  private int rowLength;
  private byte[] family;
  private int qualLength;
  private int valueLength;
  private int numOps;
  private int numThreads;
  private long reportInterval;
  private AtomicLong sumLatency = new AtomicLong(0);
  private AtomicLong totalOps = new AtomicLong(0);
  private long runtimeMs;
  private int multigetBatch;
  private int multiputBatch;
  private boolean useAsync;


  private HBaseRPCBenchmarkTool() {
  }

  private HBaseRPCBenchmarkTool(Class<?> factoryCls) {
    this.factoryCls = factoryCls;
  }

  private HBaseRPCBenchmarkTool(Class<? extends BenchmarkFactory> factoryCls,
      byte[] tableName, Configuration conf, int rowLength, byte[] cf,
      int qualLength, int valueLength, int numOps, int numThreads,
      long reportIntervalMs, int multiGetBatch, int multiputBatch) {
    this.factoryCls = factoryCls;
    this.conf = conf;
    this.tblName = tableName;
    this.rowLength = rowLength;
    this.family = cf;
    this.qualLength = qualLength;
    this.valueLength = valueLength;
    this.numOps = numOps;
    this.numThreads = numThreads;
    this.reportInterval = reportIntervalMs;
    this.multigetBatch = multiGetBatch;
    this.multiputBatch = multiputBatch;
  }

  /**
   * Builder class for the HBaseRPCBenchmarkTool.
   */
  public static class Builder {
    private final Class<? extends BenchmarkFactory> factoryCls;
    private byte[] tableName = Bytes.toBytes(DEFAULT_TABLENAME);
    private int rowLength = DEFAULT_ROW_LENGTH;
    private byte[] cf = Bytes.toBytes(DEFAULT_CF);
    private int qualLength = DEFAULT_QUAL_LENGTH;
    private int valueLength = DEFAULT_VALUE_LENGTH;
    private int numOps = DEFAULT_NUM_OPS;
    private int numThreads = DEFAULT_NUM_THREADS;
    private long reportIntervalMs = DEFAULT_REPORT_INTERVAL_MS;
    private Configuration conf;
    private int multigetBatch = 1000;
    private int multiputBatch = 1000;

    public Builder(Class<? extends BenchmarkFactory> factoryCls) {
      this.factoryCls = factoryCls;
    }

    public Builder withTableName(byte[] tableName) {
      this.tableName = tableName;
      return this;
    }

    public Builder withRowLength(int rowLength) {
      this.rowLength = rowLength;
      return this;
    }

    public Builder withColumnFamily(byte[] cf) {
      this.cf = cf;
      return this;
    }

    public Builder withQualifierLength(int qualLength) {
      this.qualLength = qualLength;
      return this;
    }

    public Builder withValue(int valueLength) {
      this.valueLength = valueLength;
      return this;
    }

    public Builder withNumOps(int numOps) {
      this.numOps = numOps;
      return this;
    }

    public Builder withNumThreads(int numThreads) {
      this.numThreads = numThreads;
      return this;
    }

    public Builder withConf(Configuration conf) {
      this.conf = conf;
      return this;
    }

    public Builder withMultiGetBatch(int multiGetBatch) {
      this.multigetBatch = multiGetBatch;
      return this;
    }

    public Builder withMultiPutBatch(int multiputBatch) {
      this.multiputBatch = multiputBatch;
      return this;
    }

    public HBaseRPCBenchmarkTool create() {
      return new HBaseRPCBenchmarkTool(this.factoryCls, this.tableName,
        this.conf, this.rowLength, this.cf, this.qualLength, this.valueLength,
        this.numOps, this.numThreads, this.reportIntervalMs,
        this.multigetBatch, this.multiputBatch);
    }
  }

  /**
   * Just adds all the following command line parameters.
   */
  @Override
  protected void addOptions() {
    addOptWithArg(OPT_CLASS, "Benchmark factory class");
    addOptWithArg(OPT_NUM_THREADS, "Number of threads");
    addOptWithArg(OPT_NUM_OPS, "Number of operations to execute per thread");
    addOptWithArg(OPT_GET_BATCH_SIZE, "Number of gets in a single batch");
    addOptWithArg(OPT_PUT_BATCH_SIZE, "Number of puts in a single batch");
    addOptWithArg(OPT_TBL_NAME, "Table name to use");
    addOptWithArg(OPT_ZK_QUORUM, "Table name");
    addOptWithArg(OPT_ZK_PORT, "Zookeeper Port");
    addOptWithArg(OPT_REPORT_INTERVAL, "Reporting interval in milliseconds");
    addOptWithArg(OPT_ROW_LENGTH, "Row key length");
    addOptWithArg(OPT_CF, "Column family to use");
    addOptWithArg(OPT_QUAL_LENGTH, "Column qualifier length");
    addOptWithArg(OPT_VALUE_LENGTH, "Value length");
    addOptWithArg(OPT_ASYNC_CALLS, "Use async calls underneath the sync calls");
  }

  /**
   * Parses the command line options.
   */
  @Override
  protected void processOptions(CommandLine cmd) {
    conf = HBaseConfiguration.create();
    // Takes ThriftBenchmarkFactory by default.
    String className = ThriftBenchmarkFactory.class.getName();
    if (cmd.hasOption(OPT_CLASS)) {
      className = cmd.getOptionValue(OPT_CLASS);
      LOG.debug("Using class name : " + className);
    }
    try {
      factoryCls = Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Can't find a class " + className, e);
    }
    this.zkPort = DEFAULT_ZK_PORT;
    if (cmd.hasOption(OPT_ZK_QUORUM)) {
      zkQuorum = cmd.getOptionValue(OPT_ZK_QUORUM);
      conf.set(HConstants.ZOOKEEPER_QUORUM, zkQuorum);
      conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, this.zkPort);
      LOG.debug("Adding zookeeper quorum : " + zkQuorum);
    }
    reportInterval = parseLong(cmd.getOptionValue(OPT_REPORT_INTERVAL,
        String.valueOf(DEFAULT_REPORT_INTERVAL_MS)),
        reportInterval, Long.MAX_VALUE);
    if (cmd.hasOption(OPT_TBL_NAME)) {
      tblName = Bytes.toBytes(cmd.getOptionValue(OPT_TBL_NAME));
    } else {
      tblName = Bytes.toBytes(DEFAULT_TABLENAME);
      ColumnFamilyProperties[] familyProperties = new ColumnFamilyProperties[1];
      familyProperties[0] = new ColumnFamilyProperties();
      familyProperties[0].familyName = DEFAULT_CF;
      familyProperties[0].maxVersions = Integer.MAX_VALUE;
      HBaseUtils.createTableIfNotExists(conf,
          tblName, familyProperties, 1);
    }
    rowLength = Integer.parseInt(
        cmd.getOptionValue(OPT_ROW_LENGTH, String.valueOf(DEFAULT_ROW_LENGTH)));
    family = Bytes.toBytes(cmd.getOptionValue(OPT_CF, DEFAULT_CF));
    qualLength = Integer.parseInt(cmd.getOptionValue(OPT_QUAL_LENGTH,
        String.valueOf(DEFAULT_QUAL_LENGTH)));
    valueLength = Integer.parseInt(cmd.getOptionValue(OPT_VALUE_LENGTH,
        String.valueOf(DEFAULT_VALUE_LENGTH)));
    numOps = parseInt(cmd.getOptionValue(OPT_NUM_OPS,
        String.valueOf(DEFAULT_NUM_OPS)), 1, Integer.MAX_VALUE);
    numThreads = parseInt(cmd.getOptionValue(OPT_NUM_THREADS,
        String.valueOf(DEFAULT_NUM_THREADS)), 1, Integer.MAX_VALUE);
    this.multigetBatch = parseInt(cmd.getOptionValue(OPT_GET_BATCH_SIZE,
        String.valueOf(1000)), 1, Integer.MAX_VALUE);
    this.multiputBatch = parseInt(cmd.getOptionValue(OPT_PUT_BATCH_SIZE,
        String.valueOf(1000)), 1, Integer.MAX_VALUE);
    this.useAsync = Boolean.parseBoolean(cmd.getOptionValue(OPT_ASYNC_CALLS));
  }

  public List<Get> performRandomMultiputs(BenchmarkClient benchmark,
      int numMultiPutOps, int multiputBatchSize) throws IOException {
    List<Get> ret = new ArrayList<Get>();
    for (int i = 0; i < numMultiPutOps; i++) {
      List<Put> puts = new ArrayList<Put>(multiputBatchSize);
      for (int j = 0; j < multiputBatchSize; j++) {
        Put p = benchmark.createRandomPut(this.rowLength, this.family,
            this.qualLength, this.valueLength);
        puts.add(p);
        ret.add(new Get(p.getRow()));
      }
      benchmark.executeMultiPut(puts);
    }
    return ret;
  }

  /**
   * Main function which does the benchmarks.
   * @throws IllegalAccessException
   * @throws InstantiationException
   * @throws IOException
   */
  @Override
  protected void doWork() throws InterruptedException,
      InstantiationException, IllegalAccessException, IOException {
    // Initializing the required objects.
    BenchmarkFactory factory = (BenchmarkFactory) factoryCls.newInstance();
    LOG.debug("Creating an instance of the factory class : " +
        factoryCls.getCanonicalName());
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    Configuration conf = HBaseConfiguration.create(this.conf);
    conf.setBoolean(HConstants.HTABLE_ASYNC_CALLS, this.useAsync);
    BenchmarkClient benchmark = factory.makeBenchmarkClient(tblName, conf);

    List<Get> gets = performRandomMultiputs(benchmark, this.numOps/10,
        this.multiputBatch);
    runtimeMs = System.currentTimeMillis();

    // Count down latches which let me synchronize all the benchmark workers to
    // run together.
    final AtomicBoolean running = new AtomicBoolean(true);
    final CountDownLatch readySignal = new CountDownLatch(numThreads);
    final CountDownLatch startSignal = new CountDownLatch(1);
    final CountDownLatch doneSignal = new CountDownLatch(numThreads);

    // Spawning the worker threads here.
    for (int i = 0; i < numThreads; i++) {
      executor.submit(new WorkerThread(histogram, sumLatency,
          totalOps, factory, tblName, conf, numOps, reportInterval, rowLength,
          family, qualLength, valueLength, gets, this.multigetBatch,
          readySignal, startSignal, doneSignal, running));
    }

    // Here we will wait for all the worker threads to kick off and then we let
    // the worker threads know that they are free to start their benchmarks.
    try {
      // Will wait for all the threads to get ready i.e. start
      readySignal.await();

      // will signal all the threads to start simultaneously
      startSignal.countDown();

      // Will wait for all the threads to finish execution upto a certain point
      doneSignal.await();

      // Will signal the threads to terminate.
      running.set(false);
    } catch (InterruptedException e) {
      LOG.error("Not able to start the worker threads together." +
          "Probably we were interrupted?");
    }
    executor.shutdown();
    executor.awaitTermination(1, TimeUnit.HOURS);
    runtimeMs = System.currentTimeMillis() - runtimeMs;
  }

  public static void printStats(String msg, int numOps, long startTime) {
    long elapsedSeconds = (System.currentTimeMillis() - startTime);
    double opsPerMSec = numOps / elapsedSeconds;
    StringBuilder sb = new StringBuilder();
    sb.append(msg);
    sb.append(" throughput : ");
    sb.append(opsPerMSec);
    sb.append(" ops/ms.");
    System.out.println(sb.toString());
  }

  /**
   * The worker thread which performs the single thread benchmark.
   * Once this thread starts, it waits for all the threads to start and then
   * it starts running the benchmark.
   */
  class WorkerThread extends Thread {
    private final Histogram histogram;
    private final AtomicLong totalLatency;
    private final AtomicLong totalOps;
    private final BenchmarkFactory factory;
    private BenchmarkClient benchmark;
    private final byte[] tableName;
    private final Configuration conf;
    private final int numOps;
    @SuppressWarnings("unused")
    private final int rowLength;
    @SuppressWarnings("unused")
    private final byte[] family;
    @SuppressWarnings("unused")
    private final int qualLength;
    @SuppressWarnings("unused")
    private final int valueLength;
    private final List<Get> gets;
    private final CountDownLatch readySignal; // To notify the controller that this thread has started
    private final CountDownLatch startSignal; // To notify this thread that it is free to start
    private final CountDownLatch doneSignal; // To notify the controller thread to shutdown the threads.
    private final AtomicBoolean running; // the state variable which tells whether the threads should be running.
    private boolean signalledDone = false;
    private final int multiGetBatch;

    WorkerThread(Histogram histogram,
        AtomicLong totalLatency,
        AtomicLong totalOps,
        BenchmarkFactory benchmarkFactory,
        byte[] tableName,
        Configuration conf,
        int numOps,
        long reportIntervalMs,
        int rowLength,
        byte[] family,
        int qualLength,
        int valueLength,
        List<Get> gets,
        int multiGetBatch,
        CountDownLatch readySignal,
        CountDownLatch startSignal,
        CountDownLatch doneSignal,
        AtomicBoolean running) {
      this.factory = benchmarkFactory;
      this.tableName = tableName;
      this.conf = conf;
      this.histogram = histogram;
      this.totalLatency = totalLatency;
      this.totalOps = totalOps;
      this.numOps = numOps;
      this.rowLength = rowLength;
      this.family = family;
      this.qualLength = qualLength;
      this.valueLength = valueLength;
      this.gets = gets;
      this.multiGetBatch = multiGetBatch;
      this.readySignal = readySignal;
      this.startSignal = startSignal;
      this.doneSignal = doneSignal;
      this.running = running;
    }

    public Result[] executeMultiGet(int batch) {
      List<Get> todogets = new ArrayList<Get>();
      Random rand = new Random();
      if (batch == 1) {
        return new Result[] {
            benchmark.executeGet(gets.get(rand.nextInt(gets.size())))
          };
      }
      for (int i = 0; i < batch; i++) {
        todogets.add(new Get(this.gets.get(rand.nextInt(this.gets.size())).getRow()));
      }
      return benchmark.executeMultiGet(todogets);
    }

    @Override
    public void run() {
      LOG.debug("Worker Thread. numOps:" + numOps);
      this.benchmark = factory.makeBenchmarkClient(tableName, conf);
      final long startTime = System.currentTimeMillis();
      // We let the master know that we are ready.
      readySignal.countDown();
      try {
        // And wait for the master to signal us to start.
        startSignal.await();
      } catch (InterruptedException e) {
        LOG.debug("Interrupted while waiting for the signal");
        e.printStackTrace();
      }
      for (int i = 0; ; ++i) {
        long opStartNs = System.nanoTime();
        try {
          if (i % (this.numOps / 10) == 0) {
            setProfilingData(true);
          }
          Result[] ret = executeMultiGet(this.multiGetBatch);
          Preconditions.checkArgument(ret.length == multigetBatch);
          if (i % (this.numOps / 10) == 0) {
            printProfilingData();
            setProfilingData(false);
          }
        } catch (Exception e) {
          LOG.debug("Encountered exception while performing get");
          e.printStackTrace();
          break;
        }
        long delta = System.nanoTime() - opStartNs;
        totalLatency.addAndGet(delta);
        totalOps.addAndGet(this.multiGetBatch);
        histogram.addValue(delta);
        if (i >= numOps) {
          if (!signalledDone) {
            doneSignal.countDown();
            signalledDone = true;
          }
          if (!running.get()) break;
        }
      }
      StringBuilder sb = new StringBuilder();
      sb.append("Printing statistics for " + factoryCls.getName());
      sb.append(". Total Ops : ");
      sb.append(totalOps.get());
      sb.append(". Get Batch Size : ");
      sb.append(this.multiGetBatch);
      sb.append(". Average latency : ");
      sb.append(totalLatency.get()/totalOps.get());
      sb.append("ns. ");
      sb.append("p95 latency : ");
      sb.append(histogram.getPercentileEstimate(PercentileMetric.P95));
      sb.append(". p99 latency : ");
      sb.append(histogram.getPercentileEstimate(PercentileMetric.P99));
      sb.append(". Throughput : ");
      sb.append((totalOps.get() * 1000)/
          (System.currentTimeMillis() - startTime));
      sb.append(" ops/s.");
      LOG.debug(sb);
    }

    private void printProfilingData() {
      this.benchmark.printProfilingData();
    }

    private void setProfilingData(boolean flag) {
      this.benchmark.setProfilingData(flag);
    }
  }

  public long getTotalOps() {
    return totalOps.get();
  }

  public double getThroughput() {
    return (this.totalOps.get() * 1000) / (double)this.runtimeMs;
  }

  public double getAverageLatency() {
    return this.sumLatency.get() / (double)this.totalOps.get();
  }

  public double getP95Latency() {
    return histogram.getPercentileEstimate(PercentileMetric.P95);
  }

  public double getP99Latency() {
    return histogram.getPercentileEstimate(PercentileMetric.P99);
  }

  public static void main(String[] args) {
    HBaseRPCBenchmarkTool tool = new HBaseRPCBenchmarkTool();
    int ret = tool.doStaticMain(args);
    System.out.println("Total throughput : " + tool.getThroughput());
    System.out.println("Avg Latency : " + tool.getAverageLatency());
    System.out.println("P99 latency : " + tool.getP99Latency());
    System.out.println("P95 latency : " + tool.getP95Latency());
    System.exit(ret);
  }
}
