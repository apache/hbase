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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.loadtest.ColumnFamilyProperties;
import org.apache.hadoop.hbase.loadtest.HBaseUtils;
import org.apache.hadoop.hbase.regionserver.metrics.PercentileMetric;
import org.apache.hadoop.hbase.util.AbstractHBaseTool;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Histogram;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Benchmark tool which compares various benchmarks by running them together.
 * This forms a layer over the HBaseRPCBenchmarkTool and will help in
 * comparison.
 * Currently it runs the benchmarks in parallel. Functionality can be added to
 * run them in a serial fashion.
 */
public class HBaseRPCProtocolComparison extends AbstractHBaseTool {
  private static final Log LOG =
      LogFactory.getLog(HBaseRPCProtocolComparison.class);

  private static final long DEFAULT_REPORT_INTERVAL_MS = 1;
  private static final int DEFAULT_NUM_OPS = 10000;
  private static final int DEFAULT_NUM_ROUNDS = 100;
  private static final int DEFAULT_NUM_THREADS = 10;
  private static final String DEFAULT_ROW = "rowkey";
  private static final String DEFAULT_CF = "cf";
  private static final String DEFAULT_QUAL ="q";
  private static final String DEFAULT_VALUE = "v";
  private static final String DEFAULT_TABLENAME = "RPCBenchmarkingTable";
  private static final int DEFAULT_ZK_PORT = 2181;
  private static final boolean DEFAULT_DO_PUT = true;

  private static final String OPT_CF= "cf";
  private static final String OPT_QUAL = "q";
  private static final String OPT_ROW = "r";
  private static final String OPT_TBL_NAME = "t";
  private static final String OPT_VALUE_LENGTH = "vlen";
  private static final String OPT_CLASSES = "c";
  private static final String OPT_NUM_OPS = "ops";
  private static final String OPT_NUM_ROUNDS = "rounds";
  private static final String OPT_NUM_THREADS = "threads";
  private static final String OPT_REPORT_INTERVAL = "interval";
  private static final String OPT_NO_PUT = "no_put";
  private static final String OPT_ZK_QUORUM = "zk";
  private static final String OPT_ZK_PORT = "zkPort";

  private Configuration conf;
  private List<Class<? extends BenchmarkFactory>> factoryClasses;
  private byte[] tblName;
  private String zkQuorum;
  private int zkPort;
  private byte[] row;
  private byte[] family;
  private byte[] qual;
  private byte[] value;
  private int valueLength;
  private int numOps;
  private int numRounds;
  private int numThreads;
  private long reportInterval;
  private boolean doPut;

  @Override
  protected void addOptions() {
    addOptWithArg(OPT_CLASSES, "Benchmark factory classes");
    addOptWithArg(OPT_NUM_THREADS, "Number of threads");
    addOptWithArg(OPT_NUM_OPS, "Number of operations to execute per thread");
    addOptWithArg(OPT_TBL_NAME, "Table name");
    addOptWithArg(OPT_ZK_QUORUM, "Table name");
    addOptWithArg(OPT_ZK_PORT, "Zookeeper Port");
    addOptWithArg(OPT_REPORT_INTERVAL, "Reporting interval in milliseconds");
    addOptWithArg(OPT_ROW, "Row key");
    addOptWithArg(OPT_NO_PUT,
        "DO NOT perform a single put (writing the value) before the benchmark");
    addOptWithArg(OPT_CF, "Column family to use");
    addOptWithArg(OPT_QUAL, "Column qualifier to use");
    addOptWithArg(OPT_VALUE_LENGTH, "Value length to use");
    addOptWithArg(OPT_NUM_ROUNDS, "Number of rounds to perform the tests");
  }

  @Override
  protected void processOptions(CommandLine cmd) {
    conf = HBaseConfiguration.create();
    if (!cmd.hasOption(OPT_CLASSES)) {
      throw new IllegalArgumentException("--" + OPT_CLASSES +
          " must be specified!");
    }
    String classNames = null;
    if (cmd.hasOption(OPT_CLASSES)) {
      classNames = cmd.getOptionValue(OPT_CLASSES);
      LOG.debug("Using class name : " + classNames);
      try {
        factoryClasses = new ArrayList<Class<? extends BenchmarkFactory>>();
        for (String s : classNames.split(",")) {
          factoryClasses.add(
              (Class<? extends BenchmarkFactory>)Class.forName(s)
                .asSubclass(BenchmarkFactory.class));
        }
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("Can't find a class " +
            classNames, e);
      }
    }
    zkPort = DEFAULT_ZK_PORT;
    if (cmd.hasOption(OPT_ZK_PORT)) {
      zkPort = Integer.parseInt(cmd.getOptionValue(OPT_ZK_PORT));
    }
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
    row = Bytes.toBytes(cmd.getOptionValue(OPT_ROW, DEFAULT_ROW));
    family = Bytes.toBytes(cmd.getOptionValue(OPT_CF, DEFAULT_CF));
    qual = Bytes.toBytes(cmd.getOptionValue(OPT_QUAL, DEFAULT_QUAL));
    valueLength = parseInt(cmd.getOptionValue(OPT_VALUE_LENGTH, DEFAULT_VALUE),
        0, Integer.MAX_VALUE);
    Random r = new Random();
    value = new byte[valueLength];
    r.nextBytes(value);
    numOps = parseInt(cmd.getOptionValue(OPT_NUM_OPS,
        String.valueOf(DEFAULT_NUM_OPS)), 1, Integer.MAX_VALUE);
    numRounds = parseInt(cmd.getOptionValue(OPT_NUM_ROUNDS,
        String.valueOf(DEFAULT_NUM_ROUNDS)), 1, Integer.MAX_VALUE);
    numThreads = parseInt(cmd.getOptionValue(OPT_NUM_THREADS,
        String.valueOf(DEFAULT_NUM_THREADS)), 1, Integer.MAX_VALUE);
    doPut = DEFAULT_DO_PUT;
    doPut = !cmd.hasOption(OPT_NO_PUT);
  }

  /**
   * A simple wrapper class which can contain the relevant metrics
   */
  private static class Stats {
    public Histogram Histogram = new Histogram(100, 0, 1*1000*1000*1000/*1s*/);
    public AtomicLong TotalRuntime = new AtomicLong(0);
    public AtomicLong TotalLatency = new AtomicLong(0);
    public AtomicLong TotalOps = new AtomicLong(0);
  }

  /**
   * The main function that performs the comparative benchmark.
   * @throws InterruptedException
   */
  @Override
  protected void doWork() throws InterruptedException {
    final Map<Class<? extends BenchmarkFactory>, Stats> statsMap =
        new HashMap<Class<? extends BenchmarkFactory>, Stats>();
    for (final Class<? extends BenchmarkFactory> factoryCls : factoryClasses) {
      statsMap.put(factoryCls, new Stats());
    }

    for (int i=0; i<numRounds; i++) {
      ExecutorService executor = Executors.newFixedThreadPool(numThreads);
      for (final Class<? extends BenchmarkFactory> factoryCls :
          factoryClasses) {
        executor.submit(new Runnable() {
          @Override
          public void run() {
            try {
              Stats stats = statsMap.get(factoryCls);
              Histogram hist = stats.Histogram;
              AtomicLong runTime = stats.TotalRuntime;
              AtomicLong totalLatency = stats.TotalLatency;
              AtomicLong totalOps = stats.TotalOps;
              long startTime = System.currentTimeMillis();
              HBaseRPCBenchmarkTool tool = new HBaseRPCBenchmarkTool
                .Builder(factoryCls).withColumnFamily(family).withNumOps(numOps)
                .withRow(row).withNumThreads(numThreads).withConf(conf)
                .withQualifier(qual).withTableName(tblName).withValue(value)
                .withDoPut(doPut).create();
              tool.doWork();
              hist.addValue(tool.getP95Latency());
              runTime.addAndGet(System.currentTimeMillis() - startTime);
              totalLatency.addAndGet((long)tool.getAverageLatency());
              totalOps.addAndGet(tool.getTotalOps());
            } catch (InterruptedException
                | InstantiationException | IllegalAccessException e) {
              LOG.debug("Cannot run the tool for factory : "
                 + factoryCls.getName());
              e.printStackTrace();
            }
          }
        });
      }
      executor.shutdown();
      executor.awaitTermination(1, TimeUnit.HOURS);
    }
    for (Entry<Class<? extends BenchmarkFactory>, Stats> entry :
        statsMap.entrySet()) {
      Stats s = entry.getValue();
      System.out.println(entry.getKey().getName() +
          " : Printing stats for " + numRounds + ":" + numOps + ":" +
          valueLength + ":" + numThreads + ":"
          + numRounds + " rounds." +
          " Average Latency : " +
          (s.TotalLatency.get() / ((double)numRounds * 1000 * 1000)) +
          "ms . throughput : " +
          ((entry.getValue().TotalOps.get() * 1000) /
              (double)entry.getValue().TotalRuntime.get()) +
          "ops/s. p95 of p95 : " +
          entry.getValue().Histogram.getPercentileEstimate(
              PercentileMetric.P95));
    }
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    int ret = new HBaseRPCProtocolComparison().doStaticMain(args);
    System.exit(ret);
  }
}
