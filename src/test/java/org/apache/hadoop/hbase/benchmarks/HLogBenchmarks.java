/**
 * Copyright 2010 The Apache Software Foundation
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

package org.apache.hadoop.hbase.benchmarks;

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.TestHFilePerformance.KeyValueGenerator;
import org.apache.hadoop.hbase.regionserver.FlushRequester;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.LogRoller;
import org.apache.hadoop.hbase.regionserver.RegionServerRunningException;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;

import com.google.common.collect.Lists;

public class HLogBenchmarks extends HDFSBenchmarks {
  private final List<HLog> hlogpool = Lists.newArrayList();
  private final List<LogRoller> hlogrollers = Lists.newArrayList();
  private ExecutorService ipcHandlerThreads = Executors.newFixedThreadPool(300,
      Threads.getNamedDemonThreadFactory("IPC Handler Thread - "));
  private FileSystem fs;
  private Path rootDir;
  private HServerInfo serverinfo;
  private AtomicInteger shouldRun = new AtomicInteger(1);

  private int totalHLogCnt;
  private static int walEditSize = 500;
  private static AtomicInteger totalEditCount = new AtomicInteger(0);
  private static volatile int totalExpectedEditCount = 1000000;
  private static volatile String benchmarkId = null;

  private static final AtomicLong totalTime = new AtomicLong(0);

  private final int DEFAULT_NUM_HLOGS = 100;
  private final static byte[] cfname = Bytes.toBytes("cf");
  private final static byte[] tname = Bytes.toBytes("tbl");

  @Override
  public void initialize() {
    try {
      this.conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT,
          HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT);
      initBenchmarks();
    } catch (IOException e) {
      LOG.warn("Failed the benchmark", e);
    }
  }

  @Override
  public Options getOptions(Options currentOptions) {
    currentOptions = super.getOptions(currentOptions);
    OptionBuilder.withArgName("numhlogs");
    OptionBuilder.hasArg();
    OptionBuilder.withDescription(
        "Gets the number of HLogs that need to be written to at a time");
    currentOptions.addOption(OptionBuilder.create("nhl"));

    OptionBuilder.withArgName("editsize");
    OptionBuilder.hasArg();
    OptionBuilder.withDescription(
        "The size of the wal edit");
    currentOptions.addOption(OptionBuilder.create("esize"));

    OptionBuilder.withArgName("totalNumOps");
    OptionBuilder.hasArg();
    OptionBuilder.withDescription(
        "Total number of wal operations");
    currentOptions.addOption(OptionBuilder.create("numops"));

    OptionBuilder.withArgName("benchmarkId");
    OptionBuilder.hasArg();
    OptionBuilder.withDescription(
        "Total number of wal operations");
    currentOptions.addOption(OptionBuilder.create("id"));

    return currentOptions;
  }

  @Override
  public CommandLine parseArgs(String args[]) throws ParseException {
    CommandLine cmd = super.parseArgs(args);
    this.totalHLogCnt = DEFAULT_NUM_HLOGS;
    if (cmd.hasOption("nhl")) {
      this.totalHLogCnt = Integer.parseInt(cmd.getOptionValue("nhl"));
    }
    if (cmd.hasOption("esize")) {
      walEditSize = Integer.parseInt(cmd.getOptionValue("esize"));
    }
    if (cmd.hasOption("numops")) {
      totalExpectedEditCount = Integer.parseInt(cmd.getOptionValue("numops"));
    }
    if (cmd.hasOption("id")) {
      benchmarkId = cmd.getOptionValue("id");
    }
    return cmd;
  }

  public void initBenchmarks() throws IOException {
    this.conf.set("fs.default.name", this.conf.get("hbase.rootdir"));
    conf.setBoolean(HConstants.HLOG_FORMAT_BACKWARD_COMPATIBILITY, false);

    this.fs = FileSystem.get(this.conf);
    if (benchmarkId == null) {
      benchmarkId = "" + (new Random()).nextInt(Integer.MAX_VALUE);
    }
    this.rootDir = new Path("/tmp/Benchmarks/HLogBenchmarks/"
        + benchmarkId + "/");
    fs.mkdirs(rootDir);
    LOG.debug("HLog Benchmarks initialized with root directory as: " + rootDir);
    String currentHostName = InetAddress.getLocalHost().getHostName();
    serverinfo = new HServerInfo(
        new HServerAddress(currentHostName + ":" +
            + HConstants.DEFAULT_REGIONSERVER_SWIFT_PORT),
        currentHostName);
    Path logdir = new Path(rootDir, HLog.getHLogDirectoryName(serverinfo));
    if (LOG.isDebugEnabled()) {
      LOG.debug("HLog dir " + logdir);
    }
    if (!fs.exists(logdir)) {
      fs.mkdirs(logdir);
    } else {
      throw new RegionServerRunningException("region server already " +
          "running at " + serverinfo.getServerName() +
          " because logdir " + logdir.toString() + " exists");
    }
    // Check the old log directory
    final Path oldLogDir = new Path(rootDir, HConstants.HREGION_OLDLOGDIR_NAME);
    if (!fs.exists(oldLogDir)) {
      fs.mkdirs(oldLogDir);
    }

    for (int i = 0; i < totalHLogCnt; i++) {
      this.hlogrollers.add(
          new LogRoller(new SimpleNoOpRegionServer(conf, hlogpool), i));
    }

    for (int i = 0; i < totalHLogCnt; i++) {
      hlogpool.add(new HLog(fs, logdir, oldLogDir, conf, hlogrollers.get(i),
          null, (serverinfo.getServerAddress().toString()), i, totalHLogCnt));
    }
    LOG.info("Initialized " + totalHLogCnt + " HLogs");
    for (HLog log : hlogpool) {
      ipcHandlerThreads.execute(
          new HLogWriter(log, shouldRun));
    }
    LOG.info("Started HLogWriters");
  }

  @Override
  public void runBenchmark() throws Throwable {
    LOG.debug("Running hlog writers");
    while (HLogBenchmarks.totalEditCount.get() < totalExpectedEditCount) {
      Thread.sleep(100);
    }
    shouldRun.set(0);
    this.ipcHandlerThreads.shutdown();
    this.ipcHandlerThreads.awaitTermination(1, TimeUnit.MINUTES);
    for (HLog log : hlogpool) {
      log.close();
    }
    System.out.println("Total time : " + totalTime.get());
    System.out.println("Total sync time : " + HLog.getGSyncTime().total);
    System.out.println("Total write size : " + HLog.getWriteSize().total);
  }

  public static class HLogWriter implements Runnable {
    private HLog wal;
    private AtomicInteger shouldRun;
    private KeyValueGenerator kvgenerator;

    public HLogWriter(HLog wal, AtomicInteger shouldRun) {
      this.wal = wal;
      this.shouldRun = shouldRun;
      kvgenerator = new KeyValueGenerator();
    }

    @Override
    public void run() {
      long startTime = System.nanoTime();
      LOG.debug("Started running " + Thread.currentThread().getName());

      HRegionInfo info = new HRegionInfo(
          new HTableDescriptor(tname), new byte[0], new byte[0]);

      long computeStartTime = System.nanoTime();
      long totalSyncTime = 0;
      long totalAppendTime = 0;
      Random r = new Random();
      int numkvs = r.nextInt(walEditSize);
      WALEdit edit = new WALEdit();
      for (int i = 0; i < numkvs; i++) {
        KeyValue kv = kvgenerator.getKeyValue(cfname);
        edit.add(kv);
      }
      while (shouldRun.get() > 0) {
        try {
          wal.append(info, tname, edit, System.currentTimeMillis());
          totalEditCount.incrementAndGet();
        } catch (IOException e) {
          LOG.error("Failed appending the edits", e);
        }
        // 1 in 20 times, do a flush.
        if (r.nextInt(20) == 0) {
          wal.completeCacheFlush(info.getRegionName(),
              tname, wal.startCacheFlush(info.getRegionName()), false);
          LOG.debug("Completed " + totalEditCount.get());
        }
      }

      long computeEndTime = System.nanoTime();
      LOG.debug("Quitting thread. Ttotal : " +
          (computeEndTime - computeStartTime) + " Tsync : " + + totalSyncTime
          + " Tcompute : " + ((computeEndTime - computeStartTime)
              - totalSyncTime) + " Tappend :" + totalAppendTime);

      long endTime = System.nanoTime();
      totalTime.addAndGet(endTime - startTime);
    }
  }

  public static class SimpleNoOpRegionServer extends HRegionServer {
    private final List<HLog> hlogs;

    public SimpleNoOpRegionServer(Configuration conf, List<HLog> hlogpool)
        throws IOException {
      this.hlogs = hlogpool;
    }

    @Override
    public boolean isStopRequestedAtStageTwo() {
      return false;
    }

    public int threadWakeFrequency = 100;

    @Override
    public HLog getLog(int logindex) {
      return hlogs.get(logindex);
    }

    @Override
    public void checkFileSystem() {
      // Do nothing
    }

    @Override
    public void forceAbort() {
      // Do nothing
    }

    @Override
    public HRegion getOnlineRegion(byte[] region) {
      return null;
    }

    @Override
    public FlushRequester getFlushRequester() {
      return null;
    }
  }

  public static void main(String[] args) throws Throwable {
    String className =
      Thread.currentThread().getStackTrace()[1].getClassName();
    System.out.println("Running benchmark " + className);
    @SuppressWarnings("unchecked")
    Class<? extends Benchmark> benchmarkClass =
      (Class<? extends Benchmark>)Class.forName(className);
    Benchmark.benchmarkRunner(benchmarkClass, args);
  }
}
