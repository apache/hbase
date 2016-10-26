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

package org.apache.hadoop.hbase.procedure2.store.wal;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.procedure2.util.*;

import org.apache.hadoop.hbase.util.AbstractHBaseTool;

public class ProcedureWALPerformanceEvaluation extends AbstractHBaseTool {
  protected static final HBaseCommonTestingUtility UTIL = new HBaseCommonTestingUtility();

  // Command line options and defaults.
  public static int DEFAULT_NUM_THREADS = 20;
  public static Option NUM_THREADS_OPTION = new Option("threads", true,
      "Number of parallel threads which will write insert/updates/deletes to WAL. Default: "
      + DEFAULT_NUM_THREADS);
  public static int DEFAULT_NUM_PROCS = 1000000;  // 1M
  public static Option NUM_PROCS_OPTION = new Option("procs", true,
      "Total number of procedures. Each procedure writes one insert and one update. Default: "
      + DEFAULT_NUM_PROCS);
  public static int DEFAULT_NUM_WALS = 0;
  public static Option NUM_WALS_OPTION = new Option("wals", true,
      "Number of WALs to write. If -ve or 0, uses " + WALProcedureStore.ROLL_THRESHOLD_CONF_KEY +
          " conf to roll the logs. Default: " + DEFAULT_NUM_WALS);
  public static int DEFAULT_STATE_SIZE = 1024;  // 1KB
  public static Option STATE_SIZE_OPTION = new Option("state_size", true,
      "Size of serialized state in bytes to write on update. Default: " + DEFAULT_STATE_SIZE
          + "bytes");
  public static Option SYNC_OPTION = new Option("sync", true,
      "Type of sync to use when writing WAL contents to file system. Accepted values: hflush, "
          + "hsync, nosync. Default: hflush");
  public static String DEFAULT_SYNC_OPTION = "hflush";

  public int numThreads;
  public long numProcs;
  public long numProcsPerWal = Long.MAX_VALUE;  // never roll wall based on this value.
  public int numWals;
  public String syncType;
  public int stateSize;
  static byte[] serializedState;
  private WALProcedureStore store;

  /** Used by {@link Worker}. */
  private AtomicLong procIds = new AtomicLong(0);
  private AtomicBoolean workersFailed = new AtomicBoolean(false);
  // Timeout for worker threads.
  private static final int WORKER_THREADS_TIMEOUT_SEC = 600;  // in seconds

  // Non-default configurations.
  private void setupConf() {
    conf.setBoolean(WALProcedureStore.USE_HSYNC_CONF_KEY, "hsync".equals(syncType));
    if (numWals > 0) {
      conf.setLong(WALProcedureStore.ROLL_THRESHOLD_CONF_KEY, Long.MAX_VALUE);
      numProcsPerWal = numProcs / numWals;
    }
  }

  private void setupProcedureStore() throws IOException {
    Path testDir = UTIL.getDataTestDir();
    FileSystem fs = testDir.getFileSystem(conf);
    Path logDir = new Path(testDir, "proc-logs");
    System.out.println("Logs directory : " + logDir.toString());
    fs.delete(logDir, true);
    if ("nosync".equals(syncType)) {
      store = new NoSyncWalProcedureStore(conf, fs, logDir);
    } else {
      store = ProcedureTestingUtility.createWalStore(conf, fs, logDir);
    }
    store.start(numThreads);
    store.recoverLease();
    store.load(new ProcedureTestingUtility.LoadCounter());
    System.out.println("Starting new log : "
        + store.getActiveLogs().get(store.getActiveLogs().size() - 1));
  }

  private void tearDownProcedureStore() {
    store.stop(false);
    try {
      store.getFileSystem().delete(store.getLogDir(), true);
    } catch (IOException e) {
      System.err.println("Error: Couldn't delete log dir. You can delete it manually to free up "
          + "disk space. Location: " + store.getLogDir().toString());
      e.printStackTrace();
    }
  }

  /**
   * Processes and validates command line options.
   */
  @Override
  public void processOptions(CommandLine cmd) {
    numThreads = getOptionAsInt(cmd, NUM_THREADS_OPTION.getOpt(), DEFAULT_NUM_THREADS);
    numProcs = getOptionAsInt(cmd, NUM_PROCS_OPTION.getOpt(), DEFAULT_NUM_PROCS);
    numWals = getOptionAsInt(cmd, NUM_WALS_OPTION.getOpt(), DEFAULT_NUM_WALS);
    syncType = cmd.getOptionValue(SYNC_OPTION.getOpt(), DEFAULT_SYNC_OPTION);
    assert "hsync".equals(syncType) || "hflush".equals(syncType) || "nosync".equals(syncType):
        "sync argument can only accept one of these three values: hsync, hflush, nosync";
    stateSize = getOptionAsInt(cmd, STATE_SIZE_OPTION.getOpt(), DEFAULT_STATE_SIZE);
    serializedState = new byte[stateSize];
    setupConf();
  }

  @Override
  public void addOptions() {
    addOption(NUM_THREADS_OPTION);
    addOption(NUM_PROCS_OPTION);
    addOption(NUM_WALS_OPTION);
    addOption(SYNC_OPTION);
    addOption(STATE_SIZE_OPTION);
  }

  @Override
  public int doWork() {
    try {
      setupProcedureStore();
      ExecutorService executor = Executors.newFixedThreadPool(numThreads);
      Future<?>[] futures = new Future<?>[numThreads];
      // Start worker threads.
      long start = System.currentTimeMillis();
      for (int i = 0; i < numThreads; i++) {
        futures[i] = executor.submit(this.new Worker(start));
      }
      boolean failure = false;
      try {
        for (Future<?> future : futures) {
          long timeout = start + WORKER_THREADS_TIMEOUT_SEC * 1000 - System.currentTimeMillis();
          failure |= (future.get(timeout, TimeUnit.MILLISECONDS).equals(EXIT_FAILURE));
        }
      } catch (Exception e) {
        System.err.println("Exception in worker thread.");
        e.printStackTrace();
        return EXIT_FAILURE;
      }
      executor.shutdown();
      if (failure) {
        return EXIT_FAILURE;
      }
      long timeTaken = System.currentTimeMillis() - start;
      System.out.println("******************************************");
      System.out.println("Num threads    : " + numThreads);
      System.out.println("Num procedures : " + numProcs);
      System.out.println("Sync type      : " + syncType);
      System.out.println("Time taken     : " + (timeTaken / 1000.0f) + "sec");
      System.out.println("******************************************");
      System.out.println("Raw format for scripts");
      System.out.println(String.format("RESULT [%s=%s, %s=%s, %s=%s, %s=%s, %s=%s, "
              + "total_time_ms=%s]",
          NUM_PROCS_OPTION.getOpt(), numProcs, STATE_SIZE_OPTION.getOpt(), stateSize,
          SYNC_OPTION.getOpt(), syncType, NUM_THREADS_OPTION.getOpt(), numThreads,
          NUM_WALS_OPTION.getOpt(), numWals, timeTaken));
      return EXIT_SUCCESS;
    } catch (IOException e) {
      e.printStackTrace();
      return EXIT_FAILURE;
    } finally {
      tearDownProcedureStore();
    }
  }

  ///////////////////////////////
  // HELPER CLASSES
  ///////////////////////////////

  /**
   * Callable to generate load for wal by inserting/deleting/updating procedures.
   * If procedure store fails to roll log file (throws IOException), all threads quit, and at
   * least one returns value of {@link AbstractHBaseTool#EXIT_FAILURE}.
   */
  class Worker implements Callable<Integer> {
    final long start;

    public Worker(long start) {
      this.start = start;
    }

    // TODO: Can also collect #procs, time taken by each thread to measure fairness.
    @Override
    public Integer call() throws IOException {
      while (true) {
        if (workersFailed.get()) {
          return EXIT_FAILURE;
        }
        long procId = procIds.getAndIncrement();
        if (procId >= numProcs) {
          break;
        }
        if (procId != 0 && procId % 10000 == 0) {
          long ms = System.currentTimeMillis() - start;
          System.out.println("Wrote " + procId + " procedures in "
              + StringUtils.humanTimeDiff(ms));
        }
        try{
          if (procId > 0 && procId % numProcsPerWal == 0) {
            store.rollWriterForTesting();
            System.out.println("Starting new log : "
                + store.getActiveLogs().get(store.getActiveLogs().size() - 1));
          }
        } catch (IOException ioe) {
          // Ask other threads to quit too.
          workersFailed.set(true);
          System.err.println("Exception when rolling log file. Current procId = " + procId);
          ioe.printStackTrace();
          return EXIT_FAILURE;
        }
        ProcedureTestingUtility.TestProcedure proc =
            new ProcedureTestingUtility.TestProcedure(procId);
        proc.setData(serializedState);
        store.insert(proc, null);
        store.update(proc);
      }
      return EXIT_SUCCESS;
    }
  }

  public class NoSyncWalProcedureStore extends WALProcedureStore {
    public NoSyncWalProcedureStore(final Configuration conf, final FileSystem fs,
        final Path logDir) {
      super(conf, fs, logDir, new WALProcedureStore.LeaseRecovery() {
        @Override
        public void recoverFileLease(FileSystem fs, Path path) throws IOException {
          // no-op
        }
      });
    }

    @Override
    protected long syncSlots(FSDataOutputStream stream, ByteSlot[] slots, int offset, int count)
        throws IOException {
      long totalSynced = 0;
      for (int i = 0; i < count; ++i) {
        totalSynced += slots[offset + i].size();
      }
      return totalSynced;
    }
  }

  public static void main(String[] args) throws IOException {
    ProcedureWALPerformanceEvaluation tool = new ProcedureWALPerformanceEvaluation();
    tool.setConf(UTIL.getConfiguration());
    tool.run(args);
  }
}