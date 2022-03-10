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
package org.apache.hadoop.hbase.procedure2.store;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import org.apache.hadoop.hbase.util.AbstractHBaseTool;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.hbase.thirdparty.org.apache.commons.cli.Option;

/**
 * Base class for testing procedure store performance.
 */
public abstract class ProcedureStorePerformanceEvaluation<T extends ProcedureStore>
  extends AbstractHBaseTool {

  // Command line options and defaults.
  public static String DEFAULT_OUTPUT_PATH = "proc-store";

  public static Option OUTPUT_PATH_OPTION =
    new Option("output", true, "The output path. Default: " + DEFAULT_OUTPUT_PATH);

  public static int DEFAULT_NUM_THREADS = 20;

  public static Option NUM_THREADS_OPTION = new Option("threads", true,
    "Number of parallel threads which will write insert/updates/deletes to store. Default: " +
      DEFAULT_NUM_THREADS);

  public static int DEFAULT_NUM_PROCS = 1000000; // 1M

  public static Option NUM_PROCS_OPTION = new Option("procs", true,
    "Total number of procedures. Each procedure writes one insert and one update. Default: " +
      DEFAULT_NUM_PROCS);

  public static int DEFAULT_STATE_SIZE = 1024; // 1KB

  public static Option STATE_SIZE_OPTION = new Option("state_size", true,
    "Size of serialized state in bytes to write on update. Default: " + DEFAULT_STATE_SIZE +
      "bytes");

  public static Option SYNC_OPTION = new Option("sync", true,
    "Type of sync to use when writing WAL contents to file system. Accepted values: hflush, " +
      "hsync, nosync. Default: hflush");

  public static String DEFAULT_SYNC_OPTION = "hflush";

  protected String outputPath;
  protected int numThreads;
  protected long numProcs;
  protected String syncType;
  protected int stateSize;
  protected static byte[] SERIALIZED_STATE;

  protected T store;

  /** Used by {@link Worker}. */
  private AtomicLong procIds = new AtomicLong(0);
  private AtomicBoolean workersFailed = new AtomicBoolean(false);

  // Timeout for worker threads.
  private static final int WORKER_THREADS_TIMEOUT_SEC = 600; // in seconds

  @Override
  protected void addOptions() {
    addOption(OUTPUT_PATH_OPTION);
    addOption(NUM_THREADS_OPTION);
    addOption(NUM_PROCS_OPTION);
    addOption(SYNC_OPTION);
    addOption(STATE_SIZE_OPTION);
  }

  @Override
  protected void processOptions(CommandLine cmd) {
    outputPath = cmd.getOptionValue(OUTPUT_PATH_OPTION.getOpt(), DEFAULT_OUTPUT_PATH);
    numThreads = getOptionAsInt(cmd, NUM_THREADS_OPTION.getOpt(), DEFAULT_NUM_THREADS);
    numProcs = getOptionAsInt(cmd, NUM_PROCS_OPTION.getOpt(), DEFAULT_NUM_PROCS);
    syncType = cmd.getOptionValue(SYNC_OPTION.getOpt(), DEFAULT_SYNC_OPTION);
    assert "hsync".equals(syncType) || "hflush".equals(syncType) || "nosync".equals(
      syncType) : "sync argument can only accept one of these three values: hsync, hflush, nosync";
    stateSize = getOptionAsInt(cmd, STATE_SIZE_OPTION.getOpt(), DEFAULT_STATE_SIZE);
    SERIALIZED_STATE = new byte[stateSize];
    Bytes.random(SERIALIZED_STATE);
  }

  private void setUpProcedureStore() throws IOException {
    FileSystem fs = FileSystem.get(conf);
    Path storeDir = fs.makeQualified(new Path(outputPath));
    System.out.println("Procedure store directory : " + storeDir.toString());
    fs.delete(storeDir, true);
    store = createProcedureStore(storeDir);
    store.start(numThreads);
    store.recoverLease();
    store.load(new ProcedureTestingUtility.LoadCounter());
    System.out.println("Starting new procedure store: " + store.getClass().getSimpleName());
  }

  protected abstract T createProcedureStore(Path storeDir) throws IOException;

  protected void postStop(T store) throws IOException {
  }

  private void tearDownProcedureStore() {
    Path storeDir = null;
    try {
      if (store != null) {
        store.stop(false);
        postStop(store);
      }
      FileSystem fs = FileSystem.get(conf);
      storeDir = fs.makeQualified(new Path(outputPath));
      fs.delete(storeDir, true);
    } catch (IOException e) {
      System.err.println("Error: Couldn't delete log dir. You can delete it manually to free up " +
        "disk space. Location: " + storeDir);
      e.printStackTrace();
    }
  }

  protected abstract void printRawFormatResult(long timeTakenNs);

  @Override
  protected int doWork() throws Exception {
    try {
      setUpProcedureStore();
      ExecutorService executor = Executors.newFixedThreadPool(numThreads);
      Future<?>[] futures = new Future<?>[numThreads];
      // Start worker threads.
      long start = System.nanoTime();
      for (int i = 0; i < numThreads; i++) {
        futures[i] = executor.submit(new Worker(start));
      }
      boolean failure = false;
      try {
        for (Future<?> future : futures) {
          long timeout = start + WORKER_THREADS_TIMEOUT_SEC * 1000 -
            EnvironmentEdgeManager.currentTime();
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
      long timeTakenNs = System.nanoTime() - start;
      System.out.println("******************************************");
      System.out.println("Num threads    : " + numThreads);
      System.out.println("Num procedures : " + numProcs);
      System.out.println("Sync type      : " + syncType);
      System.out.println("Time taken     : " + TimeUnit.NANOSECONDS.toSeconds(timeTakenNs) + "sec");
      System.out.println("******************************************");
      System.out.println("Raw format for scripts");
      printRawFormatResult(timeTakenNs);
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
   * Callable to generate load for wal by inserting/deleting/updating procedures. If procedure store
   * fails to roll log file (throws IOException), all threads quit, and at least one returns value
   * of {@link AbstractHBaseTool#EXIT_FAILURE}.
   */
  private final class Worker implements Callable<Integer> {
    private final long start;

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
          long ns = System.nanoTime() - start;
          System.out.println("Wrote " + procId + " procedures in " +
            StringUtils.humanTimeDiff(TimeUnit.NANOSECONDS.toMillis(ns)));
        }
        try {
          preWrite(procId);
        } catch (IOException ioe) {
          // Ask other threads to quit too.
          workersFailed.set(true);
          System.err.println("Exception when rolling log file. Current procId = " + procId);
          ioe.printStackTrace();
          return EXIT_FAILURE;
        }
        ProcedureTestingUtility.TestProcedure proc =
          new ProcedureTestingUtility.TestProcedure(procId);
        proc.setData(SERIALIZED_STATE);
        store.insert(proc, null);
        store.update(proc);
      }
      return EXIT_SUCCESS;
    }
  }

  protected abstract void preWrite(long procId) throws IOException;
}
