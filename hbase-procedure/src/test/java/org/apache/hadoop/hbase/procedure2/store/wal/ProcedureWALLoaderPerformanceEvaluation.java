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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility.TestProcedure;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore.ProcedureIterator;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import org.apache.hadoop.hbase.util.AbstractHBaseTool;

import static java.lang.System.currentTimeMillis;

public class ProcedureWALLoaderPerformanceEvaluation extends AbstractHBaseTool {
  protected static final HBaseCommonTestingUtility UTIL = new HBaseCommonTestingUtility();

  // Command line options and defaults.
  public static int DEFAULT_NUM_PROCS = 1000000;  // 1M
  public static Option NUM_PROCS_OPTION = new Option("procs", true,
      "Total number of procedures. Default: " + DEFAULT_NUM_PROCS);
  public static int DEFAULT_NUM_WALS = 0;
  public static Option NUM_WALS_OPTION = new Option("wals", true,
      "Number of WALs to write. If -ve or 0, uses " + WALProcedureStore.ROLL_THRESHOLD_CONF_KEY +
          " conf to roll the logs. Default: " + DEFAULT_NUM_WALS);
  public static int DEFAULT_STATE_SIZE = 1024;  // 1KB
  public static Option STATE_SIZE_OPTION = new Option("state_size", true,
      "Size of serialized state in bytes to write on update. Default: " + DEFAULT_STATE_SIZE
          + " bytes");
  public static int DEFAULT_UPDATES_PER_PROC = 5;
  public static Option UPDATES_PER_PROC_OPTION = new Option("updates_per_proc", true,
      "Number of update states to write for each proc. Default: " + DEFAULT_UPDATES_PER_PROC);
  public static double DEFAULT_DELETE_PROCS_FRACTION = 0.50;
  public static Option DELETE_PROCS_FRACTION_OPTION = new Option("delete_procs_fraction", true,
      "Fraction of procs for which to write delete state. Distribution of procs chosen for "
          + "delete is uniform across all procs. Default: " + DEFAULT_DELETE_PROCS_FRACTION);

  public int numProcs;
  public int updatesPerProc;
  public double deleteProcsFraction;
  public int numWals;
  private WALProcedureStore store;
  static byte[] serializedState;

  private class LoadCounter implements ProcedureStore.ProcedureLoader {
    public LoadCounter() {}

    @Override
    public void setMaxProcId(long maxProcId) {
    }

    @Override
    public void load(ProcedureIterator procIter) throws IOException {
      while (procIter.hasNext()) {
        if (procIter.isNextCompleted()) {
          ProcedureInfo proc = procIter.nextAsProcedureInfo();
        } else {
          Procedure proc = procIter.nextAsProcedure();
        }
      }
    }

    @Override
    public void handleCorrupted(ProcedureIterator procIter) throws IOException {
      while (procIter.hasNext()) {
        Procedure proc = procIter.nextAsProcedure();
      }
    }
  }

  @Override
  protected void addOptions() {
    addOption(NUM_PROCS_OPTION);
    addOption(UPDATES_PER_PROC_OPTION);
    addOption(DELETE_PROCS_FRACTION_OPTION);
    addOption(NUM_WALS_OPTION);
    addOption(STATE_SIZE_OPTION);
  }

  @Override
  protected void processOptions(CommandLine cmd) {
    numProcs = getOptionAsInt(cmd, NUM_PROCS_OPTION.getOpt(), DEFAULT_NUM_PROCS);
    numWals = getOptionAsInt(cmd, NUM_WALS_OPTION.getOpt(), DEFAULT_NUM_WALS);
    int stateSize = getOptionAsInt(cmd, STATE_SIZE_OPTION.getOpt(), DEFAULT_STATE_SIZE);
    serializedState = new byte[stateSize];
    updatesPerProc = getOptionAsInt(cmd, UPDATES_PER_PROC_OPTION.getOpt(),
        DEFAULT_UPDATES_PER_PROC);
    deleteProcsFraction = getOptionAsDouble(cmd, DELETE_PROCS_FRACTION_OPTION.getOpt(),
        DEFAULT_DELETE_PROCS_FRACTION);
    setupConf();
  }

  private void setupConf() {
    if (numWals > 0) {
      conf.setLong(WALProcedureStore.ROLL_THRESHOLD_CONF_KEY, Long.MAX_VALUE);
    }
  }

  public void setUpProcedureStore() throws IOException {
    Path testDir = UTIL.getDataTestDir();
    FileSystem fs = testDir.getFileSystem(conf);
    Path logDir = new Path(testDir, "proc-logs");
    System.out.println("\n\nLogs directory : " + logDir.toString() + "\n\n");
    fs.delete(logDir, true);
    store = ProcedureTestingUtility.createWalStore(conf, fs, logDir);
    store.start(1);
    store.recoverLease();
    store.load(new LoadCounter());
  }

  /**
   * @return a list of shuffled integers which represent state of proc id. First occurrence of a
   * number denotes insert state, consecutive occurrences denote update states, and -ve value
   * denotes delete state.
   */
  private List<Integer> shuffleProcWriteSequence() {
    Random rand = new Random();
    List<Integer> procStatesSequence = new ArrayList<>();
    Set<Integer> toBeDeletedProcs = new HashSet<>();
    // Add n + 1 entries of the proc id for insert + updates. If proc is chosen for delete, add
    // extra entry which is marked -ve in the loop after shuffle.
    for (int procId  = 1; procId <= numProcs; ++procId) {
      procStatesSequence.addAll(Collections.nCopies(updatesPerProc + 1, procId));
      if (rand.nextFloat() < deleteProcsFraction) {
        procStatesSequence.add(procId);
        toBeDeletedProcs.add(procId);
      }
    }
    Collections.shuffle(procStatesSequence);
    // Mark last occurrences of proc ids in toBeDeletedProcs with -ve to denote it's a delete state.
    for (int i = procStatesSequence.size() - 1; i >= 0; --i) {
      int procId = procStatesSequence.get(i);
      if (toBeDeletedProcs.contains(procId)) {
        procStatesSequence.set(i, -1 * procId);
        toBeDeletedProcs.remove(procId);
      }
    }
    return procStatesSequence;
  }

  private void writeWals() throws IOException {
    List<Integer> procStates = shuffleProcWriteSequence();
    TestProcedure[] procs = new TestProcedure[numProcs + 1];  // 0 is not used.
    int numProcsPerWal = numWals > 0 ? (int)Math.ceil(procStates.size() / numWals)
        : Integer.MAX_VALUE;
    long startTime = currentTimeMillis();
    long lastTime = startTime;
    for (int i = 0; i < procStates.size(); ++i) {
      int procId = procStates.get(i);
      if (procId < 0) {
        store.delete(procs[-procId].getProcId());
        procs[-procId] = null;
      } else if (procs[procId] == null) {
        procs[procId] = new TestProcedure(procId, 0);
        procs[procId].setData(serializedState);
        store.insert(procs[procId], null);
      } else {
        store.update(procs[procId]);
      }
      if (i > 0 && i % numProcsPerWal == 0) {
        long currentTime = currentTimeMillis();
        System.out.println("Forcing wall roll. Time taken on last WAL: " +
            (currentTime - lastTime) / 1000.0f + " sec");
        store.rollWriterForTesting();
        lastTime = currentTime;
      }
    }
    long timeTaken = currentTimeMillis() - startTime;
    System.out.println("\n\nDone writing WALs.\nNum procs : " + numProcs + "\nTotal time taken : "
        + StringUtils.humanTimeDiff(timeTaken) + "\n\n");
  }

  private void storeRestart(ProcedureStore.ProcedureLoader loader) throws IOException {
    System.out.println("Restarting procedure store to read back the WALs");
    store.stop(false);
    store.start(1);
    store.recoverLease();

    long startTime = currentTimeMillis();
    store.load(loader);
    long timeTaken = System.currentTimeMillis() - startTime;
    System.out.println("******************************************");
    System.out.println("Load time : " + (timeTaken / 1000.0f) + "sec");
    System.out.println("******************************************");
    System.out.println("Raw format for scripts");
        System.out.println(String.format("RESULT [%s=%s, %s=%s, %s=%s, %s=%s, %s=%s, "
                + "total_time_ms=%s]",
        NUM_PROCS_OPTION.getOpt(), numProcs, STATE_SIZE_OPTION.getOpt(), serializedState.length,
        UPDATES_PER_PROC_OPTION.getOpt(), updatesPerProc, DELETE_PROCS_FRACTION_OPTION.getOpt(),
        deleteProcsFraction, NUM_WALS_OPTION.getOpt(), numWals, timeTaken));
  }

  public void tearDownProcedureStore() {
    store.stop(false);
    try {
      store.getFileSystem().delete(store.getLogDir(), true);
    } catch (IOException e) {
      System.err.println("Error: Couldn't delete log dir. You can delete it manually to free up "
          + "disk space. Location: " + store.getLogDir().toString());
      System.err.println(e.toString());
    }
  }

  @Override
  protected int doWork() {
    try {
      setUpProcedureStore();
      writeWals();
      storeRestart(new LoadCounter());
      return EXIT_SUCCESS;
    } catch (IOException e) {
      e.printStackTrace();
      return EXIT_FAILURE;
    } finally {
      tearDownProcedureStore();
    }
  }

  public static void main(String[] args) throws IOException {
    ProcedureWALLoaderPerformanceEvaluation tool = new ProcedureWALLoaderPerformanceEvaluation();
    tool.setConf(UTIL.getConfiguration());
    tool.run(args);
  }
}