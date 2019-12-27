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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.procedure2.store.LeaseRecovery;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStorePerformanceEvaluation;

import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.hbase.thirdparty.org.apache.commons.cli.Option;

public class ProcedureWALPerformanceEvaluation
  extends ProcedureStorePerformanceEvaluation<WALProcedureStore> {

  // Command line options and defaults.
  public static int DEFAULT_NUM_WALS = 0;
  public static Option NUM_WALS_OPTION = new Option("wals", true,
    "Number of WALs to write. If -ve or 0, uses " + WALProcedureStore.ROLL_THRESHOLD_CONF_KEY +
      " conf to roll the logs. Default: " + DEFAULT_NUM_WALS);

  private long numProcsPerWal = Long.MAX_VALUE; // never roll wall based on this value.
  private int numWals;

  // Non-default configurations.
  private void setupConf() {
    conf.setBoolean(WALProcedureStore.USE_HSYNC_CONF_KEY, "hsync".equals(syncType));
    if (numWals > 0) {
      conf.setLong(WALProcedureStore.ROLL_THRESHOLD_CONF_KEY, Long.MAX_VALUE);
      numProcsPerWal = numProcs / numWals;
    }
  }

  /**
   * Processes and validates command line options.
   */
  @Override
  public void processOptions(CommandLine cmd) {
    super.processOptions(cmd);
    numWals = getOptionAsInt(cmd, NUM_WALS_OPTION.getOpt(), DEFAULT_NUM_WALS);
    setupConf();
  }

  @Override
  public void addOptions() {
    super.addOptions();
    addOption(NUM_WALS_OPTION);
  }

  @Override
  protected WALProcedureStore createProcedureStore(Path storeDir) throws IOException {
    if ("nosync".equals(syncType)) {
      return new NoSyncWalProcedureStore(conf, storeDir);
    } else {
      return ProcedureTestingUtility.createWalStore(conf, storeDir);
    }
  }

  @Override
  protected void printRawFormatResult(long timeTakenNs) {
    System.out
      .println(String.format("RESULT [%s=%s, %s=%s, %s=%s, %s=%s, %s=%s, " + "total_time_ms=%s]",
        NUM_PROCS_OPTION.getOpt(), numProcs, STATE_SIZE_OPTION.getOpt(), stateSize,
        SYNC_OPTION.getOpt(), syncType, NUM_THREADS_OPTION.getOpt(), numThreads,
        NUM_WALS_OPTION.getOpt(), numWals, timeTakenNs));
  }

  @Override
  protected void preWrite(long procId) throws IOException {
    if (procId > 0 && procId % numProcsPerWal == 0) {
      store.rollWriterForTesting();
      System.out.println(
        "Starting new log : " + store.getActiveLogs().get(store.getActiveLogs().size() - 1));
    }
  }
  ///////////////////////////////
  // HELPER CLASSES
  ///////////////////////////////

  private static class NoSyncWalProcedureStore extends WALProcedureStore {
    public NoSyncWalProcedureStore(final Configuration conf, final Path logDir) throws IOException {
      super(conf, logDir, null, new LeaseRecovery() {
        @Override
        public void recoverFileLease(FileSystem fs, Path path) throws IOException {
          // no-op
        }
      });
    }

    @Override
    protected void syncStream(FSDataOutputStream stream) {
      // no-op
    }
  }

  public static void main(String[] args) throws IOException {
    ProcedureWALPerformanceEvaluation tool = new ProcedureWALPerformanceEvaluation();
    tool.setConf(HBaseConfiguration.create());
    tool.run(args);
  }
}
