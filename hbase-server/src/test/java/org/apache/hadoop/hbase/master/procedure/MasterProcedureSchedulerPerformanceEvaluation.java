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

package org.apache.hadoop.hbase.master.procedure;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.TableLockManager;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility.TestProcedure;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import org.apache.hadoop.hbase.util.AbstractHBaseTool;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Tool to test performance of locks and queues in procedure scheduler independently from other
 * framework components.
 * Inserts table and region operations in the scheduler, then polls them and exercises their locks
 * Number of tables, regions and operations can be set using cli args.
 */
public class MasterProcedureSchedulerPerformanceEvaluation extends AbstractHBaseTool {
  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  // Command line options and defaults.
  public static final int DEFAULT_NUM_TABLES = 5;
  public static final Option NUM_TABLES_OPTION = new Option("num_table", true,
      "Number of tables to use for table operations. Default: " + DEFAULT_NUM_TABLES);
  public static final int DEFAULT_REGIONS_PER_TABLE = 10;
  public static final Option REGIONS_PER_TABLE_OPTION = new Option("regions_per_table", true,
      "Total number of regions per table. Default: " + DEFAULT_REGIONS_PER_TABLE);
  public static final int DEFAULT_NUM_OPERATIONS = 10000000;  // 10M
  public static final Option NUM_OPERATIONS_OPTION = new Option("num_ops", true,
      "Total number of operations to schedule. Default: " + DEFAULT_NUM_OPERATIONS);
  public static final int DEFAULT_NUM_THREADS = 10;
  public static final Option NUM_THREADS_OPTION = new Option("threads", true,
      "Number of procedure executor threads. Default: " + DEFAULT_NUM_THREADS);
  public static final String DEFAULT_OPS_TYPE = "both";
  public static final Option OPS_TYPE_OPTION = new Option("ops_type", true,
      "Type of operations to run. Value can be table/region/both. In case of 'both', "
          + "proportion of table:region ops is 1:regions_per_table. Default: "
          + DEFAULT_OPS_TYPE);

  private int numTables;
  private int regionsPerTable;
  private int numOps;
  private int numThreads;
  private String ops_type;

  private MasterProcedureScheduler procedureScheduler;
  // List of table/region procedures to schedule.
  ProcedureFactory[] ops;

  // Using factory pattern to build a collection of operations which can be executed in an
  // abstract manner by worker threads.
  private interface ProcedureFactory {
    Procedure newProcedure(long procId);
  }

  private class RegionProcedure extends TestMasterProcedureScheduler.TestRegionProcedure {
    RegionProcedure(long procId, HRegionInfo hri) {
      super(procId, hri.getTable(), TableOperationType.UNASSIGN, hri);
    }

    public boolean acquireLock(Void env) {
      return !procedureScheduler.waitRegions(this, getTableName(), getRegionInfo());
    }

    public void releaseLock(Void env) {
      procedureScheduler.wakeRegions(this, getTableName(), getRegionInfo());
    }
  }

  private class RegionProcedureFactory implements ProcedureFactory {
    final HRegionInfo hri;

    RegionProcedureFactory(HRegionInfo hri) {
      this.hri = hri;
    }

    public Procedure newProcedure(long procId) {
      return new RegionProcedure(procId, hri);
    }
  }

  private class TableProcedure extends TestMasterProcedureScheduler.TestTableProcedure {

    TableProcedure(long procId, TableName tableName) {
      super(procId, tableName, TableOperationType.EDIT);
    }

    public boolean acquireLock(Void env) {
      return procedureScheduler.tryAcquireTableExclusiveLock(this, getTableName());
    }

    public void releaseLock(Void env) {
      procedureScheduler.releaseTableExclusiveLock(this, getTableName());
    }
  }

  private class TableProcedureFactory implements ProcedureFactory {
    final TableName tableName;

    TableProcedureFactory(TableName tableName) {
      this.tableName = tableName;
    }

    public Procedure newProcedure(long procId) {
      return new TableProcedure(procId, tableName);
    }
  }

  private void setupOperations() throws Exception {
    // Create set of operations based on --ops_type command line argument.
    final ProcedureFactory[] tableOps = new ProcedureFactory[numTables];
    for (int i = 0; i < numTables; ++i) {
      tableOps[i] = new TableProcedureFactory(TableName.valueOf("testTableLock-" + i));
    }

    final ProcedureFactory[] regionOps = new ProcedureFactory[numTables * regionsPerTable];
    for (int i = 0; i < numTables; ++i) {
      for (int j = 0; j < regionsPerTable; ++j) {
        regionOps[i * regionsPerTable + j] = new RegionProcedureFactory(
            new HRegionInfo(((TableProcedureFactory)tableOps[i]).tableName, Bytes.toBytes(j),
                Bytes.toBytes(j + 1)));
      }
    }

    if (ops_type.equals("table")) {
      System.out.println("Operations: table only");
      ops = tableOps;
    } else if (ops_type.equals("region")) {
      System.out.println("Operations: region only");
      ops = regionOps;
    } else if (ops_type.equals("both")) {
      System.out.println("Operations: both (table + region)");
      ops = (ProcedureFactory[])ArrayUtils.addAll(tableOps, regionOps);
    } else {
      throw new Exception("-ops_type should be one of table/region/both.");
    }
  }

  @Override
  protected void addOptions() {
    addOption(NUM_TABLES_OPTION);
    addOption(REGIONS_PER_TABLE_OPTION);
    addOption(NUM_OPERATIONS_OPTION);
    addOption(NUM_THREADS_OPTION);
    addOption(OPS_TYPE_OPTION);
  }

  @Override
  protected void processOptions(CommandLine cmd) {
    numTables = getOptionAsInt(cmd, NUM_TABLES_OPTION.getOpt(), DEFAULT_NUM_TABLES);
    regionsPerTable = getOptionAsInt(cmd, REGIONS_PER_TABLE_OPTION.getOpt(),
        DEFAULT_REGIONS_PER_TABLE);
    numOps = getOptionAsInt(cmd, NUM_OPERATIONS_OPTION.getOpt(),
        DEFAULT_NUM_OPERATIONS);
    numThreads = getOptionAsInt(cmd, NUM_THREADS_OPTION.getOpt(), DEFAULT_NUM_THREADS);
    ops_type = cmd.getOptionValue(OPS_TYPE_OPTION.getOpt(), DEFAULT_OPS_TYPE);
  }

  /*******************
   * WORKERS
   *******************/

  private final AtomicLong procIds = new AtomicLong(0);
  private final AtomicLong yield = new AtomicLong(0);
  private final AtomicLong completed = new AtomicLong(0);

  private class AddProcsWorker extends Thread {
    public void run() {
      final Random rand = new Random(System.currentTimeMillis());
      long procId = procIds.incrementAndGet();
      int index;
      while (procId <= numOps) {
        index = rand.nextInt(ops.length);
        procedureScheduler.addBack(ops[index].newProcedure(procId));
        procId = procIds.incrementAndGet();
      }
    }
  }

  private class PollAndLockWorker extends Thread {
    public void run() {
      while (completed.get() < numOps) {
        // With lock/unlock being ~100ns, and no other workload, 1000ns wait seams reasonable.
        TestProcedure proc = (TestProcedure)procedureScheduler.poll(1000);
        if (proc == null) {
          yield.incrementAndGet();
          continue;
        }

        if (proc.acquireLock(null)) {
          completed.incrementAndGet();
          proc.releaseLock(null);
        } else {
          procedureScheduler.yield(proc);
        }
        if (completed.get() % 100000 == 0) {
          System.out.println("Completed " + completed.get() + " procedures.");
        }
      }
    }
  }

  /**
   * Starts the threads and waits for them to finish.
   * @return time taken by threads to complete, in milliseconds.
   */
  long runThreads(Thread[] threads) throws Exception {
    final long startTime = System.currentTimeMillis();
    for (Thread t : threads) {
      t.start();
    }
    for (Thread t : threads) {
      t.join();
    }
    return System.currentTimeMillis() - startTime;
  }

  @Override
  protected int doWork() throws Exception {
    procedureScheduler = new MasterProcedureScheduler(
        UTIL.getConfiguration(), new TableLockManager.NullTableLockManager());
    procedureScheduler.start();
    setupOperations();

    final Thread[] threads = new Thread[numThreads];
    for (int i = 0; i < numThreads; ++i) {
      threads[i] = new AddProcsWorker();
    }
    final long addBackTime = runThreads(threads);
    System.out.println("Added " + numOps + " procedures to scheduler.");

    for (int i = 0; i < numThreads; ++i) {
      threads[i] = new PollAndLockWorker();
    }
    final long pollTime = runThreads(threads);
    procedureScheduler.stop();

    final float pollTimeSec = pollTime / 1000.0f;
    final float addBackTimeSec = addBackTime / 1000.0f;
    System.out.println("******************************************");
    System.out.println("Time - addBack     : " + StringUtils.humanTimeDiff(addBackTime));
    System.out.println("Ops/sec - addBack  : " + StringUtils.humanSize(numOps / addBackTimeSec));
    System.out.println("Time - poll        : " + StringUtils.humanTimeDiff(pollTime));
    System.out.println("Ops/sec - poll     : " + StringUtils.humanSize(numOps / pollTimeSec));
    System.out.println("Num Operations     : " + numOps);
    System.out.println();
    System.out.println("Completed          : " + completed.get());
    System.out.println("Yield              : " + yield.get());
    System.out.println();
    System.out.println("Num Tables         : " + numTables);
    System.out.println("Regions per table  : " + regionsPerTable);
    System.out.println("Operations type    : " + ops_type);
    System.out.println("Threads            : " + numThreads);
    System.out.println("******************************************");
    return 0;
  }

  public static void main(String[] args) throws IOException {
    MasterProcedureSchedulerPerformanceEvaluation tool =
        new MasterProcedureSchedulerPerformanceEvaluation();
    tool.setConf(UTIL.getConfiguration());
    tool.run(args);
  }
}
