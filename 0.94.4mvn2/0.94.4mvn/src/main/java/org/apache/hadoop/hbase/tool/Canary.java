/**
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

package org.apache.hadoop.hbase.tool;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableNotFoundException;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HBaseAdmin;

/**
 * HBase Canary Tool, that that can be used to do
 * "canary monitoring" of a running HBase cluster.
 *
 * Foreach region tries to get one row per column family
 * and outputs some information about failure or latency.
 */
public final class Canary implements Tool {
  // Sink interface used by the canary to outputs information
  public interface Sink {
    public void publishReadFailure(HRegionInfo region);
    public void publishReadFailure(HRegionInfo region, HColumnDescriptor column);
    public void publishReadTiming(HRegionInfo region, HColumnDescriptor column, long msTime);
  }

  // Simple implementation of canary sink that allows to plot on
  // file or standard output timings or failures.
  public static class StdOutSink implements Sink {
    @Override
    public void publishReadFailure(HRegionInfo region) {
      LOG.error(String.format("read from region %s failed", region.getRegionNameAsString()));
    }

    @Override
    public void publishReadFailure(HRegionInfo region, HColumnDescriptor column) {
      LOG.error(String.format("read from region %s column family %s failed",
                region.getRegionNameAsString(), column.getNameAsString()));
    }

    @Override
    public void publishReadTiming(HRegionInfo region, HColumnDescriptor column, long msTime) {
      LOG.info(String.format("read from region %s column family %s in %dms",
               region.getRegionNameAsString(), column.getNameAsString(), msTime));
    }
  }

  private static final long DEFAULT_INTERVAL = 6000;

  private static final Log LOG = LogFactory.getLog(Canary.class);

  private Configuration conf = null;
  private HBaseAdmin admin = null;
  private long interval = 0;
  private Sink sink = null;

  public Canary() {
    this(new StdOutSink());
  }

  public Canary(Sink sink) {
    this.sink = sink;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public int run(String[] args) throws Exception {
    int tables_index = -1;

    // Process command line args
    for (int i = 0; i < args.length; i++) {
      String cmd = args[i];

      if (cmd.startsWith("-")) {
        if (tables_index >= 0) {
          // command line args must be in the form: [opts] [table 1 [table 2 ...]]
          System.err.println("Invalid command line options");
          printUsageAndExit();
        }

        if (cmd.equals("-help")) {
          // user asked for help, print the help and quit.
          printUsageAndExit();
        } else if (cmd.equals("-daemon") && interval == 0) {
          // user asked for daemon mode, set a default interval between checks
          interval = DEFAULT_INTERVAL;
        } else if (cmd.equals("-interval")) {
          // user has specified an interval for canary breaths (-interval N)
          i++;

          if (i == args.length) {
            System.err.println("-interval needs a numeric value argument.");
            printUsageAndExit();
          }

          try {
            interval = Long.parseLong(args[i]) * 1000;
          } catch (NumberFormatException e) {
            System.err.println("-interval needs a numeric value argument.");
            printUsageAndExit();
          }
        } else {
          // no options match
          System.err.println(cmd + " options is invalid.");
          printUsageAndExit();
        }
      } else if (tables_index < 0) {
        // keep track of first table name specified by the user
        tables_index = i;
      }
    }

    // initialize HBase conf and admin
    if (conf == null) conf = HBaseConfiguration.create();
    admin = new HBaseAdmin(conf);

    // lets the canary monitor the cluster
    do {
      if (admin.isAborted()) {
        LOG.error("HBaseAdmin aborted");
        return(1);
      }

      if (tables_index >= 0) {
        for (int i = tables_index; i < args.length; i++) {
          sniff(args[i]);
        }
      } else {
        sniff();
      }

      Thread.sleep(interval);
    } while (interval > 0);

    return(0);
  }

  private void printUsageAndExit() {
    System.err.printf("Usage: bin/hbase %s [opts] [table 1 [table 2...]]\n", getClass().getName());
    System.err.println(" where [opts] are:");
    System.err.println("   -help          Show this help and exit.");
    System.err.println("   -daemon        Continuous check at defined intervals.");
    System.err.println("   -interval <N>  Interval between checks (sec)");
    System.exit(1);
  }

  /*
   * canary entry point to monitor all the tables.
   */
  private void sniff() throws Exception {
    for (HTableDescriptor table : admin.listTables()) {
      sniff(table);
    }
  }

  /*
   * canary entry point to monitor specified table.
   */
  private void sniff(String tableName) throws Exception {
    if (admin.isTableAvailable(tableName)) {
      sniff(admin.getTableDescriptor(tableName.getBytes()));
    } else {
      LOG.warn(String.format("Table %s is not available", tableName));
    }
  }

  /*
   * Loops over regions that owns this table,
   * and output some information abouts the state.
   */
  private void sniff(HTableDescriptor tableDesc) throws Exception {
    HTable table = null;

    try {
      table = new HTable(admin.getConfiguration(), tableDesc.getName());
    } catch (TableNotFoundException e) {
      return;
    }

    for (HRegionInfo region : admin.getTableRegions(tableDesc.getName())) {
      try {
        sniffRegion(region, table);
      } catch (Exception e) {
        sink.publishReadFailure(region);
      }
    }
  }

  /*
   * For each column family of the region tries to get one row
   * and outputs the latency, or the failure.
   */
  private void sniffRegion(HRegionInfo region, HTable table) throws Exception {
    HTableDescriptor tableDesc = table.getTableDescriptor();
    for (HColumnDescriptor column : tableDesc.getColumnFamilies()) {
      Get get = new Get(region.getStartKey());
      get.addFamily(column.getName());

      try {
        long startTime = System.currentTimeMillis();
        table.get(get);
        long time = System.currentTimeMillis() - startTime;

        sink.publishReadTiming(region, column, time);
      } catch (Exception e) {
        sink.publishReadFailure(region, column);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Canary(), args);
    System.exit(exitCode);
  }
}

