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

import static org.apache.hadoop.hbase.HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT;
import static org.apache.hadoop.hbase.HConstants.ZOOKEEPER_ZNODE_PARENT;

import com.google.common.collect.Lists;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.time.StopWatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.tool.Canary.RegionTask.TaskType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.hbase.zookeeper.EmptyWatcher;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.client.ConnectStringParser;
import org.apache.zookeeper.data.Stat;

/**
 * HBase Canary Tool, that that can be used to do
 * "canary monitoring" of a running HBase cluster.
 *
 * Here are three modes
 * 1. region mode - Foreach region tries to get one row per column family
 * and outputs some information about failure or latency.
 *
 * 2. regionserver mode - Foreach regionserver tries to get one row from one table
 * selected randomly and outputs some information about failure or latency.
 *
 * 3. zookeeper mode - for each zookeeper instance, selects a zNode and
 * outputs some information about failure or latency.
 */
public final class Canary implements Tool {
  // Sink interface used by the canary to outputs information
  public interface Sink {
    public long getReadFailureCount();
    public long incReadFailureCount();
    public void publishReadFailure(HRegionInfo region, Exception e);
    public void publishReadFailure(HRegionInfo region, HColumnDescriptor column, Exception e);
    public void publishReadTiming(HRegionInfo region, HColumnDescriptor column, long msTime);
    public long getWriteFailureCount();
    public void publishWriteFailure(HRegionInfo region, Exception e);
    public void publishWriteFailure(HRegionInfo region, HColumnDescriptor column, Exception e);
    public void publishWriteTiming(HRegionInfo region, HColumnDescriptor column, long msTime);
  }
  // new extended sink for output regionserver mode info
  // do not change the Sink interface directly due to maintaining the API
  public interface ExtendedSink extends Sink {
    public void publishReadFailure(String table, String server);
    public void publishReadTiming(String table, String server, long msTime);
  }

  // Simple implementation of canary sink that allows to plot on
  // file or standard output timings or failures.
  public static class StdOutSink implements Sink {
    private AtomicLong readFailureCount = new AtomicLong(0),
        writeFailureCount = new AtomicLong(0);

    @Override
    public long getReadFailureCount() {
      return readFailureCount.get();
    }

    @Override
    public long incReadFailureCount() {
      return readFailureCount.incrementAndGet();
    }

    @Override
    public void publishReadFailure(HRegionInfo region, Exception e) {
      readFailureCount.incrementAndGet();
      LOG.error(String.format("read from region %s failed", region.getRegionNameAsString()), e);
    }

    @Override
    public void publishReadFailure(HRegionInfo region, HColumnDescriptor column, Exception e) {
      readFailureCount.incrementAndGet();
      LOG.error(String.format("read from region %s column family %s failed",
                region.getRegionNameAsString(), column.getNameAsString()), e);
    }

    @Override
    public void publishReadTiming(HRegionInfo region, HColumnDescriptor column, long msTime) {
      LOG.info(String.format("read from region %s column family %s in %dms",
               region.getRegionNameAsString(), column.getNameAsString(), msTime));
    }

    @Override
    public long getWriteFailureCount() {
      return writeFailureCount.get();
    }

    @Override
    public void publishWriteFailure(HRegionInfo region, Exception e) {
      writeFailureCount.incrementAndGet();
      LOG.error(String.format("write to region %s failed", region.getRegionNameAsString()), e);
    }

    @Override
    public void publishWriteFailure(HRegionInfo region, HColumnDescriptor column, Exception e) {
      writeFailureCount.incrementAndGet();
      LOG.error(String.format("write to region %s column family %s failed",
        region.getRegionNameAsString(), column.getNameAsString()), e);
    }

    @Override
    public void publishWriteTiming(HRegionInfo region, HColumnDescriptor column, long msTime) {
      LOG.info(String.format("write to region %s column family %s in %dms",
        region.getRegionNameAsString(), column.getNameAsString(), msTime));
    }
  }
  // a ExtendedSink implementation
  public static class RegionServerStdOutSink extends StdOutSink implements ExtendedSink {

    @Override
    public void publishReadFailure(String table, String server) {
      incReadFailureCount();
      LOG.error(String.format("Read from table:%s on region server:%s", table, server));
    }

    @Override
    public void publishReadTiming(String table, String server, long msTime) {
      LOG.info(String.format("Read from table:%s on region server:%s in %dms",
          table, server, msTime));
    }
  }

  public static class ZookeeperStdOutSink extends StdOutSink implements ExtendedSink {
    @Override public void publishReadFailure(String zNode, String server) {
      incReadFailureCount();
      LOG.error(String.format("Read from zNode:%s on zookeeper instance:%s", zNode, server));
    }

    @Override public void publishReadTiming(String znode, String server, long msTime) {
      LOG.info(String.format("Read from zNode:%s on zookeeper instance:%s in %dms",
          znode, server, msTime));
    }
  }

  static class ZookeeperTask implements Callable<Void> {
    private final Connection connection;
    private final String host;
    private String znode;
    private final int timeout;
    private ZookeeperStdOutSink sink;

    public ZookeeperTask(Connection connection, String host, String znode, int timeout,
        ZookeeperStdOutSink sink) {
      this.connection = connection;
      this.host = host;
      this.znode = znode;
      this.timeout = timeout;
      this.sink = sink;
    }

    @Override public Void call() throws Exception {
      ZooKeeper zooKeeper = null;
      try {
        zooKeeper = new ZooKeeper(host, timeout, EmptyWatcher.instance);
        Stat exists = zooKeeper.exists(znode, false);
        StopWatch stopwatch = new StopWatch();
        stopwatch.start();
        zooKeeper.getData(znode, false, exists);
        stopwatch.stop();
        sink.publishReadTiming(znode, host, stopwatch.getTime());
      } catch (KeeperException | InterruptedException e) {
        sink.publishReadFailure(znode, host);
      } finally {
        if (zooKeeper != null) {
          zooKeeper.close();
        }
      }
      return null;
    }
  }

  /**
   * For each column family of the region tries to get one row and outputs the latency, or the
   * failure.
   */
  static class RegionTask implements Callable<Void> {
    public enum TaskType{
      READ, WRITE
    }
    private Connection connection;
    private HRegionInfo region;
    private Sink sink;
    private TaskType taskType;

    RegionTask(Connection connection, HRegionInfo region, Sink sink, TaskType taskType) {
      this.connection = connection;
      this.region = region;
      this.sink = sink;
      this.taskType = taskType;
    }

    @Override
    public Void call() {
      switch (taskType) {
      case READ:
        return read();
      case WRITE:
        return write();
      default:
        return read();
      }
    }

    public Void read() {
      Table table = null;
      HTableDescriptor tableDesc = null;
      try {
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("reading table descriptor for table %s",
            region.getTable()));
        }
        table = connection.getTable(region.getTable());
        tableDesc = table.getTableDescriptor();
      } catch (IOException e) {
        LOG.debug("sniffRegion failed", e);
        sink.publishReadFailure(region, e);
        if (table != null) {
          try {
            table.close();
          } catch (IOException ioe) {
            LOG.error("Close table failed", e);
          }
        }
        return null;
      }

      byte[] startKey = null;
      Get get = null;
      Scan scan = null;
      ResultScanner rs = null;
      StopWatch stopWatch = new StopWatch();
      for (HColumnDescriptor column : tableDesc.getColumnFamilies()) {
        stopWatch.reset();
        startKey = region.getStartKey();
        // Can't do a get on empty start row so do a Scan of first element if any instead.
        if (startKey.length > 0) {
          get = new Get(startKey);
          get.setCacheBlocks(false);
          get.setFilter(new FirstKeyOnlyFilter());
          get.addFamily(column.getName());
        } else {
          scan = new Scan();
          scan.setCaching(1);
          scan.setCacheBlocks(false);
          scan.setFilter(new FirstKeyOnlyFilter());
          scan.addFamily(column.getName());
          scan.setMaxResultSize(1L);
          scan.setSmall(true);
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("reading from table %s region %s column family %s and key %s",
            tableDesc.getTableName(), region.getRegionNameAsString(), column.getNameAsString(),
            Bytes.toStringBinary(startKey)));
        }
        try {
          stopWatch.start();
          if (startKey.length > 0) {
            table.get(get);
          } else {
            rs = table.getScanner(scan);
            rs.next();
          }
          stopWatch.stop();
          sink.publishReadTiming(region, column, stopWatch.getTime());
        } catch (Exception e) {
          sink.publishReadFailure(region, column, e);
        } finally {
          if (rs != null) {
            rs.close();
          }
          scan = null;
          get = null;
          startKey = null;
        }
      }
      try {
        table.close();
      } catch (IOException e) {
        LOG.error("Close table failed", e);
      }
      return null;
    }

    /**
     * Check writes for the canary table
     * @return
     */
    private Void write() {
      Table table = null;
      HTableDescriptor tableDesc = null;
      try {
        table = connection.getTable(region.getTable());
        tableDesc = table.getTableDescriptor();
        byte[] rowToCheck = region.getStartKey();
        if (rowToCheck.length == 0) {
          rowToCheck = new byte[]{0x0};
        }
        int writeValueSize =
            connection.getConfiguration().getInt(HConstants.HBASE_CANARY_WRITE_VALUE_SIZE_KEY, 10);
        for (HColumnDescriptor column : tableDesc.getColumnFamilies()) {
          Put put = new Put(rowToCheck);
          byte[] value = new byte[writeValueSize];
          Bytes.random(value);
          put.addColumn(column.getName(), HConstants.EMPTY_BYTE_ARRAY, value);

          if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("writing to table %s region %s column family %s and key %s",
              tableDesc.getTableName(), region.getRegionNameAsString(), column.getNameAsString(),
              Bytes.toStringBinary(rowToCheck)));
          }
          try {
            long startTime = System.currentTimeMillis();
            table.put(put);
            long time = System.currentTimeMillis() - startTime;
            sink.publishWriteTiming(region, column, time);
          } catch (Exception e) {
            sink.publishWriteFailure(region, column, e);
          }
        }
        table.close();
      } catch (IOException e) {
        sink.publishWriteFailure(region, e);
      }
      return null;
    }
  }

  /**
   * Get one row from a region on the regionserver and outputs the latency, or the failure.
   */
  static class RegionServerTask implements Callable<Void> {
    private Connection connection;
    private String serverName;
    private HRegionInfo region;
    private ExtendedSink sink;
    private AtomicLong successes;

    RegionServerTask(Connection connection, String serverName, HRegionInfo region,
        ExtendedSink sink, AtomicLong successes) {
      this.connection = connection;
      this.serverName = serverName;
      this.region = region;
      this.sink = sink;
      this.successes = successes;
    }

    @Override
    public Void call() {
      TableName tableName = null;
      Table table = null;
      Get get = null;
      byte[] startKey = null;
      Scan scan = null;
      StopWatch stopWatch = new StopWatch();
      // monitor one region on every region server
      stopWatch.reset();
      try {
        tableName = region.getTable();
        table = connection.getTable(tableName);
        startKey = region.getStartKey();
        // Can't do a get on empty start row so do a Scan of first element if any instead.
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("reading from region server %s table %s region %s and key %s",
            serverName, region.getTable(), region.getRegionNameAsString(),
            Bytes.toStringBinary(startKey)));
        }
        if (startKey.length > 0) {
          get = new Get(startKey);
          get.setCacheBlocks(false);
          get.setFilter(new FirstKeyOnlyFilter());
          stopWatch.start();
          table.get(get);
          stopWatch.stop();
        } else {
          scan = new Scan();
          scan.setCacheBlocks(false);
          scan.setFilter(new FirstKeyOnlyFilter());
          scan.setCaching(1);
          scan.setMaxResultSize(1L);
          scan.setSmall(true);
          stopWatch.start();
          ResultScanner s = table.getScanner(scan);
          s.next();
          s.close();
          stopWatch.stop();
        }
        successes.incrementAndGet();
        sink.publishReadTiming(tableName.getNameAsString(), serverName, stopWatch.getTime());
      } catch (TableNotFoundException tnfe) {
        LOG.error("Table may be deleted", tnfe);
        // This is ignored because it doesn't imply that the regionserver is dead
      } catch (TableNotEnabledException tnee) {
        // This is considered a success since we got a response.
        successes.incrementAndGet();
        LOG.debug("The targeted table was disabled.  Assuming success.");
      } catch (DoNotRetryIOException dnrioe) {
        sink.publishReadFailure(tableName.getNameAsString(), serverName);
        LOG.error(dnrioe);
      } catch (IOException e) {
        sink.publishReadFailure(tableName.getNameAsString(), serverName);
        LOG.error(e);
      } finally {
        if (table != null) {
          try {
            table.close();
          } catch (IOException e) {/* DO NOTHING */
            LOG.error("Close table failed", e);
          }
        }
        scan = null;
        get = null;
        startKey = null;
      }
      return null;
    }
  }

  private static final int USAGE_EXIT_CODE = 1;
  private static final int INIT_ERROR_EXIT_CODE = 2;
  private static final int TIMEOUT_ERROR_EXIT_CODE = 3;
  private static final int ERROR_EXIT_CODE = 4;
  private static final int FAILURE_EXIT_CODE = 5;

  private static final long DEFAULT_INTERVAL = 6000;

  private static final long DEFAULT_TIMEOUT = 600000; // 10 mins
  private static final int MAX_THREADS_NUM = 16; // #threads to contact regions

  private static final Log LOG = LogFactory.getLog(Canary.class);

  public static final TableName DEFAULT_WRITE_TABLE_NAME = TableName.valueOf(
    NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR, "canary");

  private static final String CANARY_TABLE_FAMILY_NAME = "Test";

  private Configuration conf = null;
  private long interval = 0;
  private Sink sink = null;

  private boolean useRegExp;
  private long timeout = DEFAULT_TIMEOUT;
  private boolean failOnError = true;
  private boolean regionServerMode = false;
  private boolean zookeeperMode = false;
  private boolean regionServerAllRegions = false;
  private boolean writeSniffing = false;
  private boolean treatFailureAsError = false;
  private TableName writeTableName = DEFAULT_WRITE_TABLE_NAME;

  private ExecutorService executor; // threads to retrieve data from regionservers

  public Canary() {
    this(new ScheduledThreadPoolExecutor(1), new RegionServerStdOutSink());
  }

  public Canary(ExecutorService executor, Sink sink) {
    this.executor = executor;
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

  private int parseArgs(String[] args) {
    int index = -1;
    // Process command line args
    for (int i = 0; i < args.length; i++) {
      String cmd = args[i];

      if (cmd.startsWith("-")) {
        if (index >= 0) {
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
        } else if (cmd.equals("-zookeeper")) {
          this.zookeeperMode = true;
        } else if(cmd.equals("-regionserver")) {
          this.regionServerMode = true;
        } else if(cmd.equals("-allRegions")) {
          this.regionServerAllRegions = true;
        } else if(cmd.equals("-writeSniffing")) {
          this.writeSniffing = true;
        } else if(cmd.equals("-treatFailureAsError")) {
          this.treatFailureAsError = true;
        } else if (cmd.equals("-e")) {
          this.useRegExp = true;
        } else if (cmd.equals("-t")) {
          i++;

          if (i == args.length) {
            System.err.println("-t needs a numeric value argument.");
            printUsageAndExit();
          }

          try {
            this.timeout = Long.parseLong(args[i]);
          } catch (NumberFormatException e) {
            System.err.println("-t needs a numeric value argument.");
            printUsageAndExit();
          }
        } else if (cmd.equals("-writeTable")) {
          i++;

          if (i == args.length) {
            System.err.println("-writeTable needs a string value argument.");
            printUsageAndExit();
          }
          this.writeTableName = TableName.valueOf(args[i]);
        } else if (cmd.equals("-f")) {
          i++;

          if (i == args.length) {
            System.err
                .println("-f needs a boolean value argument (true|false).");
            printUsageAndExit();
          }

          this.failOnError = Boolean.parseBoolean(args[i]);
        } else {
          // no options match
          System.err.println(cmd + " options is invalid.");
          printUsageAndExit();
        }
      } else if (index < 0) {
        // keep track of first table name specified by the user
        index = i;
      }
    }
    if (this.regionServerAllRegions && !this.regionServerMode) {
      System.err.println("-allRegions can only be specified in regionserver mode.");
      printUsageAndExit();
    }
    if (this.zookeeperMode) {
      if (this.regionServerMode || this.regionServerAllRegions || this.writeSniffing) {
        System.err.println("-zookeeper is exclusive and cannot be combined with "
            + "other modes.");
        printUsageAndExit();
      }
    }
    return index;
  }

  @Override
  public int run(String[] args) throws Exception {
    int index = parseArgs(args);
    ChoreService choreService = null;

    // Launches chore for refreshing kerberos credentials if security is enabled.
    // Please see http://hbase.apache.org/book.html#_running_canary_in_a_kerberos_enabled_cluster
    // for more details.
    final ScheduledChore authChore = AuthUtil.getAuthChore(conf);
    if (authChore != null) {
      choreService = new ChoreService("CANARY_TOOL");
      choreService.scheduleChore(authChore);
    }

    // Start to prepare the stuffs
    Monitor monitor = null;
    Thread monitorThread = null;
    long startTime = 0;
    long currentTimeLength = 0;
    // Get a connection to use in below.
    try (Connection connection = ConnectionFactory.createConnection(this.conf)) {
      do {
        // Do monitor !!
        try {
          monitor = this.newMonitor(connection, index, args);
          monitorThread = new Thread(monitor);
          startTime = System.currentTimeMillis();
          monitorThread.start();
          while (!monitor.isDone()) {
            // wait for 1 sec
            Thread.sleep(1000);
            // exit if any error occurs
            if (this.failOnError && monitor.hasError()) {
              monitorThread.interrupt();
              if (monitor.initialized) {
                return monitor.errorCode;
              } else {
                return INIT_ERROR_EXIT_CODE;
              }
            }
            currentTimeLength = System.currentTimeMillis() - startTime;
            if (currentTimeLength > this.timeout) {
              LOG.error("The monitor is running too long (" + currentTimeLength
                  + ") after timeout limit:" + this.timeout
                  + " will be killed itself !!");
              if (monitor.initialized) {
                return TIMEOUT_ERROR_EXIT_CODE;
              } else {
                return INIT_ERROR_EXIT_CODE;
              }
            }
          }

          if (this.failOnError && monitor.finalCheckForErrors()) {
            monitorThread.interrupt();
            return monitor.errorCode;
          }
        } finally {
          if (monitor != null) monitor.close();
        }

        Thread.sleep(interval);
      } while (interval > 0);
    } // try-with-resources close

    if (choreService != null) {
      choreService.shutdown();
    }
    return monitor.errorCode;
  }

  private void printUsageAndExit() {
    System.err.printf(
      "Usage: bin/hbase %s [opts] [table1 [table2]...] | [regionserver1 [regionserver2]..]%n",
        getClass().getName());
    System.err.println(" where [opts] are:");
    System.err.println("   -help          Show this help and exit.");
    System.err.println("   -regionserver  replace the table argument to regionserver,");
    System.err.println("      which means to enable regionserver mode");
    System.err.println("   -allRegions    Tries all regions on a regionserver,");
    System.err.println("      only works in regionserver mode.");
    System.err.println("   -zookeeper    Tries to grab zookeeper.znode.parent ");
    System.err.println("      on each zookeeper instance");
    System.err.println("   -daemon        Continuous check at defined intervals.");
    System.err.println("   -interval <N>  Interval between checks (sec)");
    System.err.println("   -e             Use table/regionserver as regular expression");
    System.err.println("      which means the table/regionserver is regular expression pattern");
    System.err.println("   -f <B>         stop whole program if first error occurs," +
        " default is true");
    System.err.println("   -t <N>         timeout for a check, default is 600000 (milisecs)");
    System.err.println("   -writeSniffing enable the write sniffing in canary");
    System.err.println("   -treatFailureAsError treats read / write failure as error");
    System.err.println("   -writeTable    The table used for write sniffing."
        + " Default is hbase:canary");
    System.err
        .println("   -D<configProperty>=<value> assigning or override the configuration params");
    System.exit(USAGE_EXIT_CODE);
  }

  /**
   * A Factory method for {@link Monitor}.
   * Can be overridden by user.
   * @param index a start index for monitor target
   * @param args args passed from user
   * @return a Monitor instance
   */
  public Monitor newMonitor(final Connection connection, int index, String[] args) {
    Monitor monitor = null;
    String[] monitorTargets = null;

    if(index >= 0) {
      int length = args.length - index;
      monitorTargets = new String[length];
      System.arraycopy(args, index, monitorTargets, 0, length);
    }

    if (this.regionServerMode) {
      monitor =
          new RegionServerMonitor(connection, monitorTargets, this.useRegExp,
              (ExtendedSink) this.sink, this.executor, this.regionServerAllRegions,
              this.treatFailureAsError);
    } else if (this.zookeeperMode) {
      monitor =
          new ZookeeperMonitor(connection, monitorTargets, this.useRegExp,
              (ZookeeperStdOutSink) this.sink, this.executor, this.treatFailureAsError);
    } else {
      monitor =
          new RegionMonitor(connection, monitorTargets, this.useRegExp, this.sink, this.executor,
              this.writeSniffing, this.writeTableName, this.treatFailureAsError);
    }
    return monitor;
  }

  // a Monitor super-class can be extended by users
  public static abstract class Monitor implements Runnable, Closeable {

    protected Connection connection;
    protected Admin admin;
    protected String[] targets;
    protected boolean useRegExp;
    protected boolean treatFailureAsError;
    protected boolean initialized = false;

    protected boolean done = false;
    protected int errorCode = 0;
    protected Sink sink;
    protected ExecutorService executor;

    public boolean isDone() {
      return done;
    }

    public boolean hasError() {
      return errorCode != 0;
    }

    public boolean finalCheckForErrors() {
      if (errorCode != 0) {
        return true;
      }
      if (treatFailureAsError &&
          (sink.getReadFailureCount() > 0 || sink.getWriteFailureCount() > 0)) {
        errorCode = FAILURE_EXIT_CODE;
        return true;
      }
      return false;
    }

    @Override
    public void close() throws IOException {
      if (this.admin != null) this.admin.close();
    }

    protected Monitor(Connection connection, String[] monitorTargets, boolean useRegExp, Sink sink,
        ExecutorService executor, boolean treatFailureAsError) {
      if (null == connection) throw new IllegalArgumentException("connection shall not be null");

      this.connection = connection;
      this.targets = monitorTargets;
      this.useRegExp = useRegExp;
      this.treatFailureAsError = treatFailureAsError;
      this.sink = sink;
      this.executor = executor;
    }

    @Override
    public abstract void run();

    protected boolean initAdmin() {
      if (null == this.admin) {
        try {
          this.admin = this.connection.getAdmin();
        } catch (Exception e) {
          LOG.error("Initial HBaseAdmin failed...", e);
          this.errorCode = INIT_ERROR_EXIT_CODE;
        }
      } else if (admin.isAborted()) {
        LOG.error("HBaseAdmin aborted");
        this.errorCode = INIT_ERROR_EXIT_CODE;
      }
      return !this.hasError();
    }
  }

  // a monitor for region mode
  private static class RegionMonitor extends Monitor {
    // 10 minutes
    private static final int DEFAULT_WRITE_TABLE_CHECK_PERIOD = 10 * 60 * 1000;
    // 1 days
    private static final int DEFAULT_WRITE_DATA_TTL = 24 * 60 * 60;

    private long lastCheckTime = -1;
    private boolean writeSniffing;
    private TableName writeTableName;
    private int writeDataTTL;
    private float regionsLowerLimit;
    private float regionsUpperLimit;
    private int checkPeriod;

    public RegionMonitor(Connection connection, String[] monitorTargets, boolean useRegExp,
        Sink sink, ExecutorService executor, boolean writeSniffing, TableName writeTableName,
        boolean treatFailureAsError) {
      super(connection, monitorTargets, useRegExp, sink, executor, treatFailureAsError);
      Configuration conf = connection.getConfiguration();
      this.writeSniffing = writeSniffing;
      this.writeTableName = writeTableName;
      this.writeDataTTL =
          conf.getInt(HConstants.HBASE_CANARY_WRITE_DATA_TTL_KEY, DEFAULT_WRITE_DATA_TTL);
      this.regionsLowerLimit =
          conf.getFloat(HConstants.HBASE_CANARY_WRITE_PERSERVER_REGIONS_LOWERLIMIT_KEY, 1.0f);
      this.regionsUpperLimit =
          conf.getFloat(HConstants.HBASE_CANARY_WRITE_PERSERVER_REGIONS_UPPERLIMIT_KEY, 1.5f);
      this.checkPeriod =
          conf.getInt(HConstants.HBASE_CANARY_WRITE_TABLE_CHECK_PERIOD_KEY,
            DEFAULT_WRITE_TABLE_CHECK_PERIOD);
    }

    @Override
    public void run() {
      if (this.initAdmin()) {
        try {
          List<Future<Void>> taskFutures = new LinkedList<Future<Void>>();
          if (this.targets != null && this.targets.length > 0) {
            String[] tables = generateMonitorTables(this.targets);
            this.initialized = true;
            for (String table : tables) {
              taskFutures.addAll(Canary.sniff(admin, sink, table, executor, TaskType.READ));
            }
          } else {
            taskFutures.addAll(sniff(TaskType.READ));
          }

          if (writeSniffing) {
            if (EnvironmentEdgeManager.currentTime() - lastCheckTime > checkPeriod) {
              try {
                checkWriteTableDistribution();
              } catch (IOException e) {
                LOG.error("Check canary table distribution failed!", e);
              }
              lastCheckTime = EnvironmentEdgeManager.currentTime();
            }
            // sniff canary table with write operation
            taskFutures.addAll(Canary.sniff(admin, sink,
              admin.getTableDescriptor(writeTableName), executor, TaskType.WRITE));
          }

          for (Future<Void> future : taskFutures) {
            try {
              future.get();
            } catch (ExecutionException e) {
              LOG.error("Sniff region failed!", e);
            }
          }
        } catch (Exception e) {
          LOG.error("Run regionMonitor failed", e);
          this.errorCode = ERROR_EXIT_CODE;
        }
      }
      this.done = true;
    }

    private String[] generateMonitorTables(String[] monitorTargets) throws IOException {
      String[] returnTables = null;

      if (this.useRegExp) {
        Pattern pattern = null;
        HTableDescriptor[] tds = null;
        Set<String> tmpTables = new TreeSet<String>();
        try {
          if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("reading list of tables"));
          }
          tds = this.admin.listTables(pattern);
          if (tds == null) {
            tds = new HTableDescriptor[0];
          }
          for (String monitorTarget : monitorTargets) {
            pattern = Pattern.compile(monitorTarget);
            for (HTableDescriptor td : tds) {
              if (pattern.matcher(td.getNameAsString()).matches()) {
                tmpTables.add(td.getNameAsString());
              }
            }
          }
        } catch (IOException e) {
          LOG.error("Communicate with admin failed", e);
          throw e;
        }

        if (tmpTables.size() > 0) {
          returnTables = tmpTables.toArray(new String[tmpTables.size()]);
        } else {
          String msg = "No HTable found, tablePattern:" + Arrays.toString(monitorTargets);
          LOG.error(msg);
          this.errorCode = INIT_ERROR_EXIT_CODE;
          throw new TableNotFoundException(msg);
        }
      } else {
        returnTables = monitorTargets;
      }

      return returnTables;
    }

    /*
     * canary entry point to monitor all the tables.
     */
    private List<Future<Void>> sniff(TaskType taskType) throws Exception {
      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format("reading list of tables"));
      }
      List<Future<Void>> taskFutures = new LinkedList<Future<Void>>();
      for (HTableDescriptor table : admin.listTables()) {
        if (admin.isTableEnabled(table.getTableName())
            && (!table.getTableName().equals(writeTableName))) {
          taskFutures.addAll(Canary.sniff(admin, sink, table, executor, taskType));
        }
      }
      return taskFutures;
    }

    private void checkWriteTableDistribution() throws IOException {
      if (!admin.tableExists(writeTableName)) {
        int numberOfServers = admin.getClusterStatus().getServers().size();
        if (numberOfServers == 0) {
          throw new IllegalStateException("No live regionservers");
        }
        createWriteTable(numberOfServers);
      }

      if (!admin.isTableEnabled(writeTableName)) {
        admin.enableTable(writeTableName);
      }

      ClusterStatus status = admin.getClusterStatus();
      int numberOfServers = status.getServersSize();
      if (status.getServers().contains(status.getMaster())) {
        numberOfServers -= 1;
      }

      List<HRegionLocation> locations;
      RegionLocator locator = connection.getRegionLocator(writeTableName);
      try {
        locations = locator.getAllRegionLocations();
      } finally {
        locator.close();
      }
      int numberOfRegions = locations.size();
      if (numberOfRegions < numberOfServers * regionsLowerLimit
          || numberOfRegions > numberOfServers * regionsUpperLimit) {
        admin.disableTable(writeTableName);
        admin.deleteTable(writeTableName);
        createWriteTable(numberOfServers);
      }
      HashSet<ServerName> serverSet = new HashSet<ServerName>();
      for (HRegionLocation location: locations) {
        serverSet.add(location.getServerName());
      }
      int numberOfCoveredServers = serverSet.size();
      if (numberOfCoveredServers < numberOfServers) {
        admin.balancer();
      }
    }

    private void createWriteTable(int numberOfServers) throws IOException {
      int numberOfRegions = (int)(numberOfServers * regionsLowerLimit);
      LOG.info("Number of live regionservers: " + numberOfServers + ", "
          + "pre-splitting the canary table into " + numberOfRegions + " regions "
          + "(current lower limit of regions per server is " + regionsLowerLimit
          + " and you can change it by config: "
          + HConstants.HBASE_CANARY_WRITE_PERSERVER_REGIONS_LOWERLIMIT_KEY + " )");
      HTableDescriptor desc = new HTableDescriptor(writeTableName);
      HColumnDescriptor family = new HColumnDescriptor(CANARY_TABLE_FAMILY_NAME);
      family.setMaxVersions(1);
      family.setTimeToLive(writeDataTTL);

      desc.addFamily(family);
      byte[][] splits = new RegionSplitter.HexStringSplit().split(numberOfRegions);
      admin.createTable(desc, splits);
    }
  }

  /**
   * Canary entry point for specified table.
   * @throws Exception
   */
  public static void sniff(final Admin admin, TableName tableName)
      throws Exception {
    sniff(admin, tableName, TaskType.READ);
  }

  /**
   * Canary entry point for specified table with task type(read/write)
   * @throws Exception
   */
  public static void sniff(final Admin admin, TableName tableName, TaskType taskType)
      throws Exception {
    List<Future<Void>> taskFutures =
        Canary.sniff(admin, new StdOutSink(), tableName.getNameAsString(),
          new ScheduledThreadPoolExecutor(1), taskType);
    for (Future<Void> future : taskFutures) {
      future.get();
    }
  }

  /**
   * Canary entry point for specified table.
   * @throws Exception
   */
  private static List<Future<Void>> sniff(final Admin admin, final Sink sink, String tableName,
      ExecutorService executor, TaskType taskType) throws Exception {
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("checking table is enabled and getting table descriptor for table %s",
        tableName));
    }
    if (admin.isTableEnabled(TableName.valueOf(tableName))) {
      return Canary.sniff(admin, sink, admin.getTableDescriptor(TableName.valueOf(tableName)),
        executor, taskType);
    } else {
      LOG.warn(String.format("Table %s is not enabled", tableName));
    }
    return new LinkedList<Future<Void>>();
  }

  /*
   * Loops over regions that owns this table, and output some information abouts the state.
   */
  private static List<Future<Void>> sniff(final Admin admin, final Sink sink,
      HTableDescriptor tableDesc, ExecutorService executor, TaskType taskType) throws Exception {

    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("reading list of regions for table %s", tableDesc.getTableName()));
    }

    Table table = null;
    try {
      table = admin.getConnection().getTable(tableDesc.getTableName());
    } catch (TableNotFoundException e) {
      return new ArrayList<Future<Void>>();
    }
    List<RegionTask> tasks = new ArrayList<RegionTask>();
    try {
      for (HRegionInfo region : admin.getTableRegions(tableDesc.getTableName())) {
        tasks.add(new RegionTask(admin.getConnection(), region, sink, taskType));
      }
    } finally {
      table.close();
    }
    return executor.invokeAll(tasks);
  }

  /*
   * For each column family of the region tries to get one row and outputs the latency, or the
   * failure.
   */
  private static void sniffRegion(
      final Admin admin,
      final Sink sink,
      HRegionInfo region,
      Table table) throws Exception {
    HTableDescriptor tableDesc = table.getTableDescriptor();
    byte[] startKey = null;
    Get get = null;
    Scan scan = null;
    ResultScanner rs = null;
    StopWatch stopWatch = new StopWatch();
    for (HColumnDescriptor column : tableDesc.getColumnFamilies()) {
      stopWatch.reset();
      startKey = region.getStartKey();
      // Can't do a get on empty start row so do a Scan of first element if any instead.
      if (startKey.length > 0) {
        get = new Get(startKey);
        get.setCacheBlocks(false);
        get.setFilter(new FirstKeyOnlyFilter());
        get.addFamily(column.getName());
      } else {
        scan = new Scan();
        scan.setRaw(true);
        scan.setCaching(1);
        scan.setCacheBlocks(false);
        scan.setFilter(new FirstKeyOnlyFilter());
        scan.addFamily(column.getName());
        scan.setMaxResultSize(1L);
      }

      try {
        if (startKey.length > 0) {
          stopWatch.start();
          table.get(get);
          stopWatch.stop();
          sink.publishReadTiming(region, column, stopWatch.getTime());
        } else {
          stopWatch.start();
          rs = table.getScanner(scan);
          stopWatch.stop();
          sink.publishReadTiming(region, column, stopWatch.getTime());
        }
      } catch (Exception e) {
        sink.publishReadFailure(region, column, e);
      } finally {
        if (rs != null) {
          rs.close();
        }
        scan = null;
        get = null;
        startKey = null;
      }
    }
  }

  //  monitor for zookeeper mode
  private static class ZookeeperMonitor extends Monitor {
    private List<String> hosts;
    private final String znode;
    private final int timeout;

    protected ZookeeperMonitor(Connection connection, String[] monitorTargets, boolean useRegExp,
        ExtendedSink sink, ExecutorService executor, boolean treatFailureAsError)  {
      super(connection, monitorTargets, useRegExp, sink, executor, treatFailureAsError);
      Configuration configuration = connection.getConfiguration();
      znode =
          configuration.get(ZOOKEEPER_ZNODE_PARENT,
              DEFAULT_ZOOKEEPER_ZNODE_PARENT);
      timeout = configuration
          .getInt(HConstants.ZK_SESSION_TIMEOUT, HConstants.DEFAULT_ZK_SESSION_TIMEOUT);
      ConnectStringParser parser =
          new ConnectStringParser(ZKConfig.getZKQuorumServersString(configuration));
      hosts = Lists.newArrayList();
      for (InetSocketAddress server : parser.getServerAddresses()) {
        hosts.add(server.toString());
      }
    }

    @Override public void run() {
      List<ZookeeperTask> tasks = Lists.newArrayList();
      for (final String host : hosts) {
        tasks.add(new ZookeeperTask(connection, host, znode, timeout, getSink()));
      }
      try {
        for (Future<Void> future : this.executor.invokeAll(tasks)) {
          try {
            future.get();
          } catch (ExecutionException e) {
            LOG.error("Sniff zookeeper failed!", e);
            this.errorCode = ERROR_EXIT_CODE;
          }
        }
      } catch (InterruptedException e) {
        this.errorCode = ERROR_EXIT_CODE;
        Thread.currentThread().interrupt();
        LOG.error("Sniff zookeeper interrupted!", e);
      }
      this.done = true;
    }


    private ZookeeperStdOutSink getSink() {
      if (!(sink instanceof ZookeeperStdOutSink)) {
        throw new RuntimeException("Can only write to zookeeper sink");
      }
      return ((ZookeeperStdOutSink) sink);
    }
  }


  // a monitor for regionserver mode
  private static class RegionServerMonitor extends Monitor {

    private boolean allRegions;

    public RegionServerMonitor(Connection connection, String[] monitorTargets, boolean useRegExp,
        ExtendedSink sink, ExecutorService executor, boolean allRegions,
        boolean treatFailureAsError) {
      super(connection, monitorTargets, useRegExp, sink, executor, treatFailureAsError);
      this.allRegions = allRegions;
    }

    private ExtendedSink getSink() {
      return (ExtendedSink) this.sink;
    }

    @Override
    public void run() {
      if (this.initAdmin() && this.checkNoTableNames()) {
        Map<String, List<HRegionInfo>> rsAndRMap = this.filterRegionServerByName();
        this.initialized = true;
        this.monitorRegionServers(rsAndRMap);
      }
      this.done = true;
    }

    private boolean checkNoTableNames() {
      List<String> foundTableNames = new ArrayList<String>();
      TableName[] tableNames = null;

      if (LOG.isDebugEnabled()) {
        LOG.debug(String.format("reading list of tables"));
      }
      try {
        tableNames = this.admin.listTableNames();
      } catch (IOException e) {
        LOG.error("Get listTableNames failed", e);
        this.errorCode = INIT_ERROR_EXIT_CODE;
        return false;
      }

      if (this.targets == null || this.targets.length == 0) return true;

      for (String target : this.targets) {
        for (TableName tableName : tableNames) {
          if (target.equals(tableName.getNameAsString())) {
            foundTableNames.add(target);
          }
        }
      }

      if (foundTableNames.size() > 0) {
        System.err.println("Cannot pass a tablename when using the -regionserver " +
            "option, tablenames:" + foundTableNames.toString());
        this.errorCode = USAGE_EXIT_CODE;
      }
      return foundTableNames.size() == 0;
    }

    private void monitorRegionServers(Map<String, List<HRegionInfo>> rsAndRMap) {
      List<RegionServerTask> tasks = new ArrayList<RegionServerTask>();
      Map<String, AtomicLong> successMap = new HashMap<String, AtomicLong>();
      Random rand = new Random();
      for (Map.Entry<String, List<HRegionInfo>> entry : rsAndRMap.entrySet()) {
        String serverName = entry.getKey();
        AtomicLong successes = new AtomicLong(0);
        successMap.put(serverName, successes);
        if (entry.getValue().isEmpty()) {
          LOG.error(String.format("Regionserver not serving any regions - %s", serverName));
        } else if (this.allRegions) {
          for (HRegionInfo region : entry.getValue()) {
            tasks.add(new RegionServerTask(this.connection,
                serverName,
                region,
                getSink(),
                successes));
          }
        } else {
          // random select a region if flag not set
          HRegionInfo region = entry.getValue().get(rand.nextInt(entry.getValue().size()));
          tasks.add(new RegionServerTask(this.connection,
              serverName,
              region,
              getSink(),
              successes));
        }
      }
      try {
        for (Future<Void> future : this.executor.invokeAll(tasks)) {
          try {
            future.get();
          } catch (ExecutionException e) {
            LOG.error("Sniff regionserver failed!", e);
            this.errorCode = ERROR_EXIT_CODE;
          }
        }
        if (this.allRegions) {
          for (Map.Entry<String, List<HRegionInfo>> entry : rsAndRMap.entrySet()) {
            String serverName = entry.getKey();
            LOG.info("Successfully read " + successMap.get(serverName) + " regions out of "
                    + entry.getValue().size() + " on regionserver:" + serverName);
          }
        }
      } catch (InterruptedException e) {
        this.errorCode = ERROR_EXIT_CODE;
        LOG.error("Sniff regionserver interrupted!", e);
      }
    }

    private Map<String, List<HRegionInfo>> filterRegionServerByName() {
      Map<String, List<HRegionInfo>> regionServerAndRegionsMap = this.getAllRegionServerByName();
      regionServerAndRegionsMap = this.doFilterRegionServerByName(regionServerAndRegionsMap);
      return regionServerAndRegionsMap;
    }

    private Map<String, List<HRegionInfo>> getAllRegionServerByName() {
      Map<String, List<HRegionInfo>> rsAndRMap = new HashMap<String, List<HRegionInfo>>();
      Table table = null;
      RegionLocator regionLocator = null;
      try {
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("reading list of tables and locations"));
        }
        HTableDescriptor[] tableDescs = this.admin.listTables();
        List<HRegionInfo> regions = null;
        for (HTableDescriptor tableDesc : tableDescs) {
          table = this.admin.getConnection().getTable(tableDesc.getTableName());
          regionLocator = this.admin.getConnection().getRegionLocator(tableDesc.getTableName());

          for (HRegionLocation location : regionLocator.getAllRegionLocations()) {
            ServerName rs = location.getServerName();
            String rsName = rs.getHostname();
            HRegionInfo r = location.getRegionInfo();

            if (rsAndRMap.containsKey(rsName)) {
              regions = rsAndRMap.get(rsName);
            } else {
              regions = new ArrayList<HRegionInfo>();
              rsAndRMap.put(rsName, regions);
            }
            regions.add(r);
          }
          table.close();
        }

        //get any live regionservers not serving any regions
        for (ServerName rs : this.admin.getClusterStatus().getServers()) {
          String rsName = rs.getHostname();
          if (!rsAndRMap.containsKey(rsName)) {
            rsAndRMap.put(rsName, Collections.<HRegionInfo>emptyList());
          }
        }
      } catch (IOException e) {
        String msg = "Get HTables info failed";
        LOG.error(msg, e);
        this.errorCode = INIT_ERROR_EXIT_CODE;
      } finally {
        if (table != null) {
          try {
            table.close();
          } catch (IOException e) {
            LOG.warn("Close table failed", e);
          }
        }
      }

      return rsAndRMap;
    }

    private Map<String, List<HRegionInfo>> doFilterRegionServerByName(
        Map<String, List<HRegionInfo>> fullRsAndRMap) {

      Map<String, List<HRegionInfo>> filteredRsAndRMap = null;

      if (this.targets != null && this.targets.length > 0) {
        filteredRsAndRMap = new HashMap<String, List<HRegionInfo>>();
        Pattern pattern = null;
        Matcher matcher = null;
        boolean regExpFound = false;
        for (String rsName : this.targets) {
          if (this.useRegExp) {
            regExpFound = false;
            pattern = Pattern.compile(rsName);
            for (Map.Entry<String, List<HRegionInfo>> entry : fullRsAndRMap.entrySet()) {
              matcher = pattern.matcher(entry.getKey());
              if (matcher.matches()) {
                filteredRsAndRMap.put(entry.getKey(), entry.getValue());
                regExpFound = true;
              }
            }
            if (!regExpFound) {
              LOG.info("No RegionServerInfo found, regionServerPattern:" + rsName);
            }
          } else {
            if (fullRsAndRMap.containsKey(rsName)) {
              filteredRsAndRMap.put(rsName, fullRsAndRMap.get(rsName));
            } else {
              LOG.info("No RegionServerInfo found, regionServerName:" + rsName);
            }
          }
        }
      } else {
        filteredRsAndRMap = fullRsAndRMap;
      }
      return filteredRsAndRMap;
    }
  }

  public static void main(String[] args) throws Exception {
    final Configuration conf = HBaseConfiguration.create();
    final ChoreService choreService = new ChoreService("CANARY_TOOL");
    final ScheduledChore authChore = AuthUtil.getAuthChore(conf);
    if (authChore != null) {
      choreService.scheduleChore(authChore);
    }

    // loading the generic options to conf
    new GenericOptionsParser(conf, args);

    int numThreads = conf.getInt("hbase.canary.threads.num", MAX_THREADS_NUM);
    LOG.info("Number of execution threads " + numThreads);

    ExecutorService executor = new ScheduledThreadPoolExecutor(numThreads);

    Class<? extends Sink> sinkClass =
        conf.getClass("hbase.canary.sink.class", RegionServerStdOutSink.class, Sink.class);
    Sink sink = ReflectionUtils.newInstance(sinkClass);

    int exitCode = ToolRunner.run(conf, new Canary(executor, sink), args);
    choreService.shutdown();
    executor.shutdown();
    System.exit(exitCode);
  }
}
