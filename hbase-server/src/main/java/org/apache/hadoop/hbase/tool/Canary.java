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

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.ClusterMetrics.Option;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.tool.Canary.RegionTask.TaskType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.hbase.zookeeper.EmptyWatcher;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.client.ConnectStringParser;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * HBase Canary Tool for "canary monitoring" of a running HBase cluster.
 *
 * There are three modes:
 * <ol>
 * <li>region mode (Default): For each region, try to get one row per column family outputting
 * information on failure (ERROR) or else the latency.
 * </li>
 *
 * <li>regionserver mode: For each regionserver try to get one row from one table selected
 * randomly outputting information on failure (ERROR) or else the latency.
 * </li>
 *
 * <li>zookeeper mode: for each zookeeper instance, selects a znode outputting information on
 * failure (ERROR) or else the latency.
 * </li>
 * </ol>
 */
@InterfaceAudience.Private
public final class Canary implements Tool {
  /**
   * Sink interface used by the canary to output information
   */
  public interface Sink {
    long getReadFailureCount();
    long incReadFailureCount();
    Map<String,String> getReadFailures();
    void updateReadFailures(String regionName, String serverName);
    long getWriteFailureCount();
    long incWriteFailureCount();
    Map<String,String> getWriteFailures();
    void updateWriteFailures(String regionName, String serverName);
  }

  /**
   * Simple implementation of canary sink that allows plotting to a file or standard output.
   */
  public static class StdOutSink implements Sink {
    private AtomicLong readFailureCount = new AtomicLong(0),
        writeFailureCount = new AtomicLong(0);
    private Map<String, String> readFailures = new ConcurrentHashMap<>();
    private Map<String, String> writeFailures = new ConcurrentHashMap<>();

    @Override
    public long getReadFailureCount() {
      return readFailureCount.get();
    }

    @Override
    public long incReadFailureCount() {
      return readFailureCount.incrementAndGet();
    }

    @Override
    public Map<String, String> getReadFailures() {
      return readFailures;
    }

    @Override
    public void updateReadFailures(String regionName, String serverName) {
      readFailures.put(regionName, serverName);
    }

    @Override
    public long getWriteFailureCount() {
      return writeFailureCount.get();
    }

    @Override
    public long incWriteFailureCount() {
      return writeFailureCount.incrementAndGet();
    }

    @Override
    public Map<String, String> getWriteFailures() {
      return writeFailures;
    }

    @Override
    public void updateWriteFailures(String regionName, String serverName) {
      writeFailures.put(regionName, serverName);
    }
  }

  /**
   * By RegionServer, for 'regionserver' mode.
   */
  public static class RegionServerStdOutSink extends StdOutSink {
    public void publishReadFailure(String table, String server) {
      incReadFailureCount();
      LOG.error("Read from {} on {}", table, server);
    }

    public void publishReadTiming(String table, String server, long msTime) {
      LOG.info("Read from {} on {} in {}ms", table, server, msTime);
    }
  }

  /**
   * Output for 'zookeeper' mode.
   */
  public static class ZookeeperStdOutSink extends StdOutSink {
    public void publishReadFailure(String znode, String server) {
      incReadFailureCount();
      LOG.error("Read from {} on {}", znode, server);
    }

    public void publishReadTiming(String znode, String server, long msTime) {
      LOG.info("Read from {} on {} in {}ms", znode, server, msTime);
    }
  }

  /**
   * By Region, for 'region'  mode.
   */
  public static class RegionStdOutSink extends StdOutSink {
    private Map<String, LongAdder> perTableReadLatency = new HashMap<>();
    private LongAdder writeLatency = new LongAdder();

    public void publishReadFailure(ServerName serverName, RegionInfo region, Exception e) {
      incReadFailureCount();
      LOG.error("Read from {} on {} failed", region.getRegionNameAsString(), serverName, e);
    }

    public void publishReadFailure(ServerName serverName, RegionInfo region,
        ColumnFamilyDescriptor column, Exception e) {
      incReadFailureCount();
      LOG.error("Read from {} on {} {} failed", region.getRegionNameAsString(), serverName,
          column.getNameAsString(), e);
    }

    public void publishReadTiming(ServerName serverName, RegionInfo region,
        ColumnFamilyDescriptor column, long msTime) {
      LOG.info("Read from {} on {} {} in {}ms", region.getRegionNameAsString(), serverName,
          column.getNameAsString(), msTime);
    }

    public void publishWriteFailure(ServerName serverName, RegionInfo region, Exception e) {
      incWriteFailureCount();
      LOG.error("Write to {} on {} failed", region.getRegionNameAsString(), serverName, e);
    }

    public void publishWriteFailure(ServerName serverName, RegionInfo region,
        ColumnFamilyDescriptor column, Exception e) {
      incWriteFailureCount();
      LOG.error("Write to {} on {} {} failed", region.getRegionNameAsString(), serverName,
          column.getNameAsString(), e);
    }

    public void publishWriteTiming(ServerName serverName, RegionInfo region,
        ColumnFamilyDescriptor column, long msTime) {
      LOG.info("Write to {} on {} {} in {}ms",
        region.getRegionNameAsString(), serverName, column.getNameAsString(), msTime);
    }

    public Map<String, LongAdder> getReadLatencyMap() {
      return this.perTableReadLatency;
    }

    public LongAdder initializeAndGetReadLatencyForTable(String tableName) {
      LongAdder initLatency = new LongAdder();
      this.perTableReadLatency.put(tableName, initLatency);
      return initLatency;
    }

    public void initializeWriteLatency() {
      this.writeLatency.reset();
    }

    public LongAdder getWriteLatency() {
      return this.writeLatency;
    }
  }

  /**
   * Run a single zookeeper Task and then exit.
   */
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
   * Run a single Region Task and then exit. For each column family of the Region, get one row and
   * output latency or failure.
   */
  static class RegionTask implements Callable<Void> {
    public enum TaskType{
      READ, WRITE
    }
    private Connection connection;
    private RegionInfo region;
    private RegionStdOutSink sink;
    private TaskType taskType;
    private boolean rawScanEnabled;
    private ServerName serverName;
    private LongAdder readWriteLatency;

    RegionTask(Connection connection, RegionInfo region, ServerName serverName,
        RegionStdOutSink sink, TaskType taskType, boolean rawScanEnabled, LongAdder rwLatency) {
      this.connection = connection;
      this.region = region;
      this.serverName = serverName;
      this.sink = sink;
      this.taskType = taskType;
      this.rawScanEnabled = rawScanEnabled;
      this.readWriteLatency = rwLatency;
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
      TableDescriptor tableDesc = null;
      try {
        LOG.debug("Reading table descriptor for table {}", region.getTable());
        table = connection.getTable(region.getTable());
        tableDesc = table.getDescriptor();
      } catch (IOException e) {
        LOG.debug("sniffRegion {} of {} failed", region.getEncodedName(), e);
        sink.publishReadFailure(serverName, region, e);
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
      for (ColumnFamilyDescriptor column : tableDesc.getColumnFamilies()) {
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
          LOG.debug("rawScan {} for {}", rawScanEnabled, tableDesc.getTableName());
          scan.setRaw(rawScanEnabled);
          scan.setCaching(1);
          scan.setCacheBlocks(false);
          scan.setFilter(new FirstKeyOnlyFilter());
          scan.addFamily(column.getName());
          scan.setMaxResultSize(1L);
          scan.setOneRowLimit();
        }
        LOG.debug("Reading from {} {} {} {}", tableDesc.getTableName(),
            region.getRegionNameAsString(), column.getNameAsString(),
            Bytes.toStringBinary(startKey));
        try {
          stopWatch.start();
          if (startKey.length > 0) {
            table.get(get);
          } else {
            rs = table.getScanner(scan);
            rs.next();
          }
          stopWatch.stop();
          this.readWriteLatency.add(stopWatch.getTime());
          sink.publishReadTiming(serverName, region, column, stopWatch.getTime());
        } catch (Exception e) {
          sink.publishReadFailure(serverName, region, column, e);
          sink.updateReadFailures(region.getRegionNameAsString(), serverName.getHostname());
        } finally {
          if (rs != null) {
            rs.close();
          }
          scan = null;
          get = null;
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
     */
    private Void write() {
      Table table = null;
      TableDescriptor tableDesc = null;
      try {
        table = connection.getTable(region.getTable());
        tableDesc = table.getDescriptor();
        byte[] rowToCheck = region.getStartKey();
        if (rowToCheck.length == 0) {
          rowToCheck = new byte[]{0x0};
        }
        int writeValueSize =
            connection.getConfiguration().getInt(HConstants.HBASE_CANARY_WRITE_VALUE_SIZE_KEY, 10);
        for (ColumnFamilyDescriptor column : tableDesc.getColumnFamilies()) {
          Put put = new Put(rowToCheck);
          byte[] value = new byte[writeValueSize];
          Bytes.random(value);
          put.addColumn(column.getName(), HConstants.EMPTY_BYTE_ARRAY, value);

          LOG.debug("Writing to {} {} {} {}",
            tableDesc.getTableName(), region.getRegionNameAsString(), column.getNameAsString(),
            Bytes.toStringBinary(rowToCheck));
          try {
            long startTime = System.currentTimeMillis();
            table.put(put);
            long time = System.currentTimeMillis() - startTime;
            this.readWriteLatency.add(time);
            sink.publishWriteTiming(serverName, region, column, time);
          } catch (Exception e) {
            sink.publishWriteFailure(serverName, region, column, e);
          }
        }
        table.close();
      } catch (IOException e) {
        sink.publishWriteFailure(serverName, region, e);
        sink.updateWriteFailures(region.getRegionNameAsString(), serverName.getHostname() );
      }
      return null;
    }
  }

  /**
   * Run a single RegionServer Task and then exit.
   * Get one row from a region on the regionserver and output latency or the failure.
   */
  static class RegionServerTask implements Callable<Void> {
    private Connection connection;
    private String serverName;
    private RegionInfo region;
    private RegionServerStdOutSink sink;
    private AtomicLong successes;

    RegionServerTask(Connection connection, String serverName, RegionInfo region,
        RegionServerStdOutSink sink, AtomicLong successes) {
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
        LOG.debug("Reading from {} {} {} {}",
          serverName, region.getTable(), region.getRegionNameAsString(),
          Bytes.toStringBinary(startKey));
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
          scan.setOneRowLimit();
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
        LOG.error(dnrioe.toString(), dnrioe);
      } catch (IOException e) {
        sink.publishReadFailure(tableName.getNameAsString(), serverName);
        LOG.error(e.toString(), e);
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

  private static final long DEFAULT_INTERVAL = 60000;

  private static final long DEFAULT_TIMEOUT = 600000; // 10 mins
  private static final int MAX_THREADS_NUM = 16; // #threads to contact regions

  private static final Logger LOG = LoggerFactory.getLogger(Canary.class);

  public static final TableName DEFAULT_WRITE_TABLE_NAME = TableName.valueOf(
    NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR, "canary");

  private static final String CANARY_TABLE_FAMILY_NAME = "Test";

  private Configuration conf = null;
  private long interval = 0;
  private Sink sink = null;

  private boolean useRegExp;
  private long timeout = DEFAULT_TIMEOUT;
  private boolean failOnError = true;

  /**
   * True if we are to run in 'regionServer' mode.
   */
  private boolean regionServerMode = false;

  /**
   * True if we are to run in zookeeper 'mode'.
   */
  private boolean zookeeperMode = false;
  private long permittedFailures = 0;
  private boolean regionServerAllRegions = false;
  private boolean writeSniffing = false;
  private long configuredWriteTableTimeout = DEFAULT_TIMEOUT;
  private boolean treatFailureAsError = false;
  private TableName writeTableName = DEFAULT_WRITE_TABLE_NAME;

  /**
   * This is a Map of table to timeout. The timeout is for reading all regions in the table; i.e.
   * we aggregate time to fetch each region and it needs to be less than this value else we
   * log an ERROR.
   */
  private HashMap<String, Long> configuredReadTableTimeouts = new HashMap<>();

  private ExecutorService executor; // threads to retrieve data from regionservers

  public Canary() {
    this(new ScheduledThreadPoolExecutor(1));
  }

  public Canary(ExecutorService executor) {
    this(executor, null);
  }

  @VisibleForTesting
  Canary(ExecutorService executor, Sink sink) {
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

        if (cmd.equals("-help") || cmd.equals("-h")) {
          // user asked for help, print the help and quit.
          printUsageAndExit();
        } else if (cmd.equals("-daemon") && interval == 0) {
          // user asked for daemon mode, set a default interval between checks
          interval = DEFAULT_INTERVAL;
        } else if (cmd.equals("-interval")) {
          // user has specified an interval for canary breaths (-interval N)
          i++;

          if (i == args.length) {
            System.err.println("-interval takes a numeric seconds value argument.");
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
        } else if(cmd.equals("-treatFailureAsError") || cmd.equals("-failureAsError")) {
          this.treatFailureAsError = true;
        } else if (cmd.equals("-e")) {
          this.useRegExp = true;
        } else if (cmd.equals("-t")) {
          i++;

          if (i == args.length) {
            System.err.println("-t takes a numeric milliseconds value argument.");
            printUsageAndExit();
          }

          try {
            this.timeout = Long.parseLong(args[i]);
          } catch (NumberFormatException e) {
            System.err.println("-t takes a numeric milliseconds value argument.");
            printUsageAndExit();
          }
        } else if(cmd.equals("-writeTableTimeout")) {
          i++;

          if (i == args.length) {
            System.err.println("-writeTableTimeout takes a numeric milliseconds value argument.");
            printUsageAndExit();
          }

          try {
            this.configuredWriteTableTimeout = Long.parseLong(args[i]);
          } catch (NumberFormatException e) {
            System.err.println("-writeTableTimeout takes a numeric milliseconds value argument.");
            printUsageAndExit();
          }
        } else if (cmd.equals("-writeTable")) {
          i++;

          if (i == args.length) {
            System.err.println("-writeTable takes a string tablename value argument.");
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
        } else if (cmd.equals("-readTableTimeouts")) {
          i++;

          if (i == args.length) {
            System.err.println("-readTableTimeouts needs a comma-separated list of read " +
                "millisecond timeouts per table (without spaces).");
            printUsageAndExit();
          }
          String [] tableTimeouts = args[i].split(",");
          for (String tT: tableTimeouts) {
            String [] nameTimeout = tT.split("=");
            if (nameTimeout.length < 2) {
              System.err.println("Each -readTableTimeouts argument must be of the form " +
                  "<tableName>=<read timeout> (without spaces).");
              printUsageAndExit();
            }
            long timeoutVal = 0L;
            try {
              timeoutVal = Long.parseLong(nameTimeout[1]);
            } catch (NumberFormatException e) {
              System.err.println("-readTableTimeouts read timeout for each table must be a numeric value argument.");
              printUsageAndExit();
            }
            this.configuredReadTableTimeouts.put(nameTimeout[0], timeoutVal);
          }
        } else if (cmd.equals("-permittedZookeeperFailures")) {
          i++;

          if (i == args.length) {
            System.err.println("-permittedZookeeperFailures needs a numeric value argument.");
            printUsageAndExit();
          }
          try {
            this.permittedFailures = Long.parseLong(args[i]);
          } catch (NumberFormatException e) {
            System.err.println("-permittedZookeeperFailures needs a numeric value argument.");
            printUsageAndExit();
          }
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
    if (this.permittedFailures != 0 && !this.zookeeperMode) {
      System.err.println("-permittedZookeeperFailures requires -zookeeper mode.");
      printUsageAndExit();
    }
    if (!this.configuredReadTableTimeouts.isEmpty() && (this.regionServerMode || this.zookeeperMode)) {
      System.err.println("-readTableTimeouts can only be configured in region mode.");
      printUsageAndExit();
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
          monitorThread = new Thread(monitor, "CanaryMonitor-" + System.currentTimeMillis());
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

  public Map<String, String> getReadFailures()  {
    return sink.getReadFailures();
  }

  public Map<String, String> getWriteFailures()  {
    return sink.getWriteFailures();
  }

  private void printUsageAndExit() {
    System.err.println(
      "Usage: canary [OPTIONS] [<TABLE1> [<TABLE2]...] | [<REGIONSERVER1> [<REGIONSERVER2]..]");
    System.err.println("Where [OPTIONS] are:");
    System.err.println(" -h,-help        show this help and exit.");
    System.err.println(" -regionserver   set 'regionserver mode'; gets row from random region on " +
        "server");
    System.err.println(" -allRegions     get from ALL regions when 'regionserver mode', not just " +
        "random one.");
    System.err.println(" -zookeeper      set 'zookeeper mode'; grab zookeeper.znode.parent on " +
        "each ensemble member");
    System.err.println(" -permittedZookeeperFailures <N>     Ignore first N failures when attempting to " +
        "connect to individual zookeeper nodes in the ensemble");
    System.err.println(" -daemon         continuous check at defined intervals.");
    System.err.println(" -interval <N>   interval between checks in seconds");
    System.err.println(" -e              consider table/regionserver argument as regular " +
        "expression");
    System.err.println(" -f <B>          exit on first error; default=true");
    System.err.println(" -failureAsError treat read/write failure as error");
    System.err.println(" -t <N>          timeout for canary-test run; default=600000ms");
    System.err.println(" -writeSniffing  enable write sniffing");
    System.err.println(" -writeTable     the table used for write sniffing; default=hbase:canary");
    System.err.println(" -writeTableTimeout <N>  timeout for writeTable; default=600000ms");
    System.err.println(" -readTableTimeouts <tableName>=<read timeout>," +
        "<tableName>=<read timeout>,...");
    System.err.println("                comma-separated list of table read timeouts " +
        "(no spaces);");
    System.err.println("                logs 'ERROR' if takes longer. default=600000ms");
    System.err.println(" -permittedZookeeperFailures <N>  Ignore first N failures attempting to ");
    System.err.println("                connect to individual zookeeper nodes in ensemble");
    System.err.println("");
    System.err.println(" -D<configProperty>=<value> to assign or override configuration params");
    System.err.println(" -Dhbase.canary.read.raw.enabled=<true/false> Set to enable/disable " +
        "raw scan; default=false");
    System.err.println("");
    System.err.println("Canary runs in one of three modes: region (default), regionserver, or " +
        "zookeeper.");
    System.err.println("To sniff/probe all regions, pass no arguments.");
    System.err.println("To sniff/probe all regions of a table, pass tablename.");
    System.err.println("To sniff/probe regionservers, pass -regionserver, etc.");
    System.err.println("See http://hbase.apache.org/book.html#_canary for Canary documentation.");
    System.exit(USAGE_EXIT_CODE);
  }

  Sink getSink(Configuration configuration, Class clazz) {
    // In test context, this.sink might be set. Use it if non-null. For testing.
    return this.sink != null? this.sink:
        (Sink)ReflectionUtils.newInstance(configuration.getClass("hbase.canary.sink.class",
            clazz, Sink.class));
  }

  /**
   * A Factory method for {@link Monitor}.
   * Makes a RegionServerMonitor, or a ZooKeeperMonitor, or a RegionMonitor.
   * @param index a start index for monitor target
   * @param args args passed from user
   * @return a Monitor instance
   */
  public Monitor newMonitor(final Connection connection, int index, String[] args) {
    Monitor monitor = null;
    String[] monitorTargets = null;

    if (index >= 0) {
      int length = args.length - index;
      monitorTargets = new String[length];
      System.arraycopy(args, index, monitorTargets, 0, length);
    }

    if (this.regionServerMode) {
      monitor =
          new RegionServerMonitor(connection, monitorTargets, this.useRegExp,
              getSink(connection.getConfiguration(), RegionServerStdOutSink.class),
              this.executor, this.regionServerAllRegions,
              this.treatFailureAsError, this.permittedFailures);
    } else if (this.zookeeperMode) {
      monitor =
          new ZookeeperMonitor(connection, monitorTargets, this.useRegExp,
              getSink(connection.getConfiguration(), ZookeeperStdOutSink.class),
              this.executor, this.treatFailureAsError, this.permittedFailures);
    } else {
      monitor =
          new RegionMonitor(connection, monitorTargets, this.useRegExp,
              getSink(connection.getConfiguration(), RegionStdOutSink.class),
              this.executor, this.writeSniffing,
              this.writeTableName, this.treatFailureAsError, this.configuredReadTableTimeouts,
              this.configuredWriteTableTimeout, this.permittedFailures);
    }
    return monitor;
  }

  /**
   * A Monitor super-class can be extended by users
   */
  public static abstract class Monitor implements Runnable, Closeable {
    protected Connection connection;
    protected Admin admin;
    /**
     * 'Target' dependent on 'mode'. Could be Tables or RegionServers or ZNodes.
     * Passed on the command-line as arguments.
     */
    protected String[] targets;
    protected boolean useRegExp;
    protected boolean treatFailureAsError;
    protected boolean initialized = false;

    protected boolean done = false;
    protected int errorCode = 0;
    protected long allowedFailures = 0;
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
          (sink.getReadFailureCount() > allowedFailures || sink.getWriteFailureCount() > allowedFailures)) {
        LOG.error("Too many failures detected, treating failure as error, failing the Canary.");
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
        ExecutorService executor, boolean treatFailureAsError, long allowedFailures) {
      if (null == connection) throw new IllegalArgumentException("connection shall not be null");

      this.connection = connection;
      this.targets = monitorTargets;
      this.useRegExp = useRegExp;
      this.treatFailureAsError = treatFailureAsError;
      this.sink = sink;
      this.executor = executor;
      this.allowedFailures = allowedFailures;
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

  /**
   * A monitor for region mode.
   */
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
    private boolean rawScanEnabled;

    /**
     * This is a timeout per table. If read of each region in the table aggregated takes longer
     * than what is configured here, we log an ERROR rather than just an INFO.
     */
    private HashMap<String, Long> configuredReadTableTimeouts;

    private long configuredWriteTableTimeout;

    public RegionMonitor(Connection connection, String[] monitorTargets, boolean useRegExp,
        Sink sink, ExecutorService executor, boolean writeSniffing, TableName writeTableName,
        boolean treatFailureAsError, HashMap<String, Long> configuredReadTableTimeouts,
        long configuredWriteTableTimeout, long allowedFailures) {
      super(connection, monitorTargets, useRegExp, sink, executor, treatFailureAsError,
              allowedFailures);
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
      this.rawScanEnabled = conf.getBoolean(HConstants.HBASE_CANARY_READ_RAW_SCAN_KEY, false);
      this.configuredReadTableTimeouts = new HashMap<>(configuredReadTableTimeouts);
      this.configuredWriteTableTimeout = configuredWriteTableTimeout;
    }

    private RegionStdOutSink getSink() {
      if (!(sink instanceof RegionStdOutSink)) {
        throw new RuntimeException("Can only write to Region sink");
      }
      return ((RegionStdOutSink) sink);
    }

    @Override
    public void run() {
      if (this.initAdmin()) {
        try {
          List<Future<Void>> taskFutures = new LinkedList<>();
          RegionStdOutSink regionSink = this.getSink();
          if (this.targets != null && this.targets.length > 0) {
            String[] tables = generateMonitorTables(this.targets);
            // Check to see that each table name passed in the -readTableTimeouts argument is also
            // passed as a monitor target.
            if (!new HashSet<>(Arrays.asList(tables)).
                containsAll(this.configuredReadTableTimeouts.keySet())) {
              LOG.error("-readTableTimeouts can only specify read timeouts for monitor targets " +
                  "passed via command line.");
              this.errorCode = USAGE_EXIT_CODE;
              return;
            }
            this.initialized = true;
            for (String table : tables) {
              LongAdder readLatency = regionSink.initializeAndGetReadLatencyForTable(table);
              taskFutures.addAll(Canary.sniff(admin, regionSink, table, executor, TaskType.READ,
                this.rawScanEnabled, readLatency));
            }
          } else {
            taskFutures.addAll(sniff(TaskType.READ, regionSink));
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
            regionSink.initializeWriteLatency();
            LongAdder writeTableLatency = regionSink.getWriteLatency();
            taskFutures.addAll(Canary.sniff(admin, regionSink, admin.getDescriptor(writeTableName),
              executor, TaskType.WRITE, this.rawScanEnabled, writeTableLatency));
          }

          for (Future<Void> future : taskFutures) {
            try {
              future.get();
            } catch (ExecutionException e) {
              LOG.error("Sniff region failed!", e);
            }
          }
          Map<String, LongAdder> actualReadTableLatency = regionSink.getReadLatencyMap();
          for (Map.Entry<String, Long> entry : configuredReadTableTimeouts.entrySet()) {
            String tableName = entry.getKey();
            if (actualReadTableLatency.containsKey(tableName)) {
              Long actual = actualReadTableLatency.get(tableName).longValue();
              Long configured = entry.getValue();
              if (actual > configured) {
                LOG.error("Read operation for {} took {}ms (Configured read timeout {}ms.",
                    tableName, actual, configured);
              } else {
                LOG.info("Read operation for {} took {}ms (Configured read timeout {}ms.",
                    tableName, actual, configured);
              }
            } else {
              LOG.error("Read operation for {} failed!", tableName);
            }
          }
          if (this.writeSniffing) {
            String writeTableStringName = this.writeTableName.getNameAsString();
            long actualWriteLatency = regionSink.getWriteLatency().longValue();
            LOG.info("Write operation for {} took {}ms. Configured write timeout {}ms.",
                writeTableStringName, actualWriteLatency, this.configuredWriteTableTimeout);
            // Check that the writeTable write operation latency does not exceed the configured timeout.
            if (actualWriteLatency > this.configuredWriteTableTimeout) {
              LOG.error("Write operation for {} exceeded the configured write timeout.",
                  writeTableStringName);
            }
          }
        } catch (Exception e) {
          LOG.error("Run regionMonitor failed", e);
          this.errorCode = ERROR_EXIT_CODE;
        } finally {
          this.done = true;
        }
      }
      this.done = true;
    }

    /**
     * @return List of tables to use in test.
     */
    private String[] generateMonitorTables(String[] monitorTargets) throws IOException {
      String[] returnTables = null;

      if (this.useRegExp) {
        Pattern pattern = null;
        TableDescriptor[] tds = null;
        Set<String> tmpTables = new TreeSet<>();
        try {
          LOG.debug(String.format("reading list of tables"));
          tds = this.admin.listTables(pattern);
          if (tds == null) {
            tds = new TableDescriptor[0];
          }
          for (String monitorTarget : monitorTargets) {
            pattern = Pattern.compile(monitorTarget);
            for (TableDescriptor td : tds) {
              if (pattern.matcher(td.getTableName().getNameAsString()).matches()) {
                tmpTables.add(td.getTableName().getNameAsString());
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
     * Canary entry point to monitor all the tables.
     */
    private List<Future<Void>> sniff(TaskType taskType, RegionStdOutSink regionSink)
        throws Exception {
      LOG.debug("Reading list of tables");
      List<Future<Void>> taskFutures = new LinkedList<>();
      for (TableDescriptor td: admin.listTableDescriptors()) {
        if (admin.isTableEnabled(td.getTableName()) &&
            (!td.getTableName().equals(writeTableName))) {
          LongAdder readLatency =
              regionSink.initializeAndGetReadLatencyForTable(td.getTableName().getNameAsString());
          taskFutures.addAll(Canary.sniff(admin, sink, td, executor, taskType, this.rawScanEnabled,
              readLatency));
        }
      }
      return taskFutures;
    }

    private void checkWriteTableDistribution() throws IOException {
      if (!admin.tableExists(writeTableName)) {
        int numberOfServers =
            admin.getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS)).getLiveServerMetrics().size();
        if (numberOfServers == 0) {
          throw new IllegalStateException("No live regionservers");
        }
        createWriteTable(numberOfServers);
      }

      if (!admin.isTableEnabled(writeTableName)) {
        admin.enableTable(writeTableName);
      }

      ClusterMetrics status =
          admin.getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS, Option.MASTER));
      int numberOfServers = status.getLiveServerMetrics().size();
      if (status.getLiveServerMetrics().containsKey(status.getMasterName())) {
        numberOfServers -= 1;
      }

      List<Pair<RegionInfo, ServerName>> pairs =
          MetaTableAccessor.getTableRegionsAndLocations(connection, writeTableName);
      int numberOfRegions = pairs.size();
      if (numberOfRegions < numberOfServers * regionsLowerLimit
          || numberOfRegions > numberOfServers * regionsUpperLimit) {
        admin.disableTable(writeTableName);
        admin.deleteTable(writeTableName);
        createWriteTable(numberOfServers);
      }
      HashSet<ServerName> serverSet = new HashSet<>();
      for (Pair<RegionInfo, ServerName> pair : pairs) {
        serverSet.add(pair.getSecond());
      }
      int numberOfCoveredServers = serverSet.size();
      if (numberOfCoveredServers < numberOfServers) {
        admin.balancer();
      }
    }

    private void createWriteTable(int numberOfServers) throws IOException {
      int numberOfRegions = (int)(numberOfServers * regionsLowerLimit);
      LOG.info("Number of live regionservers {}, pre-splitting the canary table into {} regions " +
        "(current lower limit of regions per server is {} and you can change it with config {}).",
          numberOfServers, numberOfRegions, regionsLowerLimit,
          HConstants.HBASE_CANARY_WRITE_PERSERVER_REGIONS_LOWERLIMIT_KEY);
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
  private static List<Future<Void>> sniff(final Admin admin, final Sink sink, String tableName,
      ExecutorService executor, TaskType taskType, boolean rawScanEnabled, LongAdder readLatency)
      throws Exception {
    LOG.debug("Checking table is enabled and getting table descriptor for table {}", tableName);
    if (admin.isTableEnabled(TableName.valueOf(tableName))) {
      return Canary.sniff(admin, sink, admin.getDescriptor(TableName.valueOf(tableName)),
        executor, taskType, rawScanEnabled, readLatency);
    } else {
      LOG.warn("Table {} is not enabled", tableName);
    }
    return new LinkedList<>();
  }

  /*
   * Loops over regions of this table, and outputs information about the state.
   */
  private static List<Future<Void>> sniff(final Admin admin, final Sink sink,
      TableDescriptor tableDesc, ExecutorService executor, TaskType taskType,
      boolean rawScanEnabled, LongAdder rwLatency) throws Exception {
    LOG.debug("Reading list of regions for table {}", tableDesc.getTableName());
    try (Table table = admin.getConnection().getTable(tableDesc.getTableName())) {
      List<RegionTask> tasks = new ArrayList<>();
      try (RegionLocator regionLocator =
               admin.getConnection().getRegionLocator(tableDesc.getTableName())) {
        for (HRegionLocation location: regionLocator.getAllRegionLocations()) {
          ServerName rs = location.getServerName();
          RegionInfo region = location.getRegion();
          tasks.add(new RegionTask(admin.getConnection(), region, rs, (RegionStdOutSink)sink,
              taskType, rawScanEnabled, rwLatency));
        }
        return executor.invokeAll(tasks);
      }
    } catch (TableNotFoundException e) {
      return Collections.EMPTY_LIST;
    }
  }

  //  monitor for zookeeper mode
  private static class ZookeeperMonitor extends Monitor {
    private List<String> hosts;
    private final String znode;
    private final int timeout;

    protected ZookeeperMonitor(Connection connection, String[] monitorTargets, boolean useRegExp,
        Sink sink, ExecutorService executor, boolean treatFailureAsError, long allowedFailures)  {
      super(connection, monitorTargets, useRegExp,
          sink, executor, treatFailureAsError, allowedFailures);
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
      if (allowedFailures > (hosts.size() - 1) / 2) {
        LOG.warn("Confirm allowable number of failed ZooKeeper nodes, as quorum will " +
                        "already be lost. Setting of {} failures is unexpected for {} ensemble size.",
                allowedFailures, hosts.size());
      }
    }

    @Override public void run() {
      List<ZookeeperTask> tasks = Lists.newArrayList();
      ZookeeperStdOutSink zkSink = null;
      try {
        zkSink = this.getSink();
      } catch (RuntimeException e) {
        LOG.error("Run ZooKeeperMonitor failed!", e);
        this.errorCode = ERROR_EXIT_CODE;
      }
      this.initialized = true;
      for (final String host : hosts) {
        tasks.add(new ZookeeperTask(connection, host, znode, timeout, zkSink));
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


  /**
   * A monitor for regionserver mode
   */
  private static class RegionServerMonitor extends Monitor {
    private boolean allRegions;

    public RegionServerMonitor(Connection connection, String[] monitorTargets, boolean useRegExp,
        Sink sink, ExecutorService executor, boolean allRegions,
        boolean treatFailureAsError, long allowedFailures) {
      super(connection, monitorTargets, useRegExp, sink, executor, treatFailureAsError,
          allowedFailures);
      this.allRegions = allRegions;
    }

    private RegionServerStdOutSink getSink() {
      if (!(sink instanceof RegionServerStdOutSink)) {
        throw new RuntimeException("Can only write to regionserver sink");
      }
      return ((RegionServerStdOutSink) sink);
    }

    @Override
    public void run() {
      if (this.initAdmin() && this.checkNoTableNames()) {
        RegionServerStdOutSink regionServerSink = null;
        try {
          regionServerSink = this.getSink();
        } catch (RuntimeException e) {
          LOG.error("Run RegionServerMonitor failed!", e);
          this.errorCode = ERROR_EXIT_CODE;
        }
        Map<String, List<RegionInfo>> rsAndRMap = this.filterRegionServerByName();
        this.initialized = true;
        this.monitorRegionServers(rsAndRMap, regionServerSink);
      }
      this.done = true;
    }

    private boolean checkNoTableNames() {
      List<String> foundTableNames = new ArrayList<>();
      TableName[] tableNames = null;
      LOG.debug("Reading list of tables");
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
      return foundTableNames.isEmpty();
    }

    private void monitorRegionServers(Map<String, List<RegionInfo>> rsAndRMap, RegionServerStdOutSink regionServerSink) {
      List<RegionServerTask> tasks = new ArrayList<>();
      Map<String, AtomicLong> successMap = new HashMap<>();
      Random rand = new Random();
      for (Map.Entry<String, List<RegionInfo>> entry : rsAndRMap.entrySet()) {
        String serverName = entry.getKey();
        AtomicLong successes = new AtomicLong(0);
        successMap.put(serverName, successes);
        if (entry.getValue().isEmpty()) {
          LOG.error("Regionserver not serving any regions - {}", serverName);
        } else if (this.allRegions) {
          for (RegionInfo region : entry.getValue()) {
            tasks.add(new RegionServerTask(this.connection,
                serverName,
                region,
                regionServerSink,
                successes));
          }
        } else {
          // random select a region if flag not set
          RegionInfo region = entry.getValue().get(rand.nextInt(entry.getValue().size()));
          tasks.add(new RegionServerTask(this.connection,
              serverName,
              region,
              regionServerSink,
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
          for (Map.Entry<String, List<RegionInfo>> entry : rsAndRMap.entrySet()) {
            String serverName = entry.getKey();
            LOG.info("Successfully read {} regions out of {} on regionserver {}",
                successMap.get(serverName), entry.getValue().size(), serverName);
          }
        }
      } catch (InterruptedException e) {
        this.errorCode = ERROR_EXIT_CODE;
        LOG.error("Sniff regionserver interrupted!", e);
      }
    }

    private Map<String, List<RegionInfo>> filterRegionServerByName() {
      Map<String, List<RegionInfo>> regionServerAndRegionsMap = this.getAllRegionServerByName();
      regionServerAndRegionsMap = this.doFilterRegionServerByName(regionServerAndRegionsMap);
      return regionServerAndRegionsMap;
    }

    private Map<String, List<RegionInfo>> getAllRegionServerByName() {
      Map<String, List<RegionInfo>> rsAndRMap = new HashMap<>();
      try {
        LOG.debug("Reading list of tables and locations");
        List<TableDescriptor> tableDescs = this.admin.listTableDescriptors();
        List<RegionInfo> regions = null;
        for (TableDescriptor tableDesc: tableDescs) {
          try (RegionLocator regionLocator =
                   this.admin.getConnection().getRegionLocator(tableDesc.getTableName())) {
            for (HRegionLocation location : regionLocator.getAllRegionLocations()) {
              ServerName rs = location.getServerName();
              String rsName = rs.getHostname();
              RegionInfo r = location.getRegion();
              if (rsAndRMap.containsKey(rsName)) {
                regions = rsAndRMap.get(rsName);
              } else {
                regions = new ArrayList<>();
                rsAndRMap.put(rsName, regions);
              }
              regions.add(r);
            }
          }
        }

        // get any live regionservers not serving any regions
        for (ServerName rs: this.admin.getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS))
          .getLiveServerMetrics().keySet()) {
          String rsName = rs.getHostname();
          if (!rsAndRMap.containsKey(rsName)) {
            rsAndRMap.put(rsName, Collections.<RegionInfo> emptyList());
          }
        }
      } catch (IOException e) {
        LOG.error("Get HTables info failed", e);
        this.errorCode = INIT_ERROR_EXIT_CODE;
      }
      return rsAndRMap;
    }

    private Map<String, List<RegionInfo>> doFilterRegionServerByName(
        Map<String, List<RegionInfo>> fullRsAndRMap) {

      Map<String, List<RegionInfo>> filteredRsAndRMap = null;

      if (this.targets != null && this.targets.length > 0) {
        filteredRsAndRMap = new HashMap<>();
        Pattern pattern = null;
        Matcher matcher = null;
        boolean regExpFound = false;
        for (String rsName : this.targets) {
          if (this.useRegExp) {
            regExpFound = false;
            pattern = Pattern.compile(rsName);
            for (Map.Entry<String, List<RegionInfo>> entry : fullRsAndRMap.entrySet()) {
              matcher = pattern.matcher(entry.getKey());
              if (matcher.matches()) {
                filteredRsAndRMap.put(entry.getKey(), entry.getValue());
                regExpFound = true;
              }
            }
            if (!regExpFound) {
              LOG.info("No RegionServerInfo found, regionServerPattern {}", rsName);
            }
          } else {
            if (fullRsAndRMap.containsKey(rsName)) {
              filteredRsAndRMap.put(rsName, fullRsAndRMap.get(rsName));
            } else {
              LOG.info("No RegionServerInfo found, regionServerName {}", rsName);
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

    // Loading the generic options to conf
    new GenericOptionsParser(conf, args);

    int numThreads = conf.getInt("hbase.canary.threads.num", MAX_THREADS_NUM);
    LOG.info("Execution thread count={}", numThreads);

    int exitCode = 0;
    ExecutorService executor = new ScheduledThreadPoolExecutor(numThreads);
    try {
      exitCode = ToolRunner.run(conf, new Canary(executor), args);
    } finally {
      executor.shutdown();
    }
    System.exit(exitCode);
  }
}
