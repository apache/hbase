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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.time.StopWatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * HBase Canary Tool, that that can be used to do
 * "canary monitoring" of a running HBase cluster.
 *
 * Here are two modes
 * 1. region mode - Foreach region tries to get one row per column family
 * and outputs some information about failure or latency.
 *
 * 2. regionserver mode - Foreach regionserver tries to get one row from one table
 * selected randomly and outputs some information about failure or latency.
 */
public final class Canary implements Tool {
  // Sink interface used by the canary to outputs information
  public interface Sink {
    public void publishReadFailure(HRegionInfo region, Exception e);
    public void publishReadFailure(HRegionInfo region, HColumnDescriptor column, Exception e);
    public void publishReadTiming(HRegionInfo region, HColumnDescriptor column, long msTime);
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
    @Override
    public void publishReadFailure(HRegionInfo region, Exception e) {
      LOG.error(String.format("read from region %s failed", region.getRegionNameAsString()), e);
    }

    @Override
    public void publishReadFailure(HRegionInfo region, HColumnDescriptor column, Exception e) {
      LOG.error(String.format("read from region %s column family %s failed",
                region.getRegionNameAsString(), column.getNameAsString()), e);
    }

    @Override
    public void publishReadTiming(HRegionInfo region, HColumnDescriptor column, long msTime) {
      LOG.info(String.format("read from region %s column family %s in %dms",
               region.getRegionNameAsString(), column.getNameAsString(), msTime));
    }
  }
  // a ExtendedSink implementation
  public static class RegionServerStdOutSink extends StdOutSink implements ExtendedSink {

    @Override
    public void publishReadFailure(String table, String server) {
      LOG.error(String.format("Read from table:%s on region server:%s", table, server));
    }

    @Override
    public void publishReadTiming(String table, String server, long msTime) {
      LOG.info(String.format("Read from table:%s on region server:%s in %dms",
          table, server, msTime));
    }
  }

  /**
   * For each column family of the region tries to get one row and outputs the latency, or the
   * failure.
   */
  static class RegionTask implements Callable<Void> {
    private Connection connection;
    private HRegionInfo region;
    private Sink sink;

    RegionTask(Connection connection, HRegionInfo region, Sink sink) {
      this.connection = connection;
      this.region = region;
      this.sink = sink;
    }

    @Override
    public Void call() {
      Table table = null;
      HTableDescriptor tableDesc = null;
      try {
        table = connection.getTable(region.getTable());
        tableDesc = table.getTableDescriptor();
      } catch (IOException e) {
        LOG.debug("sniffRegion failed", e);
        sink.publishReadFailure(region, e);
        return null;
      } finally {
        if (table != null) {
          try {
            table.close();
          } catch (IOException e) {
          }
        }
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
          if (table != null) {
            try {
              table.close();
            } catch (IOException e) {
            }
          }
          scan = null;
          get = null;
          startKey = null;
        }
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

    RegionServerTask(Connection connection, String serverName, HRegionInfo region,
        ExtendedSink sink) {
      this.connection = connection;
      this.serverName = serverName;
      this.region = region;
      this.sink = sink;
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
          stopWatch.start();
          ResultScanner s = table.getScanner(scan);
          s.close();
          stopWatch.stop();
        }
        sink.publishReadTiming(tableName.getNameAsString(), serverName, stopWatch.getTime());
      } catch (TableNotFoundException tnfe) {
        // This is ignored because it doesn't imply that the regionserver is dead
      } catch (TableNotEnabledException tnee) {
        // This is considered a success since we got a response.
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

  private static final long DEFAULT_INTERVAL = 6000;

  private static final long DEFAULT_TIMEOUT = 600000; // 10 mins

  private static final int MAX_THREADS_NUM = 16; // #threads to contact regions

  private static final Log LOG = LogFactory.getLog(Canary.class);

  private Configuration conf = null;
  private long interval = 0;
  private Sink sink = null;

  private boolean useRegExp;
  private long timeout = DEFAULT_TIMEOUT;
  private boolean failOnError = true;
  private boolean regionServerMode = false;
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

  @Override
  public int run(String[] args) throws Exception {
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
        } else if(cmd.equals("-regionserver")) {
          this.regionServerMode = true;
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

    // Start to prepare the stuffs
    Monitor monitor = null;
    Thread monitorThread = null;
    long startTime = 0;
    long currentTimeLength = 0;
    // Get a connection to use in below.
    // try-with-resources jdk7 construct. See
    // http://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html
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
                System.exit(monitor.errorCode);
              } else {
                System.exit(INIT_ERROR_EXIT_CODE);
              }
            }
            currentTimeLength = System.currentTimeMillis() - startTime;
            if (currentTimeLength > this.timeout) {
              LOG.error("The monitor is running too long (" + currentTimeLength
                  + ") after timeout limit:" + this.timeout
                  + " will be killed itself !!");
              if (monitor.initialized) {
                System.exit(TIMEOUT_ERROR_EXIT_CODE);
              } else {
                System.exit(INIT_ERROR_EXIT_CODE);
              }
              break;
            }
          }

          if (this.failOnError && monitor.hasError()) {
            monitorThread.interrupt();
            System.exit(monitor.errorCode);
          }
        } finally {
          if (monitor != null) monitor.close();
        }

        Thread.sleep(interval);
      } while (interval > 0);
    } // try-with-resources close

    return(monitor.errorCode);
  }

  private void printUsageAndExit() {
    System.err.printf(
      "Usage: bin/hbase %s [opts] [table1 [table2]...] | [regionserver1 [regionserver2]..]%n",
        getClass().getName());
    System.err.println(" where [opts] are:");
    System.err.println("   -help          Show this help and exit.");
    System.err.println("   -regionserver  replace the table argument to regionserver,");
    System.err.println("      which means to enable regionserver mode");
    System.err.println("   -daemon        Continuous check at defined intervals.");
    System.err.println("   -interval <N>  Interval between checks (sec)");
    System.err.println("   -e             Use region/regionserver as regular expression");
    System.err.println("      which means the region/regionserver is regular expression pattern");
    System.err.println("   -f <B>         stop whole program if first error occurs," +
        " default is true");
    System.err.println("   -t <N>         timeout for a check, default is 600000 (milisecs)");
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
              (ExtendedSink) this.sink, this.executor);
    } else {
      monitor =
          new RegionMonitor(connection, monitorTargets, this.useRegExp, this.sink, this.executor);
    }
    return monitor;
  }

  // a Monitor super-class can be extended by users
  public static abstract class Monitor implements Runnable, Closeable {

    protected Connection connection;
    protected Admin admin;
    protected String[] targets;
    protected boolean useRegExp;
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

    @Override
    public void close() throws IOException {
      if (this.admin != null) this.admin.close();
    }

    protected Monitor(Connection connection, String[] monitorTargets, boolean useRegExp, Sink sink,
        ExecutorService executor) {
      if (null == connection) throw new IllegalArgumentException("connection shall not be null");

      this.connection = connection;
      this.targets = monitorTargets;
      this.useRegExp = useRegExp;
      this.sink = sink;
      this.executor = executor;
    }

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

    public RegionMonitor(Connection connection, String[] monitorTargets, boolean useRegExp,
        Sink sink, ExecutorService executor) {
      super(connection, monitorTargets, useRegExp, sink, executor);
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
              taskFutures.addAll(Canary.sniff(admin, sink, table, executor));
            }
          } else {
            taskFutures.addAll(sniff());
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
          for (String monitorTarget : monitorTargets) {
            pattern = Pattern.compile(monitorTarget);
            tds = this.admin.listTables(pattern);
            if (tds != null) {
              for (HTableDescriptor td : tds) {
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
    private List<Future<Void>> sniff() throws Exception {
      List<Future<Void>> taskFutures = new LinkedList<Future<Void>>();
      for (HTableDescriptor table : admin.listTables()) {
        if (admin.isTableEnabled(table.getTableName())) {
          taskFutures.addAll(Canary.sniff(admin, sink, table, executor));
        }
      }
      return taskFutures;
    }
  }

  /**
   * Canary entry point for specified table.
   * @throws Exception
   */
  public static void sniff(final Admin admin, TableName tableName) throws Exception {
    List<Future<Void>> taskFutures =
        Canary.sniff(admin, new StdOutSink(), tableName.getNameAsString(),
          new ScheduledThreadPoolExecutor(1));
    for (Future<Void> future : taskFutures) {
      future.get();
    }
  }

  /**
   * Canary entry point for specified table.
   * @throws Exception
   */
  private static List<Future<Void>> sniff(final Admin admin, final Sink sink, String tableName,
      ExecutorService executor) throws Exception {
    if (admin.isTableEnabled(TableName.valueOf(tableName))) {
      return Canary.sniff(admin, sink, admin.getTableDescriptor(TableName.valueOf(tableName)),
        executor);
    } else {
      LOG.warn(String.format("Table %s is not enabled", tableName));
    }
    return new LinkedList<Future<Void>>();
  }

  /*
   * Loops over regions that owns this table, and output some information abouts the state.
   */
  private static List<Future<Void>> sniff(final Admin admin, final Sink sink,
      HTableDescriptor tableDesc, ExecutorService executor) throws Exception {
    Table table = null;
    try {
      table = admin.getConnection().getTable(tableDesc.getTableName());
    } catch (TableNotFoundException e) {
      return new ArrayList<Future<Void>>();
    }
    List<RegionTask> tasks = new ArrayList<RegionTask>();
    try {
      for (HRegionInfo region : admin.getTableRegions(tableDesc.getTableName())) {
        tasks.add(new RegionTask(admin.getConnection(), region, sink));
      }
    } finally {
      table.close();
    }
    return executor.invokeAll(tasks);
  }
  // a monitor for regionserver mode
  private static class RegionServerMonitor extends Monitor {

    public RegionServerMonitor(Connection connection, String[] monitorTargets, boolean useRegExp,
        ExtendedSink sink, ExecutorService executor) {
      super(connection, monitorTargets, useRegExp, sink, executor);
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
      Random rand =new Random();
      // monitor one region on every region server
      for (Map.Entry<String, List<HRegionInfo>> entry : rsAndRMap.entrySet()) {
        String serverName = entry.getKey();
        // random select a region
        HRegionInfo region = entry.getValue().get(rand.nextInt(entry.getValue().size()));
        tasks.add(new RegionServerTask(this.connection, serverName, region, getSink()));
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
      } catch (InterruptedException e) {
        this.errorCode = ERROR_EXIT_CODE;
        LOG.error("Sniff regionserver failed!", e);
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
    int numThreads = conf.getInt("hbase.canary.threads.num", MAX_THREADS_NUM);
    ExecutorService executor = new ScheduledThreadPoolExecutor(numThreads);

    Class<? extends Sink> sinkClass =
        conf.getClass("hbase.canary.sink.class", StdOutSink.class, Sink.class);
    Sink sink = ReflectionUtils.newInstance(sinkClass);

    int exitCode = ToolRunner.run(conf, new Canary(executor, sink), args);
    choreService.shutdown();
    executor.shutdown();
    System.exit(exitCode);
  }
}
