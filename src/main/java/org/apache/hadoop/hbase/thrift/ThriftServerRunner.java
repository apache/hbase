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

package org.apache.hadoop.hbase.thrift;

import static org.apache.hadoop.hbase.util.Bytes.getBytes;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.RegionException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.thrift.CallQueue.Call;
import org.apache.hadoop.hbase.thrift.generated.AlreadyExists;
import org.apache.hadoop.hbase.thrift.generated.BatchMutation;
import org.apache.hadoop.hbase.thrift.generated.ColumnDescriptor;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.thrift.generated.IOError;
import org.apache.hadoop.hbase.thrift.generated.IllegalArgument;
import org.apache.hadoop.hbase.thrift.generated.Mutation;
import org.apache.hadoop.hbase.thrift.generated.TCell;
import org.apache.hadoop.hbase.thrift.generated.TRegionInfo;
import org.apache.hadoop.hbase.thrift.generated.TRowResult;
import org.apache.hadoop.hbase.thrift.generated.TScan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportFactory;

import com.google.common.base.Joiner;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * ThriftServerRunner - this class starts up a Thrift server which implements
 * the Hbase API specified in the Hbase.thrift IDL file.
 */
public class ThriftServerRunner implements Runnable {

  private static final Log LOG = LogFactory.getLog(ThriftServerRunner.class);

  private final Configuration conf;
  private final String confKeyPrefix;
  private final Hbase.Iface handler;
  private final ThriftMetrics metrics;

  private final int listenPort;

  volatile TServer tserver;

  private static ImplType DEFAULT_SERVER_TYPE = ImplType.THREADED_SELECTOR;

  private static String NOT_SUPPORTED_BY_PROXY_MSG = "Not supported by Thrift proxy";
  
  /** An enum of server implementation selections */
  enum ImplType {
    HS_HA("hsha", true, THsHaServer.class, false),
    NONBLOCKING("nonblocking", true, TNonblockingServer.class, false),
    THREAD_POOL("threadpool", false, TBoundedThreadPoolServer.class, true),
    THREADED_SELECTOR(
        "threadedselector", true, TThreadedSelectorServer.class, false);

    public static final ImplType DEFAULT = THREAD_POOL;

    final String option;
    final boolean isAlwaysFramed;
    final Class<? extends TServer> serverClass;
    final boolean canSpecifyBindIP;

    ImplType(String option, boolean isAlwaysFramed,
        Class<? extends TServer> serverClass, boolean canSpecifyBindIP) {
      this.option = option;
      this.isAlwaysFramed = isAlwaysFramed;
      this.serverClass = serverClass;
      this.canSpecifyBindIP = canSpecifyBindIP;
    }

    /**
     * @return <code>-option</code> so we can get the list of options from
     *         {@link #values()}
     */
    @Override
    public String toString() {
      return "-" + option;
    }

    String getDescription() {
      StringBuilder sb = new StringBuilder("Use the " +
          serverClass.getSimpleName());
      if (isAlwaysFramed) {
        sb.append(" This implies the framed transport.");
      }
      if (this == DEFAULT) {
        sb.append("This is the default.");
      }
      return sb.toString();
    }

    static OptionGroup createOptionGroup() {
      OptionGroup group = new OptionGroup();
      for (ImplType t : values()) {
        group.addOption(new Option(t.option, t.getDescription()));
      }
      return group;
    }

    static ImplType getServerImpl(Configuration conf, String confKeyPrefix) {
      String confType = conf.get(confKeyPrefix + HConstants.THRIFT_SERVER_TYPE_SUFFIX, null);
      if (confType == null) {
        return DEFAULT_SERVER_TYPE;
      }

      for (ImplType t : values()) {
        if (confType.equals(t.option)) {
          return t;
        }
      }
      throw new AssertionError("Unknown Thrift server type specified: " + confType);
    }

    /** Set Thrift server implementation type in the given configuration using command line */
    static void setServerImpl(CommandLine cmd, Configuration conf, String confKeyPrefix) {
      ImplType chosenType = null;
      int numChosen = 0;
      for (ImplType t : values()) {
        if (cmd.hasOption(t.option)) {
          chosenType = t;
          ++numChosen;
        }
      }
      if (numChosen > 1) {
        throw new AssertionError("Not more than one option out of " +
            Arrays.toString(values()) + " command-line options has to be specified");
      }

      if (numChosen == 1) {
        LOG.info("Setting thrift server to " + chosenType.option);
        conf.set(confKeyPrefix + HConstants.THRIFT_SERVER_TYPE_SUFFIX, chosenType.option);
      } else {
        LOG.info("Thrift server type not specified on the command line, proceeding with " +
            "the configured type: " + getServerImpl(conf, confKeyPrefix).option);
      }
    }

    Class<? extends TServer> getServerClass() {
      return serverClass;
    }

    public String simpleClassName() {
      return serverClass.getSimpleName();
    }

    public static List<String> serversThatCannotSpecifyBindIP() {
      List<String> l = new ArrayList<String>();
      for (ImplType t : values()) {
        if (!t.canSpecifyBindIP) {
          l.add(t.simpleClassName());
        }
      }
      return l;
    }

  }

  public ThriftServerRunner(Configuration conf, String confKeyPrefix) throws IOException {
    this(conf, confKeyPrefix, new ThriftServerRunner.HBaseHandler(conf));
  }

  public ThriftServerRunner(Configuration conf, String confKeyPrefix, HBaseHandler handler) {
    this.conf = HBaseConfiguration.create(conf);
    this.confKeyPrefix = confKeyPrefix;

    int defaultPort;
    if (confKeyPrefix.equals(HConstants.THRIFT_PROXY_PREFIX)) {
      defaultPort = HConstants.DEFAULT_THRIFT_PROXY_PORT;
    } else {
      defaultPort = HConstants.DEFAULT_RS_THRIFT_SERVER_PORT;
    }
    this.listenPort = conf.getInt(confKeyPrefix + HConstants.THRIFT_PORT_SUFFIX, defaultPort);

    this.metrics = new ThriftMetrics(listenPort, conf, Hbase.Iface.class);
    handler.initMetrics(metrics);
    this.handler = HbaseHandlerMetricsProxy.newInstance(handler, metrics, conf);
  }

  /*
   * Runs the Thrift server
   */
  @Override
  public void run() {
    try {
      setupServer();
      tserver.serve();
    } catch (Exception e) {
      LOG.fatal("Cannot run ThriftServer", e);
      throw new RuntimeException(e);
    }
  }

  public void shutdown() {
    if (tserver != null) {
      tserver.stop();
      tserver = null;
    }
    metrics.shutdown();
  }

  /**
   * Setting up the thrift TServer
   */
  private void setupServer() throws Exception {
    // Construct correct ProtocolFactory
    TProtocolFactory protocolFactory;
    if (conf.getBoolean(confKeyPrefix + HConstants.THRIFT_COMPACT_SUFFIX, false)) {
      LOG.debug("Using compact protocol");
      protocolFactory = new TCompactProtocol.Factory();
    } else {
      LOG.debug("Using binary protocol");
      protocolFactory = new TBinaryProtocol.Factory();
    }

    Hbase.Processor<Hbase.Iface> processor =
        new Hbase.Processor<Hbase.Iface>(handler);
    ImplType implType = ImplType.getServerImpl(conf, confKeyPrefix);

    // Construct correct TransportFactory
    TTransportFactory transportFactory;
    if (conf.getBoolean(confKeyPrefix + HConstants.THRIFT_FRAMED_SUFFIX, false)
        || implType.isAlwaysFramed) {
      transportFactory = new TFramedTransport.Factory();
      LOG.debug("Using framed transport");
    } else {
      transportFactory = new TTransportFactory();
    }

    final String bindConfKey = confKeyPrefix + HConstants.THRIFT_BIND_SUFFIX;
    if (conf.get(bindConfKey) != null
        && !implType.canSpecifyBindIP) {
      LOG.error("Server types " + Joiner.on(", ").join(
          ImplType.serversThatCannotSpecifyBindIP()) + " don't support IP " +
          "address binding at the moment. See " +
          "https://issues.apache.org/jira/browse/HBASE-2155 for details.");
      throw new RuntimeException(bindConfKey + " not supported with " + implType);
    }

    if (implType == ImplType.HS_HA || implType == ImplType.NONBLOCKING ||
        implType == ImplType.THREADED_SELECTOR) {

      TNonblockingServerTransport serverTransport =
          new TNonblockingServerSocket(listenPort);

      if (implType == ImplType.NONBLOCKING) {
        TNonblockingServer.Args serverArgs =
            new TNonblockingServer.Args(serverTransport);
        serverArgs.processor(processor)
                  .transportFactory(transportFactory)
                  .protocolFactory(protocolFactory);
        tserver = new TNonblockingServer(serverArgs);
      } else if (implType == ImplType.HS_HA) {
        THsHaServer.Args serverArgs = new THsHaServer.Args(serverTransport);
        CallQueue callQueue =
            new CallQueue(new LinkedBlockingQueue<Call>(), metrics);
        ExecutorService executorService = createExecutor(
            callQueue, serverArgs.getWorkerThreads());
        serverArgs.executorService(executorService)
                  .processor(processor)
                  .transportFactory(transportFactory)
                  .protocolFactory(protocolFactory);
        tserver = new THsHaServer(serverArgs);
      } else { // THREADED_SELECTOR
        TThreadedSelectorServer.Args serverArgs =
            new HThreadedSelectorServerArgs(serverTransport, conf, confKeyPrefix);
        CallQueue callQueue =
            new CallQueue(new LinkedBlockingQueue<Call>(), metrics);
        ExecutorService executorService = createExecutor(
            callQueue, serverArgs.getWorkerThreads());
        serverArgs.executorService(executorService)
                  .processor(processor)
                  .transportFactory(transportFactory)
                  .protocolFactory(protocolFactory);
        tserver = new TThreadedSelectorServer(serverArgs);
      }
      LOG.info("starting HBase " + implType.simpleClassName() +
          " server on " + Integer.toString(listenPort));
    } else if (implType == ImplType.THREAD_POOL) {
      // Thread pool server. Get the IP address to bind to.
      InetAddress listenAddress = getBindAddress(conf);

      TServerTransport serverTransport = new TServerSocket(
          new InetSocketAddress(listenAddress, listenPort));

      TBoundedThreadPoolServer.Args serverArgs =
          new TBoundedThreadPoolServer.Args(serverTransport, conf, confKeyPrefix);
      serverArgs.processor(processor)
                .transportFactory(transportFactory)
                .protocolFactory(protocolFactory);
      LOG.info("starting " + ImplType.THREAD_POOL.simpleClassName() + " on "
          + listenAddress + ":" + Integer.toString(listenPort)
          + "; " + serverArgs);
      TBoundedThreadPoolServer tserver =
          new TBoundedThreadPoolServer(serverArgs, metrics);
      this.tserver = tserver;
    } else {
      throw new AssertionError("Unsupported Thrift server implementation: " +
          implType.simpleClassName());
    }

    // A sanity check that we instantiated the right type of server.
    if (tserver.getClass() != implType.serverClass) {
      throw new AssertionError("Expected to create Thrift server class " +
          implType.serverClass.getName() + " but got " +
          tserver.getClass().getName());
    }

  }

  ExecutorService createExecutor(BlockingQueue<Runnable> callQueue,
                                 int workerThreads) {
    ThreadFactoryBuilder tfb = new ThreadFactoryBuilder();
    tfb.setDaemon(true);
    tfb.setNameFormat("thrift-worker-%d");
    return new ThreadPoolExecutor(workerThreads, workerThreads,
            Long.MAX_VALUE, TimeUnit.SECONDS, callQueue, tfb.build());
  }

  private InetAddress getBindAddress(Configuration conf)
      throws UnknownHostException {
    String bindAddressStr =
        conf.get(confKeyPrefix + HConstants.THRIFT_BIND_SUFFIX, HConstants.DEFAULT_HOST);
    return InetAddress.getByName(bindAddressStr);
  }

  /**
   * Retrieve timestamp from the given mutation Thrift object. If the mutation timestamp is not set
   * or is set to {@link HConstants#LATEST_TIMESTAMP}, the default timestamp is used.
   * @param m a mutation object optionally specifying a timestamp
   * @param defaultTimestamp default timestamp to use if the mutation does not specify timestamp
   * @return the effective mutation timestamp
   */
  public static long getMutationTimestamp(Mutation m, long defaultTimestamp) {
    if (!m.isSetTimestamp()) {
      return defaultTimestamp;
    }
    long ts = m.getTimestamp();
    if (ts == HConstants.LATEST_TIMESTAMP) {
      return defaultTimestamp;
    }
    return ts;
  }

  /**
   * The HBaseHandler is a glue object that connects Thrift RPC calls to the
   * HBase client API primarily defined in the HBaseAdmin and HTable objects.
   */
  public static class HBaseHandler implements Hbase.Iface {
    protected Configuration conf;
    protected HBaseAdmin admin = null;
    protected final Log LOG = LogFactory.getLog(this.getClass().getName());

    // nextScannerId and scannerMap are used to manage scanner state
    protected int nextScannerId = 0;
    protected HashMap<Integer, ResultScanner> scannerMap = null;

    protected ThriftMetrics metrics;

    private static ThreadLocal<Map<String, HTable>> threadLocalTables =
        new ThreadLocal<Map<String, HTable>>() {
      @Override
      protected Map<String, HTable> initialValue() {
        return new TreeMap<String, HTable>();
      }
    };

    /**
     * Returns a list of all the column families for a given htable.
     *
     * @param table
     * @return
     * @throws IOException
     */
    byte[][] getAllColumns(HTable table) throws IOException {
      HColumnDescriptor[] cds = table.getTableDescriptor().getColumnFamilies();
      byte[][] columns = new byte[cds.length][];
      for (int i = 0; i < cds.length; i++) {
        columns[i] = Bytes.add(cds[i].getName(),
            KeyValue.COLUMN_FAMILY_DELIM_ARRAY);
      }
      return columns;
    }

    /**
     * Creates and returns an HTable instance from a given table name.
     *
     * @param tableName
     *          name of table
     * @return HTable object
     * @throws IOException
     * @throws IOError
     */
    protected HTable getTable(final byte[] tableName) throws IOError,
        IOException {
      String table = Bytes.toString(tableName);
      Map<String, HTable> tables = threadLocalTables.get();
      if (!tables.containsKey(table)) {
        tables.put(table, new HTable(conf, tableName));
      }
      return tables.get(table);
    }

    protected HTable getTable(final ByteBuffer tableName) throws IOException, IOError {
      return getTable(getBytes(tableName));
    }

    /**
     * Assigns a unique ID to the scanner and adds the mapping to an internal
     * hash-map.
     *
     * @param scanner
     * @return integer scanner id 
     */
    protected synchronized int addScanner(ResultScanner scanner) {
      int id = nextScannerId++;
      scannerMap.put(id, scanner);
      return id;
    }

    /**
     * Returns the scanner associated with the specified ID.
     *
     * @param id
     * @return a Scanner, or null if ID was invalid.
     */
    protected synchronized ResultScanner getScanner(int id) {
      return scannerMap.get(id);
    }

    /**
     * Removes the scanner associated with the specified ID from the internal
     * id->scanner hash-map.
     *
     * @param id
     * @return a Scanner, or null if ID was invalid.
     */
    protected synchronized ResultScanner removeScanner(int id) {
      return scannerMap.remove(id);
    }

    protected HBaseHandler(final Configuration c) throws IOException {
      this.conf = c;
      scannerMap = new HashMap<Integer, ResultScanner>();
    }

    /**
     * Obtain HBaseAdmin. Creates the instance if it is not already created.
     */
    private HBaseAdmin getHBaseAdmin() throws IOException {
      if (admin == null) {
        synchronized (this) {
          if (admin == null) {
            admin = new HBaseAdmin(conf);
          }
        }
      }
      return admin;
    }

    @Override
    public void enableTable(ByteBuffer tableName) throws IOError {
      try{
        getHBaseAdmin().enableTable(getBytes(tableName));
      } catch (IOException e) {
        throw convertIOException(e);
      }
    }

    @Override
    public void disableTable(ByteBuffer tableName) throws IOError{
      try{
        getHBaseAdmin().disableTable(getBytes(tableName));
      } catch (IOException e) {
        throw convertIOException(e);
      }
    }

    @Override
    public boolean isTableEnabled(ByteBuffer tableName) throws IOError {
      try {
        return HTable.isTableEnabled(conf, getBytes(tableName));
      } catch (IOException e) {
        throw convertIOException(e);
      }
    }

    @Override
    public void compact(ByteBuffer tableNameOrRegionName) throws IOError {
      try{
        getHBaseAdmin().compact(getBytes(tableNameOrRegionName));
      } catch (IOException e) {
        throw convertIOException(e);
      }
    }

    @Override
    public void majorCompact(ByteBuffer tableNameOrRegionName) throws IOError {
      try{
        getHBaseAdmin().majorCompact(getBytes(tableNameOrRegionName));
      } catch (IOException e) {
        throw convertIOException(e);
      }
    }

    @Override
    public List<ByteBuffer> getTableNames() throws IOError {
      try {
        HTableDescriptor[] tables = this.getHBaseAdmin().listTables();
        ArrayList<ByteBuffer> list = new ArrayList<ByteBuffer>(tables.length);
        for (int i = 0; i < tables.length; i++) {
          list.add(ByteBuffer.wrap(tables[i].getName()));
        }
        return list;
      } catch (IOException e) {
        throw convertIOException(e);
      }
    }

    /**
     * @return the list of regions in the given table, or an empty list of the table does not exist
     */
    @Override
    public List<TRegionInfo> getTableRegions(ByteBuffer tableName)
    throws IOError {
      try{
        HTable table;
        try {
          table = getTable(tableName);
        } catch (TableNotFoundException ex) {
          return new ArrayList<TRegionInfo>();
        }
        Map<HRegionInfo, HServerAddress> regionsInfo = table.getRegionsInfo();
        List<TRegionInfo> regions = new ArrayList<TRegionInfo>();

        for (HRegionInfo regionInfo : regionsInfo.keySet()){
          TRegionInfo region = new TRegionInfo();
          region.startKey = ByteBuffer.wrap(regionInfo.getStartKey());
          region.endKey = ByteBuffer.wrap(regionInfo.getEndKey());
          region.id = regionInfo.getRegionId();
          region.name = ByteBuffer.wrap(regionInfo.getRegionName());
          region.version = regionInfo.getVersion();
          HServerAddress server = regionsInfo.get(regionInfo);
          if (server != null) {
            byte[] hostname = Bytes.toBytes(server.getHostname());
            region.serverName = ByteBuffer.wrap(hostname);
            region.port = server.getPort();
          }
          regions.add(region);
        }
        return regions;
      } catch (IOException e){
        throw convertIOException(e);
      }
    }

    @Deprecated
    @Override
    public List<TCell> get(ByteBuffer tableName, ByteBuffer row, ByteBuffer column,
        ByteBuffer regionName) throws IOError {
      byte [][] famAndQf = KeyValue.parseColumn(getBytes(column));
      byte[] family = famAndQf[0];
      byte[] qualifier = getQualifier(famAndQf);
      try {
        Get get = new Get(getBytes(row));
        if (qualifier == null || qualifier.length == 0) {
          get.addFamily(family);
        } else {
          get.addColumn(family, qualifier);
        }
        Result result = processGet(tableName, regionName, get);
        return ThriftUtilities.cellFromHBase(result.sorted());
      } catch (IOException e) {
        throw convertIOException(e);
      }
    }

    @Deprecated
    @Override
    public List<TCell> getVer(ByteBuffer tableName, ByteBuffer row,
        ByteBuffer column, int numVersions, ByteBuffer regionName) throws IOError {
      byte [][] famAndQf = KeyValue.parseColumn(getBytes(column));
      byte[] family = famAndQf[0];
      byte[] qualifier = getQualifier(famAndQf);
      try {
        Get get = new Get(getBytes(row));
        get.addColumn(family, qualifier);
        get.setMaxVersions(numVersions);
        Result result = processGet(tableName, regionName, get);
        return ThriftUtilities.cellFromHBase(result.sorted());
      } catch (IOException e) {
        throw convertIOException(e);
      }
    }

    @Override
    @Deprecated
    public List<TCell> getVerTs(ByteBuffer tableName, ByteBuffer row,
        ByteBuffer column, long timestamp, int numVersions,
        ByteBuffer regionName) throws IOError {
      byte [][] famAndQf = KeyValue.parseColumn(getBytes(column));
      byte[] family = famAndQf[0];
      byte[] qualifier = getQualifier(famAndQf);
      try {
        Get get = new Get(getBytes(row));
        get.addColumn(family, qualifier);
        get.setTimeRange(Long.MIN_VALUE, timestamp);
        get.setMaxVersions(numVersions);
        Result result = processGet(tableName, regionName, get);
        return ThriftUtilities.cellFromHBase(result.sorted());
      } catch (IOException e) {
        throw convertIOException(e);
      }
    }

    @Override
    public List<TRowResult> getRow(ByteBuffer tableName, ByteBuffer row, ByteBuffer regionName)
        throws IOError {
      return getRowWithColumnsTs(tableName, row, null,
                                 HConstants.LATEST_TIMESTAMP, regionName);
    }

    @Override
    public List<TRowResult> getRowWithColumns(ByteBuffer tableName, ByteBuffer row,
        List<ByteBuffer> columns, ByteBuffer regionName) throws IOError {
      return getRowWithColumnsTs(tableName, row, columns,
                                 HConstants.LATEST_TIMESTAMP, regionName);
    }

    @Override
    public List<TRowResult> getRowTs(ByteBuffer tableName, ByteBuffer row,
        long timestamp, ByteBuffer regionName) throws IOError {
      return getRowWithColumnsTs(tableName, row, null,
                                 timestamp, regionName);
    }

    @Override
    public List<TRowResult> getRowWithColumnsTs(ByteBuffer tableName, ByteBuffer row,
        List<ByteBuffer> columns, long timestamp, ByteBuffer regionName) throws IOError {
      try {
        HTable table = getTable(tableName);
        byte[] rowBytes = getBytes(row);
        if (columns == null) {
          Get get = new Get(rowBytes);
          get.setTimeRange(Long.MIN_VALUE, timestamp);
          Result result = processGet(tableName, regionName, get);
          return ThriftUtilities.rowResultFromHBase(result);
        }
        Get get = new Get(rowBytes);
        for(ByteBuffer column : columns) {
          byte [][] famAndQf = KeyValue.parseColumn(getBytes(column));
          if (famAndQf.length == 1) {
              get.addFamily(famAndQf[0]);
          } else {
              get.addColumn(famAndQf[0], famAndQf[1]);
          }
        }
        get.setTimeRange(Long.MIN_VALUE, timestamp);
        Result result = processGet(tableName, regionName, get);
        return ThriftUtilities.rowResultFromHBase(result);
      } catch (IOException e) {
        throw convertIOException(e);
      }
    }

    @Override
    public List<TRowResult> getRowWithColumnPrefix(ByteBuffer tableName, ByteBuffer row,
        ByteBuffer prefix, ByteBuffer regionName) throws IOError {
      return (getRowWithColumnPrefixTs(tableName, row, prefix,
                                       HConstants.LATEST_TIMESTAMP, regionName));
    }

    @Override
    public List<TRowResult> getRowWithColumnPrefixTs(ByteBuffer tableName, ByteBuffer row,
        ByteBuffer prefix, long timestamp, ByteBuffer regionName) throws IOError {
      try {
        byte[] rowBytes = getBytes(row);
        if (prefix == null) {
          Get get = new Get(rowBytes);
          get.setTimeRange(Long.MIN_VALUE, timestamp);
          Result result = processGet(tableName, regionName, get);
          return ThriftUtilities.rowResultFromHBase(result);
        }
        Get get = new Get(rowBytes);
        byte [][] famAndPrefix = KeyValue.parseColumn(getBytes(prefix));
        if (famAndPrefix.length == 2) {
          get.addFamily(famAndPrefix[0]);
          get.setFilter(new ColumnPrefixFilter(famAndPrefix[1]));
        } else {
          get.setFilter(new ColumnPrefixFilter(famAndPrefix[0]));
        }
        get.setTimeRange(Long.MIN_VALUE, timestamp);
        Result result = processGet(tableName, regionName, get);
        return ThriftUtilities.rowResultFromHBase(result);
      } catch (IOException e) {
        throw convertIOException(e);
      }
    }

    @Override
    public void deleteAll(ByteBuffer tableName, ByteBuffer row, ByteBuffer column,
        ByteBuffer regionName)
        throws IOError {
      deleteAllTs(tableName, row, column, HConstants.LATEST_TIMESTAMP, regionName);
    }

    @Override
    public List<TRowResult> getRows(ByteBuffer tableName, List<ByteBuffer> rows,
        ByteBuffer regionName)
        throws IOError, TException {
      return getRowsWithColumnsTs(tableName, rows, null,
          HConstants.LATEST_TIMESTAMP, regionName);
    }

    @Override
    public List<TRowResult> getRowsWithColumns(ByteBuffer tableName,
        List<ByteBuffer> rows, List<ByteBuffer> columns, ByteBuffer regionName) throws IOError {
      return getRowsWithColumnsTs(tableName, rows, columns,
          HConstants.LATEST_TIMESTAMP, regionName);
    }

    @Override
    public List<TRowResult> getRowsTs(ByteBuffer tableName, List<ByteBuffer> rows,
        long timestamp, ByteBuffer regionName) throws IOError {
      return getRowsWithColumnsTs(tableName, rows, null, timestamp, regionName);
    }

    @Override
    public List<TRowResult> getRowsWithColumnsTs(ByteBuffer tableName,
        List<ByteBuffer> rows, List<ByteBuffer> columns, long timestamp,
        ByteBuffer regionName) throws IOError {
      try {
        List<Get> gets = new ArrayList<Get>(rows.size());
        if (metrics != null) {
          metrics.incNumBatchGetRowKeys(rows.size());
        }

        // For now, don't support ragged gets, with different columns per row
        // Probably pretty sensible indefinitely anyways.
        for (ByteBuffer row : rows) {
          Get get = new Get(getBytes(row));
          if (columns != null) {
            for (ByteBuffer column : columns) {
              byte[][] famAndQf = KeyValue.parseColumn(getBytes(column));
              if (famAndQf.length == 1) {
                get.addFamily(famAndQf[0]);
              } else {
                get.addColumn(famAndQf[0], famAndQf[1]);
              }
            }
            get.setTimeRange(Long.MIN_VALUE, timestamp);
          }
          gets.add(get);
        }
        Result[] result = processMultiGet(tableName, regionName, gets);
        return ThriftUtilities.rowResultFromHBase(result);
      } catch (IOException e) {
        throw convertIOException(e);
      }
    }

    @Override
    public void deleteAllTs(ByteBuffer tableName, ByteBuffer row, ByteBuffer column,
        long timestamp, ByteBuffer regionName) throws IOError {
      try {
        processDelete(tableName, regionName, createDelete(row, column, timestamp));
      } catch (IOException e) {
        throw convertIOException(e);
      }
    }

    @Override
    public void deleteAllRow(ByteBuffer tableName, ByteBuffer row,
        Map<ByteBuffer, ByteBuffer> attributes, ByteBuffer regionName) throws IOError {
      deleteAllRowTs(tableName, row, HConstants.LATEST_TIMESTAMP, regionName);
    }

    @Override
    public void deleteAllRowTs(ByteBuffer tableName, ByteBuffer row, long timestamp,
        ByteBuffer regionName)
        throws IOError {
      try {
        processDelete(tableName, regionName, new Delete(getBytes(row), timestamp, null));
      } catch (IOException e) {
        throw convertIOException(e);
      }
    }

    @Override
    public void createTable(ByteBuffer tableName,
        List<ColumnDescriptor> columnFamilies) throws IOError,
        IllegalArgument, AlreadyExists {
      try {
        byte[] tableNameBytes = getBytes(tableName);
        if (getHBaseAdmin().tableExists(tableNameBytes)) {
          throw new AlreadyExists("table name already in use");
        }
        HTableDescriptor desc = new HTableDescriptor(tableNameBytes);
        for (ColumnDescriptor col : columnFamilies) {
          HColumnDescriptor colDesc = ThriftUtilities.colDescFromThrift(col);
          desc.addFamily(colDesc);
        }
        getHBaseAdmin().createTable(desc);
      } catch (IOException e) {
        throw convertIOException(e);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgument(e.getMessage());
      }
    }

    @Override
    public void deleteTable(ByteBuffer tableName) throws IOError {
      byte[] tableNameBytes = getBytes(tableName);
      if (LOG.isDebugEnabled()) {
        LOG.debug("deleteTable: table=" + new String(tableNameBytes));
      }
      try {
        if (!getHBaseAdmin().tableExists(tableNameBytes)) {
          throw new IOError("table does not exist", 0, "");
        }
        getHBaseAdmin().deleteTable(tableNameBytes);
      } catch (IOException e) {
        throw convertIOException(e);
      }
    }

    @Override
    public void mutateRow(ByteBuffer tableName, ByteBuffer row, List<Mutation> mutations,
        Map<ByteBuffer, ByteBuffer> attributes, ByteBuffer regionName)
    throws IOError, IllegalArgument {
      mutateRowTs(tableName, row, mutations, HConstants.LATEST_TIMESTAMP, attributes,
          regionName);
    }

    private void mutateRowsHelper(ByteBuffer tableName, ByteBuffer row, List<Mutation> mutations,
        long timestamp, ByteBuffer regionName, List<Put> puts, List<Delete> deletes)
        throws IllegalArgument, IOError, IOException {
      byte[] rowBytes = getBytes(row);
      Put put = null;
      Delete delete = null;

      boolean firstMutation = true;
      boolean writeToWAL = false;
      for (Mutation m : mutations) {
        byte[][] famAndQf = KeyValue.parseColumn(getBytes(m.column));
        
        // If this mutation has timestamp set, it takes precedence, otherwise we use the
        // timestamp provided in the argument.
        long effectiveTimestamp = getMutationTimestamp(m, timestamp);
        
        if (m.isDelete) {
          if (delete == null) {
            delete = new Delete(rowBytes);
          }
          updateDelete(delete, famAndQf, effectiveTimestamp);
        } else {
          if (put == null) {
            put = new Put(rowBytes, timestamp, null);
          }
          put.add(famAndQf[0], getQualifier(famAndQf), effectiveTimestamp, getBytes(m.value));
        }

        if (firstMutation) {
          // Remember the first mutation's writeToWAL status.
          firstMutation = false;
          writeToWAL = m.writeToWAL;
        } else {
          // Make sure writeToWAL status is consistent in all mutations.
          if (m.writeToWAL != writeToWAL) {
            throw new IllegalArgument("Mutations with contradicting writeToWal settings");
          }
        }
      }
      
      if (delete != null) {
        delete.setWriteToWAL(writeToWAL);
        if (deletes != null) {
          deletes.add(delete);
        } else {
          processDelete(tableName, regionName, delete);
        }
      }
      
      if (put != null) {
        put.setWriteToWAL(writeToWAL);
        if (puts != null) {
          puts.add(put);
        } else {
          processPut(tableName, regionName, put);
        }
      }
    }

    @Override
    public void mutateRowTs(ByteBuffer tableName, ByteBuffer row,
        List<Mutation> mutations, long timestamp,
        Map<ByteBuffer, ByteBuffer> attributes, ByteBuffer regionName)
    throws IOError, IllegalArgument {
      try {
        mutateRowsHelper(tableName, row, mutations, timestamp, regionName, null, null);
      } catch (IOException e) {
        throw convertIOException(e);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgument(e.getMessage());
      }
    }

    @Override
    public void mutateRows(ByteBuffer tableName, List<BatchMutation> rowBatches,
        Map<ByteBuffer, ByteBuffer> attributes, ByteBuffer regionName)
        throws IOError, IllegalArgument, TException {
      mutateRowsTs(tableName, rowBatches, HConstants.LATEST_TIMESTAMP, attributes,
          regionName);
    }

    @Override
    public void mutateRowsTs(
        ByteBuffer tableName, List<BatchMutation> rowBatches, long timestamp,
        Map<ByteBuffer, ByteBuffer> attributes, ByteBuffer regionName)
        throws IOError, IllegalArgument, TException {
      List<Put> puts = new ArrayList<Put>();
      List<Delete> deletes = new ArrayList<Delete>();
      if (metrics != null) {
        metrics.incNumBatchMutateRowKeys(rowBatches.size());
      }

      try {
        for (BatchMutation batch : rowBatches) {
          mutateRowsHelper(tableName, batch.row, batch.mutations, timestamp, regionName,
              puts, deletes);
        }

        if (puts != null) {
          processMultiPut(tableName, regionName, puts);
        }
        if (deletes != null) {
          processMultiDelete(tableName, regionName, deletes);
        }
      } catch (IOException e) {
        throw convertIOException(e);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgument(e.getMessage());
      }
    }

    /**
     * Warning; the puts and deletes are NOT atomic together and so a lot of
     * weird things can happen if you expect that to be the case!!
     *
     * A valueCheck of null means that the row can't exist before being put.
     * This is kind of a stupid thing to enforce when deleting, for obvious
     * reasons.
     */
    @Override
    public boolean checkAndMutateRow(ByteBuffer tableName, ByteBuffer row,
        ByteBuffer columnCheck, ByteBuffer valueCheck, List<Mutation> mutations,
        Map<ByteBuffer, ByteBuffer> attributes)
        throws IOError, IllegalArgument {
      return checkAndMutateRowTs(tableName, row, columnCheck, valueCheck,
          mutations, HConstants.LATEST_TIMESTAMP, attributes);
    }

    @Override
    public boolean checkAndMutateRowTs(ByteBuffer tableName, ByteBuffer row,
        ByteBuffer columnCheck, ByteBuffer valueCheck,
        List<Mutation> mutations,
        long timestamp, Map<ByteBuffer, ByteBuffer> attributes) throws IOError, IllegalArgument {
      HTable table;
      try {
        table = getTable(tableName);
        byte[] rowBytes = getBytes(row);
        Put put = new Put(rowBytes, timestamp, null);

        Delete delete = new Delete(rowBytes);

        for (Mutation m : mutations) {

          // If this mutation has timestamp set, it takes precedence, otherwise we use the
          // timestamp provided in the argument.
          long effectiveTimestamp = getMutationTimestamp(m, timestamp);

          byte[][] famAndQf = KeyValue.parseColumn(Bytes.toBytesRemaining(m.column));
          if (m.isDelete) {
            updateDelete(delete, famAndQf, effectiveTimestamp);
          } else {
            byte[] valueBytes = getBytes(m.value);
            put.add(famAndQf[0], getQualifier(famAndQf), effectiveTimestamp, valueBytes);
          }
        }
        byte[][] famAndQfCheck = KeyValue.parseColumn(getBytes(columnCheck));
        byte[] valueCheckBytes = getBytes(valueCheck);
        if (valueCheck == null) {
          // This means we are not expecting to see an existing value. An empty array would mean
          // we are looking for an zero-length existing value.
          valueCheckBytes = null;
        }

        if (!delete.isEmpty() && !put.isEmpty()) {
          // can't do both, not atomic, not good idea!
          throw new IllegalArgument(
              "Single Thrift CheckAndMutate call cannot do both puts and deletes.");
        }
        if (!delete.isEmpty()) {
          return table.checkAndDelete(rowBytes, famAndQfCheck[0],
                  getQualifier(famAndQfCheck), valueCheckBytes, delete);
        }
        if (!put.isEmpty()) {
          return table.checkAndPut(rowBytes, famAndQfCheck[0],
                  getQualifier(famAndQfCheck), valueCheckBytes, put);
        }
        throw new IllegalArgument(
            "Thrift CheckAndMutate call must do either put or delete.");
      } catch (IOException e) {
        throw convertIOException(e);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgument(e.getMessage());
      }
    }

    @Deprecated
    @Override
    public long atomicIncrement(ByteBuffer tableName, ByteBuffer row, ByteBuffer column,
        long amount) throws IOError, IllegalArgument, TException {
      byte [][] famAndQf = KeyValue.parseColumn(getBytes(column));
      return atomicIncrement(getBytes(tableName), getBytes(row), famAndQf[0],
          getQualifier(famAndQf), amount);
    }

    public long atomicIncrement(byte [] tableName, byte [] row, byte [] family,
        byte [] qualifier, long amount)
    throws IOError, IllegalArgument, TException {
      HTable table;
      try {
        table = getTable(tableName);
        return table.incrementColumnValue(row, family, qualifier, amount);
      } catch (IOException e) {
        throw convertIOException(e);
      }
    }

    @Override
    public void scannerClose(int id) throws IOError, IllegalArgument {
      LOG.debug("scannerClose: id=" + id);
      ResultScanner scanner = getScanner(id);
      if (scanner == null) {
        LOG.warn("scanner ID is invalid");
      }
      scanner.close();
      removeScanner(id);
    }

    @Override
    public List<TRowResult> scannerGetList(int id,int nbRows) throws IllegalArgument, IOError {
        LOG.debug("scannerGetList: id=" + id);
        ResultScanner scanner = getScanner(id);
        if (null == scanner) {
            throw new IllegalArgument("scanner ID is invalid");
        }

        Result [] results = null;
        try {
            results = scanner.next(nbRows);
            if (null == results) {
                return new ArrayList<TRowResult>();
            }
        } catch (IOException e) {
          scanner.close();
          removeScanner(id);
          throw convertIOException(e);
        }
        return ThriftUtilities.rowResultFromHBase(results);
    }

    @Override
    public List<TRowResult> scannerGet(int id) throws IllegalArgument, IOError {
        return scannerGetList(id,1);
    }

    @Override
    public int scannerOpen(ByteBuffer tableName, ByteBuffer startRow, List<ByteBuffer> columns)
        throws IOError {
        try {
          HTable table = getTable(tableName);
          Scan scan = new Scan(getBytes(startRow));
          if(columns != null && columns.size() != 0) {
            for(ByteBuffer column : columns) {
              byte [][] famQf = KeyValue.parseColumn(getBytes(column));
              if(famQf.length == 1) {
                scan.addFamily(famQf[0]);
              } else {
                scan.addColumn(famQf[0], famQf[1]);
              }
            }
          }
          return addScanner(table.getScanner(scan));
        } catch (IOException e) {
          throw convertIOException(e);
        }
    }

    @Override
    public int scannerOpenWithStop(ByteBuffer tableName, ByteBuffer startRow,
        ByteBuffer stopRow, List<ByteBuffer> columns) throws IOError, TException {
      try {
        HTable table = getTable(tableName);
        Scan scan = new Scan(getBytes(startRow), getBytes(stopRow));
        if(columns != null && columns.size() != 0) {
          for(ByteBuffer column : columns) {
            byte [][] famQf = KeyValue.parseColumn(getBytes(column));
            if(famQf.length == 1) {
              scan.addFamily(famQf[0]);
            } else {
              scan.addColumn(famQf[0], famQf[1]);
            }
          }
        }
        return addScanner(table.getScanner(scan));
      } catch (IOException e) {
        throw convertIOException(e);
      }
    }

    @Override
    public int scannerOpenWithPrefix(ByteBuffer tableName, ByteBuffer startAndPrefix,
        List<ByteBuffer> columns) throws IOError, TException {
      try {
        HTable table = getTable(tableName);
        byte[] startAndPrefixBytes = getBytes(startAndPrefix);
        Scan scan = new Scan(startAndPrefixBytes);
        Filter f = new WhileMatchFilter(
            new PrefixFilter(startAndPrefixBytes));
        scan.setFilter(f);
        if(columns != null && columns.size() != 0) {
          for(ByteBuffer column : columns) {
            byte [][] famQf = KeyValue.parseColumn(getBytes(column));
            if(famQf.length == 1) {
              scan.addFamily(famQf[0]);
            } else {
              scan.addColumn(famQf[0], famQf[1]);
            }
          }
        }
        return addScanner(table.getScanner(scan));
      } catch (IOException e) {
        throw convertIOException(e);
      }
    }

    @Override
    public int scannerOpenWithScan(ByteBuffer tableName, TScan tScan) throws IOError {
      try {
        HTable table = getTable(tableName);
        Scan scan = new Scan();
        if (tScan.isSetStartRow()) {
          scan.setStartRow(tScan.getStartRow());
        }
        if (tScan.isSetStopRow()) {
          scan.setStopRow(tScan.getStopRow());
        }
        if (tScan.isSetTimestamp() || tScan.isSetMinTimestamp()) {
          long minTS = tScan.isSetMinTimestamp() ? tScan.getMinTimestamp()
              : Long.MIN_VALUE;
          long maxTS = tScan.isSetTimestamp() ? tScan.getTimestamp()
              : Long.MAX_VALUE;
          scan.setTimeRange(minTS, maxTS);
        }
        if (tScan.isSetCaching()) {
          scan.setCaching(tScan.getCaching());
        }
        if (tScan.isSetCachingBlocksEnabled()) {
          scan.setCacheBlocks(tScan.isCachingBlocksEnabled());
        }
        if (tScan.isSetBatchLimit()) {
          scan.setBatch(tScan.getBatchLimit());
        }
        if(tScan.isSetColumns() && tScan.getColumns().size() != 0) {
          for(ByteBuffer column : tScan.getColumns()) {
            byte [][] famQf = KeyValue.parseColumn(getBytes(column));
            if(famQf.length == 1) {
              scan.addFamily(famQf[0]);
            } else {
              scan.addColumn(famQf[0], famQf[1]);
            }
          }
        }
        if (tScan.isSetFilterString()) {
          ParseFilter parseFilter = new ParseFilter();
          scan.setFilter(parseFilter.parseFilterString(tScan.getFilterString()));
        }
        return addScanner(table.getScanner(scan));
      } catch (IOException e) {
        throw convertIOException(e);
      }
    }

    @Override
    public int scannerOpenTs(ByteBuffer tableName, ByteBuffer startRow,
        List<ByteBuffer> columns, long timestamp) throws IOError, TException {
      try {
        HTable table = getTable(tableName);
        Scan scan = new Scan(getBytes(startRow));
        scan.setTimeRange(Long.MIN_VALUE, timestamp);
        if(columns != null && columns.size() != 0) {
          for(ByteBuffer column : columns) {
            byte [][] famQf = KeyValue.parseColumn(getBytes(column));
            if(famQf.length == 1) {
              scan.addFamily(famQf[0]);
            } else {
              scan.addColumn(famQf[0], famQf[1]);
            }
          }
        }
        return addScanner(table.getScanner(scan));
      } catch (IOException e) {
        throw convertIOException(e);
      }
    }

    @Override
    public int scannerOpenWithStopTs(ByteBuffer tableName, ByteBuffer startRow,
        ByteBuffer stopRow, List<ByteBuffer> columns, long timestamp)
        throws IOError, TException {
      try {
        HTable table = getTable(tableName);
        Scan scan = new Scan(getBytes(startRow), getBytes(stopRow));
        scan.setTimeRange(Long.MIN_VALUE, timestamp);
        if(columns != null && columns.size() != 0) {
          for(ByteBuffer column : columns) {
            byte [][] famQf = KeyValue.parseColumn(getBytes(column));
            if(famQf.length == 1) {
              scan.addFamily(famQf[0]);
            } else {
              scan.addColumn(famQf[0], famQf[1]);
            }
          }
        }
        scan.setTimeRange(Long.MIN_VALUE, timestamp);
        return addScanner(table.getScanner(scan));
      } catch (IOException e) {
        throw convertIOException(e);
      }
    }

    @Override
    public int scannerOpenWithFilterString(ByteBuffer tableName,
        ByteBuffer filterString) throws IOError, TException {
      return scannerOpenWithFilterStringTs(tableName, filterString, Long.MAX_VALUE);
    }

    @Override
    public int scannerOpenWithFilterStringTs(ByteBuffer tableName, ByteBuffer filterString,
                                             long timestamp) throws IOError, TException {
      return scannerOpenWithStopAndFilterStringTs(tableName,
                                                  HConstants.EMPTY_START_ROW_BUF,
                                                  HConstants.EMPTY_END_ROW_BUF,
                                                  filterString, timestamp);
    }

    @Override
    public int scannerOpenWithStopAndFilterString(ByteBuffer tableName, ByteBuffer startRow,
        ByteBuffer stopRow, ByteBuffer filterString) throws IOError, TException {
      return scannerOpenWithStopAndFilterStringTs(tableName, startRow, stopRow,
                                                  filterString, Long.MAX_VALUE);
    }

    @Override
    public int scannerOpenWithStopAndFilterStringTs(ByteBuffer tableName, ByteBuffer startRow,
        ByteBuffer stopRow, ByteBuffer filterString, long timestamp) throws IOError, TException {
      try {
        HTable table = getTable(tableName);
        Scan scan = new Scan(getBytes(startRow), getBytes(stopRow));
        scan.setTimeRange(Long.MIN_VALUE, timestamp);

        if (filterString != null && filterString.remaining() != 0) {
          ParseFilter parseFilter = new ParseFilter();
          scan.setFilter(parseFilter.parseFilterString(getBytes(filterString)));
        }
        return addScanner(table.getScanner(scan));
      } catch (IOException e) {
        throw convertIOException(e);
      }
    }

    @Override
    public Map<ByteBuffer, ColumnDescriptor> getColumnDescriptors(
        ByteBuffer tableName) throws IOError, TException {
      try {
        TreeMap<ByteBuffer, ColumnDescriptor> columns =
            new TreeMap<ByteBuffer, ColumnDescriptor>(Bytes.BYTE_BUFFER_COMPARATOR);

        HTable table = getTable(tableName);
        HTableDescriptor desc = table.getTableDescriptor();

        for (HColumnDescriptor e : desc.getFamilies()) {
          ColumnDescriptor col = ThriftUtilities.colDescFromHbase(e);
          columns.put(col.name, col);
        }
        return columns;
      } catch (IOException e) {
        throw convertIOException(e);
      }
    }

    @Override
    public TRegionInfo getRegionInfo(ByteBuffer searchRow) throws IOError,
        TException {
      try {
        HTable table = getTable(HConstants.META_TABLE_NAME);
        byte[] searchRowBytes = getBytes(searchRow);
        Result startRowResult = table.getRowOrBefore(searchRowBytes,
            HConstants.CATALOG_FAMILY);

        if (startRowResult == null) {
          throw new IOException("Cannot find row in .META., row="
              + Bytes.toString(searchRowBytes));
        }

        // find region start and end keys
        byte[] value = startRowResult.getValue(HConstants.CATALOG_FAMILY,
            HConstants.REGIONINFO_QUALIFIER);
        if (value == null || value.length == 0) {
          throw new IOException("HRegionInfo REGIONINFO was null or "
              + " empty in Meta for row=" + Bytes.toString(searchRowBytes));
        }
        HRegionInfo regionInfo = Writables.getHRegionInfo(value);
        TRegionInfo region = new TRegionInfo();
        region.setStartKey(regionInfo.getStartKey());
        region.setEndKey(regionInfo.getEndKey());
        region.id = regionInfo.getRegionId();
        region.setName(regionInfo.getRegionName());
        region.version = regionInfo.getVersion();

        // find region assignment to server
        value = startRowResult.getValue(HConstants.CATALOG_FAMILY,
            HConstants.SERVER_QUALIFIER);
        if (value != null && value.length > 0) {
          String address = Bytes.toString(value);
          HServerAddress server = new HServerAddress(address);
          byte[] hostname = Bytes.toBytes(server.getHostname());
          region.serverName = ByteBuffer.wrap(hostname);
          region.port = server.getPort();
        }
        return region;
      } catch (IOException e) {
        throw convertIOException(e);
      }
    }

    @Override
    public void multiPut(ByteBuffer tableName, List<BatchMutation> tPuts, ByteBuffer regionName)
        throws IOError, IllegalArgument, TException {
      if (metrics != null) {
        metrics.incNumMultiPutRowKeys(tPuts.size());
      }
      List<Put> puts = new ArrayList<Put>(tPuts.size());
      try {
        for (BatchMutation batch : tPuts) {
          byte[] row = getBytes(batch.row);
          List<Mutation> mutations = batch.mutations;
          Put put = null;
          for (Mutation m : mutations) {
            byte[][] famAndQf = KeyValue.parseColumn(getBytes(m.column));
            put = new Put(row);

            if (famAndQf.length == 1) {
              put.add(famAndQf[0], HConstants.EMPTY_BYTE_ARRAY, m.getTimestamp(),
                  getBytes(m.value));
            } else {
              put.add(famAndQf[0], famAndQf[1], m.getTimestamp(), getBytes(m.value));
            }
          }
          puts.add(put);
        }

        processMultiPut(tableName, regionName, puts);
      } catch (IOException e) {
        throw new IOError(e.getMessage(), 0, e.getClass().getName());
      }
    }

    void initMetrics(ThriftMetrics metrics) {
      this.metrics = metrics;
    }

    @Override
    public void mutateRowsAsync(ByteBuffer tableName, List<BatchMutation> rowBatches)
        throws TException {
      throw new TException("Not implemented");
    }

    @Override
    public void mutateRowsTsAsync(ByteBuffer tableName, List<BatchMutation> rowBatches,
        long timestamp) throws TException {
      throw new TException("Not implemented");
    }

    protected Result processGet(ByteBuffer tableName, ByteBuffer regionName, Get get)
        throws IOException, IOError {
      return getTable(tableName).get(get);
    }

    protected void processPut(ByteBuffer tableName, ByteBuffer regionName, Put put)
        throws IOException, IOError {
      getTable(tableName).put(put);
    }

    protected void processDelete(ByteBuffer tableName, ByteBuffer regionName, Delete delete)
        throws IOException, IOError {
      getTable(tableName).delete(delete);
    }

    protected Result[] processMultiGet(ByteBuffer tableName, ByteBuffer regionName, List<Get> gets)
        throws IOException, IOError {
      return getTable(tableName).get(gets);
    }

    protected void processMultiPut(ByteBuffer tableName, ByteBuffer regionName, List<Put> puts)
        throws IOException, IOError {
      getTable(tableName).put(puts);
    }

    protected void processMultiDelete(ByteBuffer tableName, ByteBuffer regionName,
        List<Delete> deletes) throws IOException, IOError {
      getTable(tableName).delete(deletes);
    }

    @Override
    public Map<ByteBuffer, Long> getLastFlushTimes() throws TException {
      throw new TException(NOT_SUPPORTED_BY_PROXY_MSG);
    }

    @Override
    public long getCurrentTimeMillis() throws TException {
      throw new TException(NOT_SUPPORTED_BY_PROXY_MSG);
    }

    @Override
    public void flushRegion(ByteBuffer regionName, long ifOlderThanTS) throws TException, IOError {
      throw new TException(NOT_SUPPORTED_BY_PROXY_MSG);
    }
    
  }

  public static void registerFilters(Configuration conf) {
    String[] filters = conf.getStrings("hbase.thrift.filters");
    if(filters != null) {
      for(String filterClass: filters) {
        String[] filterPart = filterClass.split(":");
        if(filterPart.length != 2) {
          LOG.warn("Invalid filter specification " + filterClass + " - skipping");
        } else {
          ParseFilter.registerFilter(filterPart[0], filterPart[1]);
        }
      }
    }
  }

  public static IOError convertIOException(IOException e) {
    long timeout = 0;
    if (e instanceof RegionException) {
      timeout = ((RegionException)e).getBackoffTimeMillis();
    }
    return new IOError(e.getMessage(), timeout, e.getClass().getSimpleName());
  }

  private static byte[] getQualifier(byte[][] familyAndQualifier) {
    if (familyAndQualifier.length > 1) {
      return familyAndQualifier[1];
    }
    return HConstants.EMPTY_BYTE_ARRAY;
  }

  /**
   * Update the given delete object.
   * 
   * @param delete the delete object to update
   * @param famAndQf family and qualifier. null or empty family means "delete from all CFs".
   * @param timestamp Delete at this timestamp and older.
   */
  private static void updateDelete(Delete delete, byte[][] famAndQf, long timestamp) { 
    if (famAndQf.length == 1) {
      // Column qualifier not specified.
      if (famAndQf[0].length == 0) {
        // Delete from all column families in the row. 
        delete.deleteRow(timestamp);
      } else {
        // Delete from all columns in the given column family
        delete.deleteFamily(famAndQf[0], timestamp);
      }
    } else {
      // Delete only from the specific column
      delete.deleteColumns(famAndQf[0], famAndQf[1], timestamp);
    }
  }

  private static Delete createDelete(ByteBuffer row, ByteBuffer column, long timestamp) {
    Delete delete  = new Delete(getBytes(row));
    byte [][] famAndQf = KeyValue.parseColumn(getBytes(column));
    updateDelete(delete, famAndQf, timestamp);
    return delete;
  }

}
