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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.thrift.generated.AlreadyExists;
import org.apache.hadoop.hbase.thrift.generated.BatchMutation;
import org.apache.hadoop.hbase.thrift.generated.ColumnDescriptor;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.thrift.generated.Hbase.Iface;
import org.apache.hadoop.hbase.thrift.generated.Hbase.Processor;
import org.apache.hadoop.hbase.thrift.generated.IOError;
import org.apache.hadoop.hbase.thrift.generated.IllegalArgument;
import org.apache.hadoop.hbase.thrift.generated.Mutation;
import org.apache.hadoop.hbase.thrift.generated.TCell;
import org.apache.hadoop.hbase.thrift.generated.TRegionInfo;
import org.apache.hadoop.hbase.thrift.generated.TRowResult;
import org.apache.hadoop.hbase.thrift.generated.TScan;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.util.Shell.ExitCodeException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.AbstractServerArgs;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportFactory;

import com.google.common.base.Joiner;

/**
 * ThriftServer - this class starts up a Thrift server which implements the
 * Hbase API specified in the Hbase.thrift IDL file.
 */
public class ThriftServer {

  private static final Log LOG = LogFactory.getLog(ThriftServer.class);

  private static final String MIN_WORKERS_OPTION = "minWorkers";
  private static final String MAX_WORKERS_OPTION = "workers";
  private static final String MAX_QUEUE_SIZE_OPTION = "queue";
  private static final String KEEP_ALIVE_SEC_OPTION = "keepAliveSec";
  static final String BIND_OPTION = "bind";
  static final String COMPACT_OPTION = "compact";
  static final String FRAMED_OPTION = "framed";
  static final String PORT_OPTION = "port";

  private static final String DEFAULT_BIND_ADDR = "0.0.0.0";
  private static final int DEFAULT_LISTEN_PORT = 9090;

  private Configuration conf;
  TServer server;

  /** An enum of server implementation selections */
  enum ImplType {
    HS_HA("hsha", true, THsHaServer.class, false),
    NONBLOCKING("nonblocking", true, TNonblockingServer.class, false),
    THREAD_POOL("threadpool", false, TBoundedThreadPoolServer.class, true);

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

    static ImplType getServerImpl(CommandLine cmd) {
      ImplType chosenType = null;
      int numChosen = 0;
      for (ImplType t : values()) {
        if (cmd.hasOption(t.option)) {
          chosenType = t;
          ++numChosen;
        }
      }
      if (numChosen != 1) {
        throw new AssertionError("Exactly one option out of " +
            Arrays.toString(values()) + " has to be specified");
      }
      return chosenType;
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

    private static ThreadLocal<Map<String, HTable>> threadLocalTables = new ThreadLocal<Map<String, HTable>>() {
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
    protected HTable getTable(final byte[] tableName) throws
        IOException {
      String table = new String(tableName);
      Map<String, HTable> tables = threadLocalTables.get();
      if (!tables.containsKey(table)) {
        tables.put(table, new HTable(conf, tableName));
      }
      return tables.get(table);
    }

    protected HTable getTable(final ByteBuffer tableName) throws IOException {
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

    /**
     * Constructs an HBaseHandler object.
     * @throws IOException
     */
    protected HBaseHandler()
    throws IOException {
      this(HBaseConfiguration.create());
    }

    protected HBaseHandler(final Configuration c)
    throws IOException {
      this.conf = c;
      admin = new HBaseAdmin(conf);
      scannerMap = new HashMap<Integer, ResultScanner>();
    }

    @Override
    public void enableTable(ByteBuffer tableName) throws IOError {
      try{
        admin.enableTable(getBytes(tableName));
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public void disableTable(ByteBuffer tableName) throws IOError{
      try{
        admin.disableTable(getBytes(tableName));
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public boolean isTableEnabled(ByteBuffer tableName) throws IOError {
      try {
        return HTable.isTableEnabled(this.conf, getBytes(tableName));
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public void compact(ByteBuffer tableNameOrRegionName) throws IOError {
      try{
        admin.compact(getBytes(tableNameOrRegionName));
      } catch (InterruptedException e) {
        throw new IOError(e.getMessage());
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public void majorCompact(ByteBuffer tableNameOrRegionName) throws IOError {
      try{
        admin.majorCompact(getBytes(tableNameOrRegionName));
      } catch (InterruptedException e) {
        throw new IOError(e.getMessage());
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public List<ByteBuffer> getTableNames() throws IOError {
      try {
        HTableDescriptor[] tables = this.admin.listTables();
        ArrayList<ByteBuffer> list = new ArrayList<ByteBuffer>(tables.length);
        for (int i = 0; i < tables.length; i++) {
          list.add(ByteBuffer.wrap(tables[i].getName()));
        }
        return list;
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public List<TRegionInfo> getTableRegions(ByteBuffer tableName)
    throws IOError {
      try{
        List<HRegionInfo> hris = this.admin.getTableRegions(tableName.array());
        List<TRegionInfo> regions = new ArrayList<TRegionInfo>();

        if (hris != null) {
          for (HRegionInfo regionInfo : hris){
            TRegionInfo region = new TRegionInfo();
            region.startKey = ByteBuffer.wrap(regionInfo.getStartKey());
            region.endKey = ByteBuffer.wrap(regionInfo.getEndKey());
            region.id = regionInfo.getRegionId();
            region.name = ByteBuffer.wrap(regionInfo.getRegionName());
            region.version = regionInfo.getVersion();
            regions.add(region);
          }
        }
        return regions;
      } catch (IOException e){
        throw new IOError(e.getMessage());
      }
    }

    @Deprecated
    @Override
    public List<TCell> get(ByteBuffer tableName, ByteBuffer row, ByteBuffer column)
        throws IOError {
      byte [][] famAndQf = KeyValue.parseColumn(getBytes(column));
      if(famAndQf.length == 1) {
        return get(tableName, row, famAndQf[0], new byte[0]);
      }
      return get(tableName, row, famAndQf[0], famAndQf[1]);
    }

    protected List<TCell> get(ByteBuffer tableName,
                              ByteBuffer row,
                              byte[] family,
                              byte[] qualifier) throws IOError {
      try {
        HTable table = getTable(tableName);
        Get get = new Get(getBytes(row));
        if (qualifier == null || qualifier.length == 0) {
          get.addFamily(family);
        } else {
          get.addColumn(family, qualifier);
        }
        Result result = table.get(get);
        return ThriftUtilities.cellFromHBase(result.raw());
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    @Deprecated
    @Override
    public List<TCell> getVer(ByteBuffer tableName, ByteBuffer row,
        ByteBuffer column, int numVersions) throws IOError {
      byte [][] famAndQf = KeyValue.parseColumn(getBytes(column));
      if(famAndQf.length == 1) {
        return getVer(tableName, row, famAndQf[0],
            new byte[0], numVersions);
      }
      return getVer(tableName, row,
          famAndQf[0], famAndQf[1], numVersions);
    }

    public List<TCell> getVer(ByteBuffer tableName, ByteBuffer row,
                              byte[] family,
        byte[] qualifier, int numVersions) throws IOError {
      try {
        HTable table = getTable(tableName);
        Get get = new Get(getBytes(row));
        get.addColumn(family, qualifier);
        get.setMaxVersions(numVersions);
        Result result = table.get(get);
        return ThriftUtilities.cellFromHBase(result.raw());
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    @Deprecated
    @Override
    public List<TCell> getVerTs(ByteBuffer tableName,
                                   ByteBuffer row,
        ByteBuffer column,
        long timestamp,
        int numVersions) throws IOError {
      byte [][] famAndQf = KeyValue.parseColumn(getBytes(column));
      if(famAndQf.length == 1) {
        return getVerTs(tableName, row, famAndQf[0], new byte[0], timestamp,
            numVersions);
      }
      return getVerTs(tableName, row, famAndQf[0], famAndQf[1], timestamp,
          numVersions);
    }

    protected List<TCell> getVerTs(ByteBuffer tableName,
                                   ByteBuffer row, byte [] family,
        byte [] qualifier, long timestamp, int numVersions) throws IOError {
      try {
        HTable table = getTable(tableName);
        Get get = new Get(getBytes(row));
        get.addColumn(family, qualifier);
        get.setTimeRange(Long.MIN_VALUE, timestamp);
        get.setMaxVersions(numVersions);
        Result result = table.get(get);
        return ThriftUtilities.cellFromHBase(result.raw());
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public List<TRowResult> getRow(ByteBuffer tableName, ByteBuffer row)
        throws IOError {
      return getRowWithColumnsTs(tableName, row, null,
                                 HConstants.LATEST_TIMESTAMP);
    }

    @Override
    public List<TRowResult> getRowWithColumns(ByteBuffer tableName,
                                              ByteBuffer row,
        List<ByteBuffer> columns) throws IOError {
      return getRowWithColumnsTs(tableName, row, columns,
                                 HConstants.LATEST_TIMESTAMP);
    }

    @Override
    public List<TRowResult> getRowTs(ByteBuffer tableName, ByteBuffer row,
        long timestamp) throws IOError {
      return getRowWithColumnsTs(tableName, row, null,
                                 timestamp);
    }

    @Override
    public List<TRowResult> getRowWithColumnsTs(ByteBuffer tableName, ByteBuffer row,
        List<ByteBuffer> columns, long timestamp) throws IOError {
      try {
        HTable table = getTable(tableName);
        if (columns == null) {
          Get get = new Get(getBytes(row));
          get.setTimeRange(Long.MIN_VALUE, timestamp);
          Result result = table.get(get);
          return ThriftUtilities.rowResultFromHBase(result);
        }
        Get get = new Get(getBytes(row));
        for(ByteBuffer column : columns) {
          byte [][] famAndQf = KeyValue.parseColumn(getBytes(column));
          if (famAndQf.length == 1) {
              get.addFamily(famAndQf[0]);
          } else {
              get.addColumn(famAndQf[0], famAndQf[1]);
          }
        }
        get.setTimeRange(Long.MIN_VALUE, timestamp);
        Result result = table.get(get);
        return ThriftUtilities.rowResultFromHBase(result);
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public List<TRowResult> getRows(ByteBuffer tableName,
                                    List<ByteBuffer> rows)
        throws IOError {
      return getRowsWithColumnsTs(tableName, rows, null,
                                  HConstants.LATEST_TIMESTAMP);
    }

    @Override
    public List<TRowResult> getRowsWithColumns(ByteBuffer tableName,
                                               List<ByteBuffer> rows,
        List<ByteBuffer> columns) throws IOError {
      return getRowsWithColumnsTs(tableName, rows, columns,
                                  HConstants.LATEST_TIMESTAMP);
    }

    @Override
    public List<TRowResult> getRowsTs(ByteBuffer tableName,
                                      List<ByteBuffer> rows,
        long timestamp) throws IOError {
      return getRowsWithColumnsTs(tableName, rows, null,
                                  timestamp);
    }

    @Override
    public List<TRowResult> getRowsWithColumnsTs(ByteBuffer tableName,
                                                 List<ByteBuffer> rows,
        List<ByteBuffer> columns, long timestamp) throws IOError {
      try {
        List<Get> gets = new ArrayList<Get>(rows.size());
        HTable table = getTable(tableName);
        for (ByteBuffer row : rows) {
          Get get = new Get(getBytes(row));
          if (columns != null) {

            for(ByteBuffer column : columns) {
              byte [][] famAndQf = KeyValue.parseColumn(getBytes(column));
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
        Result[] result = table.get(gets);
        return ThriftUtilities.rowResultFromHBase(result);
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public void deleteAll(ByteBuffer tableName, ByteBuffer row, ByteBuffer column)
        throws IOError {
      deleteAllTs(tableName, row, column, HConstants.LATEST_TIMESTAMP);
    }

    @Override
    public void deleteAllTs(ByteBuffer tableName,
                            ByteBuffer row,
                            ByteBuffer column,
        long timestamp) throws IOError {
      try {
        HTable table = getTable(tableName);
        Delete delete  = new Delete(getBytes(row));
        byte [][] famAndQf = KeyValue.parseColumn(getBytes(column));
        if (famAndQf.length == 1) {
          delete.deleteFamily(famAndQf[0], timestamp);
        } else {
          delete.deleteColumns(famAndQf[0], famAndQf[1], timestamp);
        }
        table.delete(delete);

      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public void deleteAllRow(ByteBuffer tableName, ByteBuffer row) throws IOError {
      deleteAllRowTs(tableName, row, HConstants.LATEST_TIMESTAMP);
    }

    @Override
    public void deleteAllRowTs(ByteBuffer tableName, ByteBuffer row, long timestamp)
        throws IOError {
      try {
        HTable table = getTable(tableName);
        Delete delete  = new Delete(getBytes(row), timestamp, null);
        table.delete(delete);
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public void createTable(ByteBuffer in_tableName,
        List<ColumnDescriptor> columnFamilies) throws IOError,
        IllegalArgument, AlreadyExists {
      byte [] tableName = getBytes(in_tableName);
      try {
        if (admin.tableExists(tableName)) {
          throw new AlreadyExists("table name already in use");
        }
        HTableDescriptor desc = new HTableDescriptor(tableName);
        for (ColumnDescriptor col : columnFamilies) {
          HColumnDescriptor colDesc = ThriftUtilities.colDescFromThrift(col);
          desc.addFamily(colDesc);
        }
        admin.createTable(desc);
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      } catch (IllegalArgumentException e) {
        throw new IllegalArgument(e.getMessage());
      }
    }

    @Override
    public void deleteTable(ByteBuffer in_tableName) throws IOError {
      byte [] tableName = getBytes(in_tableName);
      if (LOG.isDebugEnabled()) {
        LOG.debug("deleteTable: table=" + Bytes.toString(tableName));
      }
      try {
        if (!admin.tableExists(tableName)) {
          throw new IOError("table does not exist");
        }
        admin.deleteTable(tableName);
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public void mutateRow(ByteBuffer tableName, ByteBuffer row,
        List<Mutation> mutations) throws IOError, IllegalArgument {
      mutateRowTs(tableName, row, mutations, HConstants.LATEST_TIMESTAMP);
    }

    @Override
    public void mutateRowTs(ByteBuffer tableName, ByteBuffer row,
        List<Mutation> mutations, long timestamp) throws IOError, IllegalArgument {
      HTable table = null;
      try {
        table = getTable(tableName);
        Put put = new Put(getBytes(row), timestamp, null);

        Delete delete = new Delete(getBytes(row));

        // I apologize for all this mess :)
        for (Mutation m : mutations) {
          byte[][] famAndQf = KeyValue.parseColumn(getBytes(m.column));
          if (m.isDelete) {
            if (famAndQf.length == 1) {
              delete.deleteFamily(famAndQf[0], timestamp);
            } else {
              delete.deleteColumns(famAndQf[0], famAndQf[1], timestamp);
            }
          } else {
            if(famAndQf.length == 1) {
              put.add(famAndQf[0], HConstants.EMPTY_BYTE_ARRAY,
                  m.value != null ? m.value.array()
                      : HConstants.EMPTY_BYTE_ARRAY);
            } else {
              put.add(famAndQf[0], famAndQf[1],
                  m.value != null ? m.value.array()
                      : HConstants.EMPTY_BYTE_ARRAY);
            }
          }
        }
        if (!delete.isEmpty())
          table.delete(delete);
        if (!put.isEmpty())
          table.put(put);
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      } catch (IllegalArgumentException e) {
        throw new IllegalArgument(e.getMessage());
      }
    }

    @Override
    public void mutateRows(ByteBuffer tableName, List<BatchMutation> rowBatches)
        throws IOError, IllegalArgument, TException {
      mutateRowsTs(tableName, rowBatches, HConstants.LATEST_TIMESTAMP);
    }

    @Override
    public void mutateRowsTs(ByteBuffer tableName, List<BatchMutation> rowBatches, long timestamp)
        throws IOError, IllegalArgument, TException {
      List<Put> puts = new ArrayList<Put>();
      List<Delete> deletes = new ArrayList<Delete>();

      for (BatchMutation batch : rowBatches) {
        byte[] row = getBytes(batch.row);
        List<Mutation> mutations = batch.mutations;
        Delete delete = new Delete(row);
        Put put = new Put(row, timestamp, null);
        for (Mutation m : mutations) {
          byte[][] famAndQf = KeyValue.parseColumn(getBytes(m.column));
          if (m.isDelete) {
            // no qualifier, family only.
            if (famAndQf.length == 1) {
              delete.deleteFamily(famAndQf[0], timestamp);
            } else {
              delete.deleteColumns(famAndQf[0], famAndQf[1], timestamp);
            }
          } else {
            if(famAndQf.length == 1) {
              put.add(famAndQf[0], HConstants.EMPTY_BYTE_ARRAY,
                  m.value != null ? m.value.array()
                      : HConstants.EMPTY_BYTE_ARRAY);
            } else {
              put.add(famAndQf[0], famAndQf[1],
                  m.value != null ? m.value.array()
                      : HConstants.EMPTY_BYTE_ARRAY);
            }
          }
        }
        if (!delete.isEmpty())
          deletes.add(delete);
        if (!put.isEmpty())
          puts.add(put);
      }

      HTable table = null;
      try {
        table = getTable(tableName);
        if (!puts.isEmpty())
          table.put(puts);
        for (Delete del : deletes) {
          table.delete(del);
        }
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      } catch (IllegalArgumentException e) {
        throw new IllegalArgument(e.getMessage());
      }
    }

    @Deprecated
    @Override
    public long atomicIncrement(ByteBuffer tableName, ByteBuffer row, ByteBuffer column,
        long amount) throws IOError, IllegalArgument, TException {
      byte [][] famAndQf = KeyValue.parseColumn(getBytes(column));
      if(famAndQf.length == 1) {
        return atomicIncrement(tableName, row, famAndQf[0], new byte[0],
            amount);
      }
      return atomicIncrement(tableName, row, famAndQf[0], famAndQf[1], amount);
    }

    protected long atomicIncrement(ByteBuffer tableName, ByteBuffer row, byte [] family,
        byte [] qualifier, long amount)
    throws IOError, IllegalArgument, TException {
      HTable table;
      try {
        table = getTable(tableName);
        return table.incrementColumnValue(getBytes(row), family, qualifier, amount);
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    public void scannerClose(int id) throws IOError, IllegalArgument {
      LOG.debug("scannerClose: id=" + id);
      ResultScanner scanner = getScanner(id);
      if (scanner == null) {
        throw new IllegalArgument("scanner ID is invalid");
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
        throw new IOError(e.getMessage());
      }
      return ThriftUtilities.rowResultFromHBase(results);
    }

    @Override
    public List<TRowResult> scannerGet(int id) throws IllegalArgument, IOError {
      return scannerGetList(id,1);
    }

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
        if (tScan.isSetTimestamp()) {
          scan.setTimeRange(Long.MIN_VALUE, tScan.getTimestamp());
        }
        if (tScan.isSetCaching()) {
          scan.setCaching(tScan.getCaching());
        }
        if (tScan.isSetColumns() && tScan.getColumns().size() != 0) {
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
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public int scannerOpen(ByteBuffer tableName, ByteBuffer startRow,
        List<ByteBuffer> columns) throws IOError {
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
        throw new IOError(e.getMessage());
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
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public int scannerOpenWithPrefix(ByteBuffer tableName,
                                     ByteBuffer startAndPrefix,
                                     List<ByteBuffer> columns)
        throws IOError, TException {
      try {
        HTable table = getTable(tableName);
        Scan scan = new Scan(getBytes(startAndPrefix));
        Filter f = new WhileMatchFilter(
            new PrefixFilter(getBytes(startAndPrefix)));
        scan.setFilter(f);
        if (columns != null && columns.size() != 0) {
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
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public int scannerOpenTs(ByteBuffer tableName, ByteBuffer startRow,
        List<ByteBuffer> columns, long timestamp) throws IOError, TException {
      try {
        HTable table = getTable(tableName);
        Scan scan = new Scan(getBytes(startRow));
        scan.setTimeRange(Long.MIN_VALUE, timestamp);
        if (columns != null && columns.size() != 0) {
          for (ByteBuffer column : columns) {
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
        throw new IOError(e.getMessage());
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
        if (columns != null && columns.size() != 0) {
          for (ByteBuffer column : columns) {
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
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public Map<ByteBuffer, ColumnDescriptor> getColumnDescriptors(
        ByteBuffer tableName) throws IOError, TException {
      try {
        TreeMap<ByteBuffer, ColumnDescriptor> columns =
          new TreeMap<ByteBuffer, ColumnDescriptor>();

        HTable table = getTable(tableName);
        HTableDescriptor desc = table.getTableDescriptor();

        for (HColumnDescriptor e : desc.getFamilies()) {
          ColumnDescriptor col = ThriftUtilities.colDescFromHbase(e);
          columns.put(col.name, col);
        }
        return columns;
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public List<TCell> getRowOrBefore(ByteBuffer tableName, ByteBuffer row,
        ByteBuffer family) throws IOError {
      try {
        HTable table = getTable(getBytes(tableName));
        Result result = table.getRowOrBefore(getBytes(row), getBytes(family));
        return ThriftUtilities.cellFromHBase(result.raw());
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    @Override
    public TRegionInfo getRegionInfo(ByteBuffer searchRow) throws IOError {
      try {
        HTable table = getTable(HConstants.META_TABLE_NAME);
        Result startRowResult = table.getRowOrBefore(
          searchRow.array(), HConstants.CATALOG_FAMILY);

        if (startRowResult == null) {
          throw new IOException("Cannot find row in .META., row="
                                + Bytes.toString(searchRow.array()));
        }

        // find region start and end keys
        byte[] value = startRowResult.getValue(HConstants.CATALOG_FAMILY,
                                               HConstants.REGIONINFO_QUALIFIER);
        if (value == null || value.length == 0) {
          throw new IOException("HRegionInfo REGIONINFO was null or " +
                                " empty in Meta for row="
                                + Bytes.toString(searchRow.array()));
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
          String hostAndPort = Bytes.toString(value);
          region.setServerName(Bytes.toBytes(
              Addressing.parseHostname(hostAndPort)));
          region.port = Addressing.parsePort(hostAndPort);
        }
        return region;
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }
  }

  public ThriftServer(Configuration conf) {
    this.conf = HBaseConfiguration.create(conf);
  }

  //
  // Main program and support routines
  //

  private static void printUsageAndExit(Options options, int exitCode)
      throws ExitCodeException {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("Thrift", null, options,
        "To start the Thrift server run 'bin/hbase-daemon.sh start thrift'\n" +
        "To shutdown the thrift server run 'bin/hbase-daemon.sh stop " +
        "thrift' or send a kill signal to the thrift server pid",
        true);
    throw new ExitCodeException(exitCode, "");
  }

  /*
   * Start up or shuts down the Thrift server, depending on the arguments.
   * @param args
   */
   void doMain(final String[] args) throws Exception {
    Options options = new Options();
    options.addOption("b", BIND_OPTION, true, "Address to bind " +
        "the Thrift server to. Not supported by the Nonblocking and " +
        "HsHa server [default: " + DEFAULT_BIND_ADDR + "]");
    options.addOption("p", PORT_OPTION, true, "Port to bind to [default: " +
        DEFAULT_LISTEN_PORT + "]");
    options.addOption("f", FRAMED_OPTION, false, "Use framed transport");
    options.addOption("c", COMPACT_OPTION, false, "Use the compact protocol");
    options.addOption("h", "help", false, "Print help information");

    options.addOption("m", MIN_WORKERS_OPTION, true,
        "The minimum number of worker threads for " +
        ImplType.THREAD_POOL.simpleClassName());

    options.addOption("w", MAX_WORKERS_OPTION, true,
        "The maximum number of worker threads for " +
        ImplType.THREAD_POOL.simpleClassName());

    options.addOption("q", MAX_QUEUE_SIZE_OPTION, true,
        "The maximum number of queued requests in " +
        ImplType.THREAD_POOL.simpleClassName());

    options.addOption("k", KEEP_ALIVE_SEC_OPTION, true,
        "The amount of time in secods to keep a thread alive when idle in " +
        ImplType.THREAD_POOL.simpleClassName());

    options.addOptionGroup(ImplType.createOptionGroup());

    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args);

    // This is so complicated to please both bin/hbase and bin/hbase-daemon.
    // hbase-daemon provides "start" and "stop" arguments
    // hbase should print the help if no argument is provided
    List<String> commandLine = Arrays.asList(args);
    boolean stop = commandLine.contains("stop");
    boolean start = commandLine.contains("start");
    boolean invalidStartStop = (start && stop) || (!start && !stop);
    if (cmd.hasOption("help") || invalidStartStop) {
      if (invalidStartStop) {
        LOG.error("Exactly one of 'start' and 'stop' has to be specified");
      }
      printUsageAndExit(options, 1);
    }

    // Get port to bind to
    int listenPort = 0;
    try {
      listenPort = Integer.parseInt(cmd.getOptionValue(PORT_OPTION,
          String.valueOf(DEFAULT_LISTEN_PORT)));
    } catch (NumberFormatException e) {
      LOG.error("Could not parse the value provided for the port option", e);
      printUsageAndExit(options, -1);
    }

    // Make optional changes to the configuration based on command-line options
    optionToConf(cmd, MIN_WORKERS_OPTION,
        conf, TBoundedThreadPoolServer.MIN_WORKER_THREADS_CONF_KEY);
    optionToConf(cmd, MAX_WORKERS_OPTION,
        conf, TBoundedThreadPoolServer.MAX_WORKER_THREADS_CONF_KEY);
    optionToConf(cmd, MAX_QUEUE_SIZE_OPTION,
        conf, TBoundedThreadPoolServer.MAX_QUEUED_REQUESTS_CONF_KEY);
    optionToConf(cmd, KEEP_ALIVE_SEC_OPTION,
        conf, TBoundedThreadPoolServer.THREAD_KEEP_ALIVE_TIME_SEC_CONF_KEY);

    // Construct correct ProtocolFactory
    TProtocolFactory protocolFactory;
    if (cmd.hasOption(COMPACT_OPTION)) {
      LOG.debug("Using compact protocol");
      protocolFactory = new TCompactProtocol.Factory();
    } else {
      LOG.debug("Using binary protocol");
      protocolFactory = new TBinaryProtocol.Factory();
    }

    HBaseHandler handler = new HBaseHandler(conf);
    Hbase.Processor<Hbase.Iface> processor =
        new Hbase.Processor<Hbase.Iface>(handler);
    ImplType implType = ImplType.getServerImpl(cmd);

    // Construct correct TransportFactory
    TTransportFactory transportFactory;
    if (cmd.hasOption(FRAMED_OPTION) || implType.isAlwaysFramed) {
      transportFactory = new TFramedTransport.Factory();
      LOG.debug("Using framed transport");
    } else {
      transportFactory = new TTransportFactory();
    }

    if (cmd.hasOption(BIND_OPTION) && !implType.canSpecifyBindIP) {
      LOG.error("Server types " + Joiner.on(", ").join(
          ImplType.serversThatCannotSpecifyBindIP()) + " don't support IP " +
          "address binding at the moment. See " +
          "https://issues.apache.org/jira/browse/HBASE-2155 for details.");
      printUsageAndExit(options, -1);
    }

    if (implType == ImplType.HS_HA || implType == ImplType.NONBLOCKING) {
      if (cmd.hasOption(BIND_OPTION)) {
        throw new RuntimeException("-" + BIND_OPTION + " not supported with " +
            implType);
      }

      TNonblockingServerTransport serverTransport =
          new TNonblockingServerSocket(listenPort);

      if (implType == ImplType.NONBLOCKING) {
        TNonblockingServer.Args serverArgs =
            new TNonblockingServer.Args(serverTransport);
        setServerArgs(serverArgs, processor, transportFactory,
            protocolFactory);
        server = new TNonblockingServer(serverArgs);
      } else {
        THsHaServer.Args serverArgs = new THsHaServer.Args(serverTransport);
        serverArgs.processor(processor);
        serverArgs.transportFactory(transportFactory);
        serverArgs.protocolFactory(protocolFactory);
        server = new THsHaServer(serverArgs);
      }
      LOG.info("starting HBase " + implType.simpleClassName() +
          " server on " + Integer.toString(listenPort));
    } else if (implType == ImplType.THREAD_POOL) {
      // Thread pool server. Get the IP address to bind to.
      InetAddress listenAddress = getBindAddress(options, cmd);

      TServerTransport serverTransport = new TServerSocket(
          new InetSocketAddress(listenAddress, listenPort));

      TBoundedThreadPoolServer.Args serverArgs = new TBoundedThreadPoolServer.Args(
          serverTransport, conf);
      setServerArgs(serverArgs, processor, transportFactory, protocolFactory);
      LOG.info("starting " + ImplType.THREAD_POOL.simpleClassName() + " on "
          + listenAddress + ":" + Integer.toString(listenPort)
          + "; " + serverArgs);
      server = new TBoundedThreadPoolServer(serverArgs);
    } else {
      throw new AssertionError("Unsupported Thrift server implementation: " +
          implType.simpleClassName());
    }

    // A sanity check that we instantiated the right type of server.
    if (server.getClass() != implType.serverClass) {
      throw new AssertionError("Expected to create Thrift server class " +
          implType.serverClass.getName() + " but got " +
          server.getClass().getName());
    }

    server.serve();
  }

  public void stop() {
    server.stop();
  }

  private InetAddress getBindAddress(Options options, CommandLine cmd)
      throws ExitCodeException {
    InetAddress listenAddress = null;
    String bindAddressStr = cmd.getOptionValue(BIND_OPTION, DEFAULT_BIND_ADDR);
    try {
      listenAddress = InetAddress.getByName(bindAddressStr);
    } catch (UnknownHostException e) {
      LOG.error("Could not resolve the bind address specified: " +
          bindAddressStr, e);
      printUsageAndExit(options, -1);
    }
    return listenAddress;
  }

  private static void setServerArgs(AbstractServerArgs<?> serverArgs,
      Processor<Iface> processor, TTransportFactory transportFactory,
      TProtocolFactory protocolFactory) {
    serverArgs.processor(processor);
    serverArgs.transportFactory(transportFactory);
    serverArgs.protocolFactory(protocolFactory);
  }

  private static void optionToConf(CommandLine cmd, String option,
      Configuration conf, String destConfKey) {
    if (cmd.hasOption(option)) {
      conf.set(destConfKey, cmd.getOptionValue(option));
    }
  }

  /**
   * @param args
   * @throws Exception
   */
  public static void main(String [] args) throws Exception {
    VersionInfo.logVersion();
    try {
      new ThriftServer(HBaseConfiguration.create()).doMain(args);
    } catch (ExitCodeException ex) {
      System.exit(ex.getExitCode());
    }
  }

}
