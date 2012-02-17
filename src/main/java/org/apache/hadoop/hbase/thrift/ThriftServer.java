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

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
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
import org.apache.hadoop.hbase.HServerAddress;
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
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
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
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportFactory;

/**
 * ThriftServer - this class starts up a Thrift server which implements the
 * Hbase API specified in the Hbase.thrift IDL file.
 */
public class ThriftServer {

  private static final Class<? extends TServer>
      THREAD_POOL_SERVER_CLASS = HBaseThreadPoolServer.class;

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

    final private ThriftMetrics metrics;

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
    protected HTable getTable(final byte[] tableName) throws IOError,
        IOException {
      String table = new String(tableName);
      Map<String, HTable> tables = threadLocalTables.get();
      if (!tables.containsKey(table)) {
        tables.put(table, new HTable(conf, tableName));
      }
      return tables.get(table);
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

    /** Constructs a handler with configuration based on the given one. */
    protected HBaseHandler(
        HBaseConfiguration conf, ThriftMetrics metrics) throws IOException {
      this(HBaseConfiguration.create(conf), metrics);
      LOG.debug("Creating HBaseHandler with ZK client port " +
          conf.get(HConstants.ZOOKEEPER_CLIENT_PORT));
    }

    protected HBaseHandler(
        final Configuration c, ThriftMetrics metrics) throws IOException {
      this.metrics = metrics;
      this.conf = c;
      admin = new HBaseAdmin(conf);
      scannerMap = new HashMap<Integer, ResultScanner>();
    }

    /** Create a handler without metrics. Used by unit test only */
    protected HBaseHandler(final Configuration c) throws IOException {
      this(c, null);
    }

    public void enableTable(final byte[] tableName) throws IOError {
      try{
        admin.enableTable(tableName);
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    public void disableTable(final byte[] tableName) throws IOError{
      try{
        admin.disableTable(tableName);
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    public boolean isTableEnabled(final byte[] tableName) throws IOError {
      try {
        return HTable.isTableEnabled(conf, tableName);
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    public void compact(byte[] tableNameOrRegionName) throws IOError {
      try{
        admin.compact(tableNameOrRegionName);
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    public void majorCompact(byte[] tableNameOrRegionName) throws IOError {
      try{
        admin.majorCompact(tableNameOrRegionName);
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    public List<byte[]> getTableNames() throws IOError {
      try {
        HTableDescriptor[] tables = this.admin.listTables();
        ArrayList<byte[]> list = new ArrayList<byte[]>(tables.length);
        for (int i = 0; i < tables.length; i++) {
          list.add(tables[i].getName());
        }
        return list;
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    public List<TRegionInfo> getTableRegions(byte[] tableName)
    throws IOError {
      try{
        HTable table = getTable(tableName);
        Map<HRegionInfo, HServerAddress> regionsInfo = table.getRegionsInfo();
        List<TRegionInfo> regions = new ArrayList<TRegionInfo>();

        for (HRegionInfo regionInfo : regionsInfo.keySet()){
          TRegionInfo region = new TRegionInfo();
          region.startKey = regionInfo.getStartKey();
          region.endKey = regionInfo.getEndKey();
          region.id = regionInfo.getRegionId();
          region.name = regionInfo.getRegionName();
          region.version = regionInfo.getVersion();
          HServerAddress server = regionsInfo.get(regionInfo);
          if (server != null) {
            byte[] hostname = Bytes.toBytes(server.getHostname());
            region.serverName = hostname;
            region.port = server.getPort();
          }
          regions.add(region);
        }
        return regions;
      } catch (IOException e){
        throw new IOError(e.getMessage());
      }
    }

    @Deprecated
    public List<TCell> get(byte[] tableName, byte[] row, byte[] column)
        throws IOError {
      byte [][] famAndQf = KeyValue.parseColumn(column);
      if(famAndQf.length == 1) {
        return get(tableName, row, famAndQf[0], new byte[0]);
      }
      return get(tableName, row, famAndQf[0], famAndQf[1]);
    }

    public List<TCell> get(byte [] tableName, byte [] row, byte [] family,
        byte [] qualifier) throws IOError {
      try {
        HTable table = getTable(tableName);
        Get get = new Get(row);
        if (qualifier == null || qualifier.length == 0) {
          get.addFamily(family);
        } else {
          get.addColumn(family, qualifier);
        }
        Result result = table.get(get);
        return ThriftUtilities.cellFromHBase(result.sorted());
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    @Deprecated
    public List<TCell> getVer(byte[] tableName, byte[] row,
        byte[] column, int numVersions) throws IOError {
      byte [][] famAndQf = KeyValue.parseColumn(column);
      if(famAndQf.length == 1) {
        return getVer(tableName, row, famAndQf[0], new byte[0], numVersions);
      }
      return getVer(tableName, row, famAndQf[0], famAndQf[1], numVersions);
    }

    public List<TCell> getVer(byte [] tableName, byte [] row, byte [] family,
        byte [] qualifier, int numVersions) throws IOError {
      try {
        HTable table = getTable(tableName);
        Get get = new Get(row);
        get.addColumn(family, qualifier);
        get.setMaxVersions(numVersions);
        Result result = table.get(get);
        return ThriftUtilities.cellFromHBase(result.sorted());
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    @Deprecated
    public List<TCell> getVerTs(byte[] tableName, byte[] row,
        byte[] column, long timestamp, int numVersions) throws IOError {
      byte [][] famAndQf = KeyValue.parseColumn(column);
      if(famAndQf.length == 1) {
        return getVerTs(tableName, row, famAndQf[0], new byte[0], timestamp,
            numVersions);
      }
      return getVerTs(tableName, row, famAndQf[0], famAndQf[1], timestamp,
          numVersions);
    }

    public List<TCell> getVerTs(byte [] tableName, byte [] row, byte [] family,
        byte [] qualifier, long timestamp, int numVersions) throws IOError {
      try {
        HTable table = getTable(tableName);
        Get get = new Get(row);
        get.addColumn(family, qualifier);
        get.setTimeRange(Long.MIN_VALUE, timestamp);
        get.setMaxVersions(numVersions);
        Result result = table.get(get);
        return ThriftUtilities.cellFromHBase(result.sorted());
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    public List<TRowResult> getRow(byte[] tableName, byte[] row)
        throws IOError {
      return getRowWithColumnsTs(tableName, row, null,
                                 HConstants.LATEST_TIMESTAMP);
    }

    public List<TRowResult> getRowWithColumns(byte[] tableName, byte[] row,
        List<byte[]> columns) throws IOError {
      return getRowWithColumnsTs(tableName, row, columns,
                                 HConstants.LATEST_TIMESTAMP);
    }

    public List<TRowResult> getRowTs(byte[] tableName, byte[] row,
        long timestamp) throws IOError {
      return getRowWithColumnsTs(tableName, row, null,
                                 timestamp);
    }

    public List<TRowResult> getRowWithColumnsTs(byte[] tableName, byte[] row,
        List<byte[]> columns, long timestamp) throws IOError {
      try {
        HTable table = getTable(tableName);
        if (columns == null) {
          Get get = new Get(row);
          get.setTimeRange(Long.MIN_VALUE, timestamp);
          Result result = table.get(get);
          return ThriftUtilities.rowResultFromHBase(result);
        }
        byte[][] columnArr = columns.toArray(new byte[columns.size()][]);
        Get get = new Get(row);
        for(byte [] column : columnArr) {
          byte [][] famAndQf = KeyValue.parseColumn(column);
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
    public List<TRowResult> getRowWithColumnPrefix(byte[] tableName,
                                                   byte[] row, byte[] prefix) throws IOError {
      return (getRowWithColumnPrefixTs(tableName, row, prefix,
                                       HConstants.LATEST_TIMESTAMP));
    }

    @Override
    public List<TRowResult> getRowWithColumnPrefixTs(byte[] tableName,
                                                     byte[] row, byte[] prefix, long timestamp) throws IOError {
      try {
        HTable table = getTable(tableName);
        if (prefix == null) {
          Get get = new Get(row);
          get.setTimeRange(Long.MIN_VALUE, timestamp);
          Result result = table.get(get);
          return ThriftUtilities.rowResultFromHBase(result);
        }
        Get get = new Get(row);
        byte [][] famAndPrefix = KeyValue.parseColumn(prefix);
        if (famAndPrefix.length == 2) {
          get.addFamily(famAndPrefix[0]);
          get.setFilter(new ColumnPrefixFilter(famAndPrefix[1]));
        } else {
          get.setFilter(new ColumnPrefixFilter(famAndPrefix[0]));
        }
        get.setTimeRange(Long.MIN_VALUE, timestamp);
        Result result = table.get(get);
        return ThriftUtilities.rowResultFromHBase(result);
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    public void deleteAll(byte[] tableName, byte[] row, byte[] column)
        throws IOError {
      deleteAllTs(tableName, row, column, HConstants.LATEST_TIMESTAMP);
    }

    @Override
    public List<TRowResult> getRows(byte[] tableName, List<byte[]> rows)
        throws IOError {
      return getRowsWithColumnsTs(tableName, rows, null,
          HConstants.LATEST_TIMESTAMP);
    }

    @Override
    public List<TRowResult> getRowsWithColumns(byte[] tableName,
        List<byte[]> rows, List<byte[]> columns) throws IOError {
      return getRowsWithColumnsTs(tableName, rows, columns,
          HConstants.LATEST_TIMESTAMP);
    }

    @Override
    public List<TRowResult> getRowsTs(byte[] tableName, List<byte[]> rows,
        long timestamp) throws IOError {
      return getRowsWithColumnsTs(tableName, rows, null, timestamp);
    }

    @Override
    public List<TRowResult> getRowsWithColumnsTs(byte[] tableName,
        List<byte[]> rows, List<byte[]> columns, long timestamp) throws IOError {
      try {
        List<Get> gets = new ArrayList<Get>(rows.size());
        HTable table = getTable(tableName);
        if (metrics != null) {
          metrics.incNumBatchGetRowKeys(rows.size());
        }

        // For now, don't support ragged gets, with different columns per row
        // Probably pretty sensible indefinitely anyways.
        for (byte[] row : rows) {
          Get get = new Get(row);
          if (columns != null) {
            for (byte[] column : columns) {
              byte[][] famAndQf = KeyValue.parseColumn(column);
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

    public void deleteAllTs(byte[] tableName, byte[] row, byte[] column,
        long timestamp) throws IOError {
      try {
        HTable table = getTable(tableName);
        Delete delete  = new Delete(row);
        byte [][] famAndQf = KeyValue.parseColumn(column);
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

    public void deleteAllRow(byte[] tableName, byte[] row) throws IOError {
      deleteAllRowTs(tableName, row, HConstants.LATEST_TIMESTAMP);
    }

    public void deleteAllRowTs(byte[] tableName, byte[] row, long timestamp)
        throws IOError {
      try {
        HTable table = getTable(tableName);
        Delete delete  = new Delete(row, timestamp, null);
        table.delete(delete);
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    public void createTable(byte[] tableName,
        List<ColumnDescriptor> columnFamilies) throws IOError,
        IllegalArgument, AlreadyExists {
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

    public void deleteTable(byte[] tableName) throws IOError {
      if (LOG.isDebugEnabled()) {
        LOG.debug("deleteTable: table=" + new String(tableName));
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

    public void mutateRow(byte[] tableName, byte[] row,
        List<Mutation> mutations) throws IOError, IllegalArgument {
      mutateRowTs(tableName, row, mutations, HConstants.LATEST_TIMESTAMP);
    }

    public void mutateRowTs(byte[] tableName, byte[] row,
        List<Mutation> mutations, long timestamp) throws IOError, IllegalArgument {
      HTable table = null;
      try {
        table = getTable(tableName);
        Put put = new Put(row, timestamp, null);

        Delete delete = new Delete(row);

        // I apologize for all this mess :)
        for (Mutation m : mutations) {
          byte[][] famAndQf = KeyValue.parseColumn(m.column);
          if (m.isDelete) {
            if (famAndQf.length == 1) {
              delete.deleteFamily(famAndQf[0], timestamp);
            } else {
              delete.deleteColumns(famAndQf[0], famAndQf[1], timestamp);
            }
          } else {
            if(famAndQf.length == 1) {
              put.add(famAndQf[0], new byte[0], m.value);
            } else {
              put.add(famAndQf[0], famAndQf[1], m.value);
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

    public void mutateRows(byte[] tableName, List<BatchMutation> rowBatches)
        throws IOError, IllegalArgument, TException {
      mutateRowsTs(tableName, rowBatches, HConstants.LATEST_TIMESTAMP);
    }

    public void mutateRowsTs(byte[] tableName, List<BatchMutation> rowBatches, long timestamp)
        throws IOError, IllegalArgument, TException {
      List<Put> puts = new ArrayList<Put>();
      List<Delete> deletes = new ArrayList<Delete>();
      if (metrics != null) {
        metrics.incNumBatchMutateRowKeys(rowBatches.size());
      }

      for (BatchMutation batch : rowBatches) {
        byte[] row = batch.row;
        List<Mutation> mutations = batch.mutations;
        Delete delete = new Delete(row);
        Put put = new Put(row, timestamp, null);
        for (Mutation m : mutations) {
          byte[][] famAndQf = KeyValue.parseColumn(m.column);
          if (m.isDelete) {
            // no qualifier, family only.
            if (famAndQf.length == 1) {
              delete.deleteFamily(famAndQf[0], timestamp);
            } else {
              delete.deleteColumns(famAndQf[0], famAndQf[1], timestamp);
            }
          } else {
            if(famAndQf.length == 1) {
              put.add(famAndQf[0], new byte[0], m.value);
            } else {
              put.add(famAndQf[0], famAndQf[1], m.value);
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

    /**
     * Warning; the puts and deletes are NOT atomic together and so a lot of
     * weird things can happen if you expect that to be the case!!
     *
     * A valueCheck of null means that the row can't exist before being put.
     * This is kind of a stupid thing to enforce when deleting, for obvious
     * reasons.
     */
    @Override
    public boolean checkAndMutateRow(byte[] tableName, byte[] row,
        byte[] columnCheck, byte[] valueCheck, List<Mutation> mutations)
        throws IOError, IllegalArgument {
      return checkAndMutateRowTs(tableName, row, columnCheck, valueCheck,
          mutations, HConstants.LATEST_TIMESTAMP);
    }

    @Override
    public boolean checkAndMutateRowTs(byte[] tableName, byte[] row,
        byte[] columnCheck, byte[] valueCheck,
        List<Mutation> mutations,
        long timestamp) throws IOError, IllegalArgument {
      HTable table;
      try {
        table = getTable(tableName);
        Put put = new Put(row, timestamp, null);

        Delete delete = new Delete(row);

        for (Mutation m : mutations) {
          byte[][] famAndQf = KeyValue.parseColumn(m.column);
          if (m.isDelete) {
            if (famAndQf.length == 1) {
              delete.deleteFamily(famAndQf[0], timestamp);
            } else {
              delete.deleteColumns(famAndQf[0], famAndQf[1], timestamp);
            }
          } else {
            if (famAndQf.length == 1) {
              put.add(famAndQf[0], HConstants.EMPTY_BYTE_ARRAY, m.value);
            } else {
              put.add(famAndQf[0], famAndQf[1], m.value);
            }
          }
        }
        byte[][] famAndQfCheck = KeyValue.parseColumn(columnCheck);

        if (!delete.isEmpty() && !put.isEmpty()) {
          // can't do both, not atomic, not good idea!
          throw new IllegalArgumentException(
              "Single Thrift CheckAndMutate call cannot do both puts and deletes.");
        }
        if (!delete.isEmpty()) {
          return table.checkAndDelete(row, famAndQfCheck[0],
                  famAndQfCheck.length != 1 ? famAndQfCheck[1]
                      : HConstants.EMPTY_BYTE_ARRAY, valueCheck, delete);
        }
        if (!put.isEmpty()) {
          return table.checkAndPut(row, famAndQfCheck[0],
                  famAndQfCheck.length != 1 ? famAndQfCheck[1]
                      : HConstants.EMPTY_BYTE_ARRAY, valueCheck, put);
        }
        throw new IllegalArgumentException(
            "Thrift CheckAndMutate call must do either put or delete.");
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      } catch (IllegalArgumentException e) {
        throw new IllegalArgument(e.getMessage());
      }
    }

    @Deprecated
    public long atomicIncrement(byte[] tableName, byte[] row, byte[] column,
        long amount) throws IOError, IllegalArgument, TException {
      byte [][] famAndQf = KeyValue.parseColumn(column);
      if(famAndQf.length == 1) {
        return atomicIncrement(tableName, row, famAndQf[0], new byte[0],
            amount);
      }
      return atomicIncrement(tableName, row, famAndQf[0], famAndQf[1], amount);
    }

    public long atomicIncrement(byte [] tableName, byte [] row, byte [] family,
        byte [] qualifier, long amount)
    throws IOError, IllegalArgument, TException {
      HTable table;
      try {
        table = getTable(tableName);
        return table.incrementColumnValue(row, family, qualifier, amount);
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
    public List<TRowResult> scannerGet(int id) throws IllegalArgument, IOError {
        return scannerGetList(id,1);
    }
    public int scannerOpen(byte[] tableName, byte[] startRow,
            List<byte[]> columns) throws IOError {
        try {
          HTable table = getTable(tableName);
          Scan scan = new Scan(startRow);
          if(columns != null && columns.size() != 0) {
            for(byte [] column : columns) {
              byte [][] famQf = KeyValue.parseColumn(column);
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

    public int scannerOpenWithStop(byte[] tableName, byte[] startRow,
        byte[] stopRow, List<byte[]> columns) throws IOError, TException {
      try {
        HTable table = getTable(tableName);
        Scan scan = new Scan(startRow, stopRow);
        if(columns != null && columns.size() != 0) {
          for(byte [] column : columns) {
            byte [][] famQf = KeyValue.parseColumn(column);
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
    public int scannerOpenWithPrefix(byte[] tableName, byte[] startAndPrefix, List<byte[]> columns) throws IOError, TException {
      try {
        HTable table = getTable(tableName);
        Scan scan = new Scan(startAndPrefix);
        Filter f = new WhileMatchFilter(
            new PrefixFilter(startAndPrefix));
        scan.setFilter(f);
        if(columns != null && columns.size() != 0) {
          for(byte [] column : columns) {
            byte [][] famQf = KeyValue.parseColumn(column);
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

    public int scannerOpenWithScan(byte [] tableName, TScan tScan) throws IOError {
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
        if(tScan.isSetColumns() && tScan.getColumns().size() != 0) {
          for(byte [] column : tScan.getColumns()) {
            byte [][] famQf = KeyValue.parseColumn(column);
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

    public int scannerOpenTs(byte[] tableName, byte[] startRow,
        List<byte[]> columns, long timestamp) throws IOError, TException {
      try {
        HTable table = getTable(tableName);
        Scan scan = new Scan(startRow);
        scan.setTimeRange(Long.MIN_VALUE, timestamp);
        if(columns != null && columns.size() != 0) {
          for(byte [] column : columns) {
            byte [][] famQf = KeyValue.parseColumn(column);
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

    public int scannerOpenWithStopTs(byte[] tableName, byte[] startRow,
        byte[] stopRow, List<byte[]> columns, long timestamp)
        throws IOError, TException {
      try {
        HTable table = getTable(tableName);
        Scan scan = new Scan(startRow, stopRow);
        scan.setTimeRange(Long.MIN_VALUE, timestamp);
        if(columns != null && columns.size() != 0) {
          for(byte [] column : columns) {
            byte [][] famQf = KeyValue.parseColumn(column);
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
    public int scannerOpenWithFilterString(byte [] tableName,
                                           byte [] filterString) throws IOError, TException {
      return scannerOpenWithFilterStringTs(tableName, filterString, Long.MAX_VALUE);
    }

    @Override
    public int scannerOpenWithFilterStringTs(byte [] tableName, byte [] filterString,
                                             long timestamp) throws IOError, TException {
      return scannerOpenWithStopAndFilterStringTs(tableName,
                                                  HConstants.EMPTY_START_ROW,
                                                  HConstants.EMPTY_END_ROW,
                                                  filterString, timestamp);
    }

    @Override
    public int scannerOpenWithStopAndFilterString(byte [] tableName,
                                                  byte [] startRow, byte [] stopRow,
                                                  byte [] filterString)
      throws IOError, TException {
      return scannerOpenWithStopAndFilterStringTs(tableName, startRow, stopRow,
                                                  filterString, Long.MAX_VALUE);
    }

    @Override
    public int scannerOpenWithStopAndFilterStringTs(byte [] tableName, byte [] startRow,
                                                    byte [] stopRow, byte [] filterString,
                                                    long timestamp) throws IOError, TException {
      try {
        HTable table = getTable(tableName);
        Scan scan = new Scan(startRow, stopRow);
        scan.setTimeRange(Long.MIN_VALUE, timestamp);

        if (filterString != null && filterString.length != 0) {
          ParseFilter parseFilter = new ParseFilter();
          scan.setFilter(parseFilter.parseFilterString(filterString));
        }
        return addScanner(table.getScanner(scan));
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

    public Map<byte[], ColumnDescriptor> getColumnDescriptors(
        byte[] tableName) throws IOError, TException {
      try {
        TreeMap<byte[], ColumnDescriptor> columns =
          new TreeMap<byte[], ColumnDescriptor>(Bytes.BYTES_COMPARATOR);

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
    public TRegionInfo getRegionInfo(byte[] searchRow) throws IOError,
        TException {
      try {
        HTable table = getTable(HConstants.META_TABLE_NAME);
        Result startRowResult = table.getRowOrBefore(searchRow,
            HConstants.CATALOG_FAMILY);

        if (startRowResult == null) {
          throw new IOException("Cannot find row in .META., row="
              + Bytes.toString(searchRow));
        }

        // find region start and end keys
        byte[] value = startRowResult.getValue(HConstants.CATALOG_FAMILY,
            HConstants.REGIONINFO_QUALIFIER);
        if (value == null || value.length == 0) {
          throw new IOException("HRegionInfo REGIONINFO was null or "
              + " empty in Meta for row=" + Bytes.toString(searchRow));
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
          region.serverName = hostname;
          region.port = server.getPort();
        }
        return region;
      } catch (IOException e) {
        throw new IOError(e.getMessage());
      }
    }

  }

  //
  // Main program and support routines
  //

  private static void printUsageAndExit(Options options, int exitCode) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("Thrift", null, options,
            "To start the Thrift server run 'bin/hbase-daemon.sh start thrift'\n" +
            "To shutdown the thrift server run 'bin/hbase-daemon.sh stop thrift' or" +
            " send a kill signal to the thrift server pid",
            true);
      System.exit(exitCode);
  }

  static final String DEFAULT_LISTEN_PORT = "9090";

  /*
   * Start up the Thrift server.
   * @param args
   */
  static private void doMain(final String[] args) throws Exception {
    Log LOG = LogFactory.getLog("ThriftServer");

    Options options = new Options();
    options.addOption("b", "bind", true, "Address to bind the Thrift server to. Not supported by the Nonblocking and HsHa server [default: 0.0.0.0]");
    options.addOption("p", "port", true, "Port to bind to [default: 9090]");
    options.addOption("f", "framed", false, "Use framed transport");
    options.addOption("c", "compact", false, "Use the compact protocol");
    options.addOption("h", "help", false, "Print help information");
    options.addOption("m", "minWorkers", true, "The minimum number of worker " +
        "threads for " + THREAD_POOL_SERVER_CLASS.getSimpleName());
    options.addOption("w", "workers", true, "The maximum number of worker " +
        "threads for " + THREAD_POOL_SERVER_CLASS.getSimpleName());
    options.addOption("q", "queue", true, "The maximum number of queued " +
        "requests in " + THREAD_POOL_SERVER_CLASS.getSimpleName());

    OptionGroup servers = new OptionGroup();
    servers.addOption(new Option("nonblocking", false, "Use the TNonblockingServer. This implies the framed transport."));
    servers.addOption(new Option("hsha", false, "Use the THsHaServer. This implies the framed transport."));
    servers.addOption(new Option("threadpool", false, "Use "
        + THREAD_POOL_SERVER_CLASS.getSimpleName() + ". This is the default."));
    options.addOptionGroup(servers);

    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args);

    Configuration conf = HBaseConfiguration.create();

    /**
     * This is so complicated to please both bin/hbase and bin/hbase-daemon.
     * hbase-daemon provides "start" and "stop" arguments
     * hbase should print the help if no argument is provided
     */
    List<String> commandLine = Arrays.asList(args);
    boolean stop = commandLine.contains("stop");
    boolean start = commandLine.contains("start");
    if (cmd.hasOption("help") || !start || stop) {
      printUsageAndExit(options, 1);
    }

    // Get port to bind to
    int listenPort = 0;
    try {
      listenPort = Integer.parseInt(cmd.getOptionValue("port", DEFAULT_LISTEN_PORT));
    } catch (NumberFormatException e) {
      LOG.error("Could not parse the value provided for the port option", e);
      printUsageAndExit(options, -1);
    }

    // Make optional changes to the configuration based on command-line options
    if (cmd.hasOption("minWorkers")) {
      conf.set(HBaseThreadPoolServer.MIN_WORKER_THREADS_CONF_KEY,
          cmd.getOptionValue("minWorkers"));
    }

    if (cmd.hasOption("workers")) {
      conf.set(HBaseThreadPoolServer.MAX_WORKER_THREADS_CONF_KEY,
          cmd.getOptionValue("workers"));
    }

    if (cmd.hasOption("queue")) {
      conf.set(HBaseThreadPoolServer.MAX_QUEUED_REQUESTS_CONF_KEY,
          cmd.getOptionValue("queue"));
    }

    // Only instantiate this when finished modifying the configuration
    HBaseThreadPoolServer.Options serverOptions =
      new HBaseThreadPoolServer.Options(conf);

    // Construct correct ProtocolFactory
    TProtocolFactory protocolFactory;
    if (cmd.hasOption("compact")) {
      LOG.debug("Using compact protocol");
      protocolFactory = new TCompactProtocol.Factory();
    } else {
      LOG.debug("Using binary protocol");
      protocolFactory = new TBinaryProtocol.Factory();
    }

    ThriftMetrics metrics = new ThriftMetrics(listenPort, conf);
    Hbase.Iface handler = new HBaseHandler(conf, metrics);
    handler = HbaseHandlerMetricsProxy.newInstance(handler, metrics, conf);
    Hbase.Processor processor = new Hbase.Processor(handler);

    TServer server;
    if (cmd.hasOption("nonblocking") || cmd.hasOption("hsha")) {
      if (cmd.hasOption("bind")) {
        LOG.error("The Nonblocking and HsHa servers don't support IP address binding at the moment." +
                " See https://issues.apache.org/jira/browse/HBASE-2155 for details.");
        printUsageAndExit(options, -1);
      }

      TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(listenPort);
      TFramedTransport.Factory transportFactory = new TFramedTransport.Factory();

      if (cmd.hasOption("nonblocking")) {
        LOG.info("starting HBase Nonblocking Thrift server on " + Integer.toString(listenPort));
        server = new TNonblockingServer(processor, serverTransport, transportFactory, protocolFactory);
      } else {
        LOG.info("starting HBase HsHA Thrift server on " + Integer.toString(listenPort));
        server = new THsHaServer(processor, serverTransport, transportFactory, protocolFactory);
      }
    } else {
      // Get IP address to bind to
      InetAddress listenAddress = null;
      if (cmd.hasOption("bind")) {
        try {
          listenAddress = InetAddress.getByName(cmd.getOptionValue("bind"));
        } catch (UnknownHostException e) {
          LOG.error("Could not bind to provided ip address", e);
          printUsageAndExit(options, -1);
        }
      } else {
        listenAddress = InetAddress.getLocalHost();
      }
      TServerTransport serverTransport = new TServerSocket(new InetSocketAddress(listenAddress, listenPort));

      // Construct correct TransportFactory
      TTransportFactory transportFactory;
      if (cmd.hasOption("framed")) {
        transportFactory = new TFramedTransport.Factory();
        LOG.debug("Using framed transport");
      } else {
        transportFactory = new TTransportFactory();
      }

      LOG.info("starting " + THREAD_POOL_SERVER_CLASS.getSimpleName() + " on "
          + listenAddress + ":" + Integer.toString(listenPort)
          + "; minimum number of worker threads="
          + serverOptions.minWorkerThreads
          + ", maximum number of worker threads="
          + serverOptions.maxWorkerThreads + ", queued requests="
          + serverOptions.maxQueuedRequests);

      server = new HBaseThreadPoolServer(processor, serverTransport,
          transportFactory, protocolFactory, serverOptions, metrics);

      if (server.getClass() != THREAD_POOL_SERVER_CLASS) {
        // A sanity check that we instantiated the right thing.
        throw new RuntimeException("Expected thread pool server class " +
            THREAD_POOL_SERVER_CLASS.getName() + " but got " +
            server.getClass().getName());
      }
    }

    server.serve();
  }

  /**
   * @param args
   * @throws Exception
   */
  public static void main(String [] args) throws Exception {
    doMain(args);
  }
}
