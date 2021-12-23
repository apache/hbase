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

package org.apache.hadoop.hbase.thrift;

import static org.apache.hadoop.hbase.HConstants.DEFAULT_HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD;
import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD;
import static org.apache.hadoop.hbase.thrift.Constants.COALESCE_INC_KEY;
import static org.apache.hadoop.hbase.util.Bytes.getBytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CatalogFamilyFormat;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilder;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.access.AccessControlClient;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.thrift.generated.AlreadyExists;
import org.apache.hadoop.hbase.thrift.generated.BatchMutation;
import org.apache.hadoop.hbase.thrift.generated.ColumnDescriptor;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.thrift.generated.IOError;
import org.apache.hadoop.hbase.thrift.generated.IllegalArgument;
import org.apache.hadoop.hbase.thrift.generated.Mutation;
import org.apache.hadoop.hbase.thrift.generated.TAccessControlEntity;
import org.apache.hadoop.hbase.thrift.generated.TAppend;
import org.apache.hadoop.hbase.thrift.generated.TCell;
import org.apache.hadoop.hbase.thrift.generated.TIncrement;
import org.apache.hadoop.hbase.thrift.generated.TPermissionScope;
import org.apache.hadoop.hbase.thrift.generated.TRegionInfo;
import org.apache.hadoop.hbase.thrift.generated.TRowResult;
import org.apache.hadoop.hbase.thrift.generated.TScan;
import org.apache.hadoop.hbase.thrift.generated.TThriftServerType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbase.thirdparty.com.google.common.cache.Cache;
import org.apache.hbase.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.thrift.TException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Throwables;

/**
 * The HBaseServiceHandler is a glue object that connects Thrift RPC calls to the
 * HBase client API primarily defined in the Admin and Table objects.
 */
@InterfaceAudience.Private
@SuppressWarnings("deprecation")
public class ThriftHBaseServiceHandler extends HBaseServiceHandler implements Hbase.Iface {
  private static final Logger LOG = LoggerFactory.getLogger(ThriftHBaseServiceHandler.class);

  public static final int HREGION_VERSION = 1;

  // nextScannerId and scannerMap are used to manage scanner state
  private int nextScannerId = 0;
  private Cache<Integer, ResultScannerWrapper> scannerMap;
  IncrementCoalescer coalescer;

  /**
   * Returns a list of all the column families for a given Table.
   */
  byte[][] getAllColumns(Table table) throws IOException {
    ColumnFamilyDescriptor[] cds = table.getDescriptor().getColumnFamilies();
    byte[][] columns = new byte[cds.length][];
    for (int i = 0; i < cds.length; i++) {
      columns[i] = Bytes.add(cds[i].getName(), KeyValue.COLUMN_FAMILY_DELIM_ARRAY);
    }
    return columns;
  }


  /**
   * Assigns a unique ID to the scanner and adds the mapping to an internal
   * hash-map.
   *
   * @param scanner the {@link ResultScanner} to add
   * @return integer scanner id
   */
  protected synchronized int addScanner(ResultScanner scanner, boolean sortColumns) {
    int id = nextScannerId++;
    ResultScannerWrapper resultScannerWrapper =
        new ResultScannerWrapper(scanner, sortColumns);
    scannerMap.put(id, resultScannerWrapper);
    return id;
  }

  /**
   * Returns the scanner associated with the specified ID.
   *
   * @param id the ID of the scanner to get
   * @return a Scanner, or null if ID was invalid.
   */
  private synchronized ResultScannerWrapper getScanner(int id) {
    return scannerMap.getIfPresent(id);
  }

  /**
   * Removes the scanner associated with the specified ID from the internal
   * id-&gt;scanner hash-map.
   *
   * @param id the ID of the scanner to remove
   */
  private synchronized void removeScanner(int id) {
    scannerMap.invalidate(id);
  }

  protected ThriftHBaseServiceHandler(final Configuration c,
      final UserProvider userProvider) throws IOException {
    super(c, userProvider);
    long cacheTimeout = c.getLong(HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, DEFAULT_HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD) * 2;

    scannerMap = CacheBuilder.newBuilder()
      .expireAfterAccess(cacheTimeout, TimeUnit.MILLISECONDS)
      .build();

    this.coalescer = new IncrementCoalescer(this);
  }


  @Override
  public void enableTable(ByteBuffer tableName) throws IOError {
    try{
      getAdmin().enableTable(getTableName(tableName));
    } catch (IOException e) {
      LOG.warn(e.getMessage(), e);
      throw getIOError(e);
    }
  }

  @Override
  public void disableTable(ByteBuffer tableName) throws IOError{
    try{
      getAdmin().disableTable(getTableName(tableName));
    } catch (IOException e) {
      LOG.warn(e.getMessage(), e);
      throw getIOError(e);
    }
  }

  @Override
  public boolean isTableEnabled(ByteBuffer tableName) throws IOError {
    try {
      return this.connectionCache.getAdmin().isTableEnabled(getTableName(tableName));
    } catch (IOException e) {
      LOG.warn(e.getMessage(), e);
      throw getIOError(e);
    }
  }

  @Override
  public Map<ByteBuffer, Boolean> getTableNamesWithIsTableEnabled() throws IOError {
    try {
      HashMap<ByteBuffer, Boolean> tables = new HashMap<>();
      for (ByteBuffer tableName: this.getTableNames()) {
        tables.put(tableName, this.isTableEnabled(tableName));
      }
      return tables;
    } catch (IOError e) {
      LOG.warn(e.getMessage(), e);
      throw getIOError(e);
    }
  }

  // ThriftServerRunner.compact should be deprecated and replaced with methods specific to
  // table and region.
  @Override
  public void compact(ByteBuffer tableNameOrRegionName) throws IOError {
    try {
      try {
        getAdmin().compactRegion(getBytes(tableNameOrRegionName));
      } catch (IllegalArgumentException e) {
        // Invalid region, try table
        getAdmin().compact(TableName.valueOf(getBytes(tableNameOrRegionName)));
      }
    } catch (IOException e) {
      LOG.warn(e.getMessage(), e);
      throw getIOError(e);
    }
  }

  // ThriftServerRunner.majorCompact should be deprecated and replaced with methods specific
  // to table and region.
  @Override
  public void majorCompact(ByteBuffer tableNameOrRegionName) throws IOError {
    try {
      try {
        getAdmin().compactRegion(getBytes(tableNameOrRegionName));
      } catch (IllegalArgumentException e) {
        // Invalid region, try table
        getAdmin().compact(TableName.valueOf(getBytes(tableNameOrRegionName)));
      }
    } catch (IOException e) {
      LOG.warn(e.getMessage(), e);
      throw getIOError(e);
    }
  }

  @Override
  public List<ByteBuffer> getTableNames() throws IOError {
    try {
      TableName[] tableNames = this.getAdmin().listTableNames();
      ArrayList<ByteBuffer> list = new ArrayList<>(tableNames.length);
      for (TableName tableName : tableNames) {
        list.add(ByteBuffer.wrap(tableName.getName()));
      }
      return list;
    } catch (IOException e) {
      LOG.warn(e.getMessage(), e);
      throw getIOError(e);
    }
  }

  /**
   * @return the list of regions in the given table, or an empty list if the table does not exist
   */
  @Override
  public List<TRegionInfo> getTableRegions(ByteBuffer tableName) throws IOError {
    try (RegionLocator locator = connectionCache.getRegionLocator(getBytes(tableName))) {
      List<HRegionLocation> regionLocations = locator.getAllRegionLocations();
      List<TRegionInfo> results = new ArrayList<>(regionLocations.size());
      for (HRegionLocation regionLocation : regionLocations) {
        RegionInfo info = regionLocation.getRegion();
        ServerName serverName = regionLocation.getServerName();
        TRegionInfo region = new TRegionInfo();
        region.serverName = ByteBuffer.wrap(
            Bytes.toBytes(serverName.getHostname()));
        region.port = serverName.getPort();
        region.startKey = ByteBuffer.wrap(info.getStartKey());
        region.endKey = ByteBuffer.wrap(info.getEndKey());
        region.id = info.getRegionId();
        region.name = ByteBuffer.wrap(info.getRegionName());
        region.version = HREGION_VERSION; // HRegion now not versioned, PB encoding used
        results.add(region);
      }
      return results;
    } catch (TableNotFoundException e) {
      // Return empty list for non-existing table
      return Collections.emptyList();
    } catch (IOException e){
      LOG.warn(e.getMessage(), e);
      throw getIOError(e);
    }
  }

  @Override
  public List<TCell> get(
      ByteBuffer tableName, ByteBuffer row, ByteBuffer column,
      Map<ByteBuffer, ByteBuffer> attributes)
      throws IOError {
    byte [][] famAndQf = CellUtil.parseColumn(getBytes(column));
    if (famAndQf.length == 1) {
      return get(tableName, row, famAndQf[0], null, attributes);
    }
    if (famAndQf.length == 2) {
      return get(tableName, row, famAndQf[0], famAndQf[1], attributes);
    }
    throw new IllegalArgumentException("Invalid familyAndQualifier provided.");
  }

  /**
   * Note: this internal interface is slightly different from public APIs in regard to handling
   * of the qualifier. Here we differ from the public Java API in that null != byte[0]. Rather,
   * we respect qual == null as a request for the entire column family. The caller (
   * {@link #get(ByteBuffer, ByteBuffer, ByteBuffer, Map)}) interface IS consistent in that the
   * column is parse like normal.
   */
  protected List<TCell> get(ByteBuffer tableName,
      ByteBuffer row,
      byte[] family,
      byte[] qualifier,
      Map<ByteBuffer, ByteBuffer> attributes) throws IOError {
    Table table = null;
    try {
      table = getTable(tableName);
      Get get = new Get(getBytes(row));
      addAttributes(get, attributes);
      if (qualifier == null) {
        get.addFamily(family);
      } else {
        get.addColumn(family, qualifier);
      }
      Result result = table.get(get);
      return ThriftUtilities.cellFromHBase(result.rawCells());
    } catch (IOException e) {
      LOG.warn(e.getMessage(), e);
      throw getIOError(e);
    } finally {
      closeTable(table);
    }
  }

  @Override
  public List<TCell> getVer(ByteBuffer tableName, ByteBuffer row, ByteBuffer column,
      int numVersions, Map<ByteBuffer, ByteBuffer> attributes) throws IOError {
    byte [][] famAndQf = CellUtil.parseColumn(getBytes(column));
    if(famAndQf.length == 1) {
      return getVer(tableName, row, famAndQf[0], null, numVersions, attributes);
    }
    if (famAndQf.length == 2) {
      return getVer(tableName, row, famAndQf[0], famAndQf[1], numVersions, attributes);
    }
    throw new IllegalArgumentException("Invalid familyAndQualifier provided.");

  }

  /**
   * Note: this public interface is slightly different from public Java APIs in regard to
   * handling of the qualifier. Here we differ from the public Java API in that null != byte[0].
   * Rather, we respect qual == null as a request for the entire column family. If you want to
   * access the entire column family, use
   * {@link #getVer(ByteBuffer, ByteBuffer, ByteBuffer, int, Map)} with a {@code column} value
   * that lacks a {@code ':'}.
   */
  public List<TCell> getVer(ByteBuffer tableName, ByteBuffer row, byte[] family,
      byte[] qualifier, int numVersions, Map<ByteBuffer, ByteBuffer> attributes) throws IOError {

    Table table = null;
    try {
      table = getTable(tableName);
      Get get = new Get(getBytes(row));
      addAttributes(get, attributes);
      if (null == qualifier) {
        get.addFamily(family);
      } else {
        get.addColumn(family, qualifier);
      }
      get.readVersions(numVersions);
      Result result = table.get(get);
      return ThriftUtilities.cellFromHBase(result.rawCells());
    } catch (IOException e) {
      LOG.warn(e.getMessage(), e);
      throw getIOError(e);
    } finally{
      closeTable(table);
    }
  }

  @Override
  public List<TCell> getVerTs(ByteBuffer tableName, ByteBuffer row, ByteBuffer column,
      long timestamp, int numVersions, Map<ByteBuffer, ByteBuffer> attributes) throws IOError {
    byte [][] famAndQf = CellUtil.parseColumn(getBytes(column));
    if (famAndQf.length == 1) {
      return getVerTs(tableName, row, famAndQf[0], null, timestamp, numVersions, attributes);
    }
    if (famAndQf.length == 2) {
      return getVerTs(tableName, row, famAndQf[0], famAndQf[1], timestamp, numVersions,
          attributes);
    }
    throw new IllegalArgumentException("Invalid familyAndQualifier provided.");
  }

  /**
   * Note: this internal interface is slightly different from public APIs in regard to handling
   * of the qualifier. Here we differ from the public Java API in that null != byte[0]. Rather,
   * we respect qual == null as a request for the entire column family. The caller (
   * {@link #getVerTs(ByteBuffer, ByteBuffer, ByteBuffer, long, int, Map)}) interface IS
   * consistent in that the column is parse like normal.
   */
  protected List<TCell> getVerTs(ByteBuffer tableName, ByteBuffer row, byte[] family,
      byte[] qualifier, long timestamp, int numVersions, Map<ByteBuffer, ByteBuffer> attributes)
      throws IOError {

    Table table = null;
    try {
      table = getTable(tableName);
      Get get = new Get(getBytes(row));
      addAttributes(get, attributes);
      if (null == qualifier) {
        get.addFamily(family);
      } else {
        get.addColumn(family, qualifier);
      }
      get.setTimeRange(0, timestamp);
      get.readVersions(numVersions);
      Result result = table.get(get);
      return ThriftUtilities.cellFromHBase(result.rawCells());
    } catch (IOException e) {
      LOG.warn(e.getMessage(), e);
      throw getIOError(e);
    } finally{
      closeTable(table);
    }
  }

  @Override
  public List<TRowResult> getRow(ByteBuffer tableName, ByteBuffer row,
      Map<ByteBuffer, ByteBuffer> attributes) throws IOError {
    return getRowWithColumnsTs(tableName, row, null,
        HConstants.LATEST_TIMESTAMP,
        attributes);
  }

  @Override
  public List<TRowResult> getRowWithColumns(ByteBuffer tableName,
      ByteBuffer row,
      List<ByteBuffer> columns,
      Map<ByteBuffer, ByteBuffer> attributes) throws IOError {
    return getRowWithColumnsTs(tableName, row, columns,
        HConstants.LATEST_TIMESTAMP,
        attributes);
  }

  @Override
  public List<TRowResult> getRowTs(ByteBuffer tableName, ByteBuffer row,
      long timestamp, Map<ByteBuffer, ByteBuffer> attributes) throws IOError {
    return getRowWithColumnsTs(tableName, row, null,
        timestamp, attributes);
  }

  @Override
  public List<TRowResult> getRowWithColumnsTs(
      ByteBuffer tableName, ByteBuffer row, List<ByteBuffer> columns,
      long timestamp, Map<ByteBuffer, ByteBuffer> attributes) throws IOError {

    Table table = null;
    try {
      table = getTable(tableName);
      if (columns == null) {
        Get get = new Get(getBytes(row));
        addAttributes(get, attributes);
        get.setTimeRange(0, timestamp);
        Result result = table.get(get);
        return ThriftUtilities.rowResultFromHBase(result);
      }
      Get get = new Get(getBytes(row));
      addAttributes(get, attributes);
      for(ByteBuffer column : columns) {
        byte [][] famAndQf = CellUtil.parseColumn(getBytes(column));
        if (famAndQf.length == 1) {
          get.addFamily(famAndQf[0]);
        } else {
          get.addColumn(famAndQf[0], famAndQf[1]);
        }
      }
      get.setTimeRange(0, timestamp);
      Result result = table.get(get);
      return ThriftUtilities.rowResultFromHBase(result);
    } catch (IOException e) {
      LOG.warn(e.getMessage(), e);
      throw getIOError(e);
    } finally{
      closeTable(table);
    }
  }

  @Override
  public List<TRowResult> getRows(ByteBuffer tableName,
      List<ByteBuffer> rows,
      Map<ByteBuffer, ByteBuffer> attributes)
      throws IOError {
    return getRowsWithColumnsTs(tableName, rows, null,
        HConstants.LATEST_TIMESTAMP,
        attributes);
  }

  @Override
  public List<TRowResult> getRowsWithColumns(ByteBuffer tableName,
      List<ByteBuffer> rows,
      List<ByteBuffer> columns,
      Map<ByteBuffer, ByteBuffer> attributes) throws IOError {
    return getRowsWithColumnsTs(tableName, rows, columns,
        HConstants.LATEST_TIMESTAMP,
        attributes);
  }

  @Override
  public List<TRowResult> getRowsTs(ByteBuffer tableName,
      List<ByteBuffer> rows,
      long timestamp,
      Map<ByteBuffer, ByteBuffer> attributes) throws IOError {
    return getRowsWithColumnsTs(tableName, rows, null,
        timestamp, attributes);
  }

  @Override
  public List<TRowResult> getRowsWithColumnsTs(ByteBuffer tableName,
      List<ByteBuffer> rows,
      List<ByteBuffer> columns, long timestamp,
      Map<ByteBuffer, ByteBuffer> attributes) throws IOError {

    Table table= null;
    try {
      List<Get> gets = new ArrayList<>(rows.size());
      table = getTable(tableName);
      if (metrics != null) {
        metrics.incNumRowKeysInBatchGet(rows.size());
      }
      for (ByteBuffer row : rows) {
        Get get = new Get(getBytes(row));
        addAttributes(get, attributes);
        if (columns != null) {

          for(ByteBuffer column : columns) {
            byte [][] famAndQf = CellUtil.parseColumn(getBytes(column));
            if (famAndQf.length == 1) {
              get.addFamily(famAndQf[0]);
            } else {
              get.addColumn(famAndQf[0], famAndQf[1]);
            }
          }
        }
        get.setTimeRange(0, timestamp);
        gets.add(get);
      }
      Result[] result = table.get(gets);
      return ThriftUtilities.rowResultFromHBase(result);
    } catch (IOException e) {
      LOG.warn(e.getMessage(), e);
      throw getIOError(e);
    } finally{
      closeTable(table);
    }
  }

  @Override
  public void deleteAll(
      ByteBuffer tableName, ByteBuffer row, ByteBuffer column,
      Map<ByteBuffer, ByteBuffer> attributes)
      throws IOError {
    deleteAllTs(tableName, row, column, HConstants.LATEST_TIMESTAMP,
        attributes);
  }

  @Override
  public void deleteAllTs(ByteBuffer tableName,
      ByteBuffer row,
      ByteBuffer column,
      long timestamp, Map<ByteBuffer, ByteBuffer> attributes) throws IOError {
    Table table = null;
    try {
      table = getTable(tableName);
      Delete delete  = new Delete(getBytes(row));
      addAttributes(delete, attributes);
      byte [][] famAndQf = CellUtil.parseColumn(getBytes(column));
      if (famAndQf.length == 1) {
        delete.addFamily(famAndQf[0], timestamp);
      } else {
        delete.addColumns(famAndQf[0], famAndQf[1], timestamp);
      }
      table.delete(delete);

    } catch (IOException e) {
      LOG.warn(e.getMessage(), e);
      throw getIOError(e);
    } finally {
      closeTable(table);
    }
  }

  @Override
  public void deleteAllRow(
      ByteBuffer tableName, ByteBuffer row,
      Map<ByteBuffer, ByteBuffer> attributes) throws IOError {
    deleteAllRowTs(tableName, row, HConstants.LATEST_TIMESTAMP, attributes);
  }

  @Override
  public void deleteAllRowTs(
      ByteBuffer tableName, ByteBuffer row, long timestamp,
      Map<ByteBuffer, ByteBuffer> attributes) throws IOError {
    Table table = null;
    try {
      table = getTable(tableName);
      Delete delete  = new Delete(getBytes(row), timestamp);
      addAttributes(delete, attributes);
      table.delete(delete);
    } catch (IOException e) {
      LOG.warn(e.getMessage(), e);
      throw getIOError(e);
    } finally {
      closeTable(table);
    }
  }

  @Override
  public void createTable(ByteBuffer in_tableName, List<ColumnDescriptor> columnFamilies)
    throws IOError, IllegalArgument, AlreadyExists {
    TableName tableName = getTableName(in_tableName);
    try {
      if (getAdmin().tableExists(tableName)) {
        throw new AlreadyExists("table name already in use");
      }
      TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
      for (ColumnDescriptor col : columnFamilies) {
        builder.setColumnFamily(ThriftUtilities.colDescFromThrift(col));
      }
      getAdmin().createTable(builder.build());
    } catch (IOException e) {
      LOG.warn(e.getMessage(), e);
      throw getIOError(e);
    } catch (IllegalArgumentException e) {
      LOG.warn(e.getMessage(), e);
      throw new IllegalArgument(Throwables.getStackTraceAsString(e));
    }
  }

  private static TableName getTableName(ByteBuffer buffer) {
    return TableName.valueOf(getBytes(buffer));
  }

  @Override
  public void deleteTable(ByteBuffer in_tableName) throws IOError {
    TableName tableName = getTableName(in_tableName);
    if (LOG.isDebugEnabled()) {
      LOG.debug("deleteTable: table={}", tableName);
    }
    try {
      if (!getAdmin().tableExists(tableName)) {
        throw new IOException("table does not exist");
      }
      getAdmin().deleteTable(tableName);
    } catch (IOException e) {
      LOG.warn(e.getMessage(), e);
      throw getIOError(e);
    }
  }

  @Override
  public void mutateRow(ByteBuffer tableName, ByteBuffer row,
      List<Mutation> mutations, Map<ByteBuffer, ByteBuffer> attributes)
      throws IOError, IllegalArgument {
    mutateRowTs(tableName, row, mutations, HConstants.LATEST_TIMESTAMP, attributes);
  }

  @Override
  public void mutateRowTs(ByteBuffer tableName, ByteBuffer row,
      List<Mutation> mutations, long timestamp,
      Map<ByteBuffer, ByteBuffer> attributes)
      throws IOError, IllegalArgument {
    Table table = null;
    try {
      table = getTable(tableName);
      Put put = new Put(getBytes(row), timestamp);
      addAttributes(put, attributes);

      Delete delete = new Delete(getBytes(row));
      addAttributes(delete, attributes);
      if (metrics != null) {
        metrics.incNumRowKeysInBatchMutate(mutations.size());
      }

      // I apologize for all this mess :)
      CellBuilder builder = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY);
      for (Mutation m : mutations) {
        byte[][] famAndQf = CellUtil.parseColumn(getBytes(m.column));
        if (m.isDelete) {
          if (famAndQf.length == 1) {
            delete.addFamily(famAndQf[0], timestamp);
          } else {
            delete.addColumns(famAndQf[0], famAndQf[1], timestamp);
          }
          delete.setDurability(m.writeToWAL ? Durability.SYNC_WAL : Durability.SKIP_WAL);
        } else {
          if(famAndQf.length == 1) {
            LOG.warn("No column qualifier specified. Delete is the only mutation supported "
                + "over the whole column family.");
          } else {
            put.add(builder.clear()
                .setRow(put.getRow())
                .setFamily(famAndQf[0])
                .setQualifier(famAndQf[1])
                .setTimestamp(put.getTimestamp())
                .setType(Cell.Type.Put)
                .setValue(m.value != null ? getBytes(m.value)
                    : HConstants.EMPTY_BYTE_ARRAY)
                .build());
          }
          put.setDurability(m.writeToWAL ? Durability.SYNC_WAL : Durability.SKIP_WAL);
        }
      }
      if (!delete.isEmpty()) {
        table.delete(delete);
      }
      if (!put.isEmpty()) {
        table.put(put);
      }
    } catch (IOException e) {
      LOG.warn(e.getMessage(), e);
      throw getIOError(e);
    } catch (IllegalArgumentException e) {
      LOG.warn(e.getMessage(), e);
      throw new IllegalArgument(Throwables.getStackTraceAsString(e));
    } finally{
      closeTable(table);
    }
  }

  @Override
  public void mutateRows(ByteBuffer tableName, List<BatchMutation> rowBatches,
      Map<ByteBuffer, ByteBuffer> attributes)
      throws IOError, IllegalArgument, TException {
    mutateRowsTs(tableName, rowBatches, HConstants.LATEST_TIMESTAMP, attributes);
  }

  @Override
  public void mutateRowsTs(
      ByteBuffer tableName, List<BatchMutation> rowBatches, long timestamp,
      Map<ByteBuffer, ByteBuffer> attributes)
      throws IOError, IllegalArgument, TException {
    List<Put> puts = new ArrayList<>();
    List<Delete> deletes = new ArrayList<>();
    CellBuilder builder = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY);
    for (BatchMutation batch : rowBatches) {
      byte[] row = getBytes(batch.row);
      List<Mutation> mutations = batch.mutations;
      Delete delete = new Delete(row);
      addAttributes(delete, attributes);
      Put put = new Put(row, timestamp);
      addAttributes(put, attributes);
      for (Mutation m : mutations) {
        byte[][] famAndQf = CellUtil.parseColumn(getBytes(m.column));
        if (m.isDelete) {
          // no qualifier, family only.
          if (famAndQf.length == 1) {
            delete.addFamily(famAndQf[0], timestamp);
          } else {
            delete.addColumns(famAndQf[0], famAndQf[1], timestamp);
          }
          delete.setDurability(m.writeToWAL ? Durability.SYNC_WAL
              : Durability.SKIP_WAL);
        } else {
          if (famAndQf.length == 1) {
            LOG.warn("No column qualifier specified. Delete is the only mutation supported "
                + "over the whole column family.");
          }
          if (famAndQf.length == 2) {
            try {
              put.add(builder.clear()
                  .setRow(put.getRow())
                  .setFamily(famAndQf[0])
                  .setQualifier(famAndQf[1])
                  .setTimestamp(put.getTimestamp())
                  .setType(Cell.Type.Put)
                  .setValue(m.value != null ? getBytes(m.value)
                      : HConstants.EMPTY_BYTE_ARRAY)
                  .build());
            } catch (IOException e) {
              throw new IllegalArgumentException(e);
            }
          } else {
            throw new IllegalArgumentException("Invalid famAndQf provided.");
          }
          put.setDurability(m.writeToWAL ? Durability.SYNC_WAL : Durability.SKIP_WAL);
        }
      }
      if (!delete.isEmpty()) {
        deletes.add(delete);
      }
      if (!put.isEmpty()) {
        puts.add(put);
      }
    }

    Table table = null;
    try {
      table = getTable(tableName);
      if (!puts.isEmpty()) {
        table.put(puts);
      }
      if (!deletes.isEmpty()) {
        table.delete(deletes);
      }
    } catch (IOException e) {
      LOG.warn(e.getMessage(), e);
      throw getIOError(e);
    } catch (IllegalArgumentException e) {
      LOG.warn(e.getMessage(), e);
      throw new IllegalArgument(Throwables.getStackTraceAsString(e));
    } finally{
      closeTable(table);
    }
  }

  @Override
  public long atomicIncrement(
      ByteBuffer tableName, ByteBuffer row, ByteBuffer column, long amount)
      throws IOError, IllegalArgument, TException {
    byte [][] famAndQf = CellUtil.parseColumn(getBytes(column));
    if(famAndQf.length == 1) {
      return atomicIncrement(tableName, row, famAndQf[0], HConstants.EMPTY_BYTE_ARRAY, amount);
    }
    return atomicIncrement(tableName, row, famAndQf[0], famAndQf[1], amount);
  }

  protected long atomicIncrement(ByteBuffer tableName, ByteBuffer row,
      byte [] family, byte [] qualifier, long amount)
      throws IOError, IllegalArgument, TException {
    Table table = null;
    try {
      table = getTable(tableName);
      return table.incrementColumnValue(
          getBytes(row), family, qualifier, amount);
    } catch (IOException e) {
      LOG.warn(e.getMessage(), e);
      throw getIOError(e);
    } finally {
      closeTable(table);
    }
  }

  @Override
  public void scannerClose(int id) throws IOError, IllegalArgument {
    LOG.debug("scannerClose: id={}", id);
    ResultScannerWrapper resultScannerWrapper = getScanner(id);
    if (resultScannerWrapper == null) {
      LOG.warn("scanner ID is invalid");
      throw new IllegalArgument("scanner ID is invalid");
    }
    resultScannerWrapper.getScanner().close();
    removeScanner(id);
  }

  @Override
  public List<TRowResult> scannerGetList(int id,int nbRows)
      throws IllegalArgument, IOError {
    LOG.debug("scannerGetList: id={}", id);
    ResultScannerWrapper resultScannerWrapper = getScanner(id);
    if (null == resultScannerWrapper) {
      String message = "scanner ID is invalid";
      LOG.warn(message);
      throw new IllegalArgument("scanner ID is invalid");
    }

    Result [] results;
    try {
      results = resultScannerWrapper.getScanner().next(nbRows);
      if (null == results) {
        return new ArrayList<>();
      }
    } catch (IOException e) {
      LOG.warn(e.getMessage(), e);
      throw getIOError(e);
    } finally {
      // Add scanner back to scannerMap; protects against case
      // where scanner expired during processing of request.
      scannerMap.put(id, resultScannerWrapper);
    }
    return ThriftUtilities.rowResultFromHBase(results, resultScannerWrapper.isColumnSorted());
  }

  @Override
  public List<TRowResult> scannerGet(int id) throws IllegalArgument, IOError {
    return scannerGetList(id,1);
  }

  @Override
  public int scannerOpenWithScan(ByteBuffer tableName, TScan tScan,
      Map<ByteBuffer, ByteBuffer> attributes)
      throws IOError {

    Table table = null;
    try {
      table = getTable(tableName);
      Scan scan = new Scan();
      addAttributes(scan, attributes);
      if (tScan.isSetStartRow()) {
        scan.withStartRow(tScan.getStartRow());
      }
      if (tScan.isSetStopRow()) {
        scan.withStopRow(tScan.getStopRow());
      }
      if (tScan.isSetTimestamp()) {
        scan.setTimeRange(0, tScan.getTimestamp());
      }
      if (tScan.isSetCaching()) {
        scan.setCaching(tScan.getCaching());
      }
      if (tScan.isSetBatchSize()) {
        scan.setBatch(tScan.getBatchSize());
      }
      if (tScan.isSetColumns() && !tScan.getColumns().isEmpty()) {
        for(ByteBuffer column : tScan.getColumns()) {
          byte [][] famQf = CellUtil.parseColumn(getBytes(column));
          if(famQf.length == 1) {
            scan.addFamily(famQf[0]);
          } else {
            scan.addColumn(famQf[0], famQf[1]);
          }
        }
      }
      if (tScan.isSetFilterString()) {
        ParseFilter parseFilter = new ParseFilter();
        scan.setFilter(
            parseFilter.parseFilterString(tScan.getFilterString()));
      }
      if (tScan.isSetReversed()) {
        scan.setReversed(tScan.isReversed());
      }
      if (tScan.isSetCacheBlocks()) {
        scan.setCacheBlocks(tScan.isCacheBlocks());
      }
      return addScanner(table.getScanner(scan), tScan.sortColumns);
    } catch (IOException e) {
      LOG.warn(e.getMessage(), e);
      throw getIOError(e);
    } finally{
      closeTable(table);
    }
  }

  @Override
  public int scannerOpen(ByteBuffer tableName, ByteBuffer startRow,
      List<ByteBuffer> columns,
      Map<ByteBuffer, ByteBuffer> attributes) throws IOError {

    Table table = null;
    try {
      table = getTable(tableName);
      Scan scan = new Scan().withStartRow(getBytes(startRow));
      addAttributes(scan, attributes);
      if(columns != null && !columns.isEmpty()) {
        for(ByteBuffer column : columns) {
          byte [][] famQf = CellUtil.parseColumn(getBytes(column));
          if(famQf.length == 1) {
            scan.addFamily(famQf[0]);
          } else {
            scan.addColumn(famQf[0], famQf[1]);
          }
        }
      }
      return addScanner(table.getScanner(scan), false);
    } catch (IOException e) {
      LOG.warn(e.getMessage(), e);
      throw getIOError(e);
    } finally{
      closeTable(table);
    }
  }

  @Override
  public int scannerOpenWithStop(ByteBuffer tableName, ByteBuffer startRow,
      ByteBuffer stopRow, List<ByteBuffer> columns,
      Map<ByteBuffer, ByteBuffer> attributes)
      throws IOError, TException {

    Table table = null;
    try {
      table = getTable(tableName);
      Scan scan = new Scan().withStartRow(getBytes(startRow)).withStopRow(getBytes(stopRow));
      addAttributes(scan, attributes);
      if(columns != null && !columns.isEmpty()) {
        for(ByteBuffer column : columns) {
          byte [][] famQf = CellUtil.parseColumn(getBytes(column));
          if(famQf.length == 1) {
            scan.addFamily(famQf[0]);
          } else {
            scan.addColumn(famQf[0], famQf[1]);
          }
        }
      }
      return addScanner(table.getScanner(scan), false);
    } catch (IOException e) {
      LOG.warn(e.getMessage(), e);
      throw getIOError(e);
    } finally{
      closeTable(table);
    }
  }

  @Override
  public int scannerOpenWithPrefix(ByteBuffer tableName,
      ByteBuffer startAndPrefix,
      List<ByteBuffer> columns,
      Map<ByteBuffer, ByteBuffer> attributes)
      throws IOError, TException {

    Table table = null;
    try {
      table = getTable(tableName);
      Scan scan = new Scan().withStartRow(getBytes(startAndPrefix));
      addAttributes(scan, attributes);
      Filter f = new WhileMatchFilter(
          new PrefixFilter(getBytes(startAndPrefix)));
      scan.setFilter(f);
      if (columns != null && !columns.isEmpty()) {
        for(ByteBuffer column : columns) {
          byte [][] famQf = CellUtil.parseColumn(getBytes(column));
          if(famQf.length == 1) {
            scan.addFamily(famQf[0]);
          } else {
            scan.addColumn(famQf[0], famQf[1]);
          }
        }
      }
      return addScanner(table.getScanner(scan), false);
    } catch (IOException e) {
      LOG.warn(e.getMessage(), e);
      throw getIOError(e);
    } finally{
      closeTable(table);
    }
  }

  @Override
  public int scannerOpenTs(ByteBuffer tableName, ByteBuffer startRow,
      List<ByteBuffer> columns, long timestamp,
      Map<ByteBuffer, ByteBuffer> attributes) throws IOError, TException {

    Table table = null;
    try {
      table = getTable(tableName);
      Scan scan = new Scan().withStartRow(getBytes(startRow));
      addAttributes(scan, attributes);
      scan.setTimeRange(0, timestamp);
      if (columns != null && !columns.isEmpty()) {
        for (ByteBuffer column : columns) {
          byte [][] famQf = CellUtil.parseColumn(getBytes(column));
          if(famQf.length == 1) {
            scan.addFamily(famQf[0]);
          } else {
            scan.addColumn(famQf[0], famQf[1]);
          }
        }
      }
      return addScanner(table.getScanner(scan), false);
    } catch (IOException e) {
      LOG.warn(e.getMessage(), e);
      throw getIOError(e);
    } finally{
      closeTable(table);
    }
  }

  @Override
  public int scannerOpenWithStopTs(ByteBuffer tableName, ByteBuffer startRow,
      ByteBuffer stopRow, List<ByteBuffer> columns, long timestamp,
      Map<ByteBuffer, ByteBuffer> attributes)
      throws IOError, TException {

    Table table = null;
    try {
      table = getTable(tableName);
      Scan scan = new Scan().withStartRow(getBytes(startRow)).withStopRow(getBytes(stopRow));
      addAttributes(scan, attributes);
      scan.setTimeRange(0, timestamp);
      if (columns != null && !columns.isEmpty()) {
        for (ByteBuffer column : columns) {
          byte [][] famQf = CellUtil.parseColumn(getBytes(column));
          if(famQf.length == 1) {
            scan.addFamily(famQf[0]);
          } else {
            scan.addColumn(famQf[0], famQf[1]);
          }
        }
      }
      scan.setTimeRange(0, timestamp);
      return addScanner(table.getScanner(scan), false);
    } catch (IOException e) {
      LOG.warn(e.getMessage(), e);
      throw getIOError(e);
    } finally{
      closeTable(table);
    }
  }

  @Override
  public Map<ByteBuffer, ColumnDescriptor> getColumnDescriptors(
      ByteBuffer tableName) throws IOError, TException {

    Table table = null;
    try {
      TreeMap<ByteBuffer, ColumnDescriptor> columns = new TreeMap<>();

      table = getTable(tableName);
      TableDescriptor desc = table.getDescriptor();

      for (ColumnFamilyDescriptor e : desc.getColumnFamilies()) {
        ColumnDescriptor col = ThriftUtilities.colDescFromHbase(e);
        columns.put(col.name, col);
      }
      return columns;
    } catch (IOException e) {
      LOG.warn(e.getMessage(), e);
      throw getIOError(e);
    } finally {
      closeTable(table);
    }
  }

  private void closeTable(Table table) throws IOError {
    try{
      if(table != null){
        table.close();
      }
    } catch (IOException e){
      LOG.error(e.getMessage(), e);
      throw getIOError(e);
    }
  }

  @Override
  public TRegionInfo getRegionInfo(ByteBuffer searchRow) throws IOError {
    try {
      byte[] row = getBytes(searchRow);
      Result startRowResult = getReverseScanResult(TableName.META_TABLE_NAME.getName(), row,
          HConstants.CATALOG_FAMILY);

      if (startRowResult == null) {
        throw new IOException("Cannot find row in "+ TableName.META_TABLE_NAME+", row="
            + Bytes.toStringBinary(row));
      }

      // find region start and end keys
      RegionInfo regionInfo = CatalogFamilyFormat.getRegionInfo(startRowResult);
      if (regionInfo == null) {
        throw new IOException("RegionInfo REGIONINFO was null or " +
            " empty in Meta for row="
            + Bytes.toStringBinary(row));
      }
      TRegionInfo region = new TRegionInfo();
      region.setStartKey(regionInfo.getStartKey());
      region.setEndKey(regionInfo.getEndKey());
      region.id = regionInfo.getRegionId();
      region.setName(regionInfo.getRegionName());
      region.version = HREGION_VERSION; // version not used anymore, PB encoding used.

      // find region assignment to server
      ServerName serverName = CatalogFamilyFormat.getServerName(startRowResult, 0);
      if (serverName != null) {
        region.setServerName(Bytes.toBytes(serverName.getHostname()));
        region.port = serverName.getPort();
      }
      return region;
    } catch (IOException e) {
      LOG.warn(e.getMessage(), e);
      throw getIOError(e);
    }
  }

  private Result getReverseScanResult(byte[] tableName, byte[] row, byte[] family)
      throws IOException {
    Scan scan = new Scan().withStartRow(row);
    scan.setReversed(true);
    scan.addFamily(family);
    scan.withStartRow(row);
    try (Table table = getTable(tableName);
         ResultScanner scanner = table.getScanner(scan)) {
      return scanner.next();
    }
  }

  @Override
  public void increment(TIncrement tincrement) throws IOError, TException {

    if (tincrement.getRow().length == 0 || tincrement.getTable().length == 0) {
      throw new TException("Must supply a table and a row key; can't increment");
    }

    if (conf.getBoolean(COALESCE_INC_KEY, false)) {
      this.coalescer.queueIncrement(tincrement);
      return;
    }

    Table table = null;
    try {
      table = getTable(tincrement.getTable());
      Increment inc = ThriftUtilities.incrementFromThrift(tincrement);
      table.increment(inc);
    } catch (IOException e) {
      LOG.warn(e.getMessage(), e);
      throw getIOError(e);
    } finally{
      closeTable(table);
    }
  }

  @Override
  public void incrementRows(List<TIncrement> tincrements) throws IOError, TException {
    if (conf.getBoolean(COALESCE_INC_KEY, false)) {
      this.coalescer.queueIncrements(tincrements);
      return;
    }
    for (TIncrement tinc : tincrements) {
      increment(tinc);
    }
  }

  @Override
  public List<TCell> append(TAppend tappend) throws IOError, TException {
    if (tappend.getRow().length == 0 || tappend.getTable().length == 0) {
      throw new TException("Must supply a table and a row key; can't append");
    }

    Table table = null;
    try {
      table = getTable(tappend.getTable());
      Append append = ThriftUtilities.appendFromThrift(tappend);
      Result result = table.append(append);
      return ThriftUtilities.cellFromHBase(result.rawCells());
    } catch (IOException e) {
      LOG.warn(e.getMessage(), e);
      throw getIOError(e);
    } finally{
      closeTable(table);
    }
  }

  @Override
  public boolean checkAndPut(ByteBuffer tableName, ByteBuffer row, ByteBuffer column,
      ByteBuffer value, Mutation mput, Map<ByteBuffer, ByteBuffer> attributes) throws IOError,
      IllegalArgument, TException {
    Put put;
    try {
      put = new Put(getBytes(row), HConstants.LATEST_TIMESTAMP);
      addAttributes(put, attributes);

      byte[][] famAndQf = CellUtil.parseColumn(getBytes(mput.column));
      put.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
          .setRow(put.getRow())
          .setFamily(famAndQf[0])
          .setQualifier(famAndQf[1])
          .setTimestamp(put.getTimestamp())
          .setType(Cell.Type.Put)
          .setValue(mput.value != null ? getBytes(mput.value)
              : HConstants.EMPTY_BYTE_ARRAY)
          .build());
      put.setDurability(mput.writeToWAL ? Durability.SYNC_WAL : Durability.SKIP_WAL);
    } catch (IOException | IllegalArgumentException e) {
      LOG.warn(e.getMessage(), e);
      throw new IllegalArgument(Throwables.getStackTraceAsString(e));
    }

    Table table = null;
    try {
      table = getTable(tableName);
      byte[][] famAndQf = CellUtil.parseColumn(getBytes(column));
      Table.CheckAndMutateBuilder mutateBuilder =
          table.checkAndMutate(getBytes(row), famAndQf[0]).qualifier(famAndQf[1]);
      if (value != null) {
        return mutateBuilder.ifEquals(getBytes(value)).thenPut(put);
      } else {
        return mutateBuilder.ifNotExists().thenPut(put);
      }
    } catch (IOException e) {
      LOG.warn(e.getMessage(), e);
      throw getIOError(e);
    } catch (IllegalArgumentException e) {
      LOG.warn(e.getMessage(), e);
      throw new IllegalArgument(Throwables.getStackTraceAsString(e));
    } finally {
      closeTable(table);
    }
  }

  @Override
  public TThriftServerType getThriftServerType() {
    return TThriftServerType.ONE;
  }

  @Override
  public String getClusterId() throws TException {
    return connectionCache.getClusterId();
  }

  @Override
  public boolean grant(TAccessControlEntity info) throws IOError, TException {
    Permission.Action[] actions = ThriftUtilities.permissionActionsFromString(info.actions);
    try {
      if (info.scope == TPermissionScope.NAMESPACE) {
        AccessControlClient.grant(connectionCache.getAdmin().getConnection(),
          info.getNsName(), info.getUsername(), actions);
      } else if (info.scope == TPermissionScope.TABLE) {
        TableName tableName = TableName.valueOf(info.getTableName());
        AccessControlClient.grant(connectionCache.getAdmin().getConnection(),
          tableName, info.getUsername(), null, null, actions);
      }
    } catch (Throwable t) {
      if (t instanceof IOException) {
        throw getIOError(t);
      } else {
        throw getIOError(new DoNotRetryIOException(t.getMessage()));
      }
    }
    return true;
  }

  @Override
  public boolean revoke(TAccessControlEntity info) throws IOError, TException {
    Permission.Action[] actions = ThriftUtilities.permissionActionsFromString(info.actions);
    try {
      if (info.scope == TPermissionScope.NAMESPACE) {
        AccessControlClient.revoke(connectionCache.getAdmin().getConnection(),
          info.getNsName(), info.getUsername(), actions);
      } else if (info.scope == TPermissionScope.TABLE) {
        TableName tableName = TableName.valueOf(info.getTableName());
        AccessControlClient.revoke(connectionCache.getAdmin().getConnection(),
          tableName, info.getUsername(), null, null, actions);
      }
    } catch (Throwable t) {
      if (t instanceof IOException) {
        throw getIOError(t);
      } else {
        throw getIOError(new DoNotRetryIOException(t.getMessage()));
      }
    }
    return true;
  }

  private static IOError getIOError(Throwable throwable) {
    IOError error = new IOErrorWithCause(throwable);
    error.setCanRetry(!(throwable instanceof DoNotRetryIOException));
    error.setMessage(Throwables.getStackTraceAsString(throwable));
    return error;
  }

  /**
   * Adds all the attributes into the Operation object
   */
  private static void addAttributes(OperationWithAttributes op,
      Map<ByteBuffer, ByteBuffer> attributes) {
    if (attributes == null || attributes.isEmpty()) {
      return;
    }
    for (Map.Entry<ByteBuffer, ByteBuffer> entry : attributes.entrySet()) {
      String name = Bytes.toStringBinary(getBytes(entry.getKey()));
      byte[] value =  getBytes(entry.getValue());
      op.setAttribute(name, value);
    }
  }

  protected static class ResultScannerWrapper {

    private final ResultScanner scanner;
    private final boolean sortColumns;
    public ResultScannerWrapper(ResultScanner resultScanner,
        boolean sortResultColumns) {
      scanner = resultScanner;
      sortColumns = sortResultColumns;
    }

    public ResultScanner getScanner() {
      return scanner;
    }

    public boolean isColumnSorted() {
      return sortColumns;
    }
  }

  public static class IOErrorWithCause extends IOError {
    private final Throwable cause;
    public IOErrorWithCause(Throwable cause) {
      this.cause = cause;
    }

    @Override
    public synchronized Throwable getCause() {
      return cause;
    }

    @Override
    public boolean equals(Object other) {
      if (super.equals(other) &&
          other instanceof IOErrorWithCause) {
        Throwable otherCause = ((IOErrorWithCause) other).getCause();
        if (this.getCause() != null) {
          return otherCause != null && this.getCause().equals(otherCause);
        } else {
          return otherCause == null;
        }
      }
      return false;
    }

    @Override
    public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (cause != null ? cause.hashCode() : 0);
      return result;
    }
  }


}
