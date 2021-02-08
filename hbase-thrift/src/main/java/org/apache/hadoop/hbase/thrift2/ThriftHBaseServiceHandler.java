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
package org.apache.hadoop.hbase.thrift2;

import static org.apache.hadoop.hbase.thrift.Constants.THRIFT_READONLY_ENABLED;
import static org.apache.hadoop.hbase.thrift.Constants.THRIFT_READONLY_ENABLED_DEFAULT;
import static org.apache.hadoop.hbase.thrift2.ThriftUtilities.appendFromThrift;
import static org.apache.hadoop.hbase.thrift2.ThriftUtilities.deleteFromThrift;
import static org.apache.hadoop.hbase.thrift2.ThriftUtilities.deletesFromThrift;
import static org.apache.hadoop.hbase.thrift2.ThriftUtilities.getFromThrift;
import static org.apache.hadoop.hbase.thrift2.ThriftUtilities.getsFromThrift;
import static org.apache.hadoop.hbase.thrift2.ThriftUtilities.incrementFromThrift;
import static org.apache.hadoop.hbase.thrift2.ThriftUtilities.putFromThrift;
import static org.apache.hadoop.hbase.thrift2.ThriftUtilities.putsFromThrift;
import static org.apache.hadoop.hbase.thrift2.ThriftUtilities.resultFromHBase;
import static org.apache.hadoop.hbase.thrift2.ThriftUtilities.resultsFromHBase;
import static org.apache.hadoop.hbase.thrift2.ThriftUtilities.rowMutationsFromThrift;
import static org.apache.hadoop.hbase.thrift2.ThriftUtilities.scanFromThrift;
import static org.apache.hadoop.hbase.thrift2.ThriftUtilities.splitKeyFromThrift;
import static org.apache.hadoop.hbase.thrift2.ThriftUtilities.tableNameFromThrift;
import static org.apache.hadoop.hbase.thrift2.ThriftUtilities.tableNamesFromHBase;
import static org.apache.thrift.TBaseHelper.byteBufferToByteArray;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.thrift.HBaseServiceHandler;
import org.apache.hadoop.hbase.thrift2.generated.TAppend;
import org.apache.hadoop.hbase.thrift2.generated.TColumnFamilyDescriptor;
import org.apache.hadoop.hbase.thrift2.generated.TCompareOp;
import org.apache.hadoop.hbase.thrift2.generated.TDelete;
import org.apache.hadoop.hbase.thrift2.generated.TGet;
import org.apache.hadoop.hbase.thrift2.generated.THBaseService;
import org.apache.hadoop.hbase.thrift2.generated.THRegionLocation;
import org.apache.hadoop.hbase.thrift2.generated.TIOError;
import org.apache.hadoop.hbase.thrift2.generated.TIllegalArgument;
import org.apache.hadoop.hbase.thrift2.generated.TIncrement;
import org.apache.hadoop.hbase.thrift2.generated.TLogQueryFilter;
import org.apache.hadoop.hbase.thrift2.generated.TNamespaceDescriptor;
import org.apache.hadoop.hbase.thrift2.generated.TOnlineLogRecord;
import org.apache.hadoop.hbase.thrift2.generated.TPut;
import org.apache.hadoop.hbase.thrift2.generated.TResult;
import org.apache.hadoop.hbase.thrift2.generated.TRowMutations;
import org.apache.hadoop.hbase.thrift2.generated.TScan;
import org.apache.hadoop.hbase.thrift2.generated.TServerName;
import org.apache.hadoop.hbase.thrift2.generated.TTableDescriptor;
import org.apache.hadoop.hbase.thrift2.generated.TTableName;
import org.apache.hadoop.hbase.thrift2.generated.TThriftServerType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.thrift.TException;

/**
 * This class is a glue object that connects Thrift RPC calls to the HBase client API primarily
 * defined in the Table interface.
 */
@InterfaceAudience.Private
@SuppressWarnings("deprecation")
public class ThriftHBaseServiceHandler extends HBaseServiceHandler implements THBaseService.Iface {

  // TODO: Size of pool configuraple
  private static final Log LOG = LogFactory.getLog(ThriftHBaseServiceHandler.class);

  // nextScannerId and scannerMap are used to manage scanner state
  // TODO: Cleanup thread for Scanners, Scanner id wrap
  private final AtomicInteger nextScannerId = new AtomicInteger(0);
  private final Map<Integer, ResultScanner> scannerMap = new ConcurrentHashMap<>();

  private static final IOException ioe
      = new DoNotRetryIOException("Thrift Server is in Read-only mode.");
  private boolean isReadOnly;

  private static class TIOErrorWithCause extends TIOError {
    private Throwable cause;

    public TIOErrorWithCause(Throwable cause) {
      super();
      this.cause = cause;
    }

    @Override
    public synchronized Throwable getCause() {
      return cause;
    }

    @Override
    public boolean equals(Object other) {
      if (super.equals(other) &&
          other instanceof TIOErrorWithCause) {
        Throwable otherCause = ((TIOErrorWithCause) other).getCause();
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

  public ThriftHBaseServiceHandler(final Configuration conf,
      final UserProvider userProvider) throws IOException {
    super(conf, userProvider);
    isReadOnly = conf.getBoolean(THRIFT_READONLY_ENABLED, THRIFT_READONLY_ENABLED_DEFAULT);
  }

  @Override
  protected Table getTable(ByteBuffer tableName) {
    try {
      return connectionCache.getTable(Bytes.toString(byteBufferToByteArray(tableName)));
    } catch (IOException ie) {
      throw new RuntimeException(ie);
    }
  }

  private RegionLocator getLocator(ByteBuffer tableName) {
    try {
      return connectionCache.getRegionLocator(byteBufferToByteArray(tableName));
    } catch (IOException ie) {
      throw new RuntimeException(ie);
    }
  }

  private void closeTable(Table table) throws TIOError {
    try {
      table.close();
    } catch (IOException e) {
      throw getTIOError(e);
    }
  }

  private TIOError getTIOError(IOException e) {
    TIOError err = new TIOErrorWithCause(e);
    err.setMessage(e.getMessage());
    return err;
  }

  /**
   * Assigns a unique ID to the scanner and adds the mapping to an internal HashMap.
   * @param scanner to add
   * @return Id for this Scanner
   */
  private int addScanner(ResultScanner scanner) {
    int id = nextScannerId.getAndIncrement();
    scannerMap.put(id, scanner);
    return id;
  }

  /**
   * Returns the Scanner associated with the specified Id.
   * @param id of the Scanner to get
   * @return a Scanner, or null if the Id is invalid
   */
  private ResultScanner getScanner(int id) {
    return scannerMap.get(id);
  }

  /**
   * Removes the scanner associated with the specified ID from the internal HashMap.
   * @param id of the Scanner to remove
   * @return the removed Scanner, or <code>null</code> if the Id is invalid
   */
  protected ResultScanner removeScanner(int id) {
    return scannerMap.remove(id);
  }

  @Override
  public boolean exists(ByteBuffer table, TGet get) throws TIOError, TException {
    Table htable = getTable(table);
    try {
      return htable.exists(getFromThrift(get));
    } catch (IOException e) {
      throw getTIOError(e);
    } finally {
      closeTable(htable);
    }
  }

  @Override
  public List<Boolean> existsAll(ByteBuffer table, List<TGet> gets) throws TIOError, TException {
    Table htable = getTable(table);
    try {
      boolean[] exists = htable.existsAll(getsFromThrift(gets));
      List<Boolean> result = new ArrayList<>(exists.length);
      for (boolean exist : exists) {
        result.add(exist);
      }
      return result;
    } catch (IOException e) {
      throw getTIOError(e);
    } finally {
      closeTable(htable);
    }
  }

  @Override
  public TResult get(ByteBuffer table, TGet get) throws TIOError, TException {
    Table htable = getTable(table);
    try {
      return resultFromHBase(htable.get(getFromThrift(get)));
    } catch (IOException e) {
      throw getTIOError(e);
    } finally {
      closeTable(htable);
    }
  }

  @Override
  public List<TResult> getMultiple(ByteBuffer table, List<TGet> gets) throws TIOError, TException {
    Table htable = getTable(table);
    try {
      return resultsFromHBase(htable.get(getsFromThrift(gets)));
    } catch (IOException e) {
      throw getTIOError(e);
    } finally {
      closeTable(htable);
    }
  }

  @Override
  public void put(ByteBuffer table, TPut put) throws TIOError, TException {
    checkReadOnlyMode();
    Table htable = getTable(table);
    try {
      htable.put(putFromThrift(put));
    } catch (IOException e) {
      throw getTIOError(e);
    } finally {
      closeTable(htable);
    }
  }

  @Override
  public boolean checkAndPut(ByteBuffer table, ByteBuffer row, ByteBuffer family,
      ByteBuffer qualifier, ByteBuffer value, TPut put) throws TIOError, TException {
    checkReadOnlyMode();

    Table htable = getTable(table);
    try {
      return htable.checkAndPut(byteBufferToByteArray(row), byteBufferToByteArray(family),
          byteBufferToByteArray(qualifier), byteBufferToByteArray(value), putFromThrift(put));
    } catch (IOException e) {
      throw getTIOError(e);
    } finally {
      closeTable(htable);
    }
  }

  @Override
  public void putMultiple(ByteBuffer table, List<TPut> puts) throws TIOError, TException {
    checkReadOnlyMode();
    Table htable = getTable(table);
    try {
      htable.put(putsFromThrift(puts));
    } catch (IOException e) {
      throw getTIOError(e);
    } finally {
      closeTable(htable);
    }
  }

  @Override
  public void deleteSingle(ByteBuffer table, TDelete deleteSingle) throws TIOError, TException {
    checkReadOnlyMode();
    Table htable = getTable(table);
    try {
      htable.delete(deleteFromThrift(deleteSingle));
    } catch (IOException e) {
      throw getTIOError(e);
    } finally {
      closeTable(htable);
    }
  }

  @Override
  public List<TDelete> deleteMultiple(ByteBuffer table, List<TDelete> deletes) throws TIOError,
      TException {
    checkReadOnlyMode();
    Table htable = getTable(table);
    try {
      htable.delete(deletesFromThrift(deletes));
    } catch (IOException e) {
      throw getTIOError(e);
    } finally {
      closeTable(htable);
    }
    return Collections.emptyList();
  }

  @Override
  public boolean checkAndMutate(ByteBuffer table, ByteBuffer row, ByteBuffer family,
      ByteBuffer qualifier, TCompareOp compareOp, ByteBuffer value, TRowMutations rowMutations)
      throws TIOError, TException {
    checkReadOnlyMode();
    try (final Table htable = getTable(table)) {
      return htable.checkAndMutate(byteBufferToByteArray(row), byteBufferToByteArray(family),
          byteBufferToByteArray(qualifier), CompareFilter.CompareOp.EQUAL,
          byteBufferToByteArray(value), rowMutationsFromThrift(rowMutations));
    } catch (IOException e) {
      throw getTIOError(e);
    }
  }

  @Override public TTableDescriptor getTableDescriptor(TTableName table)
      throws TIOError, TException {
    return null;
  }

  @Override public List<TTableDescriptor> getTableDescriptors(List<TTableName> tables)
      throws TIOError, TException {
    return null;
  }

  @Override
  public boolean checkAndDelete(ByteBuffer table, ByteBuffer row, ByteBuffer family,
      ByteBuffer qualifier, ByteBuffer value, TDelete deleteSingle) throws TIOError, TException {
    checkReadOnlyMode();
    Table htable = getTable(table);
    try {
      return htable.checkAndDelete(byteBufferToByteArray(row),
          byteBufferToByteArray(family), byteBufferToByteArray(qualifier),
          byteBufferToByteArray(value), deleteFromThrift(deleteSingle));
    } catch (IOException e) {
      throw getTIOError(e);
    } finally {
      closeTable(htable);
    }
  }

  @Override
  public TResult increment(ByteBuffer table, TIncrement increment) throws TIOError, TException {
    checkReadOnlyMode();
    Table htable = getTable(table);
    try {
      return resultFromHBase(htable.increment(incrementFromThrift(increment)));
    } catch (IOException e) {
      throw getTIOError(e);
    } finally {
      closeTable(htable);
    }
  }

  @Override
  public TResult append(ByteBuffer table, TAppend append) throws TIOError, TException {
    checkReadOnlyMode();
    Table htable = getTable(table);
    try {
      return resultFromHBase(htable.append(appendFromThrift(append)));
    } catch (IOException e) {
      throw getTIOError(e);
    } finally {
      closeTable(htable);
    }
  }

  @Override
  public int openScanner(ByteBuffer table, TScan scan) throws TIOError, TException {
    Table htable = getTable(table);
    ResultScanner resultScanner = null;
    try {
      resultScanner = htable.getScanner(scanFromThrift(scan));
    } catch (IOException e) {
      throw getTIOError(e);
    } finally {
      closeTable(htable);
    }
    return addScanner(resultScanner);
  }

  @Override
  public List<TResult> getScannerRows(int scannerId, int numRows) throws TIOError,
      TIllegalArgument, TException {
    ResultScanner scanner = getScanner(scannerId);
    if (scanner == null) {
      TIllegalArgument ex = new TIllegalArgument();
      ex.setMessage("Invalid scanner Id");
      throw ex;
    }
    try {
      connectionCache.updateConnectionAccessTime();
      return resultsFromHBase(scanner.next(numRows));
    } catch (IOException e) {
      throw getTIOError(e);
    }
  }

  @Override
  public List<TResult> getScannerResults(ByteBuffer table, TScan scan, int numRows)
      throws TIOError, TException {
    Table htable = getTable(table);
    List<TResult> results = null;
    ResultScanner scanner = null;
    try {
      scanner = htable.getScanner(scanFromThrift(scan));
      results = resultsFromHBase(scanner.next(numRows));
    } catch (IOException e) {
      throw getTIOError(e);
    } finally {
      if (scanner != null) {
        scanner.close();
      }
      closeTable(htable);
    }
    return results;
  }



  @Override
  public void closeScanner(int scannerId) throws TIOError, TIllegalArgument, TException {
    LOG.debug("scannerClose: id=" + scannerId);
    ResultScanner scanner = getScanner(scannerId);
    if (scanner == null) {
      String message = "scanner ID is invalid";
      LOG.warn(message);
      TIllegalArgument ex = new TIllegalArgument();
      ex.setMessage("Invalid scanner Id");
      throw ex;
    }
    scanner.close();
    removeScanner(scannerId);
  }

  @Override
  public void mutateRow(ByteBuffer table, TRowMutations rowMutations) throws TIOError, TException {
    checkReadOnlyMode();
    Table htable = getTable(table);
    try {
      htable.mutateRow(rowMutationsFromThrift(rowMutations));
    } catch (IOException e) {
      throw getTIOError(e);
    } finally {
      closeTable(htable);
    }
  }

  @Override
  public List<THRegionLocation> getAllRegionLocations(ByteBuffer table)
      throws TIOError, TException {
    RegionLocator locator = null;
    try {
      locator = getLocator(table);
      return ThriftUtilities.regionLocationsFromHBase(locator.getAllRegionLocations());

    } catch (IOException e) {
      throw getTIOError(e);
    } finally {
      if (locator != null) {
        try {
          locator.close();
        } catch (IOException e) {
          LOG.warn("Couldn't close the locator.", e);
        }
      }
    }
  }

  @Override
  public THRegionLocation getRegionLocation(ByteBuffer table, ByteBuffer row, boolean reload)
      throws TIOError, TException {

    RegionLocator locator = null;
    try {
      locator = getLocator(table);
      byte[] rowBytes = byteBufferToByteArray(row);
      HRegionLocation hrl = locator.getRegionLocation(rowBytes, reload);
      return ThriftUtilities.regionLocationFromHBase(hrl);

    } catch (IOException e) {
      throw getTIOError(e);
    } finally {
      if (locator != null) {
        try {
          locator.close();
        } catch (IOException e) {
          LOG.warn("Couldn't close the locator.", e);
        }
      }
    }
  }

  private void checkReadOnlyMode() throws TIOError {
    if (isReadOnly()) {
      throw getTIOError(ioe);
    }
  }

  private boolean isReadOnly() {
    return isReadOnly;
  }

  @Override
  public boolean tableExists(TTableName tTableName) throws TIOError, TException {
    try {
      TableName tableName = tableNameFromThrift(tTableName);
      return connectionCache.getAdmin().tableExists(tableName);
    } catch (IOException e) {
      throw getTIOError(e);
    }
  }

  @Override
  public List<TTableDescriptor> getTableDescriptorsByPattern(String regex, boolean includeSysTables)
      throws TIOError, TException {
    return null;
  }

  @Override public List<TTableDescriptor> getTableDescriptorsByNamespace(String name)
      throws TIOError, TException {
    return null;
  }

  @Override
  public List<TTableName> getTableNamesByPattern(String regex, boolean includeSysTables)
      throws TIOError, TException {
    try {
      Pattern pattern = (regex == null ? null : Pattern.compile(regex));
      TableName[] tableNames = connectionCache.getAdmin()
          .listTableNames(pattern, includeSysTables);
      return tableNamesFromHBase(tableNames);
    } catch (IOException e) {
      throw getTIOError(e);
    }
  }

  @Override
  public List<TTableName> getTableNamesByNamespace(String name) throws TIOError, TException {
    try {
      TableName[] tableNames = connectionCache.getAdmin().listTableNamesByNamespace(name);
      return tableNamesFromHBase(tableNames);
    } catch (IOException e) {
      throw getTIOError(e);
    }
  }

  @Override public void createTable(TTableDescriptor desc, List<ByteBuffer> splitKeys)
      throws TIOError, TException {

  }

  @Override public void deleteTable(TTableName tableName) throws TIOError, TException {

  }

  @Override public void truncateTable(TTableName tableName, boolean preserveSplits)
      throws TIOError, TException {

  }

  @Override public void enableTable(TTableName tableName) throws TIOError, TException {

  }

  @Override public void disableTable(TTableName tableName) throws TIOError, TException {

  }

  @Override
  public boolean isTableEnabled(TTableName tableName) throws TIOError, TException {
    try {
      TableName table = tableNameFromThrift(tableName);
      return connectionCache.getAdmin().isTableEnabled(table);
    } catch (IOException e) {
      throw getTIOError(e);
    }
  }

  @Override
  public boolean isTableDisabled(TTableName tableName) throws TIOError, TException {
    try {
      TableName table = tableNameFromThrift(tableName);
      return connectionCache.getAdmin().isTableDisabled(table);
    } catch (IOException e) {
      throw getTIOError(e);
    }
  }

  @Override
  public boolean isTableAvailable(TTableName tableName) throws TIOError, TException {
    try {
      TableName table = tableNameFromThrift(tableName);
      return connectionCache.getAdmin().isTableAvailable(table);
    } catch (IOException e) {
      throw getTIOError(e);
    }
  }

  @Override
  public boolean isTableAvailableWithSplit(TTableName tableName, List<ByteBuffer> splitKeys)
      throws TIOError, TException {
    try {
      TableName table = tableNameFromThrift(tableName);
      byte[][] split = splitKeyFromThrift(splitKeys);
      return connectionCache.getAdmin().isTableAvailable(table, split);
    } catch (IOException e) {
      throw getTIOError(e);
    }
  }

  @Override public void addColumnFamily(TTableName tableName, TColumnFamilyDescriptor column)
      throws TIOError, TException {
    throw new NotImplementedException();
  }

  @Override public void deleteColumnFamily(TTableName tableName, ByteBuffer column)
      throws TIOError, TException {
    throw new NotImplementedException();
  }

  @Override public void modifyColumnFamily(TTableName tableName, TColumnFamilyDescriptor column)
      throws TIOError, TException {
    throw new NotImplementedException();
  }

  @Override public void modifyTable(TTableDescriptor desc) throws TIOError, TException {
    throw new NotImplementedException();
  }

  @Override public void createNamespace(TNamespaceDescriptor namespaceDesc)
      throws TIOError, TException {
    throw new NotImplementedException();
  }

  @Override public void modifyNamespace(TNamespaceDescriptor namespaceDesc)
      throws TIOError, TException {
    throw new NotImplementedException();
  }

  @Override public void deleteNamespace(String name) throws TIOError, TException {
    throw new NotImplementedException();
  }

  @Override public TNamespaceDescriptor getNamespaceDescriptor(String name)
      throws TIOError, TException {
    throw new NotImplementedException();
  }

  @Override public List<TNamespaceDescriptor> listNamespaceDescriptors()
      throws TIOError, TException {
    throw new NotImplementedException();
  }

  @Override public List<String> listNamespaces() throws TIOError, TException {
    throw new NotImplementedException();
  }

  @Override
  public TThriftServerType getThriftServerType() {
    return TThriftServerType.TWO;
  }

  @Override public List<TOnlineLogRecord> getSlowLogResponses(Set<TServerName> serverNames,
      TLogQueryFilter logQueryFilter) throws TIOError, TException {
    throw new NotImplementedException();
  }

  @Override public List<Boolean> clearSlowLogResponses(Set<TServerName> serverNames)
      throws TIOError, TException {
    throw new NotImplementedException();
  }
}
