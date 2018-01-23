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

import static org.apache.hadoop.hbase.thrift2.ThriftUtilities.appendFromThrift;
import static org.apache.hadoop.hbase.thrift2.ThriftUtilities.compareOpFromThrift;
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
import static org.apache.thrift.TBaseHelper.byteBufferToByteArray;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.thrift.ThriftMetrics;
import org.apache.hadoop.hbase.thrift2.generated.TAppend;
import org.apache.hadoop.hbase.thrift2.generated.TCompareOp;
import org.apache.hadoop.hbase.thrift2.generated.TDelete;
import org.apache.hadoop.hbase.thrift2.generated.TGet;
import org.apache.hadoop.hbase.thrift2.generated.THBaseService;
import org.apache.hadoop.hbase.thrift2.generated.THRegionLocation;
import org.apache.hadoop.hbase.thrift2.generated.TIOError;
import org.apache.hadoop.hbase.thrift2.generated.TIllegalArgument;
import org.apache.hadoop.hbase.thrift2.generated.TIncrement;
import org.apache.hadoop.hbase.thrift2.generated.TPut;
import org.apache.hadoop.hbase.thrift2.generated.TResult;
import org.apache.hadoop.hbase.thrift2.generated.TRowMutations;
import org.apache.hadoop.hbase.thrift2.generated.TScan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ConnectionCache;
import org.apache.thrift.TException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is a glue object that connects Thrift RPC calls to the HBase client API primarily
 * defined in the Table interface.
 */
@InterfaceAudience.Private
@SuppressWarnings("deprecation")
public class ThriftHBaseServiceHandler implements THBaseService.Iface {

  // TODO: Size of pool configuraple
  private static final Logger LOG = LoggerFactory.getLogger(ThriftHBaseServiceHandler.class);

  // nextScannerId and scannerMap are used to manage scanner state
  // TODO: Cleanup thread for Scanners, Scanner id wrap
  private final AtomicInteger nextScannerId = new AtomicInteger(0);
  private final Map<Integer, ResultScanner> scannerMap = new ConcurrentHashMap<>();

  private final ConnectionCache connectionCache;

  static final String CLEANUP_INTERVAL = "hbase.thrift.connection.cleanup-interval";
  static final String MAX_IDLETIME = "hbase.thrift.connection.max-idletime";

  private static final IOException ioe
      = new DoNotRetryIOException("Thrift Server is in Read-only mode.");
  private boolean isReadOnly;

  public static THBaseService.Iface newInstance(
      THBaseService.Iface handler, ThriftMetrics metrics) {
    return (THBaseService.Iface) Proxy.newProxyInstance(handler.getClass().getClassLoader(),
      new Class[] { THBaseService.Iface.class }, new THBaseServiceMetricsProxy(handler, metrics));
  }

  private static final class THBaseServiceMetricsProxy implements InvocationHandler {
    private final THBaseService.Iface handler;
    private final ThriftMetrics metrics;

    private THBaseServiceMetricsProxy(THBaseService.Iface handler, ThriftMetrics metrics) {
      this.handler = handler;
      this.metrics = metrics;
    }

    @Override
    public Object invoke(Object proxy, Method m, Object[] args) throws Throwable {
      Object result;
      long start = now();
      try {
        result = m.invoke(handler, args);
      } catch (InvocationTargetException e) {
        metrics.exception(e.getCause());
        throw e.getTargetException();
      } catch (Exception e) {
        metrics.exception(e);
        throw new RuntimeException("unexpected invocation exception: " + e.getMessage());
      } finally {
        long processTime = now() - start;
        metrics.incMethodTime(m.getName(), processTime);
      }
      return result;
    }
  }

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

  private static long now() {
    return System.nanoTime();
  }

  ThriftHBaseServiceHandler(final Configuration conf,
      final UserProvider userProvider) throws IOException {
    int cleanInterval = conf.getInt(CLEANUP_INTERVAL, 10 * 1000);
    int maxIdleTime = conf.getInt(MAX_IDLETIME, 10 * 60 * 1000);
    connectionCache = new ConnectionCache(
      conf, userProvider, cleanInterval, maxIdleTime);
    isReadOnly = conf.getBoolean("hbase.thrift.readonly", false);
  }

  private Table getTable(ByteBuffer tableName) {
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

  void setEffectiveUser(String effectiveUser) {
    connectionCache.setEffectiveUser(effectiveUser);
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
      Table.CheckAndMutateBuilder builder = htable.checkAndMutate(byteBufferToByteArray(row),
          byteBufferToByteArray(family)).qualifier(byteBufferToByteArray(qualifier));
      if (value == null) {
        return builder.ifNotExists().thenPut(putFromThrift(put));
      } else {
        return builder.ifEquals(byteBufferToByteArray(value)).thenPut(putFromThrift(put));
      }
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
      return htable.checkAndMutate(byteBufferToByteArray(row), byteBufferToByteArray(family))
          .qualifier(byteBufferToByteArray(qualifier))
          .ifMatches(compareOpFromThrift(compareOp), byteBufferToByteArray(value))
          .thenMutate(rowMutationsFromThrift(rowMutations));
    } catch (IOException e) {
      throw getTIOError(e);
    }
  }

  @Override
  public boolean checkAndDelete(ByteBuffer table, ByteBuffer row, ByteBuffer family,
      ByteBuffer qualifier, ByteBuffer value, TDelete deleteSingle) throws TIOError, TException {
    checkReadOnlyMode();
    Table htable = getTable(table);
    try {
      Table.CheckAndMutateBuilder mutateBuilder =
          htable.checkAndMutate(byteBufferToByteArray(row), byteBufferToByteArray(family))
              .qualifier(byteBufferToByteArray(qualifier));
      if (value == null) {
        return mutateBuilder.ifNotExists().thenDelete(deleteFromThrift(deleteSingle));
      } else {
        return mutateBuilder.ifEquals(byteBufferToByteArray(value))
            .thenDelete(deleteFromThrift(deleteSingle));
      }
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
}
