/*
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

package org.apache.hadoop.hbase.rest.client;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.client.coprocessor.Batch.Callback;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.rest.Constants;
import org.apache.hadoop.hbase.rest.model.CellModel;
import org.apache.hadoop.hbase.rest.model.CellSetModel;
import org.apache.hadoop.hbase.rest.model.RowModel;
import org.apache.hadoop.hbase.rest.model.ScannerModel;
import org.apache.hadoop.hbase.rest.model.TableSchemaModel;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;

/**
 * HTable interface to remote tables accessed via REST gateway
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class RemoteHTable implements Table {

  private static final Log LOG = LogFactory.getLog(RemoteHTable.class);

  final Client client;
  final Configuration conf;
  final byte[] name;
  final int maxRetries;
  final long sleepTime;

  @SuppressWarnings("rawtypes")
  protected String buildRowSpec(final byte[] row, final Map familyMap,
      final long startTime, final long endTime, final int maxVersions) {
    StringBuffer sb = new StringBuffer();
    sb.append('/');
    sb.append(Bytes.toStringBinary(name));
    sb.append('/');
    sb.append(Bytes.toStringBinary(row));
    Set families = familyMap.entrySet();
    if (families != null) {
      Iterator i = familyMap.entrySet().iterator();
      sb.append('/');
      while (i.hasNext()) {
        Map.Entry e = (Map.Entry)i.next();
        Collection quals = (Collection)e.getValue();
        if (quals == null || quals.isEmpty()) {
          // this is an unqualified family. append the family name and NO ':'
          sb.append(Bytes.toStringBinary((byte[])e.getKey()));
        } else {
          Iterator ii = quals.iterator();
          while (ii.hasNext()) {
            sb.append(Bytes.toStringBinary((byte[])e.getKey()));
            sb.append(':');
            Object o = ii.next();
            // Puts use byte[] but Deletes use KeyValue
            if (o instanceof byte[]) {
              sb.append(Bytes.toStringBinary((byte[])o));
            } else if (o instanceof KeyValue) {
              sb.append(Bytes.toStringBinary(((KeyValue)o).getQualifier()));
            } else {
              throw new RuntimeException("object type not handled");
            }
            if (ii.hasNext()) {
              sb.append(',');
            }
          }
        }
        if (i.hasNext()) {
          sb.append(',');
        }
      }
    }
    if (startTime >= 0 && endTime != Long.MAX_VALUE) {
      sb.append('/');
      sb.append(startTime);
      if (startTime != endTime) {
        sb.append(',');
        sb.append(endTime);
      }
    } else if (endTime != Long.MAX_VALUE) {
      sb.append('/');
      sb.append(endTime);
    }
    if (maxVersions > 1) {
      sb.append("?v=");
      sb.append(maxVersions);
    }
    return sb.toString();
  }

  protected String buildMultiRowSpec(final byte[][] rows, int maxVersions) {
    StringBuilder sb = new StringBuilder();
    sb.append('/');
    sb.append(Bytes.toStringBinary(name));
    sb.append("/multiget/");
    if (rows == null || rows.length == 0) {
      return sb.toString();
    }
    sb.append("?");
    for(int i=0; i<rows.length; i++) {
      byte[] rk = rows[i];
      if (i != 0) {
        sb.append('&');
      }
      sb.append("row=");
      sb.append(Bytes.toStringBinary(rk));
    }
    sb.append("&v=");
    sb.append(maxVersions);

    return sb.toString();
  }

  protected Result[] buildResultFromModel(final CellSetModel model) {
    List<Result> results = new ArrayList<Result>();
    for (RowModel row: model.getRows()) {
      List<Cell> kvs = new ArrayList<Cell>();
      for (CellModel cell: row.getCells()) {
        byte[][] split = KeyValue.parseColumn(cell.getColumn());
        byte[] column = split[0];
        byte[] qualifier = null;
        if (split.length == 1) {
          qualifier = HConstants.EMPTY_BYTE_ARRAY;
        } else if (split.length == 2) {
          qualifier = split[1];
        } else {
          throw new IllegalArgumentException("Invalid familyAndQualifier provided.");
        }
        kvs.add(new KeyValue(row.getKey(), column, qualifier,
          cell.getTimestamp(), cell.getValue()));
      }
      results.add(Result.create(kvs));
    }
    return results.toArray(new Result[results.size()]);
  }

  protected CellSetModel buildModelFromPut(Put put) {
    RowModel row = new RowModel(put.getRow());
    long ts = put.getTimeStamp();
    for (List<Cell> cells: put.getFamilyCellMap().values()) {
      for (Cell cell: cells) {
        row.addCell(new CellModel(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell),
          ts != HConstants.LATEST_TIMESTAMP ? ts : cell.getTimestamp(),
          CellUtil.cloneValue(cell)));
      }
    }
    CellSetModel model = new CellSetModel();
    model.addRow(row);
    return model;
  }

  /**
   * Constructor
   * @param client
   * @param name
   */
  public RemoteHTable(Client client, String name) {
    this(client, HBaseConfiguration.create(), Bytes.toBytes(name));
  }

  /**
   * Constructor
   * @param client
   * @param conf
   * @param name
   */
  public RemoteHTable(Client client, Configuration conf, String name) {
    this(client, conf, Bytes.toBytes(name));
  }

  /**
   * Constructor
   * @param client
   * @param conf
   * @param name
   */
  public RemoteHTable(Client client, Configuration conf, byte[] name) {
    this.client = client;
    this.conf = conf;
    this.name = name;
    this.maxRetries = conf.getInt("hbase.rest.client.max.retries", 10);
    this.sleepTime = conf.getLong("hbase.rest.client.sleep", 1000);
  }

  public byte[] getTableName() {
    return name.clone();
  }

  @Override
  public TableName getName() {
    return TableName.valueOf(name);
  }

  @Override
  public Configuration getConfiguration() {
    return conf;
  }

  @Override
  public HTableDescriptor getTableDescriptor() throws IOException {
    StringBuilder sb = new StringBuilder();
    sb.append('/');
    sb.append(Bytes.toStringBinary(name));
    sb.append('/');
    sb.append("schema");
    for (int i = 0; i < maxRetries; i++) {
      Response response = client.get(sb.toString(), Constants.MIMETYPE_PROTOBUF);
      int code = response.getCode();
      switch (code) {
      case 200:
        TableSchemaModel schema = new TableSchemaModel();
        schema.getObjectFromMessage(response.getBody());
        return schema.getTableDescriptor();
      case 509:
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          throw (InterruptedIOException)new InterruptedIOException().initCause(e);
        }
        break;
      default:
        throw new IOException("schema request returned " + code);
      }
    }
    throw new IOException("schema request timed out");
  }

  @Override
  public void close() throws IOException {
    client.shutdown();
  }

  @Override
  public Result get(Get get) throws IOException {
    TimeRange range = get.getTimeRange();
    String spec = buildRowSpec(get.getRow(), get.getFamilyMap(),
      range.getMin(), range.getMax(), get.getMaxVersions());
    if (get.getFilter() != null) {
      LOG.warn("filters not supported on gets");
    }
    Result[] results = getResults(spec);
    if (results.length > 0) {
      if (results.length > 1) {
        LOG.warn("too many results for get (" + results.length + ")");
      }
      return results[0];
    } else {
      return new Result();
    }
  }

  @Override
  public Result[] get(List<Get> gets) throws IOException {
    byte[][] rows = new byte[gets.size()][];
    int maxVersions = 1;
    int count = 0;

    for(Get g:gets) {

      if ( count == 0 ) {
        maxVersions = g.getMaxVersions();
      } else if (g.getMaxVersions() != maxVersions) {
        LOG.warn("MaxVersions on Gets do not match, using the first in the list ("+maxVersions+")");
      }

      if (g.getFilter() != null) {
        LOG.warn("filters not supported on gets");
      }

      rows[count] = g.getRow();
      count ++;
    }

    String spec = buildMultiRowSpec(rows, maxVersions);

    return getResults(spec);
  }

  private Result[] getResults(String spec) throws IOException {
    for (int i = 0; i < maxRetries; i++) {
      Response response = client.get(spec, Constants.MIMETYPE_PROTOBUF);
      int code = response.getCode();
      switch (code) {
        case 200:
          CellSetModel model = new CellSetModel();
          model.getObjectFromMessage(response.getBody());
          Result[] results = buildResultFromModel(model);
          if ( results.length > 0) {
            return results;
          }
          // fall through
        case 404:
          return new Result[0];

        case 509:
          try {
            Thread.sleep(sleepTime);
          } catch (InterruptedException e) {
            throw (InterruptedIOException)new InterruptedIOException().initCause(e);
          }
          break;
        default:
          throw new IOException("get request returned " + code);
      }
    }
    throw new IOException("get request timed out");
  }

  @Override
  public boolean exists(Get get) throws IOException {
    LOG.warn("exists() is really get(), just use get()");
    Result result = get(get);
    return (result != null && !(result.isEmpty()));
  }

  /**
   * exists(List) is really a list of get() calls. Just use get().
   * @param gets list of Get to test for the existence
   */
  @Override
  public boolean[] existsAll(List<Get> gets) throws IOException {
    LOG.warn("exists(List<Get>) is really list of get() calls, just use get()");
    boolean[] results = new boolean[gets.size()];
    for (int i = 0; i < results.length; i++) {
      results[i] = exists(gets.get(i));
    }
    return results;
  }

  @Deprecated
  public Boolean[] exists(List<Get> gets) throws IOException {
    boolean[] results = existsAll(gets);
    Boolean[] objectResults = new Boolean[results.length];
    for (int i = 0; i < results.length; ++i) {
      objectResults[i] = results[i];
    }
    return objectResults;
  }

  @Override
  public void put(Put put) throws IOException {
    CellSetModel model = buildModelFromPut(put);
    StringBuilder sb = new StringBuilder();
    sb.append('/');
    sb.append(Bytes.toStringBinary(name));
    sb.append('/');
    sb.append(Bytes.toStringBinary(put.getRow()));
    for (int i = 0; i < maxRetries; i++) {
      Response response = client.put(sb.toString(), Constants.MIMETYPE_PROTOBUF,
        model.createProtobufOutput());
      int code = response.getCode();
      switch (code) {
      case 200:
        return;
      case 509:
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          throw (InterruptedIOException)new InterruptedIOException().initCause(e);
        }
        break;
      default:
        throw new IOException("put request failed with " + code);
      }
    }
    throw new IOException("put request timed out");
  }

  @Override
  public void put(List<Put> puts) throws IOException {
    // this is a trick: The gateway accepts multiple rows in a cell set and
    // ignores the row specification in the URI

    // separate puts by row
    TreeMap<byte[],List<Cell>> map =
      new TreeMap<byte[],List<Cell>>(Bytes.BYTES_COMPARATOR);
    for (Put put: puts) {
      byte[] row = put.getRow();
      List<Cell> cells = map.get(row);
      if (cells == null) {
        cells = new ArrayList<Cell>();
        map.put(row, cells);
      }
      for (List<Cell> l: put.getFamilyCellMap().values()) {
        cells.addAll(l);
      }
    }

    // build the cell set
    CellSetModel model = new CellSetModel();
    for (Map.Entry<byte[], List<Cell>> e: map.entrySet()) {
      RowModel row = new RowModel(e.getKey());
      for (Cell cell: e.getValue()) {
        row.addCell(new CellModel(cell));
      }
      model.addRow(row);
    }

    // build path for multiput
    StringBuilder sb = new StringBuilder();
    sb.append('/');
    sb.append(Bytes.toStringBinary(name));
    sb.append("/$multiput"); // can be any nonexistent row
    for (int i = 0; i < maxRetries; i++) {
      Response response = client.put(sb.toString(), Constants.MIMETYPE_PROTOBUF,
        model.createProtobufOutput());
      int code = response.getCode();
      switch (code) {
      case 200:
        return;
      case 509:
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          throw (InterruptedIOException)new InterruptedIOException().initCause(e);
        }
        break;
      default:
        throw new IOException("multiput request failed with " + code);
      }
    }
    throw new IOException("multiput request timed out");
  }

  @Override
  public void delete(Delete delete) throws IOException {
    String spec = buildRowSpec(delete.getRow(), delete.getFamilyCellMap(),
      delete.getTimeStamp(), delete.getTimeStamp(), 1);
    for (int i = 0; i < maxRetries; i++) {
      Response response = client.delete(spec);
      int code = response.getCode();
      switch (code) {
      case 200:
        return;
      case 509:
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          throw (InterruptedIOException)new InterruptedIOException().initCause(e);
        }
        break;
      default:
        throw new IOException("delete request failed with " + code);
      }
    }
    throw new IOException("delete request timed out");
  }

  @Override
  public void delete(List<Delete> deletes) throws IOException {
    for (Delete delete: deletes) {
      delete(delete);
    }
  }

  public void flushCommits() throws IOException {
    // no-op
  }

  class Scanner implements ResultScanner {

    String uri;

    public Scanner(Scan scan) throws IOException {
      ScannerModel model;
      try {
        model = ScannerModel.fromScan(scan);
      } catch (Exception e) {
        throw new IOException(e);
      }
      StringBuffer sb = new StringBuffer();
      sb.append('/');
      sb.append(Bytes.toStringBinary(name));
      sb.append('/');
      sb.append("scanner");
      for (int i = 0; i < maxRetries; i++) {
        Response response = client.post(sb.toString(),
          Constants.MIMETYPE_PROTOBUF, model.createProtobufOutput());
        int code = response.getCode();
        switch (code) {
        case 201:
          uri = response.getLocation();
          return;
        case 509:
          try {
            Thread.sleep(sleepTime);
          } catch (InterruptedException e) {
            throw (InterruptedIOException)new InterruptedIOException().initCause(e);
          }
          break;
        default:
          throw new IOException("scan request failed with " + code);
        }
      }
      throw new IOException("scan request timed out");
    }

    @Override
    public Result[] next(int nbRows) throws IOException {
      StringBuilder sb = new StringBuilder(uri);
      sb.append("?n=");
      sb.append(nbRows);
      for (int i = 0; i < maxRetries; i++) {
        Response response = client.get(sb.toString(),
          Constants.MIMETYPE_PROTOBUF);
        int code = response.getCode();
        switch (code) {
        case 200:
          CellSetModel model = new CellSetModel();
          model.getObjectFromMessage(response.getBody());
          return buildResultFromModel(model);
        case 204:
        case 206:
          return null;
        case 509:
          try {
            Thread.sleep(sleepTime);
          } catch (InterruptedException e) {
            throw (InterruptedIOException)new InterruptedIOException().initCause(e);
          }
          break;
        default:
          throw new IOException("scanner.next request failed with " + code);
        }
      }
      throw new IOException("scanner.next request timed out");
    }

    @Override
    public Result next() throws IOException {
      Result[] results = next(1);
      if (results == null || results.length < 1) {
        return null;
      }
      return results[0];
    }

    class Iter implements Iterator<Result> {

      Result cache;

      public Iter() {
        try {
          cache = Scanner.this.next();
        } catch (IOException e) {
          LOG.warn(StringUtils.stringifyException(e));
        }
      }

      @Override
      public boolean hasNext() {
        return cache != null;
      }

      @Override
      public Result next() {
        Result result = cache;
        try {
          cache = Scanner.this.next();
        } catch (IOException e) {
          LOG.warn(StringUtils.stringifyException(e));
          cache = null;
        }
        return result;
      }

      @Override
      public void remove() {
        throw new RuntimeException("remove() not supported");
      }

    }

    @Override
    public Iterator<Result> iterator() {
      return new Iter();
    }

    @Override
    public void close() {
      try {
        client.delete(uri);
      } catch (IOException e) {
        LOG.warn(StringUtils.stringifyException(e));
      }
    }

    @Override
    public boolean renewLease() {
      throw new RuntimeException("renewLease() not supported");
    }
  }

  @Override
  public ResultScanner getScanner(Scan scan) throws IOException {
    return new Scanner(scan);
  }

  @Override
  public ResultScanner getScanner(byte[] family) throws IOException {
    Scan scan = new Scan();
    scan.addFamily(family);
    return new Scanner(scan);
  }

  @Override
  public ResultScanner getScanner(byte[] family, byte[] qualifier)
      throws IOException {
    Scan scan = new Scan();
    scan.addColumn(family, qualifier);
    return new Scanner(scan);
  }

  public boolean isAutoFlush() {
    return true;
  }

  public Result getRowOrBefore(byte[] row, byte[] family) throws IOException {
    throw new IOException("getRowOrBefore not supported");
  }

  @Override
  public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier,
      byte[] value, Put put) throws IOException {
    // column to check-the-value
    put.add(new KeyValue(row, family, qualifier, value));

    CellSetModel model = buildModelFromPut(put);
    StringBuilder sb = new StringBuilder();
    sb.append('/');
    sb.append(Bytes.toStringBinary(name));
    sb.append('/');
    sb.append(Bytes.toStringBinary(put.getRow()));
    sb.append("?check=put");

    for (int i = 0; i < maxRetries; i++) {
      Response response = client.put(sb.toString(),
        Constants.MIMETYPE_PROTOBUF, model.createProtobufOutput());
      int code = response.getCode();
      switch (code) {
      case 200:
        return true;
      case 304: // NOT-MODIFIED
        return false;
      case 509:
        try {
          Thread.sleep(sleepTime);
        } catch (final InterruptedException e) {
          throw (InterruptedIOException)new InterruptedIOException().initCause(e);
        }
        break;
      default:
        throw new IOException("checkAndPut request failed with " + code);
      }
    }
    throw new IOException("checkAndPut request timed out");
  }

  @Override
  public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier,
      CompareOp compareOp, byte[] value, Put put) throws IOException {
    throw new IOException("checkAndPut for non-equal comparison not implemented");
  }

  @Override
  public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
      byte[] value, Delete delete) throws IOException {
    Put put = new Put(row);
    // column to check-the-value
    put.add(new KeyValue(row, family, qualifier, value));
    CellSetModel model = buildModelFromPut(put);
    StringBuilder sb = new StringBuilder();
    sb.append('/');
    sb.append(Bytes.toStringBinary(name));
    sb.append('/');
    sb.append(Bytes.toStringBinary(row));
    sb.append("?check=delete");

    for (int i = 0; i < maxRetries; i++) {
      Response response = client.put(sb.toString(),
        Constants.MIMETYPE_PROTOBUF, model.createProtobufOutput());
      int code = response.getCode();
      switch (code) {
      case 200:
        return true;
      case 304: // NOT-MODIFIED
        return false;
      case 509:
        try {
          Thread.sleep(sleepTime);
        } catch (final InterruptedException e) {
          throw (InterruptedIOException)new InterruptedIOException().initCause(e);
        }
        break;
      default:
        throw new IOException("checkAndDelete request failed with " + code);
      }
    }
    throw new IOException("checkAndDelete request timed out");
  }

  @Override
  public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
      CompareOp compareOp, byte[] value, Delete delete) throws IOException {
    throw new IOException("checkAndDelete for non-equal comparison not implemented");
  }

  @Override
  public Result increment(Increment increment) throws IOException {
    throw new IOException("Increment not supported");
  }

  @Override
  public Result append(Append append) throws IOException {
    throw new IOException("Append not supported");
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier,
      long amount) throws IOException {
    throw new IOException("incrementColumnValue not supported");
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier,
      long amount, Durability durability) throws IOException {
    throw new IOException("incrementColumnValue not supported");
  }

  @Override
  public void batch(List<? extends Row> actions, Object[] results) throws IOException {
    throw new IOException("batch not supported");
  }

  @Override
  public <R> void batchCallback(List<? extends Row> actions, Object[] results,
      Batch.Callback<R> callback) throws IOException, InterruptedException {
    throw new IOException("batchCallback not supported");
  }

  @Override
  public CoprocessorRpcChannel coprocessorService(byte[] row) {
    throw new UnsupportedOperationException("coprocessorService not implemented");
  }

  @Override
  public <T extends Service, R> Map<byte[], R> coprocessorService(Class<T> service,
      byte[] startKey, byte[] endKey, Batch.Call<T, R> callable)
      throws ServiceException, Throwable {
    throw new UnsupportedOperationException("coprocessorService not implemented");
  }

  @Override
  public <T extends Service, R> void coprocessorService(Class<T> service,
      byte[] startKey, byte[] endKey, Batch.Call<T, R> callable, Batch.Callback<R> callback)
      throws ServiceException, Throwable {
    throw new UnsupportedOperationException("coprocessorService not implemented");
  }

  @Override
  public void mutateRow(RowMutations rm) throws IOException {
    throw new IOException("atomicMutation not supported");
  }

  @Override
  public long getWriteBufferSize() {
    throw new UnsupportedOperationException("getWriteBufferSize not implemented");
  }

  @Override
  public void setWriteBufferSize(long writeBufferSize) throws IOException {
    throw new IOException("setWriteBufferSize not supported");
  }

  @Override
  public <R extends Message> Map<byte[], R> batchCoprocessorService(
      Descriptors.MethodDescriptor method, Message request,
      byte[] startKey, byte[] endKey, R responsePrototype) throws ServiceException, Throwable {
    throw new UnsupportedOperationException("batchCoprocessorService not implemented");
  }

  @Override
  public <R extends Message> void batchCoprocessorService(
      Descriptors.MethodDescriptor method, Message request,
      byte[] startKey, byte[] endKey, R responsePrototype, Callback<R> callback)
      throws ServiceException, Throwable {
    throw new UnsupportedOperationException("batchCoprocessorService not implemented");
  }

  @Override public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier,
      CompareOp compareOp, byte[] value, RowMutations rm) throws IOException {
    throw new UnsupportedOperationException("checkAndMutate not implemented");
  }
}
