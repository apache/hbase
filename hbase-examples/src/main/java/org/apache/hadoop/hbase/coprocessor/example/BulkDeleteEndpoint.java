/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.coprocessor.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HConstants.OperationStatusCode;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.example.generated.BulkDeleteProtos.BulkDeleteRequest;
import org.apache.hadoop.hbase.coprocessor.example.generated.BulkDeleteProtos.BulkDeleteRequest.DeleteType;
import org.apache.hadoop.hbase.coprocessor.example.generated.BulkDeleteProtos.BulkDeleteResponse;
import org.apache.hadoop.hbase.coprocessor.example.generated.BulkDeleteProtos.BulkDeleteResponse.Builder;
import org.apache.hadoop.hbase.coprocessor.example.generated.BulkDeleteProtos.BulkDeleteService;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner.NextState;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

/**
 * Defines a protocol to delete data in bulk based on a scan. The scan can be range scan or with
 * conditions(filters) etc.This can be used to delete rows, column family(s), column qualifier(s) 
 * or version(s) of columns.When delete type is FAMILY or COLUMN, which all family(s) or column(s)
 * getting deleted will be determined by the Scan. Scan need to select all the families/qualifiers
 * which need to be deleted.When delete type is VERSION, Which column(s) and version(s) to be
 * deleted will be determined by the Scan. Scan need to select all the qualifiers and its versions
 * which needs to be deleted.When a timestamp is passed only one version at that timestamp will be
 * deleted(even if Scan fetches many versions). When timestamp passed as null, all the versions
 * which the Scan selects will get deleted.
 * 
 * </br> Example: <code><pre>
 * Scan scan = new Scan();
 * // set scan properties(rowkey range, filters, timerange etc).
 * HTable ht = ...;
 * long noOfDeletedRows = 0L;
 * Batch.Call&lt;BulkDeleteService, BulkDeleteResponse&gt; callable = 
 *     new Batch.Call&lt;BulkDeleteService, BulkDeleteResponse&gt;() {
 *   ServerRpcController controller = new ServerRpcController();
 *   BlockingRpcCallback&lt;BulkDeleteResponse&gt; rpcCallback = 
 *     new BlockingRpcCallback&lt;BulkDeleteResponse&gt;();
 *
 *   public BulkDeleteResponse call(BulkDeleteService service) throws IOException {
 *     Builder builder = BulkDeleteRequest.newBuilder();
 *     builder.setScan(ProtobufUtil.toScan(scan));
 *     builder.setDeleteType(DeleteType.VERSION);
 *     builder.setRowBatchSize(rowBatchSize);
 *     // Set optional timestamp if needed
 *     builder.setTimestamp(timeStamp);
 *     service.delete(controller, builder.build(), rpcCallback);
 *     return rpcCallback.get();
 *   }
 * };
 * Map&lt;byte[], BulkDeleteResponse&gt; result = ht.coprocessorService(BulkDeleteService.class, scan
 *     .getStartRow(), scan.getStopRow(), callable);
 * for (BulkDeleteResponse response : result.values()) {
 *   noOfDeletedRows += response.getRowsDeleted();
 * }
 * </pre></code>
 */
public class BulkDeleteEndpoint extends BulkDeleteService implements CoprocessorService,
    Coprocessor {
  private static final String NO_OF_VERSIONS_TO_DELETE = "noOfVersionsToDelete";
  private static final Log LOG = LogFactory.getLog(BulkDeleteEndpoint.class);

  private RegionCoprocessorEnvironment env;

  @Override
  public Service getService() {
    return this;
  }

  @Override
  public void delete(RpcController controller, BulkDeleteRequest request,
      RpcCallback<BulkDeleteResponse> done) {
    long totalRowsDeleted = 0L;
    long totalVersionsDeleted = 0L;
    HRegion region = env.getRegion();
    int rowBatchSize = request.getRowBatchSize();
    Long timestamp = null;
    if (request.hasTimestamp()) {
      timestamp = request.getTimestamp();
    }
    DeleteType deleteType = request.getDeleteType();
    boolean hasMore = true;
    RegionScanner scanner = null;
    try {
      Scan scan = ProtobufUtil.toScan(request.getScan());
      if (scan.getFilter() == null && deleteType == DeleteType.ROW) {
        // What we need is just the rowkeys. So only 1st KV from any row is enough.
        // Only when it is a row delete, we can apply this filter.
        // In other types we rely on the scan to know which all columns to be deleted.
        scan.setFilter(new FirstKeyOnlyFilter());
      }
      // Here by assume that the scan is perfect with the appropriate
      // filter and having necessary column(s).
      scanner = region.getScanner(scan);
      while (hasMore) {
        List<List<Cell>> deleteRows = new ArrayList<List<Cell>>(rowBatchSize);
        for (int i = 0; i < rowBatchSize; i++) {
          List<Cell> results = new ArrayList<Cell>();
          hasMore = NextState.hasMoreValues(scanner.next(results));
          if (results.size() > 0) {
            deleteRows.add(results);
          }
          if (!hasMore) {
            // There are no more rows.
            break;
          }
        }
        if (deleteRows.size() > 0) {
          Mutation[] deleteArr = new Mutation[deleteRows.size()];
          int i = 0;
          for (List<Cell> deleteRow : deleteRows) {
            deleteArr[i++] = createDeleteMutation(deleteRow, deleteType, timestamp);
          }
          OperationStatus[] opStatus = region.batchMutate(deleteArr);
          for (i = 0; i < opStatus.length; i++) {
            if (opStatus[i].getOperationStatusCode() != OperationStatusCode.SUCCESS) {
              break;
            }
            totalRowsDeleted++;
            if (deleteType == DeleteType.VERSION) {
              byte[] versionsDeleted = deleteArr[i].getAttribute(
                  NO_OF_VERSIONS_TO_DELETE);
              if (versionsDeleted != null) {
                totalVersionsDeleted += Bytes.toInt(versionsDeleted);
              }
            }
          }
        }
      }
    } catch (IOException ioe) {
      LOG.error(ioe);
      // Call ServerRpcController#getFailedOn() to retrieve this IOException at client side.
      ResponseConverter.setControllerException(controller, ioe);
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException ioe) {
          LOG.error(ioe);
        }
      }
    }
    Builder responseBuilder = BulkDeleteResponse.newBuilder();
    responseBuilder.setRowsDeleted(totalRowsDeleted);
    if (deleteType == DeleteType.VERSION) {
      responseBuilder.setVersionsDeleted(totalVersionsDeleted);
    }
    BulkDeleteResponse result = responseBuilder.build();
    done.run(result);
  }

  private Delete createDeleteMutation(List<Cell> deleteRow, DeleteType deleteType,
      Long timestamp) {
    long ts;
    if (timestamp == null) {
      ts = HConstants.LATEST_TIMESTAMP;
    } else {
      ts = timestamp;
    }
    // We just need the rowkey. Get it from 1st KV.
    byte[] row = CellUtil.cloneRow(deleteRow.get(0));
    Delete delete = new Delete(row, ts);
    if (deleteType == DeleteType.FAMILY) {
      Set<byte[]> families = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
      for (Cell kv : deleteRow) {
        if (families.add(CellUtil.cloneFamily(kv))) {
          delete.deleteFamily(CellUtil.cloneFamily(kv), ts);
        }
      }
    } else if (deleteType == DeleteType.COLUMN) {
      Set<Column> columns = new HashSet<Column>();
      for (Cell kv : deleteRow) {
        Column column = new Column(CellUtil.cloneFamily(kv), CellUtil.cloneQualifier(kv));
        if (columns.add(column)) {
          // Making deleteColumns() calls more than once for the same cf:qualifier is not correct
          // Every call to deleteColumns() will add a new KV to the familymap which will finally
          // get written to the memstore as part of delete().
          delete.deleteColumns(column.family, column.qualifier, ts);
        }
      }
    } else if (deleteType == DeleteType.VERSION) {
      // When some timestamp was passed to the delete() call only one version of the column (with
      // given timestamp) will be deleted. If no timestamp passed, it will delete N versions.
      // How many versions will get deleted depends on the Scan being passed. All the KVs that
      // the scan fetched will get deleted.
      int noOfVersionsToDelete = 0;
      if (timestamp == null) {
        for (Cell kv : deleteRow) {
          delete.deleteColumn(CellUtil.cloneFamily(kv), CellUtil.cloneQualifier(kv), kv.getTimestamp());
          noOfVersionsToDelete++;
        }
      } else {
        Set<Column> columns = new HashSet<Column>();
        for (Cell kv : deleteRow) {
          Column column = new Column(CellUtil.cloneFamily(kv), CellUtil.cloneQualifier(kv));
          // Only one version of particular column getting deleted.
          if (columns.add(column)) {
            delete.deleteColumn(column.family, column.qualifier, ts);
            noOfVersionsToDelete++;
          }
        }
      }
      delete.setAttribute(NO_OF_VERSIONS_TO_DELETE, Bytes.toBytes(noOfVersionsToDelete));
    }
    return delete;
  }

  private static class Column {
    private byte[] family;
    private byte[] qualifier;

    public Column(byte[] family, byte[] qualifier) {
      this.family = family;
      this.qualifier = qualifier;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof Column)) {
        return false;
      }
      Column column = (Column) other;
      return Bytes.equals(this.family, column.family)
          && Bytes.equals(this.qualifier, column.qualifier);
    }

    @Override
    public int hashCode() {
      int h = 31;
      h = h + 13 * Bytes.hashCode(this.family);
      h = h + 13 * Bytes.hashCode(this.qualifier);
      return h;
    }
  }

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    if (env instanceof RegionCoprocessorEnvironment) {
      this.env = (RegionCoprocessorEnvironment) env;
    } else {
      throw new CoprocessorException("Must be loaded on a table region!");
    }
  }

  @Override
  public void stop(CoprocessorEnvironment env) throws IOException {
    // nothing to do
  }
}
