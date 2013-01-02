/*
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

package org.apache.hadoop.hbase.coprocessor.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HConstants.OperationStatusCode;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

public class BulkDeleteEndpoint extends BaseEndpointCoprocessor implements BulkDeleteProtocol {
  private static final String NO_OF_VERSIONS_TO_DELETE = "noOfVersionsToDelete";
  private static final Log LOG = LogFactory.getLog(BulkDeleteEndpoint.class);
  
  @Override
  public BulkDeleteResponse delete(Scan scan, byte deleteType, Long timestamp,
      int rowBatchSize) {
    long totalRowsDeleted = 0L;
    long totalVersionsDeleted = 0L;
    BulkDeleteResponse response = new BulkDeleteResponse();
    HRegion region = ((RegionCoprocessorEnvironment) getEnvironment()).getRegion();
    boolean hasMore = true;
    RegionScanner scanner = null;
    if (scan.getFilter() == null && deleteType == DeleteType.ROW) {
      // What we need is just the rowkeys. So only 1st KV from any row is enough.
      // Only when it is a row delete, we can apply this filter
      // In other types we rely on the scan to know which all columns to be deleted.
      scan.setFilter(new FirstKeyOnlyFilter());
    }
    // When the delete is based on some conditions so that Filters are available in the scan,
    // we assume that the scan is perfect having necessary column(s) only.
    try {
      scanner = region.getScanner(scan);
      while (hasMore) {
        List<List<KeyValue>> deleteRows = new ArrayList<List<KeyValue>>(rowBatchSize);
        for (int i = 0; i < rowBatchSize; i++) {
          List<KeyValue> results = new ArrayList<KeyValue>();
          hasMore = scanner.next(results);
          if (results.size() > 0) {
            deleteRows.add(results);
          }
          if (!hasMore) {
            // There are no more rows.
            break;
          }
        }
        if (deleteRows.size() > 0) {
          Pair<Mutation, Integer>[] deleteWithLockArr = new Pair[deleteRows.size()];
          int i = 0;
          for (List<KeyValue> deleteRow : deleteRows) {
            Delete delete = createDeleteMutation(deleteRow, deleteType, timestamp);
            deleteWithLockArr[i++] = new Pair<Mutation, Integer>(delete, null);
          }
          OperationStatus[] opStatus = region.batchMutate(deleteWithLockArr);
          for (i = 0; i < opStatus.length; i++) {
            if (opStatus[i].getOperationStatusCode() != OperationStatusCode.SUCCESS) {
              break;
            }
            totalRowsDeleted++;
            if (deleteType == DeleteType.VERSION) {
              byte[] versionsDeleted = deleteWithLockArr[i].getFirst().getAttribute(
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
      response.setIoException(ioe);
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException ioe) {
          LOG.error(ioe);
        }
      }
    }
    response.setRowsDeleted(totalRowsDeleted);
    response.setVersionsDeleted(totalVersionsDeleted);
    return response;
  }

  private Delete createDeleteMutation(List<KeyValue> deleteRow, byte deleteType, Long timestamp) {
    long ts;
    if (timestamp == null) {
      ts = HConstants.LATEST_TIMESTAMP;
    } else {
      ts = timestamp;
    }
    // We just need the rowkey. Get it from 1st KV.
    byte[] row = deleteRow.get(0).getRow();
    Delete delete = new Delete(row, ts, null);
    if (deleteType != DeleteType.ROW) {
      switch (deleteType) {
      case DeleteType.FAMILY:
        Set<byte[]> families = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
        for (KeyValue kv : deleteRow) {
          if (families.add(kv.getFamily())) {
            delete.deleteFamily(kv.getFamily(), ts);
          }
        }
        break;

      case DeleteType.COLUMN:
        Set<Column> columns = new HashSet<Column>();
        for (KeyValue kv : deleteRow) {
          Column column = new Column(kv.getFamily(), kv.getQualifier());
          if (columns.add(column)) {
            // Making deleteColumns() calls more than once for the same cf:qualifier is not correct
            // Every call to deleteColumns() will add a new KV to the familymap which will finally
            // get written to the memstore as part of delete().
            delete.deleteColumns(column.family, column.qualifier, ts);
          }
        }
        break;

      case DeleteType.VERSION:
        // When some timestamp was passed to the delete() call only one version of the column (with
        // given timestamp) will be deleted. If no timestamp passed, it will delete N versions.
        // How many versions will get deleted depends on the Scan being passed. All the KVs that
        // the scan fetched will get deleted.
        int noOfVersionsToDelete = 0;
        if (timestamp == null) {
          for (KeyValue kv : deleteRow) {
            delete.deleteColumn(kv.getFamily(), kv.getQualifier(), kv.getTimestamp());
            noOfVersionsToDelete++;
          }
        } else {
          columns = new HashSet<Column>();
          for (KeyValue kv : deleteRow) {
            Column column = new Column(kv.getFamily(), kv.getQualifier());
            // Only one version of particular column getting deleted.
            if (columns.add(column)) {
              delete.deleteColumn(column.family, column.qualifier, ts);
              noOfVersionsToDelete++;
            }
          }
        }
        delete.setAttribute(NO_OF_VERSIONS_TO_DELETE, Bytes.toBytes(noOfVersionsToDelete));
      }
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
}