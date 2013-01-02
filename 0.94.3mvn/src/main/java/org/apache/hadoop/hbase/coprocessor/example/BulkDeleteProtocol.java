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

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

/**
 * Defines a protocol to delete data in bulk based on a scan. The scan can be range scan or with
 * conditions(filters) etc.
 * </br> Example: <code><pre>
 * Scan scan = new Scan();
 * // set scan properties(rowkey range, filters, timerange etc).
 * HTable ht = ...;
 * long noOfDeletedRows = 0L;
 * Batch.Call&lt;BulkDeleteProtocol, BulkDeleteResponse&gt; callable = 
 *     new Batch.Call&lt;BulkDeleteProtocol, BulkDeleteResponse&gt;() {
 *   public BulkDeleteResponse call(BulkDeleteProtocol instance) throws IOException {
 *     return instance.deleteRows(scan, BulkDeleteProtocol.DeleteType, timestamp, rowBatchSize);
 *   }
 * };
 * Map&lt;byte[], BulkDeleteResponse&gt; result = ht.coprocessorExec(BulkDeleteProtocol.class,
 *      scan.getStartRow(), scan.getStopRow(), callable);
 *  for (BulkDeleteResponse response : result.values()) {
 *    noOfDeletedRows = response.getRowsDeleted();
 *  }
 * </pre></code>
 */
public interface BulkDeleteProtocol extends CoprocessorProtocol {
  
  public interface DeleteType {
    /** 
     * Delete full row
     */
    byte ROW = 0;
    /**
     * Delete full family(s).
     * Which family(s) to be deleted will be determined by the Scan.
     * Scan need to select all the families which need to be deleted.
     */
    byte FAMILY = 1;
    /**
     * Delete full column(s).
     * Which column(s) to be deleted will be determined by the Scan.
     * Scan need to select all the qualifiers which need to be deleted.
     */
    byte COLUMN = 2;
    /**
     * Delete one or more version(s) of column(s).
     * Which column(s) and version(s) to be deleted will be determined by the Scan.
     * Scan need to select all the qualifiers and its versions which need to be deleted.
     * When a timestamp is passed only one version at that timestamp will be deleted(even if scan
     * fetches many versions)
     */
    byte VERSION = 3;
  }
  
  /**
   * 
   * @param scan
   * @param deleteType
   * @param timestamp
   * @param rowBatchSize
   *          The number of rows which need to be accumulated by scan and delete as one batch
   * @return
   */
  BulkDeleteResponse delete(Scan scan, byte deleteType, Long timestamp, int rowBatchSize);
}