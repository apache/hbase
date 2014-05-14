/**
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * ParallelScanner is a utility class for the HBase client to perform multiple scan
 * requests in parallel.
 *
 * ParallelScanner requires all the scan requests having the same caching size for the
 * simplicity purpose. It provides 3 very basic functionalities: {@link #initialize()},
 * {@link #next()},  {@link #close()}
 *
 */
public class ParallelScanner {
  public static final Log LOG = LogFactory.getLog(ParallelScanner.class);
  private final List<Scan> scans;
  private final HTable hTable;
  private final int numRows;
  private List<ResultScanner> resultScanners = new ArrayList<ResultScanner>();

  /**
   * To construct a ParallelScanner
   * @param table The common HTable instance for each scan request
   * @param scans The list of scan requests
   * @param numRows The number of rows that each scan request would fetch in one RPC call.
   */
  public ParallelScanner(HTable table, List<Scan> scans, int numRows) {
    this.hTable = table;
    this.scans = scans;
    this.numRows = numRows;
  }

  /**
   * Initializes all the ResultScanners by calling
   * {@link HTable#getScanner(Scan)} in parallel for each scan request.
   *
   * @throws IOException if any of the getScanners throws out the exceptions
   */
  public void initialize() throws IOException {
    for (final Scan scan : scans) {
      // setCaching for each scan
      scan.setCaching(numRows);
      resultScanners.add(hTable.getScanner(scan));
    }
  }

  /**
   * After the user has called {@link #initialize()}, this function will call the
   * corresponding {@link ResultScanner#next(int numRows)} from each scan request in parallel,
   * and then return all the results together as a list.  Also, if result list is empty,
   * it indicates there is no data left for all the scanners and the user can call
   * {@link #close()} afterwards.
   *
   * @return a list of result, which comes from the return of each
   * {@link ResultScanner#next(int  numRows)}
   * @throws IOException
   */
  public List<Result> next() throws IOException{
    if (resultScanners.isEmpty()) {
      LOG.warn("There is no ResultScanner available for this ConcurrentScanner.");
      return null;
    }

    ArrayList<Result> results = new ArrayList<>();
    for (final ResultScanner scanner : resultScanners) {
      if (scanner.isClosed()) {
        continue;   // Skip the closed scanner
      }

      Result[] tmp = scanner.next(numRows);
      if (tmp != null && tmp.length > 0) {
        results.ensureCapacity(results.size() + tmp.length);
        Collections.addAll(results, tmp);
      }
    }

    return results;
  }

  /**
   * Closes all the scanners and shutdown the thread pool
   */
  public void close() {
    for (final ResultScanner scanner : resultScanners) {
      scanner.close();
    }
  }
}
