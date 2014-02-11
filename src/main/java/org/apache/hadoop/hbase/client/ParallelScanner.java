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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.DaemonThreadFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

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
  private final ThreadPoolExecutor parallelScannerPool;
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
    int threads = table.getConfiguration().getInt(
      HConstants.HBASE_CLIENT_PARALLEL_SCANNER_THREAD,
      HConstants.HBASE_CLIENT_PARALLEL_SCANNER_THREAD_DEFAULT);

    // TODO: if launching the thread pool for each ParallelScanner turns out to be a performance
    // bottleneck, it can be optimized by sharing a common thread pool.
    parallelScannerPool = new ThreadPoolExecutor(threads, threads,
      60, TimeUnit.SECONDS,
      new LinkedBlockingQueue<Runnable>(),
      new DaemonThreadFactory("ParallelScanner-Thread-"));
    parallelScannerPool.allowCoreThreadTimeOut(true);
  }

  /**
   * Initialize all the ResultScanners by calling
   * {@link HTable#getScanner(Scan)} in parallel for each scan request.
   *
   * @throws IOException if any of the getScanners throws out the exceptions
   */
  public void initialize() throws IOException {
    Map<Scan, Future<ResultScanner>> results = new HashMap();

    for (final Scan scan : scans) {
      // setCaching for each scan
      scan.setCaching(numRows);

      results.put(scan,
        parallelScannerPool.submit(new Callable<ResultScanner>() {
          public ResultScanner call() throws IOException {
            return hTable.getScanner(scan);
          }
        })
      );
    }

    for (Map.Entry<Scan, Future<ResultScanner>> resultEntry : results.entrySet()) {
      try {
        resultScanners.add(resultEntry.getValue().get());
        // TODO: switch to use multi-catch when compiling client jar with JDK7
      } catch (InterruptedException e) {
        throw new IOException("Could not get scanners for the scan: "
          + resultEntry.getKey() + " due to " + e);
      } catch (ExecutionException e) {
        throw new IOException("Could not get scanners for the scan: "
          + resultEntry.getKey() + " due to " + e);
      }
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

    List<Result> results = new ArrayList();
    Map<ResultScanner, Future<Result[]>> context = new HashMap();
    for (final ResultScanner scanner : resultScanners) {

      if (scanner.isClosed()) {
        continue;   // Skip the closed scanner
      }

      context.put(scanner,
        parallelScannerPool.submit(new Callable<Result[]>() {
          public Result[] call() throws IOException {
            Result[] tmp = scanner.next(numRows);
            if (tmp.length == 0) { // scanner.next() returns a NON-NULL result.
              scanner.close(); // close the scanner as there is no data left.
            }
            return tmp;
          }
        })
      );
    }

    for (Map.Entry<ResultScanner, Future<Result[]>> contextEntry : context.entrySet()) {
      try {
        Result[] result = contextEntry.getValue().get();
        if (result.length != 0) {
          results.addAll(Arrays.asList(result));
        }
      // TODO: switch to use multi-catch when compiling client jar with JDK7
      } catch (InterruptedException e) {
        throw new IOException("Could not get scanners for the scan: "
          + contextEntry.getKey() + " due to " + e);
      } catch (ExecutionException e) {
        throw new IOException("Could not get scanners for the scan: "
          + contextEntry.getKey() + " due to " + e);
      }
    }
    return results;
  }

  /**
   * Close all the scanners and shutdown the thread pool
   */
  public void close() {
    if (resultScanners.isEmpty()) {
      return;
    }
    for (final ResultScanner scanner : resultScanners) {
      scanner.close();
    }
    this.parallelScannerPool.shutdownNow();
  }
}
