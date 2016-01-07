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

package org.apache.hadoop.hbase.client.coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.AggregateProtocol;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

/**
 * This client class is for invoking the aggregate functions deployed on the
 * Region Server side via the AggregateProtocol. This class will implement the
 * supporting functionality for summing/processing the individual results
 * obtained from the AggregateProtocol for each region.
 * <p>
 * This will serve as the client side handler for invoking the aggregate
 * functions.
 * <ul>
 * For all aggregate functions,
 * <li>start row < end row is an essential condition (if they are not
 * {@link HConstants#EMPTY_BYTE_ARRAY})
 * <li>Column family can't be null. In case where multiple families are
 * provided, an IOException will be thrown. An optional column qualifier can
 * also be defined.
 * <li>For methods to find maximum, minimum, sum, rowcount, it returns the
 * parameter type. For average and std, it returns a double value. For row
 * count, it returns a long value.
 */
public class AggregationClient {

  private static final Log log = LogFactory.getLog(AggregationClient.class);
  Configuration conf;

  /**
   * Constructor with Conf object
   * @param cfg
   */
  public AggregationClient(Configuration cfg) {
    this.conf = cfg;
  }

  /**
   * It gives the maximum value of a column for a given column family for the
   * given range. In case qualifier is null, a max of all values for the given
   * family is returned.
   * @param tableName
   * @param ci
   * @param scan
   * @return max val <R>
   * @throws Throwable
   *           The caller is supposed to handle the exception as they are thrown
   *           & propagated to it.
   */
  public <R, S> R max(final byte[] tableName, final ColumnInterpreter<R, S> ci,
      final Scan scan) throws Throwable {
    validateParameters(scan);
    class MaxCallBack implements Batch.Callback<R> {
      R max = null;

      R getMax() {
        return max;
      }

      @Override
      public synchronized void update(byte[] region, byte[] row, R result) {
        max = (max == null || (result != null && ci.compare(max, result) < 0)) ? result : max;
      }
    }
    MaxCallBack aMaxCallBack = new MaxCallBack();
    HTable table = null;
    try {
      table = new HTable(conf, tableName);
      table.coprocessorExec(AggregateProtocol.class, scan.getStartRow(),
          scan.getStopRow(), new Batch.Call<AggregateProtocol, R>() {
            @Override
            public R call(AggregateProtocol instance) throws IOException {
              return instance.getMax(ci, scan);
            }
          }, aMaxCallBack);
    } finally {
      if (table != null) {
        table.close();
      }
    }
    return aMaxCallBack.getMax();
  }

  private void validateParameters(Scan scan) throws IOException {
    if (scan == null
        || (Bytes.equals(scan.getStartRow(), scan.getStopRow()) && !Bytes
            .equals(scan.getStartRow(), HConstants.EMPTY_START_ROW))
        || ((Bytes.compareTo(scan.getStartRow(), scan.getStopRow()) > 0) &&
        	!Bytes.equals(scan.getStopRow(), HConstants.EMPTY_END_ROW))) {
      throw new IOException(
          "Agg client Exception: Startrow should be smaller than Stoprow");
    } else if (scan.getFamilyMap().size() != 1) {
      throw new IOException("There must be only one family.");
    }
  }

  /**
   * It gives the minimum value of a column for a given column family for the
   * given range. In case qualifier is null, a min of all values for the given
   * family is returned.
   * @param tableName
   * @param ci
   * @param scan
   * @return min val <R>
   * @throws Throwable
   */
  public <R, S> R min(final byte[] tableName, final ColumnInterpreter<R, S> ci,
      final Scan scan) throws Throwable {
    validateParameters(scan);
    class MinCallBack implements Batch.Callback<R> {

      private R min = null;

      public R getMinimum() {
        return min;
      }

      @Override
      public synchronized void update(byte[] region, byte[] row, R result) {
        min = (min == null || (result != null && ci.compare(result, min) < 0)) ? result : min;
      }
    }
    MinCallBack minCallBack = new MinCallBack();
    HTable table = null;
    try {
      table = new HTable(conf, tableName);
      table.coprocessorExec(AggregateProtocol.class, scan.getStartRow(),
          scan.getStopRow(), new Batch.Call<AggregateProtocol, R>() {

            @Override
            public R call(AggregateProtocol instance) throws IOException {
              return instance.getMin(ci, scan);
            }
          }, minCallBack);
    } finally {
      if (table != null) {
        table.close();
      }
    }
    log.debug("Min fom all regions is: " + minCallBack.getMinimum());
    return minCallBack.getMinimum();
  }

  /**
   * It gives the row count, by summing up the individual results obtained from
   * regions. In case the qualifier is null, FirstKEyValueFilter is used to
   * optimised the operation. In case qualifier is provided, I can't use the
   * filter as it may set the flag to skip to next row, but the value read is
   * not of the given filter: in this case, this particular row will not be
   * counted ==> an error.
   * @param tableName
   * @param ci
   * @param scan
   * @return <R, S>
   * @throws Throwable
   */
  public <R, S> long rowCount(final byte[] tableName,
      final ColumnInterpreter<R, S> ci, final Scan scan) throws Throwable {
    validateParameters(scan);
    class RowNumCallback implements Batch.Callback<Long> {
      private final AtomicLong rowCountL = new AtomicLong(0);

      public long getRowNumCount() {
        return rowCountL.get();
      }

      @Override
      public void update(byte[] region, byte[] row, Long result) {
        rowCountL.addAndGet(result.longValue());
      }
    }
    RowNumCallback rowNum = new RowNumCallback();
    HTable table = null;
    try {
      table = new HTable(conf, tableName);
      table.coprocessorExec(AggregateProtocol.class, scan.getStartRow(),
          scan.getStopRow(), new Batch.Call<AggregateProtocol, Long>() {
            @Override
            public Long call(AggregateProtocol instance) throws IOException {
              return instance.getRowNum(ci, scan);
            }
          }, rowNum);
    } finally {
      if (table != null) {
        table.close();
      }
    }
    return rowNum.getRowNumCount();
  }

  /**
   * It sums up the value returned from various regions. In case qualifier is
   * null, summation of all the column qualifiers in the given family is done.
   * @param tableName
   * @param ci
   * @param scan
   * @return sum <S>
   * @throws Throwable
   */
  public <R, S> S sum(final byte[] tableName, final ColumnInterpreter<R, S> ci,
      final Scan scan) throws Throwable {
    validateParameters(scan);
    class SumCallBack implements Batch.Callback<S> {
      S sumVal = null;

      public S getSumResult() {
        return sumVal;
      }

      @Override
      public synchronized void update(byte[] region, byte[] row, S result) {
        sumVal = ci.add(sumVal, result);
      }
    }
    SumCallBack sumCallBack = new SumCallBack();
    HTable table = null;
    try {
      table = new HTable(conf, tableName);
      table.coprocessorExec(AggregateProtocol.class, scan.getStartRow(),
          scan.getStopRow(), new Batch.Call<AggregateProtocol, S>() {
            @Override
            public S call(AggregateProtocol instance) throws IOException {
              return instance.getSum(ci, scan);
            }
          }, sumCallBack);
    } finally {
      if (table != null) {
        table.close();
      }
    }
    return sumCallBack.getSumResult();
  }

  /**
   * It computes average while fetching sum and row count from all the
   * corresponding regions. Approach is to compute a global sum of region level
   * sum and rowcount and then compute the average.
   * @param tableName
   * @param scan
   * @throws Throwable
   */
  private <R, S> Pair<S, Long> getAvgArgs(final byte[] tableName,
      final ColumnInterpreter<R, S> ci, final Scan scan) throws Throwable {
    validateParameters(scan);
    class AvgCallBack implements Batch.Callback<Pair<S, Long>> {
      S sum = null;
      Long rowCount = 0l;

      public Pair<S, Long> getAvgArgs() {
        return new Pair<S, Long>(sum, rowCount);
      }

      @Override
      public synchronized void update(byte[] region, byte[] row, Pair<S, Long> result) {
        sum = ci.add(sum, result.getFirst());
        rowCount += result.getSecond();
      }
    }
    AvgCallBack avgCallBack = new AvgCallBack();
    HTable table = null;
    try {
      table = new HTable(conf, tableName);
      table.coprocessorExec(AggregateProtocol.class, scan.getStartRow(),
          scan.getStopRow(),
          new Batch.Call<AggregateProtocol, Pair<S, Long>>() {
            @Override
            public Pair<S, Long> call(AggregateProtocol instance)
                throws IOException {
              return instance.getAvg(ci, scan);
            }
          }, avgCallBack);
    } finally {
      if (table != null) {
        table.close();
      }
    }
    return avgCallBack.getAvgArgs();
  }

  /**
   * This is the client side interface/handle for calling the average method for
   * a given cf-cq combination. It was necessary to add one more call stack as
   * its return type should be a decimal value, irrespective of what
   * columninterpreter says. So, this methods collects the necessary parameters
   * to compute the average and returs the double value.
   * @param tableName
   * @param ci
   * @param scan
   * @return <R, S>
   * @throws Throwable
   */
  public <R, S> double avg(final byte[] tableName,
      final ColumnInterpreter<R, S> ci, Scan scan) throws Throwable {
    Pair<S, Long> p = getAvgArgs(tableName, ci, scan);
    return ci.divideForAvg(p.getFirst(), p.getSecond());
  }

  /**
   * It computes a global standard deviation for a given column and its value.
   * Standard deviation is square root of (average of squares -
   * average*average). From individual regions, it obtains sum, square sum and
   * number of rows. With these, the above values are computed to get the global
   * std.
   * @param tableName
   * @param scan
   * @return
   * @throws Throwable
   */
  private <R, S> Pair<List<S>, Long> getStdArgs(final byte[] tableName,
      final ColumnInterpreter<R, S> ci, final Scan scan) throws Throwable {
    validateParameters(scan);
    class StdCallback implements Batch.Callback<Pair<List<S>, Long>> {
      long rowCountVal = 0l;
      S sumVal = null, sumSqVal = null;

      public Pair<List<S>, Long> getStdParams() {
        List<S> l = new ArrayList<S>();
        l.add(sumVal);
        l.add(sumSqVal);
        Pair<List<S>, Long> p = new Pair<List<S>, Long>(l, rowCountVal);
        return p;
      }

      @Override
      public synchronized void update(byte[] region, byte[] row, Pair<List<S>, Long> result) {
        sumVal = ci.add(sumVal, result.getFirst().get(0));
        sumSqVal = ci.add(sumSqVal, result.getFirst().get(1));
        rowCountVal += result.getSecond();
      }
    }
    StdCallback stdCallback = new StdCallback();
    HTable table = null;
    try {
      table = new HTable(conf, tableName);
      table.coprocessorExec(AggregateProtocol.class, scan.getStartRow(),
          scan.getStopRow(),
          new Batch.Call<AggregateProtocol, Pair<List<S>, Long>>() {
            @Override
            public Pair<List<S>, Long> call(AggregateProtocol instance)
                throws IOException {
              return instance.getStd(ci, scan);
            }

          }, stdCallback);
    } finally {
      if (table != null) {
        table.close();
      }
    }
    return stdCallback.getStdParams();
  }

  /**
   * This is the client side interface/handle for calling the std method for a
   * given cf-cq combination. It was necessary to add one more call stack as its
   * return type should be a decimal value, irrespective of what
   * columninterpreter says. So, this methods collects the necessary parameters
   * to compute the std and returns the double value.
   * @param tableName
   * @param ci
   * @param scan
   * @return <R, S>
   * @throws Throwable
   */
  public <R, S> double std(final byte[] tableName, ColumnInterpreter<R, S> ci,
      Scan scan) throws Throwable {
    Pair<List<S>, Long> p = getStdArgs(tableName, ci, scan);
    double res = 0d;
    double avg = ci.divideForAvg(p.getFirst().get(0), p.getSecond());
    double avgOfSumSq = ci.divideForAvg(p.getFirst().get(1), p.getSecond());
    res = avgOfSumSq - (avg) * (avg); // variance
    res = Math.pow(res, 0.5);
    return res;
  }

  /**
   * It helps locate the region with median for a given column whose weight 
   * is specified in an optional column.
   * From individual regions, it obtains sum of values and sum of weights.
   * @param tableName
   * @param ci
   * @param scan
   * @return pair whose first element is a map between start row of the region
   *  and (sum of values, sum of weights) for the region, the second element is
   *  (sum of values, sum of weights) for all the regions chosen
   * @throws Throwable
   */
  private <R, S> Pair<NavigableMap<byte[], List<S>>, List<S>>
  getMedianArgs(final byte[] tableName,
      final ColumnInterpreter<R, S> ci, final Scan scan) throws Throwable {
    validateParameters(scan);
    final NavigableMap<byte[], List<S>> map =
      new TreeMap<byte[], List<S>>(Bytes.BYTES_COMPARATOR);
    class StdCallback implements Batch.Callback<List<S>> {
      S sumVal = null, sumWeights = null;

      public Pair<NavigableMap<byte[], List<S>>, List<S>> getMedianParams() {
        List<S> l = new ArrayList<S>();
        l.add(sumVal);
        l.add(sumWeights);
        Pair<NavigableMap<byte[], List<S>>, List<S>> p =
          new Pair<NavigableMap<byte[], List<S>>, List<S>>(map, l);
        return p;
      }

      @Override
      public synchronized void update(byte[] region, byte[] row, List<S> result) {
        map.put(row, result);
        sumVal = ci.add(sumVal, result.get(0));
        sumWeights = ci.add(sumWeights, result.get(1));
      }
    }
    StdCallback stdCallback = new StdCallback();
    HTable table = null;
    try {
      table = new HTable(conf, tableName);
      table.coprocessorExec(AggregateProtocol.class, scan.getStartRow(),
          scan.getStopRow(), new Batch.Call<AggregateProtocol, List<S>>() {
            @Override
            public List<S> call(AggregateProtocol instance) throws IOException {
              return instance.getMedian(ci, scan);
            }

          }, stdCallback);
    } finally {
      if (table != null) {
        table.close();
      }
    }
    return stdCallback.getMedianParams();
  }

  /**
   * This is the client side interface/handler for calling the median method for a
   * given cf-cq combination. This method collects the necessary parameters
   * to compute the median and returns the median.
   * @param tableName
   * @param ci
   * @param scan
   * @return R the median
   * @throws Throwable
   */
  public <R, S> R median(final byte[] tableName, ColumnInterpreter<R, S> ci,
      Scan scan) throws Throwable {
    Pair<NavigableMap<byte[], List<S>>, List<S>> p = getMedianArgs(tableName, ci, scan);
    byte[] startRow = null;
    byte[] colFamily = scan.getFamilies()[0];
    NavigableSet<byte[]> quals = scan.getFamilyMap().get(colFamily);
    NavigableMap<byte[], List<S>> map = p.getFirst();
    S sumVal = p.getSecond().get(0);
    S sumWeights = p.getSecond().get(1);
    double halfSumVal = ci.divideForAvg(sumVal, 2L);
    double movingSumVal = 0;
    boolean weighted = false;
    if (quals.size() > 1) {
      weighted = true;
      halfSumVal = ci.divideForAvg(sumWeights, 2L);
    }
    
    for (Map.Entry<byte[], List<S>> entry : map.entrySet()) {
      S s = weighted ? entry.getValue().get(1) : entry.getValue().get(0);
      double newSumVal = movingSumVal + ci.divideForAvg(s, 1L);
      if (newSumVal > halfSumVal) break;  // we found the region with the median
      movingSumVal = newSumVal;
      startRow = entry.getKey();
    }
    // scan the region with median and find it
    Scan scan2 = new Scan(scan);
    // inherit stop row from method parameter
    if (startRow != null) scan2.setStartRow(startRow);
    HTable table = null;
    ResultScanner scanner = null;
    try {
      table = new HTable(conf, tableName);
      int cacheSize = scan2.getCaching();
      if (!scan2.getCacheBlocks() || scan2.getCaching() < 2) {
        scan2.setCacheBlocks(true);
        cacheSize = 5;
        scan2.setCaching(cacheSize);
      }
      scanner = table.getScanner(scan2);
      Result[] results = null;
      byte[] qualifier = quals.pollFirst();
      // qualifier for the weight column
      byte[] weightQualifier = weighted ? quals.pollLast() : qualifier;
      R value = null;
      do {
        results = scanner.next(cacheSize);
        if (results != null && results.length > 0) {
          for (int i = 0; i < results.length; i++) {
            Result r = results[i];
            // retrieve weight
            KeyValue kv = r.getColumnLatest(colFamily, weightQualifier);
            R newValue = ci.getValue(colFamily, weightQualifier, kv);
            S s = ci.castToReturnType(newValue);
            double newSumVal = movingSumVal + ci.divideForAvg(s, 1L);
            // see if we have moved past the median
            if (newSumVal > halfSumVal) {
              return value;
            }
            movingSumVal = newSumVal;
            kv = r.getColumnLatest(colFamily, qualifier);
            value = ci.getValue(colFamily, qualifier, kv);
          }
        }
      } while (results != null && results.length > 0);
    } finally {
      if (scanner != null) {
        scanner.close();
      }
      if (table != null) {
        table.close();
      }
    }
    return null;
  }
}
