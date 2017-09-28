/**
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

import static org.apache.hadoop.hbase.client.coprocessor.AggregationHelper.getParsedGenericInstance;
import static org.apache.hadoop.hbase.client.coprocessor.AggregationHelper.validateArgAndGetPB;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.RawAsyncTable;
import org.apache.hadoop.hbase.client.RawAsyncTable.CoprocessorCallback;
import org.apache.hadoop.hbase.client.RawScanResultConsumer;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.protobuf.generated.AggregateProtos.AggregateRequest;
import org.apache.hadoop.hbase.protobuf.generated.AggregateProtos.AggregateResponse;
import org.apache.hadoop.hbase.protobuf.generated.AggregateProtos.AggregateService;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;

import com.google.protobuf.Message;

/**
 * This client class is for invoking the aggregate functions deployed on the Region Server side via
 * the AggregateService. This class will implement the supporting functionality for
 * summing/processing the individual results obtained from the AggregateService for each region.
 */
@InterfaceAudience.Public
public class AsyncAggregationClient {

  private static abstract class AbstractAggregationCallback<T>
      implements CoprocessorCallback<AggregateResponse> {

    private final CompletableFuture<T> future;

    protected boolean finished = false;

    private void completeExceptionally(Throwable error) {
      if (finished) {
        return;
      }
      finished = true;
      future.completeExceptionally(error);
    }

    protected AbstractAggregationCallback(CompletableFuture<T> future) {
      this.future = future;
    }

    @Override
    public synchronized void onRegionError(RegionInfo region, Throwable error) {
      completeExceptionally(error);
    }

    @Override
    public synchronized void onError(Throwable error) {
      completeExceptionally(error);
    }

    protected abstract void aggregate(RegionInfo region, AggregateResponse resp)
        throws IOException;

    @Override
    public synchronized void onRegionComplete(RegionInfo region, AggregateResponse resp) {
      try {
        aggregate(region, resp);
      } catch (IOException e) {
        completeExceptionally(e);
      }
    }

    protected abstract T getFinalResult();

    @Override
    public synchronized void onComplete() {
      if (finished) {
        return;
      }
      finished = true;
      future.complete(getFinalResult());
    }
  }

  private static <R, S, P extends Message, Q extends Message, T extends Message> R
      getCellValueFromProto(ColumnInterpreter<R, S, P, Q, T> ci, AggregateResponse resp,
          int firstPartIndex) throws IOException {
    Q q = getParsedGenericInstance(ci.getClass(), 3, resp.getFirstPart(firstPartIndex));
    return ci.getCellValueFromProto(q);
  }

  private static <R, S, P extends Message, Q extends Message, T extends Message> S
      getPromotedValueFromProto(ColumnInterpreter<R, S, P, Q, T> ci, AggregateResponse resp,
          int firstPartIndex) throws IOException {
    T t = getParsedGenericInstance(ci.getClass(), 4, resp.getFirstPart(firstPartIndex));
    return ci.getPromotedValueFromProto(t);
  }

  public static <R, S, P extends Message, Q extends Message, T extends Message> CompletableFuture<R>
      max(RawAsyncTable table, ColumnInterpreter<R, S, P, Q, T> ci, Scan scan) {
    CompletableFuture<R> future = new CompletableFuture<>();
    AggregateRequest req;
    try {
      req = validateArgAndGetPB(scan, ci, false);
    } catch (IOException e) {
      future.completeExceptionally(e);
      return future;
    }
    AbstractAggregationCallback<R> callback = new AbstractAggregationCallback<R>(future) {

      private R max;

      @Override
      protected void aggregate(RegionInfo region, AggregateResponse resp) throws IOException {
        if (resp.getFirstPartCount() > 0) {
          R result = getCellValueFromProto(ci, resp, 0);
          if (max == null || (result != null && ci.compare(max, result) < 0)) {
            max = result;
          }
        }
      }

      @Override
      protected R getFinalResult() {
        return max;
      }
    };
    table.coprocessorService(channel -> AggregateService.newStub(channel),
      (stub, controller, rpcCallback) -> stub.getMax(controller, req, rpcCallback),
      scan.getStartRow(), scan.includeStartRow(), scan.getStopRow(), scan.includeStopRow(),
      callback);
    return future;
  }

  public static <R, S, P extends Message, Q extends Message, T extends Message> CompletableFuture<R>
      min(RawAsyncTable table, ColumnInterpreter<R, S, P, Q, T> ci, Scan scan) {
    CompletableFuture<R> future = new CompletableFuture<>();
    AggregateRequest req;
    try {
      req = validateArgAndGetPB(scan, ci, false);
    } catch (IOException e) {
      future.completeExceptionally(e);
      return future;
    }
    AbstractAggregationCallback<R> callback = new AbstractAggregationCallback<R>(future) {

      private R min;

      @Override
      protected void aggregate(RegionInfo region, AggregateResponse resp) throws IOException {
        if (resp.getFirstPartCount() > 0) {
          R result = getCellValueFromProto(ci, resp, 0);
          if (min == null || (result != null && ci.compare(min, result) > 0)) {
            min = result;
          }
        }
      }

      @Override
      protected R getFinalResult() {
        return min;
      }
    };
    table.coprocessorService(channel -> AggregateService.newStub(channel),
      (stub, controller, rpcCallback) -> stub.getMin(controller, req, rpcCallback),
      scan.getStartRow(), scan.includeStartRow(), scan.getStopRow(), scan.includeStopRow(),
      callback);
    return future;
  }

  public static <R, S, P extends Message, Q extends Message, T extends Message>
      CompletableFuture<Long>
      rowCount(RawAsyncTable table, ColumnInterpreter<R, S, P, Q, T> ci, Scan scan) {
    CompletableFuture<Long> future = new CompletableFuture<>();
    AggregateRequest req;
    try {
      req = validateArgAndGetPB(scan, ci, true);
    } catch (IOException e) {
      future.completeExceptionally(e);
      return future;
    }
    AbstractAggregationCallback<Long> callback = new AbstractAggregationCallback<Long>(future) {

      private long count;

      @Override
      protected void aggregate(RegionInfo region, AggregateResponse resp) throws IOException {
        count += resp.getFirstPart(0).asReadOnlyByteBuffer().getLong();
      }

      @Override
      protected Long getFinalResult() {
        return count;
      }
    };
    table.coprocessorService(channel -> AggregateService.newStub(channel),
      (stub, controller, rpcCallback) -> stub.getRowNum(controller, req, rpcCallback),
      scan.getStartRow(), scan.includeStartRow(), scan.getStopRow(), scan.includeStopRow(),
      callback);
    return future;
  }

  public static <R, S, P extends Message, Q extends Message, T extends Message> CompletableFuture<S>
      sum(RawAsyncTable table, ColumnInterpreter<R, S, P, Q, T> ci, Scan scan) {
    CompletableFuture<S> future = new CompletableFuture<>();
    AggregateRequest req;
    try {
      req = validateArgAndGetPB(scan, ci, false);
    } catch (IOException e) {
      future.completeExceptionally(e);
      return future;
    }
    AbstractAggregationCallback<S> callback = new AbstractAggregationCallback<S>(future) {

      private S sum;

      @Override
      protected void aggregate(RegionInfo region, AggregateResponse resp) throws IOException {
        if (resp.getFirstPartCount() > 0) {
          S s = getPromotedValueFromProto(ci, resp, 0);
          sum = ci.add(sum, s);
        }
      }

      @Override
      protected S getFinalResult() {
        return sum;
      }
    };
    table.coprocessorService(channel -> AggregateService.newStub(channel),
      (stub, controller, rpcCallback) -> stub.getSum(controller, req, rpcCallback),
      scan.getStartRow(), scan.includeStartRow(), scan.getStopRow(), scan.includeStopRow(),
      callback);
    return future;
  }

  public static <R, S, P extends Message, Q extends Message, T extends Message>
      CompletableFuture<Double>
      avg(RawAsyncTable table, ColumnInterpreter<R, S, P, Q, T> ci, Scan scan) {
    CompletableFuture<Double> future = new CompletableFuture<>();
    AggregateRequest req;
    try {
      req = validateArgAndGetPB(scan, ci, false);
    } catch (IOException e) {
      future.completeExceptionally(e);
      return future;
    }
    AbstractAggregationCallback<Double> callback = new AbstractAggregationCallback<Double>(future) {

      private S sum;

      long count = 0L;

      @Override
      protected void aggregate(RegionInfo region, AggregateResponse resp) throws IOException {
        if (resp.getFirstPartCount() > 0) {
          sum = ci.add(sum, getPromotedValueFromProto(ci, resp, 0));
          count += resp.getSecondPart().asReadOnlyByteBuffer().getLong();
        }
      }

      @Override
      protected Double getFinalResult() {
        return ci.divideForAvg(sum, count);
      }
    };
    table.coprocessorService(channel -> AggregateService.newStub(channel),
      (stub, controller, rpcCallback) -> stub.getAvg(controller, req, rpcCallback),
      scan.getStartRow(), scan.includeStartRow(), scan.getStopRow(), scan.includeStopRow(),
      callback);
    return future;
  }

  public static <R, S, P extends Message, Q extends Message, T extends Message>
      CompletableFuture<Double>
      std(RawAsyncTable table, ColumnInterpreter<R, S, P, Q, T> ci, Scan scan) {
    CompletableFuture<Double> future = new CompletableFuture<>();
    AggregateRequest req;
    try {
      req = validateArgAndGetPB(scan, ci, false);
    } catch (IOException e) {
      future.completeExceptionally(e);
      return future;
    }
    AbstractAggregationCallback<Double> callback = new AbstractAggregationCallback<Double>(future) {

      private S sum;

      private S sumSq;

      private long count;

      @Override
      protected void aggregate(RegionInfo region, AggregateResponse resp) throws IOException {
        if (resp.getFirstPartCount() > 0) {
          sum = ci.add(sum, getPromotedValueFromProto(ci, resp, 0));
          sumSq = ci.add(sumSq, getPromotedValueFromProto(ci, resp, 1));
          count += resp.getSecondPart().asReadOnlyByteBuffer().getLong();
        }
      }

      @Override
      protected Double getFinalResult() {
        double avg = ci.divideForAvg(sum, count);
        double avgSq = ci.divideForAvg(sumSq, count);
        return Math.sqrt(avgSq - avg * avg);
      }
    };
    table.coprocessorService(channel -> AggregateService.newStub(channel),
      (stub, controller, rpcCallback) -> stub.getStd(controller, req, rpcCallback),
      scan.getStartRow(), scan.includeStartRow(), scan.getStopRow(), scan.includeStopRow(),
      callback);
    return future;
  }

  // the map key is the startRow of the region
  private static <R, S, P extends Message, Q extends Message, T extends Message>
      CompletableFuture<NavigableMap<byte[], S>>
      sumByRegion(RawAsyncTable table, ColumnInterpreter<R, S, P, Q, T> ci, Scan scan) {
    CompletableFuture<NavigableMap<byte[], S>> future =
        new CompletableFuture<NavigableMap<byte[], S>>();
    AggregateRequest req;
    try {
      req = validateArgAndGetPB(scan, ci, false);
    } catch (IOException e) {
      future.completeExceptionally(e);
      return future;
    }
    int firstPartIndex = scan.getFamilyMap().get(scan.getFamilies()[0]).size() - 1;
    AbstractAggregationCallback<NavigableMap<byte[], S>> callback =
        new AbstractAggregationCallback<NavigableMap<byte[], S>>(future) {

          private final NavigableMap<byte[], S> map = new TreeMap<>(Bytes.BYTES_COMPARATOR);

          @Override
          protected void aggregate(RegionInfo region, AggregateResponse resp) throws IOException {
            if (resp.getFirstPartCount() > 0) {
              map.put(region.getStartKey(), getPromotedValueFromProto(ci, resp, firstPartIndex));
            }
          }

          @Override
          protected NavigableMap<byte[], S> getFinalResult() {
            return map;
          }
        };
    table.coprocessorService(channel -> AggregateService.newStub(channel),
      (stub, controller, rpcCallback) -> stub.getMedian(controller, req, rpcCallback),
      scan.getStartRow(), scan.includeStartRow(), scan.getStopRow(), scan.includeStopRow(),
      callback);
    return future;
  }

  private static <R, S, P extends Message, Q extends Message, T extends Message> void findMedian(
      CompletableFuture<R> future, RawAsyncTable table, ColumnInterpreter<R, S, P, Q, T> ci,
      Scan scan, NavigableMap<byte[], S> sumByRegion) {
    double halfSum = ci.divideForAvg(sumByRegion.values().stream().reduce(ci::add).get(), 2L);
    S movingSum = null;
    byte[] startRow = null;
    for (Map.Entry<byte[], S> entry : sumByRegion.entrySet()) {
      startRow = entry.getKey();
      S newMovingSum = ci.add(movingSum, entry.getValue());
      if (ci.divideForAvg(newMovingSum, 1L) > halfSum) {
        break;
      }
      movingSum = newMovingSum;
    }
    if (startRow != null) {
      scan.withStartRow(startRow);
    }
    // we can not pass movingSum directly to an anonymous class as it is not final.
    S baseSum = movingSum;
    byte[] family = scan.getFamilies()[0];
    NavigableSet<byte[]> qualifiers = scan.getFamilyMap().get(family);
    byte[] weightQualifier = qualifiers.last();
    byte[] valueQualifier = qualifiers.first();
    table.scan(scan, new RawScanResultConsumer() {

      private S sum = baseSum;

      private R value = null;

      @Override
      public void onNext(Result[] results, ScanController controller) {
        try {
          for (Result result : results) {
            Cell weightCell = result.getColumnLatestCell(family, weightQualifier);
            R weight = ci.getValue(family, weightQualifier, weightCell);
            sum = ci.add(sum, ci.castToReturnType(weight));
            if (ci.divideForAvg(sum, 1L) > halfSum) {
              if (value != null) {
                future.complete(value);
              } else {
                future.completeExceptionally(new NoSuchElementException());
              }
              controller.terminate();
              return;
            }
            Cell valueCell = result.getColumnLatestCell(family, valueQualifier);
            value = ci.getValue(family, valueQualifier, valueCell);
          }
        } catch (IOException e) {
          future.completeExceptionally(e);
          controller.terminate();
        }
      }

      @Override
      public void onError(Throwable error) {
        future.completeExceptionally(error);
      }

      @Override
      public void onComplete() {
        if (!future.isDone()) {
          // we should not reach here as the future should be completed in onNext.
          future.completeExceptionally(new NoSuchElementException());
        }
      }
    });
  }

  public static <R, S, P extends Message, Q extends Message, T extends Message> CompletableFuture<R>
      median(RawAsyncTable table, ColumnInterpreter<R, S, P, Q, T> ci, Scan scan) {
    CompletableFuture<R> future = new CompletableFuture<>();
    sumByRegion(table, ci, scan).whenComplete((sumByRegion, error) -> {
      if (error != null) {
        future.completeExceptionally(error);
      } else if (sumByRegion.isEmpty()) {
        future.completeExceptionally(new NoSuchElementException());
      } else {
        findMedian(future, table, ci, ReflectionUtils.newInstance(scan.getClass(), scan),
          sumByRegion);
      }
    });
    return future;
  }
}
