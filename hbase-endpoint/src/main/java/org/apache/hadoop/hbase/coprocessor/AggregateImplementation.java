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
package org.apache.hadoop.hbase.coprocessor;

import static org.apache.hadoop.hbase.client.coprocessor.AggregationHelper.getParsedGenericInstance;

import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.function.Function;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.ClientUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.quotas.OperationQuota;
import org.apache.hadoop.hbase.quotas.RpcThrottlingException;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcCallback;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.Service;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AggregateProtos.AggregateRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AggregateProtos.AggregateResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AggregateProtos.AggregateService;

/**
 * A concrete AggregateProtocol implementation. Its system level coprocessor that computes the
 * aggregate function at a region level. {@link ColumnInterpreter} is used to interpret column
 * value. This class is parameterized with the following (these are the types with which the
 * {@link ColumnInterpreter} is parameterized, and for more description on these, refer to
 * {@link ColumnInterpreter}):<br>
 * &lt;T&gt; Cell value data type<br>
 * &lt;S&gt; Promoted data type<br>
 * &lt;P&gt; PB message that is used to transport initializer specific bytes<br>
 * &lt;Q&gt; PB message that is used to transport Cell (&lt;T&gt;) instance<br>
 * &lt;R&gt; PB message that is used to transport Promoted (&lt;S&gt;) instance<br>
 */
@InterfaceAudience.Private
public class AggregateImplementation<T, S, P extends Message, Q extends Message, R extends Message>
  extends AggregateService implements RegionCoprocessor {
  protected static final Logger log = LoggerFactory.getLogger(AggregateImplementation.class);
  private RegionCoprocessorEnvironment env;

  /**
   * Gives the maximum for a given combination of column qualifier and column family, in the given
   * row range as defined in the Scan object. In its current implementation, it takes one column
   * family and one column qualifier (if provided). In case of null column qualifier, maximum value
   * for the entire column family will be returned.
   */
  @Override
  public void getMax(RpcController controller, AggregateRequest request,
    RpcCallback<AggregateResponse> done) {
    InternalScanner scanner = null;
    AggregateResponse response = null;
    PartialResultContext partialResultContext = new PartialResultContext();
    T max = null;
    boolean hasMoreRows = true;
    try {
      ColumnInterpreter<T, S, P, Q, R> ci = constructColumnInterpreterFromRequest(request);
      T temp;
      Scan scan = ProtobufUtil.toScan(request.getScan());
      scanner = env.getRegion().getScanner(scan);
      List<Cell> results = new ArrayList<>();
      byte[] colFamily = scan.getFamilies()[0];
      NavigableSet<byte[]> qualifiers = scan.getFamilyMap().get(colFamily);
      // qualifier can be null.
      byte[] qualifier = null;
      if (qualifiers != null && !qualifiers.isEmpty()) {
        qualifier = qualifiers.pollFirst();
      }
      do {
        if (shouldBreakForThrottling(request, scan, partialResultContext)) {
          break;
        }
        hasMoreRows = scanner.next(results);
        int listSize = results.size();
        for (int i = 0; i < listSize; i++) {
          temp = ci.getValue(colFamily, qualifier, results.get(i));
          max = (max == null || (temp != null && ci.compare(temp, max) > 0)) ? temp : max;
        }
        postScanPartialResultUpdate(results, partialResultContext);
        results.clear();
      } while (hasMoreRows);
      response = singlePartResponse(request, hasMoreRows, partialResultContext, max,
        ci::getProtoForCellType);
    } catch (IOException e) {
      CoprocessorRpcUtils.setControllerException(controller, e);
    } finally {
      if (scanner != null) {
        IOUtils.closeQuietly(scanner);
      }
      closeQuota(partialResultContext);
    }
    log.debug("Maximum from this region is {}: {} (partial result: {}) (client {})",
      env.getRegion().getRegionInfo().getRegionNameAsString(), max, hasMoreRows,
      RpcServer.getRequestUser());
    done.run(response);
  }

  /**
   * Gives the minimum for a given combination of column qualifier and column family, in the given
   * row range as defined in the Scan object. In its current implementation, it takes one column
   * family and one column qualifier (if provided). In case of null column qualifier, minimum value
   * for the entire column family will be returned.
   */
  @Override
  public void getMin(RpcController controller, AggregateRequest request,
    RpcCallback<AggregateResponse> done) {
    AggregateResponse response = null;
    InternalScanner scanner = null;
    PartialResultContext partialResultContext = new PartialResultContext();
    T min = null;
    boolean hasMoreRows = true;
    try {
      ColumnInterpreter<T, S, P, Q, R> ci = constructColumnInterpreterFromRequest(request);
      T temp;
      Scan scan = ProtobufUtil.toScan(request.getScan());
      scanner = env.getRegion().getScanner(scan);
      List<Cell> results = new ArrayList<>();
      byte[] colFamily = scan.getFamilies()[0];
      NavigableSet<byte[]> qualifiers = scan.getFamilyMap().get(colFamily);
      byte[] qualifier = null;
      if (qualifiers != null && !qualifiers.isEmpty()) {
        qualifier = qualifiers.pollFirst();
      }
      do {
        if (shouldBreakForThrottling(request, scan, partialResultContext)) {
          break;
        }
        hasMoreRows = scanner.next(results);
        int listSize = results.size();
        for (int i = 0; i < listSize; i++) {
          temp = ci.getValue(colFamily, qualifier, results.get(i));
          min = (min == null || (temp != null && ci.compare(temp, min) < 0)) ? temp : min;
        }
        postScanPartialResultUpdate(results, partialResultContext);
        results.clear();
      } while (hasMoreRows);
      response = singlePartResponse(request, hasMoreRows, partialResultContext, min,
        ci::getProtoForCellType);
    } catch (IOException e) {
      CoprocessorRpcUtils.setControllerException(controller, e);
    } finally {
      if (scanner != null) {
        IOUtils.closeQuietly(scanner);
      }
      closeQuota(partialResultContext);
    }
    log.debug("Minimum from this region is {}: {} (partial result: {}) (client {})",
      env.getRegion().getRegionInfo().getRegionNameAsString(), min, hasMoreRows,
      RpcServer.getRequestUser());
    done.run(response);
  }

  /**
   * Gives the sum for a given combination of column qualifier and column family, in the given row
   * range as defined in the Scan object. In its current implementation, it takes one column family
   * and one column qualifier (if provided). In case of null column qualifier, sum for the entire
   * column family will be returned.
   */
  @Override
  public void getSum(RpcController controller, AggregateRequest request,
    RpcCallback<AggregateResponse> done) {
    AggregateResponse response = null;
    InternalScanner scanner = null;
    PartialResultContext partialResultContext = new PartialResultContext();
    long sum = 0L;
    boolean hasMoreRows = true;
    try {
      ColumnInterpreter<T, S, P, Q, R> ci = constructColumnInterpreterFromRequest(request);
      S sumVal = null;
      T temp;
      Scan scan = ProtobufUtil.toScan(request.getScan());
      scanner = env.getRegion().getScanner(scan);
      byte[] colFamily = scan.getFamilies()[0];
      NavigableSet<byte[]> qualifiers = scan.getFamilyMap().get(colFamily);
      byte[] qualifier = null;
      if (qualifiers != null && !qualifiers.isEmpty()) {
        qualifier = qualifiers.pollFirst();
      }
      List<Cell> results = new ArrayList<>();
      do {
        if (shouldBreakForThrottling(request, scan, partialResultContext)) {
          break;
        }
        hasMoreRows = scanner.next(results);
        int listSize = results.size();
        for (int i = 0; i < listSize; i++) {
          temp = ci.getValue(colFamily, qualifier, results.get(i));
          if (temp != null) {
            sumVal = ci.add(sumVal, ci.castToReturnType(temp));
          }
        }
        postScanPartialResultUpdate(results, partialResultContext);
        results.clear();
      } while (hasMoreRows);
      response = singlePartResponse(request, hasMoreRows, partialResultContext, sumVal,
        ci::getProtoForPromotedType);
    } catch (IOException e) {
      CoprocessorRpcUtils.setControllerException(controller, e);
    } finally {
      if (scanner != null) {
        IOUtils.closeQuietly(scanner);
      }
      closeQuota(partialResultContext);
    }
    log.debug("Sum from this region is {}: {} (partial result: {}) (client {})",
      env.getRegion().getRegionInfo().getRegionNameAsString(), sum, hasMoreRows,
      RpcServer.getRequestUser());
    done.run(response);
  }

  /**
   * Gives the row count for the given column family and column qualifier, in the given row range as
   * defined in the Scan object.
   */
  @Override
  public void getRowNum(RpcController controller, AggregateRequest request,
    RpcCallback<AggregateResponse> done) {
    AggregateResponse response = null;
    long counter = 0L;
    List<Cell> results = new ArrayList<>();
    InternalScanner scanner = null;
    PartialResultContext partialResultContext = new PartialResultContext();
    boolean hasMoreRows = true;
    try {
      Scan scan = ProtobufUtil.toScan(request.getScan());
      byte[][] colFamilies = scan.getFamilies();
      byte[] colFamily = colFamilies != null ? colFamilies[0] : null;
      NavigableSet<byte[]> qualifiers =
        colFamilies != null ? scan.getFamilyMap().get(colFamily) : null;
      byte[] qualifier = null;
      if (qualifiers != null && !qualifiers.isEmpty()) {
        qualifier = qualifiers.pollFirst();
      }
      if (scan.getFilter() == null && qualifier == null) {
        scan.setFilter(new FirstKeyOnlyFilter());
      }
      scanner = env.getRegion().getScanner(scan);
      do {
        if (shouldBreakForThrottling(request, scan, partialResultContext)) {
          break;
        }
        hasMoreRows = scanner.next(results);
        if (!results.isEmpty()) {
          counter++;
        }
        postScanPartialResultUpdate(results, partialResultContext);
        results.clear();
      } while (hasMoreRows);
      ByteBuffer bb = ByteBuffer.allocate(8).putLong(counter);
      bb.rewind();
      response = responseBuilder(request, hasMoreRows, partialResultContext)
        .addFirstPart(ByteString.copyFrom(bb)).build();
    } catch (IOException e) {
      CoprocessorRpcUtils.setControllerException(controller, e);
    } finally {
      if (scanner != null) {
        IOUtils.closeQuietly(scanner);
      }
      closeQuota(partialResultContext);
    }
    log.debug("Row counter from this region is {}: {} (partial result: {}) (client {})",
      env.getRegion().getRegionInfo().getRegionNameAsString(), counter, hasMoreRows,
      RpcServer.getRequestUser());
    done.run(response);
  }

  /**
   * Gives a Pair with first object as Sum and second object as row count, computed for a given
   * combination of column qualifier and column family in the given row range as defined in the Scan
   * object. In its current implementation, it takes one column family and one column qualifier (if
   * provided). In case of null column qualifier, an aggregate sum over all the entire column family
   * will be returned.
   * <p>
   * The average is computed in AggregationClient#avg(byte[], ColumnInterpreter, Scan) by processing
   * results from all regions, so its "ok" to pass sum and a Long type.
   */
  @Override
  public void getAvg(RpcController controller, AggregateRequest request,
    RpcCallback<AggregateResponse> done) {
    AggregateResponse response = null;
    InternalScanner scanner = null;
    PartialResultContext partialResultContext = new PartialResultContext();
    try {
      ColumnInterpreter<T, S, P, Q, R> ci = constructColumnInterpreterFromRequest(request);
      S sumVal = null;
      Long rowCountVal = 0L;
      Scan scan = ProtobufUtil.toScan(request.getScan());
      scanner = env.getRegion().getScanner(scan);
      byte[] colFamily = scan.getFamilies()[0];
      NavigableSet<byte[]> qualifiers = scan.getFamilyMap().get(colFamily);
      byte[] qualifier = null;
      if (qualifiers != null && !qualifiers.isEmpty()) {
        qualifier = qualifiers.pollFirst();
      }
      List<Cell> results = new ArrayList<>();
      boolean hasMoreRows = true;

      do {
        results.clear();
        if (shouldBreakForThrottling(request, scan, partialResultContext)) {
          break;
        }
        hasMoreRows = scanner.next(results);
        int listSize = results.size();
        for (int i = 0; i < listSize; i++) {
          sumVal =
            ci.add(sumVal, ci.castToReturnType(ci.getValue(colFamily, qualifier, results.get(i))));
        }
        rowCountVal++;
        postScanPartialResultUpdate(results, partialResultContext);
      } while (hasMoreRows);

      if (sumVal != null && !request.getClientSupportsPartialResult()) {
        AggregateResponse.Builder pair = AggregateResponse.newBuilder();
        ByteString first = ci.getProtoForPromotedType(sumVal).toByteString();
        pair.addFirstPart(first);
        ByteBuffer bb = ByteBuffer.allocate(8).putLong(rowCountVal);
        bb.rewind();
        pair.setSecondPart(ByteString.copyFrom(bb));
        response = pair.build();
      } else if (request.getClientSupportsPartialResult()) {
        AggregateResponse.Builder pair =
          responseBuilder(request, hasMoreRows, partialResultContext);
        if (sumVal != null) {
          ByteString first = ci.getProtoForPromotedType(sumVal).toByteString();
          pair.addFirstPart(first);
          ByteBuffer bb = ByteBuffer.allocate(8).putLong(rowCountVal);
          bb.rewind();
          pair.setSecondPart(ByteString.copyFrom(bb));
        }
        response = pair.build();
      }
    } catch (IOException e) {
      CoprocessorRpcUtils.setControllerException(controller, e);
    } finally {
      if (scanner != null) {
        IOUtils.closeQuietly(scanner);
      }
      closeQuota(partialResultContext);
    }
    done.run(response);
  }

  /**
   * Gives a Pair with first object a List containing Sum and sum of squares, and the second object
   * as row count. It is computed for a given combination of column qualifier and column family in
   * the given row range as defined in the Scan object. In its current implementation, it takes one
   * column family and one column qualifier (if provided). The idea is get the value of variance
   * first: the average of the squares less the square of the average a standard deviation is square
   * root of variance.
   */
  @Override
  public void getStd(RpcController controller, AggregateRequest request,
    RpcCallback<AggregateResponse> done) {
    InternalScanner scanner = null;
    AggregateResponse response = null;
    PartialResultContext partialResultContext = new PartialResultContext();
    try {
      ColumnInterpreter<T, S, P, Q, R> ci = constructColumnInterpreterFromRequest(request);
      S sumVal = null, sumSqVal = null, tempVal = null;
      long rowCountVal = 0L;
      Scan scan = ProtobufUtil.toScan(request.getScan());
      scanner = env.getRegion().getScanner(scan);
      byte[] colFamily = scan.getFamilies()[0];
      NavigableSet<byte[]> qualifiers = scan.getFamilyMap().get(colFamily);
      byte[] qualifier = null;
      if (qualifiers != null && !qualifiers.isEmpty()) {
        qualifier = qualifiers.pollFirst();
      }
      List<Cell> results = new ArrayList<>();

      boolean hasMoreRows = true;

      do {
        if (shouldBreakForThrottling(request, scan, partialResultContext)) {
          break;
        }
        tempVal = null;
        hasMoreRows = scanner.next(results);
        int listSize = results.size();
        for (int i = 0; i < listSize; i++) {
          tempVal =
            ci.add(tempVal, ci.castToReturnType(ci.getValue(colFamily, qualifier, results.get(i))));
        }
        postScanPartialResultUpdate(results, partialResultContext);
        results.clear();
        sumVal = ci.add(sumVal, tempVal);
        sumSqVal = ci.add(sumSqVal, ci.multiply(tempVal, tempVal));
        rowCountVal++;
      } while (hasMoreRows);

      if (sumVal != null && !request.getClientSupportsPartialResult()) {
        AggregateResponse.Builder pair = AggregateResponse.newBuilder();
        ByteString first_sumVal = ci.getProtoForPromotedType(sumVal).toByteString();
        ByteString first_sumSqVal = ci.getProtoForPromotedType(sumSqVal).toByteString();
        pair.addFirstPart(first_sumVal);
        pair.addFirstPart(first_sumSqVal);
        ByteBuffer bb = ByteBuffer.allocate(8).putLong(rowCountVal);
        bb.rewind();
        pair.setSecondPart(ByteString.copyFrom(bb));
        response = pair.build();
      } else if (request.getClientSupportsPartialResult()) {
        AggregateResponse.Builder pair =
          responseBuilder(request, hasMoreRows, partialResultContext);
        if (sumVal != null) {
          ByteString first_sumVal = ci.getProtoForPromotedType(sumVal).toByteString();
          ByteString first_sumSqVal = ci.getProtoForPromotedType(sumSqVal).toByteString();
          pair.addFirstPart(first_sumVal);
          pair.addFirstPart(first_sumSqVal);
          ByteBuffer bb = ByteBuffer.allocate(8).putLong(rowCountVal);
          bb.rewind();
          pair.setSecondPart(ByteString.copyFrom(bb));
        }
        response = pair.build();
      }
    } catch (IOException e) {
      CoprocessorRpcUtils.setControllerException(controller, e);
    } finally {
      if (scanner != null) {
        IOUtils.closeQuietly(scanner);
      }
      closeQuota(partialResultContext);
    }
    done.run(response);
  }

  /**
   * Gives a List containing sum of values and sum of weights. It is computed for the combination of
   * column family and column qualifier(s) in the given row range as defined in the Scan object. In
   * its current implementation, it takes one column family and two column qualifiers. The first
   * qualifier is for values column and the second qualifier (optional) is for weight column.
   */
  @Override
  public void getMedian(RpcController controller, AggregateRequest request,
    RpcCallback<AggregateResponse> done) {
    AggregateResponse response = null;
    InternalScanner scanner = null;
    PartialResultContext partialResultContext = new PartialResultContext();
    try {
      ColumnInterpreter<T, S, P, Q, R> ci = constructColumnInterpreterFromRequest(request);
      S sumVal = null, sumWeights = null, tempVal = null, tempWeight = null;
      Scan scan = ProtobufUtil.toScan(request.getScan());
      scanner = env.getRegion().getScanner(scan);
      byte[] colFamily = scan.getFamilies()[0];
      NavigableSet<byte[]> qualifiers = scan.getFamilyMap().get(colFamily);
      byte[] valQualifier = null, weightQualifier = null;
      if (qualifiers != null && !qualifiers.isEmpty()) {
        valQualifier = qualifiers.pollFirst();
        // if weighted median is requested, get qualifier for the weight column
        weightQualifier = qualifiers.pollLast();
      }
      List<Cell> results = new ArrayList<>();

      boolean hasMoreRows = true;
      do {
        if (shouldBreakForThrottling(request, scan, partialResultContext)) {
          break;
        }
        tempVal = null;
        tempWeight = null;
        hasMoreRows = scanner.next(results);
        int listSize = results.size();
        for (int i = 0; i < listSize; i++) {
          Cell kv = results.get(i);
          tempVal = ci.add(tempVal, ci.castToReturnType(ci.getValue(colFamily, valQualifier, kv)));
          if (weightQualifier != null) {
            tempWeight =
              ci.add(tempWeight, ci.castToReturnType(ci.getValue(colFamily, weightQualifier, kv)));
          }
        }
        postScanPartialResultUpdate(results, partialResultContext);
        results.clear();
        sumVal = ci.add(sumVal, tempVal);
        sumWeights = ci.add(sumWeights, tempWeight);
      } while (hasMoreRows);
      if (sumVal != null && !request.getClientSupportsPartialResult()) {
        AggregateResponse.Builder pair = AggregateResponse.newBuilder();
        ByteString first_sumVal = ci.getProtoForPromotedType(sumVal).toByteString();
        S s = sumWeights == null ? ci.castToReturnType(ci.getMinValue()) : sumWeights;
        ByteString first_sumWeights = ci.getProtoForPromotedType(s).toByteString();
        pair.addFirstPart(first_sumVal);
        pair.addFirstPart(first_sumWeights);
        response = pair.build();
      } else if (request.getClientSupportsPartialResult()) {
        AggregateResponse.Builder pair =
          responseBuilder(request, hasMoreRows, partialResultContext);
        if (sumVal != null) {
          ByteString first_sumVal = ci.getProtoForPromotedType(sumVal).toByteString();
          S s = sumWeights == null ? ci.castToReturnType(ci.getMinValue()) : sumWeights;
          ByteString first_sumWeights = ci.getProtoForPromotedType(s).toByteString();
          pair.addFirstPart(first_sumVal);
          pair.addFirstPart(first_sumWeights);
        }
        response = pair.build();
      }
    } catch (IOException e) {
      CoprocessorRpcUtils.setControllerException(controller, e);
    } finally {
      if (scanner != null) {
        IOUtils.closeQuietly(scanner);
      }
      closeQuota(partialResultContext);
    }
    done.run(response);
  }

  private final static class PartialResultContext {
    private OperationQuota quota = null;
    private long waitIntervalMs = 0;
    private byte[] lastRowSuccessfullyProcessedArray = null;
    private int lastRowSuccessfullyProcessedOffset = 0;
    private int lastRowSuccessfullyProcessedLength = 0;
    private long previousReadConsumed = 0;
    private long previousReadConsumedDifference = 0;
  }

  private boolean shouldBreakForThrottling(AggregateRequest request, Scan scan,
    PartialResultContext context) throws IOException {
    if (request.getClientSupportsPartialResult()) {
      long maxBlockBytesScanned;
      if (context.quota == null) {
        maxBlockBytesScanned = Long.MAX_VALUE;
      } else {
        maxBlockBytesScanned = context.quota.getMaxResultSize();
      }
      try {
        context.quota =
          env.checkScanQuota(scan, maxBlockBytesScanned, context.previousReadConsumedDifference);
      } catch (RpcThrottlingException e) {
        if (log.isDebugEnabled()) {
          log.debug("Ending early for throttling for region {}",
            env.getRegion().getRegionInfo().getRegionNameAsString());
        }
        context.waitIntervalMs = e.getWaitInterval();
        return true;
      }
    }
    return false;
  }

  private void postScanPartialResultUpdate(List<Cell> results, PartialResultContext context) {
    if (context.quota != null) {
      context.quota.addScanResultCells(results);
    }
    if (!results.isEmpty()) {
      Cell result = results.get(results.size() - 1);
      context.lastRowSuccessfullyProcessedArray = result.getRowArray();
      context.lastRowSuccessfullyProcessedOffset = result.getRowOffset();
      context.lastRowSuccessfullyProcessedLength = result.getRowLength();
    }
  }

  @Nullable
  private <ACC, RES extends Message> AggregateResponse singlePartResponse(AggregateRequest request,
    boolean hasMoreRows, PartialResultContext partialResultContext, ACC acc,
    Function<ACC, RES> toRes) {
    AggregateResponse response = null;
    if (acc != null && !request.getClientSupportsPartialResult()) {
      ByteString first = toRes.apply(acc).toByteString();
      response = AggregateResponse.newBuilder().addFirstPart(first).build();
    } else if (request.getClientSupportsPartialResult()) {
      AggregateResponse.Builder responseBuilder =
        responseBuilder(request, hasMoreRows, partialResultContext);
      if (acc != null) {
        responseBuilder.addFirstPart(toRes.apply(acc).toByteString());
      }
      response = responseBuilder.build();
    }
    return response;
  }

  private AggregateResponse.Builder responseBuilder(AggregateRequest request, boolean hasMoreRows,
    PartialResultContext context) {
    AggregateResponse.Builder builder = AggregateResponse.newBuilder();
    if (request.getClientSupportsPartialResult() && hasMoreRows) {
      if (context.lastRowSuccessfullyProcessedArray != null) {
        byte[] lastRowSuccessfullyProcessed = Arrays.copyOfRange(
          context.lastRowSuccessfullyProcessedArray, context.lastRowSuccessfullyProcessedOffset,
          context.lastRowSuccessfullyProcessedOffset + context.lastRowSuccessfullyProcessedLength);
        builder.setNextChunkStartRow(ByteString.copyFrom(
          ClientUtil.calculateTheClosestNextRowKeyForPrefix(lastRowSuccessfullyProcessed)));
      } else {
        builder.setNextChunkStartRow(request.getScan().getStartRow());
      }
      builder.setWaitIntervalMs(context.waitIntervalMs);
    }
    return builder;
  }

  @SuppressWarnings("unchecked")
  // Used server-side too by Aggregation Coprocesor Endpoint. Undo this interdependence. TODO.
  ColumnInterpreter<T, S, P, Q, R> constructColumnInterpreterFromRequest(AggregateRequest request)
    throws IOException {
    String className = request.getInterpreterClassName();
    try {
      ColumnInterpreter<T, S, P, Q, R> ci;
      Class<?> cls = Class.forName(className);
      ci = (ColumnInterpreter<T, S, P, Q, R>) cls.getDeclaredConstructor().newInstance();

      if (request.hasInterpreterSpecificBytes()) {
        ByteString b = request.getInterpreterSpecificBytes();
        P initMsg = getParsedGenericInstance(ci.getClass(), 2, b);
        ci.initialize(initMsg);
      }
      return ci;
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException
      | NoSuchMethodException | InvocationTargetException e) {
      throw new IOException(e);
    }
  }

  @Override
  public Iterable<Service> getServices() {
    return Collections.singleton(this);
  }

  /**
   * Stores a reference to the coprocessor environment provided by the
   * {@link org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost} from the region where this
   * coprocessor is loaded. Since this is a coprocessor endpoint, it always expects to be loaded on
   * a table region, so always expects this to be an instance of
   * {@link RegionCoprocessorEnvironment}.
   * @param env the environment provided by the coprocessor host
   * @throws IOException if the provided environment is not an instance of
   *                     {@code RegionCoprocessorEnvironment}
   */
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

  private void closeQuota(PartialResultContext context) {
    if (context != null && context.quota != null) {
      context.quota.close();
      long readConsumed = context.quota.getReadConsumed();
      context.previousReadConsumedDifference = readConsumed - context.previousReadConsumed;
      context.previousReadConsumed = readConsumed;
    }
  }

}
