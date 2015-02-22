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
package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.AggregateProtos.AggregateRequest;
import org.apache.hadoop.hbase.protobuf.generated.AggregateProtos.AggregateResponse;
import org.apache.hadoop.hbase.protobuf.generated.AggregateProtos.AggregateService;
import org.apache.hadoop.hbase.regionserver.InternalScanner;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

/**
 * A concrete AggregateProtocol implementation. Its system level coprocessor
 * that computes the aggregate function at a region level.
 * {@link ColumnInterpreter} is used to interpret column value. This class is
 * parameterized with the following (these are the types with which the {@link ColumnInterpreter}
 * is parameterized, and for more description on these, refer to {@link ColumnInterpreter}):
 * @param <T> Cell value data type
 * @param <S> Promoted data type
 * @param <P> PB message that is used to transport initializer specific bytes
 * @param <Q> PB message that is used to transport Cell (<T>) instance
 * @param <R> PB message that is used to transport Promoted (<S>) instance
 */
@InterfaceAudience.Private
public class AggregateImplementation<T, S, P extends Message, Q extends Message, R extends Message> 
extends AggregateService implements CoprocessorService, Coprocessor {
  protected static final Log log = LogFactory.getLog(AggregateImplementation.class);
  private RegionCoprocessorEnvironment env;

  /**
   * Gives the maximum for a given combination of column qualifier and column
   * family, in the given row range as defined in the Scan object. In its
   * current implementation, it takes one column family and one column qualifier
   * (if provided). In case of null column qualifier, maximum value for the
   * entire column family will be returned.
   */
  @Override
  public void getMax(RpcController controller, AggregateRequest request,
      RpcCallback<AggregateResponse> done) {
    InternalScanner scanner = null;
    AggregateResponse response = null;
    T max = null;
    try {
      ColumnInterpreter<T, S, P, Q, R> ci = constructColumnInterpreterFromRequest(request);
      T temp;
      Scan scan = ProtobufUtil.toScan(request.getScan());
      scanner = env.getRegion().getScanner(scan);
      List<Cell> results = new ArrayList<Cell>();
      byte[] colFamily = scan.getFamilies()[0];
      NavigableSet<byte[]> qualifiers = scan.getFamilyMap().get(colFamily);
      byte[] qualifier = null;
      if (qualifiers != null && !qualifiers.isEmpty()) {
        qualifier = qualifiers.pollFirst();
      }
      // qualifier can be null.
      boolean hasMoreRows = false;
      do {
        hasMoreRows = scanner.next(results);
        int listSize = results.size();
        for (int i = 0; i < listSize; i++) {
          temp = ci.getValue(colFamily, qualifier, results.get(i));
          max = (max == null || (temp != null && ci.compare(temp, max) > 0)) ? temp : max;
        }
        results.clear();
      } while (hasMoreRows);
      if (max != null) {
        AggregateResponse.Builder builder = AggregateResponse.newBuilder();
        builder.addFirstPart(ci.getProtoForCellType(max).toByteString());
        response = builder.build();
      }
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException ignored) {}
      }
    }
    log.info("Maximum from this region is "
        + env.getRegion().getRegionNameAsString() + ": " + max);
    done.run(response);
  }

  /**
   * Gives the minimum for a given combination of column qualifier and column
   * family, in the given row range as defined in the Scan object. In its
   * current implementation, it takes one column family and one column qualifier
   * (if provided). In case of null column qualifier, minimum value for the
   * entire column family will be returned.
   */
  @Override
  public void getMin(RpcController controller, AggregateRequest request,
      RpcCallback<AggregateResponse> done) {
    AggregateResponse response = null;
    InternalScanner scanner = null;
    T min = null;
    try {
      ColumnInterpreter<T, S, P, Q, R> ci = constructColumnInterpreterFromRequest(request);
      T temp;
      Scan scan = ProtobufUtil.toScan(request.getScan());
      scanner = env.getRegion().getScanner(scan);
      List<Cell> results = new ArrayList<Cell>();
      byte[] colFamily = scan.getFamilies()[0];
      NavigableSet<byte[]> qualifiers = scan.getFamilyMap().get(colFamily);
      byte[] qualifier = null;
      if (qualifiers != null && !qualifiers.isEmpty()) {
        qualifier = qualifiers.pollFirst();
      }
      boolean hasMoreRows = false;
      do {
        hasMoreRows = scanner.next(results);
        int listSize = results.size();
        for (int i = 0; i < listSize; i++) {
          temp = ci.getValue(colFamily, qualifier, results.get(i));
          min = (min == null || (temp != null && ci.compare(temp, min) < 0)) ? temp : min;
        }
        results.clear();
      } while (hasMoreRows);
      if (min != null) {
        response = AggregateResponse.newBuilder().addFirstPart( 
          ci.getProtoForCellType(min).toByteString()).build();
      }
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException ignored) {}
      }
    }
    log.info("Minimum from this region is "
        + env.getRegion().getRegionNameAsString() + ": " + min);
    done.run(response);
  }

  /**
   * Gives the sum for a given combination of column qualifier and column
   * family, in the given row range as defined in the Scan object. In its
   * current implementation, it takes one column family and one column qualifier
   * (if provided). In case of null column qualifier, sum for the entire column
   * family will be returned.
   */
  @Override
  public void getSum(RpcController controller, AggregateRequest request,
      RpcCallback<AggregateResponse> done) {
    AggregateResponse response = null;
    InternalScanner scanner = null;
    long sum = 0l;
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
      List<Cell> results = new ArrayList<Cell>();
      boolean hasMoreRows = false;
      do {
        hasMoreRows = scanner.next(results);
        int listSize = results.size();
        for (int i = 0; i < listSize; i++) {
          temp = ci.getValue(colFamily, qualifier, results.get(i));
          if (temp != null)
            sumVal = ci.add(sumVal, ci.castToReturnType(temp));
        }
        results.clear();
      } while (hasMoreRows);
      if (sumVal != null) {
        response = AggregateResponse.newBuilder().addFirstPart( 
          ci.getProtoForPromotedType(sumVal).toByteString()).build();
      }
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException ignored) {}
      }
    }
    log.debug("Sum from this region is "
        + env.getRegion().getRegionNameAsString() + ": " + sum);
    done.run(response);
  }

  /**
   * Gives the row count for the given column family and column qualifier, in
   * the given row range as defined in the Scan object.
   * @throws IOException
   */
  @Override
  public void getRowNum(RpcController controller, AggregateRequest request,
      RpcCallback<AggregateResponse> done) {
    AggregateResponse response = null;
    long counter = 0l;
    List<Cell> results = new ArrayList<Cell>();
    InternalScanner scanner = null;
    try {
      Scan scan = ProtobufUtil.toScan(request.getScan());
      byte[][] colFamilies = scan.getFamilies();
      byte[] colFamily = colFamilies != null ? colFamilies[0] : null;
      NavigableSet<byte[]> qualifiers = colFamilies != null ?
          scan.getFamilyMap().get(colFamily) : null;
      byte[] qualifier = null;
      if (qualifiers != null && !qualifiers.isEmpty()) {
        qualifier = qualifiers.pollFirst();
      }
      if (scan.getFilter() == null && qualifier == null)
        scan.setFilter(new FirstKeyOnlyFilter());
      scanner = env.getRegion().getScanner(scan);
      boolean hasMoreRows = false;
      do {
        hasMoreRows = scanner.next(results);
        if (results.size() > 0) {
          counter++;
        }
        results.clear();
      } while (hasMoreRows);
      ByteBuffer bb = ByteBuffer.allocate(8).putLong(counter);
      bb.rewind();
      response = AggregateResponse.newBuilder().addFirstPart( 
          ByteString.copyFrom(bb)).build();
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException ignored) {}
      }
    }
    log.info("Row counter from this region is "
        + env.getRegion().getRegionNameAsString() + ": " + counter);
    done.run(response);
  }

  /**
   * Gives a Pair with first object as Sum and second object as row count,
   * computed for a given combination of column qualifier and column family in
   * the given row range as defined in the Scan object. In its current
   * implementation, it takes one column family and one column qualifier (if
   * provided). In case of null column qualifier, an aggregate sum over all the
   * entire column family will be returned.
   * <p>
   * The average is computed in
   * AggregationClient#avg(byte[], ColumnInterpreter, Scan) by
   * processing results from all regions, so its "ok" to pass sum and a Long
   * type.
   */
  @Override
  public void getAvg(RpcController controller, AggregateRequest request,
      RpcCallback<AggregateResponse> done) {
    AggregateResponse response = null;
    InternalScanner scanner = null;
    try {
      ColumnInterpreter<T, S, P, Q, R> ci = constructColumnInterpreterFromRequest(request);
      S sumVal = null;
      Long rowCountVal = 0l;
      Scan scan = ProtobufUtil.toScan(request.getScan());
      scanner = env.getRegion().getScanner(scan);
      byte[] colFamily = scan.getFamilies()[0];
      NavigableSet<byte[]> qualifiers = scan.getFamilyMap().get(colFamily);
      byte[] qualifier = null;
      if (qualifiers != null && !qualifiers.isEmpty()) {
        qualifier = qualifiers.pollFirst();
      }
      List<Cell> results = new ArrayList<Cell>();
      boolean hasMoreRows = false;
    
      do {
        results.clear();
        hasMoreRows = scanner.next(results);
        int listSize = results.size();
        for (int i = 0; i < listSize; i++) {
          sumVal = ci.add(sumVal, ci.castToReturnType(ci.getValue(colFamily,
              qualifier, results.get(i))));
        }
        rowCountVal++;
      } while (hasMoreRows);
      if (sumVal != null) {
        ByteString first = ci.getProtoForPromotedType(sumVal).toByteString();
        AggregateResponse.Builder pair = AggregateResponse.newBuilder();
        pair.addFirstPart(first);
        ByteBuffer bb = ByteBuffer.allocate(8).putLong(rowCountVal);
        bb.rewind();
        pair.setSecondPart(ByteString.copyFrom(bb));
        response = pair.build();
      }
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException ignored) {}
      }
    }
    done.run(response);
  }

  /**
   * Gives a Pair with first object a List containing Sum and sum of squares,
   * and the second object as row count. It is computed for a given combination of
   * column qualifier and column family in the given row range as defined in the
   * Scan object. In its current implementation, it takes one column family and
   * one column qualifier (if provided). The idea is get the value of variance first:
   * the average of the squares less the square of the average a standard
   * deviation is square root of variance.
   */
  @Override
  public void getStd(RpcController controller, AggregateRequest request,
      RpcCallback<AggregateResponse> done) {
    InternalScanner scanner = null;
    AggregateResponse response = null;
    try {
      ColumnInterpreter<T, S, P, Q, R> ci = constructColumnInterpreterFromRequest(request);
      S sumVal = null, sumSqVal = null, tempVal = null;
      long rowCountVal = 0l;
      Scan scan = ProtobufUtil.toScan(request.getScan());
      scanner = env.getRegion().getScanner(scan);
      byte[] colFamily = scan.getFamilies()[0];
      NavigableSet<byte[]> qualifiers = scan.getFamilyMap().get(colFamily);
      byte[] qualifier = null;
      if (qualifiers != null && !qualifiers.isEmpty()) {
        qualifier = qualifiers.pollFirst();
      }
      List<Cell> results = new ArrayList<Cell>();

      boolean hasMoreRows = false;
    
      do {
        tempVal = null;
        hasMoreRows = scanner.next(results);
        int listSize = results.size();
        for (int i = 0; i < listSize; i++) {
          tempVal = ci.add(tempVal, ci.castToReturnType(ci.getValue(colFamily,
              qualifier, results.get(i))));
        }
        results.clear();
        sumVal = ci.add(sumVal, tempVal);
        sumSqVal = ci.add(sumSqVal, ci.multiply(tempVal, tempVal));
        rowCountVal++;
      } while (hasMoreRows);
      if (sumVal != null) {
        ByteString first_sumVal = ci.getProtoForPromotedType(sumVal).toByteString();
        ByteString first_sumSqVal = ci.getProtoForPromotedType(sumSqVal).toByteString();
        AggregateResponse.Builder pair = AggregateResponse.newBuilder();
        pair.addFirstPart(first_sumVal);
        pair.addFirstPart(first_sumSqVal);
        ByteBuffer bb = ByteBuffer.allocate(8).putLong(rowCountVal);
        bb.rewind();
        pair.setSecondPart(ByteString.copyFrom(bb));
        response = pair.build();
      }
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException ignored) {}
      }
    }
    done.run(response);
  }

  /**
   * Gives a List containing sum of values and sum of weights.
   * It is computed for the combination of column
   * family and column qualifier(s) in the given row range as defined in the
   * Scan object. In its current implementation, it takes one column family and
   * two column qualifiers. The first qualifier is for values column and 
   * the second qualifier (optional) is for weight column.
   */
  @Override
  public void getMedian(RpcController controller, AggregateRequest request,
      RpcCallback<AggregateResponse> done) {
    AggregateResponse response = null;
    InternalScanner scanner = null;
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
      List<Cell> results = new ArrayList<Cell>();

      boolean hasMoreRows = false;
    
      do {
        tempVal = null;
        tempWeight = null;
        hasMoreRows = scanner.next(results);
        int listSize = results.size();
        for (int i = 0; i < listSize; i++) {
          Cell kv = results.get(i);
          tempVal = ci.add(tempVal, ci.castToReturnType(ci.getValue(colFamily,
              valQualifier, kv)));
          if (weightQualifier != null) {
            tempWeight = ci.add(tempWeight,
                ci.castToReturnType(ci.getValue(colFamily, weightQualifier, kv)));
          }
        }
        results.clear();
        sumVal = ci.add(sumVal, tempVal);
        sumWeights = ci.add(sumWeights, tempWeight);
      } while (hasMoreRows);
      ByteString first_sumVal = ci.getProtoForPromotedType(sumVal).toByteString();
      S s = sumWeights == null ? ci.castToReturnType(ci.getMinValue()) : sumWeights;
      ByteString first_sumWeights = ci.getProtoForPromotedType(s).toByteString();
      AggregateResponse.Builder pair = AggregateResponse.newBuilder();
      pair.addFirstPart(first_sumVal);
      pair.addFirstPart(first_sumWeights); 
      response = pair.build();
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException ignored) {}
      }
    }
    done.run(response);
  }

  @SuppressWarnings("unchecked")
  ColumnInterpreter<T,S,P,Q,R> constructColumnInterpreterFromRequest(
      AggregateRequest request) throws IOException {
    String className = request.getInterpreterClassName();
    Class<?> cls;
    try {
      cls = Class.forName(className);
      ColumnInterpreter<T,S,P,Q,R> ci = (ColumnInterpreter<T, S, P, Q, R>) cls.newInstance();
      if (request.hasInterpreterSpecificBytes()) {
        ByteString b = request.getInterpreterSpecificBytes();
        P initMsg = ProtobufUtil.getParsedGenericInstance(ci.getClass(), 2, b);
        ci.initialize(initMsg);
      }
      return ci;
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    } catch (InstantiationException e) {
      throw new IOException(e);
    } catch (IllegalAccessException e) {
      throw new IOException(e);
    }
  }

  @Override
  public Service getService() {
    return this;
  }

  /**
   * Stores a reference to the coprocessor environment provided by the
   * {@link org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost} from the region where this
   * coprocessor is loaded.  Since this is a coprocessor endpoint, it always expects to be loaded
   * on a table region, so always expects this to be an instance of
   * {@link RegionCoprocessorEnvironment}.
   * @param env the environment provided by the coprocessor host
   * @throws IOException if the provided environment is not an instance of
   * {@code RegionCoprocessorEnvironment}
   */
  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    if (env instanceof RegionCoprocessorEnvironment) {
      this.env = (RegionCoprocessorEnvironment)env;
    } else {
      throw new CoprocessorException("Must be loaded on a table region!");
    }
  }

  @Override
  public void stop(CoprocessorEnvironment env) throws IOException {
    // nothing to do
  }
  
}
