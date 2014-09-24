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
package org.apache.hadoop.hbase.client.coprocessor;

import java.io.IOException;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.EmptyMsg;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.DoubleMsg;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * a concrete column interpreter implementation. The cell value is a Double value
 * and its promoted data type is also a Double value. For computing aggregation
 * function, this class is used to find the datatype of the cell value. Client
 * is supposed to instantiate it and passed along as a parameter. See
 * TestDoubleColumnInterpreter methods for its sample usage.
 * Its methods handle null arguments gracefully. 
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class DoubleColumnInterpreter extends ColumnInterpreter<Double, Double, 
      EmptyMsg, DoubleMsg, DoubleMsg>{
 
  @Override
  public Double getValue(byte[] colFamily, byte[] colQualifier, Cell c)
      throws IOException {
    if (c == null || c.getValueLength() != Bytes.SIZEOF_DOUBLE)
      return null;
    return Bytes.toDouble(c.getValueArray(), c.getValueOffset());
  }

  @Override
  public Double add(Double d1, Double d2) {
    if (d1 == null || d2 == null) {
      return (d1 == null) ? d2 : d1; 
    }
    return d1 + d2;
  }

  @Override
  public int compare(final Double d1, final Double d2) {
    if (d1 == null ^ d2 == null) {
      return d1 == null ? -1 : 1; // either of one is null.
    } else if (d1 == null)
      return 0; // both are null
    return d1.compareTo(d2); // natural ordering.
  }

  @Override
  public Double getMaxValue() {
    return Double.MAX_VALUE;
  }

  @Override
  public Double increment(Double o) {
    return o == null ? null : (o + 1.00d);
  }

  @Override
  public Double multiply(Double d1, Double d2) {
    return (d1 == null || d2 == null) ? null : d1 * d2;
  }

  @Override
  public Double getMinValue() {
    return Double.MIN_VALUE;
  }

  @Override
  public double divideForAvg(Double d1, Long l2) {
    return (l2 == null || d1 == null) ? Double.NaN : (d1.doubleValue() / l2
        .doubleValue());
  }

  @Override
  public Double castToReturnType(Double o) {
    return o;
  }

  @Override
  public Double castToCellType(Double d) {
    return d;
  }

  @Override
  public EmptyMsg getRequestData() {
    return EmptyMsg.getDefaultInstance();
  }

  @Override
  public void initialize(EmptyMsg msg) {
    //nothing 
  }

  @Override
  public DoubleMsg getProtoForCellType(Double t) {
    DoubleMsg.Builder builder = DoubleMsg.newBuilder();
    return builder.setDoubleMsg(t).build();
  }

  @Override
  public DoubleMsg getProtoForPromotedType(Double s) {
    DoubleMsg.Builder builder = DoubleMsg.newBuilder();
    return builder.setDoubleMsg(s).build();
  }

  @Override
  public Double getPromotedValueFromProto(DoubleMsg r) {
    return r.getDoubleMsg();
  }

  @Override
  public Double getCellValueFromProto(DoubleMsg q) {
    return q.getDoubleMsg();
  }
}
