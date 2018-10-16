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
import java.math.BigDecimal;
import java.math.RoundingMode;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.BigDecimalMsg;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.EmptyMsg;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * ColumnInterpreter for doing Aggregation's with BigDecimal columns. This class
 * is required at the RegionServer also.
 *
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public class BigDecimalColumnInterpreter extends ColumnInterpreter<BigDecimal, BigDecimal,
  EmptyMsg, BigDecimalMsg, BigDecimalMsg> {

  @Override
  public BigDecimal getValue(byte[] colFamily, byte[] colQualifier, Cell kv)
      throws IOException {
    if (kv == null || CellUtil.cloneValue(kv) == null) {
      return null;
    }
    return PrivateCellUtil.getValueAsBigDecimal(kv).setScale(2, RoundingMode.HALF_EVEN);
  }

  @Override
  public BigDecimal add(BigDecimal bd1, BigDecimal bd2) {
    if (bd1 == null ^ bd2 == null) {
      return (bd1 == null) ? bd2 : bd1; // either of one is null.
    }
    if (bd1 == null) {
      return null;
    }
    return bd1.add(bd2);
  }

  @Override
  public int compare(final BigDecimal bd1, final BigDecimal bd2) {
    if (bd1 == null ^ bd2 == null) {
      return bd1 == null ? -1 : 1; // either of one is null.
    }
    if (bd1 == null) {
      return 0; // both are null
    }
    return bd1.compareTo(bd2); // natural ordering.
  }

  @Override
  public BigDecimal getMaxValue() {
    return BigDecimal.valueOf(Double.MAX_VALUE);
  }

  @Override
  public BigDecimal increment(BigDecimal bd) {
    return bd == null ? null : (bd.add(BigDecimal.ONE));
  }

  @Override
  public BigDecimal multiply(BigDecimal bd1, BigDecimal bd2) {
    return (bd1 == null || bd2 == null) ? null : bd1.multiply(bd2)
        .setScale(2,RoundingMode.HALF_EVEN);
  }

  @Override
  public BigDecimal getMinValue() {
    return BigDecimal.valueOf(Double.MIN_VALUE);
  }

  @Override
  public double divideForAvg(BigDecimal bd1, Long l2) {
    return (l2 == null || bd1 == null) ? Double.NaN : (bd1.doubleValue() / l2
        .doubleValue());
  }

  @Override
  public BigDecimal castToReturnType(BigDecimal bd) {
    return bd;
  }

  @Override
  public BigDecimal castToCellType(BigDecimal bd) {
    return bd;
  }

  @Override
  public EmptyMsg getRequestData() {
    return EmptyMsg.getDefaultInstance();
  }

  @Override
  public void initialize(EmptyMsg msg) {
    //nothing
  }

  private BigDecimalMsg getProtoForType(BigDecimal t) {
    BigDecimalMsg.Builder builder = BigDecimalMsg.newBuilder();
    return builder.setBigdecimalMsg(ByteStringer.wrap(Bytes.toBytes(t))).build();
  }

  @Override
  public BigDecimalMsg getProtoForCellType(BigDecimal t) {
    return getProtoForType(t);
  }

  @Override
  public BigDecimalMsg getProtoForPromotedType(BigDecimal s) {
    return getProtoForType(s);
  }

  @Override
  public BigDecimal getPromotedValueFromProto(BigDecimalMsg r) {
    return Bytes.toBigDecimal(r.getBigdecimalMsg().toByteArray());
  }

  @Override
  public BigDecimal getCellValueFromProto(BigDecimalMsg q) {
    return Bytes.toBigDecimal(q.getBigdecimalMsg().toByteArray());
  }
}
