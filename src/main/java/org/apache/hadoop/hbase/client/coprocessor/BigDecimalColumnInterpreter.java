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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * ColumnInterpreter for doing Aggregation's with BigDecimal columns. 
 * This class is required at the RegionServer also.
 *
 */
public class BigDecimalColumnInterpreter implements ColumnInterpreter<BigDecimal, BigDecimal> {
  private static final Log log = LogFactory.getLog(BigDecimalColumnInterpreter.class);

  @Override
  public void readFields(DataInput arg0) throws IOException {
  }

  @Override
  public void write(DataOutput arg0) throws IOException {
  }

  @Override
  public BigDecimal getValue(byte[] family, byte[] qualifier, KeyValue kv)
      throws IOException {
    if ((kv == null || kv.getValue() == null)) return null;
    return Bytes.toBigDecimal(kv.getValue()).setScale(2, RoundingMode.HALF_EVEN);
  }

  @Override
  public BigDecimal add(BigDecimal val1, BigDecimal val2) {
    if ((((val1 == null) ? 1 : 0) ^ ((val2 == null) ? 1 : 0)) != 0) {
      return ((val1 == null) ? val2 : val1);
    }
    if (val1 == null) return null;
    return val1.add(val2).setScale(2, RoundingMode.HALF_EVEN);
  }

  @Override
  public BigDecimal getMaxValue() {
    return BigDecimal.valueOf(Double.MAX_VALUE);
  }

  @Override
  public BigDecimal getMinValue() {
    return BigDecimal.valueOf(Double.MIN_VALUE);
  }

  @Override
  public BigDecimal multiply(BigDecimal val1, BigDecimal val2) {
    return (((val1 == null) || (val2 == null)) ? null : val1.multiply(val2).setScale(2,
      RoundingMode.HALF_EVEN));
  }

  @Override
  public BigDecimal increment(BigDecimal val) {
    return ((val == null) ? null : val.add(BigDecimal.ONE));
  }

  @Override
  public BigDecimal castToReturnType(BigDecimal val) {
    return val;
  }

  @Override
  public int compare(BigDecimal val1, BigDecimal val2) {
    if ((((val1 == null) ? 1 : 0) ^ ((val2 == null) ? 1 : 0)) != 0) {
      return ((val1 == null) ? -1 : 1);
    }
    if (val1 == null) return 0;
    return val1.compareTo(val2);
  }

  @Override
  public double divideForAvg(BigDecimal val1, Long paramLong) {
    return (((paramLong == null) || (val1 == null)) ? (Double.NaN) :
      val1.doubleValue() / paramLong.doubleValue());
  }
}
