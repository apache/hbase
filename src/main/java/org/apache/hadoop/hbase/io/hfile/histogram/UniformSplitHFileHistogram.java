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
package org.apache.hadoop.hbase.io.hfile.histogram;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Uniform split histogram splits the range uniformly while creating the
 * Histogram i.e. if we split range [a00, z00] into 25 parts, we should get
 * [a00, b00], [b00, c00], ... , [y00, z00]
 *
 * This layer is provided to be able to easily swap other underlyingHistogram
 * implementations in the future.
 */
public class UniformSplitHFileHistogram implements HFileHistogram {
  protected NumericHistogram underlyingHistogram;
  // TODO manukranthk : make this configurable.
  int padding = 8;

  public UniformSplitHFileHistogram(int binCount) {
    this.underlyingHistogram = new HiveBasedNumericHistogram(
        getMinusInfinity(), getInfinity());
    this.underlyingHistogram.allocate(binCount);
  }

  private UniformSplitHFileHistogram(List<Bucket> buckets) {
    List<NumericHistogram.Bucket> nbuckets = Lists.newArrayList();
    for (Bucket b : buckets) {
      nbuckets.add(this.getFromHFileHistogramBucket(b));
    }
    this.underlyingHistogram = HiveBasedNumericHistogram.getHistogram(nbuckets);
  }

  @Override
  public void add(KeyValue kv) {
    double val = convertBytesToDouble(kv.getRow());
    underlyingHistogram.add(val);
  }

  private double getInfinity() {
    return new BigInteger(getInfinityArr()).doubleValue();
  }

  /**
   * This returns the maximum number that we can represent using padding bytes.
   * Returns {0x00, 0xff, 0xff .... 0xff }
   *                <----  padding  ---->
   * @return
   */
  private byte[] getInfinityArr() {
    byte[] row = new byte[1];
    row[0] = (byte) 0;
    return Bytes.appendToTail(row, padding, (byte)0xFF);
  }

  private double getMinusInfinity() {
    return 0.0;
  }

  /**
   * Bytes are are sorted lexicographically, so for the purposes of
   * HFileHistogram, we need to convert a byte[] to a double so that it still
   * compares correctly.
   *
   * We initially take the first 'padding' amount of bytes and convert the bytes
   * into a BigInteger assuming the byte[] was in 2's complement representation
   *
   * We will add an extra 0 at the start so that we don't have to deal with -ve
   * numbers.
   *
   * @param row
   * @return
   */
  protected double convertBytesToDouble(byte[] row) {
    byte[] tmpRow = Bytes.head(row, Math.min(row.length, padding));
    byte[] newRow = Bytes.padTail(tmpRow, padding - tmpRow.length);
    // To avoid messing with 2's complement.
    newRow = Bytes.padHead(newRow, 1);
    return new BigInteger(newRow).doubleValue();
  }

  /**
   * Double is converted to Bytes in a similar manner.
   *
   * @param d
   * @return
   */
  protected byte[] convertDoubleToBytes(double d) {
    BigDecimal tmpDecimal = new BigDecimal(d);
    BigInteger tmp = tmpDecimal.toBigInteger();
    byte[] arr = tmp.toByteArray();
    if (arr[0] == 0) {
      // to represent {0xff, 0xff}, big integer uses {0x00, 0xff, 0xff}
      // due to the one's compliment representation.
      Preconditions.checkArgument(arr.length == 1 || arr[1] != 0);
      arr = Bytes.tail(arr, arr.length - 1);
    }
    if (arr.length > padding) {
      // Can happen due to loose precision guarentee in double.
      // while doing the conversion,
      // {0x00, 0xff, ... , 0xff, 0xff}=>double=>{0x01, 0x00, ... , 0x00, 0x00}
      // might happen.
      arr = Bytes.tail(getInfinityArr(), padding);
    }
    return Bytes.padHead(arr, padding - arr.length);
  }

  @Override
  public List<Bucket> getUniformBuckets() {
    List<NumericHistogram.Bucket> buckets = this.underlyingHistogram
        .getUniformBuckets();
    List<Bucket> ret = Lists.newArrayList();
    for (NumericHistogram.Bucket b : buckets) {
      ret.add(getFromNumericHistogramBucket(b));
    }
    return ret;
  }

  public static HFileHistogram getHistogram(List<Bucket> buckets) {
    HFileHistogram ret = new UniformSplitHFileHistogram(buckets);
    return ret;
  }

  private NumericHistogram.Bucket getFromHFileHistogramBucket(
      HFileHistogram.Bucket bucket) {
    NumericHistogram.Bucket b = (new NumericHistogram.Bucket.Builder())
        .setStartRow(convertBytesToDouble(bucket.getStartRow()))
        .setEndRow(convertBytesToDouble(bucket.getEndRow()))
        .setNumRows(bucket.getCount()).create();
    return b;
  }

  private HFileHistogram.Bucket getFromNumericHistogramBucket(
      NumericHistogram.Bucket bucket) {
    Bucket b = (new Bucket.Builder())
        .setStartRow(this.convertDoubleToBytes(bucket.getStart()))
        .setEndRow(this.convertDoubleToBytes(bucket.getEnd()))
        .setNumRows(bucket.getCount()).create();
    return b;
  }

  @Override
  public Writable serialize() {
    return new Writable() {
      List<Bucket> buckets;

      public Writable setVal(List<Bucket> buckets) {
        this.buckets = buckets;
        return this;
      }

      @Override
      public void readFields(DataInput in) throws IOException {
        int len = in.readInt();
        buckets = new ArrayList<Bucket>(len);
        for (int i = 0; i < len; i++) {
          Bucket b = new Bucket();
          b.readFields(in);
          buckets.add(b);
        }
      }

      @Override
      public void write(DataOutput out) throws IOException {
        out.writeInt(buckets.size());
        for (Bucket bucket : buckets) {
          bucket.write(out);
        }
      }
    }.setVal(getUniformBuckets());
  }

  @Override
  public HFileHistogram deserialize(ByteBuffer buf) throws IOException {
    if (buf == null) return null;
    ByteArrayInputStream bais = new ByteArrayInputStream(buf.array(),
        buf.arrayOffset(), buf.limit());
    DataInput in = new DataInputStream(bais);
    int len = in.readInt();
    List<Bucket> buckets = new ArrayList<Bucket>(len);
    for (int i = 0; i < len; i++) {
      Bucket b = new Bucket();
      b.readFields(in);
      buckets.add(b);
    }
    bais.close();
    if (buckets.isEmpty()) {
      return null;
    }
    HFileHistogram ret = getHistogram(buckets);
    return ret;
  }

  @Override
  public HFileHistogram merge(HFileHistogram h2) {
    Preconditions.checkNotNull(h2);
    Preconditions.checkArgument(h2 instanceof UniformSplitHFileHistogram);
    UniformSplitHFileHistogram h = (UniformSplitHFileHistogram) h2;
    this.underlyingHistogram.merge(h.underlyingHistogram);
    return this;
  }

  @Override
  public int getBinCount() {
    return this.underlyingHistogram.getBinCount();
  }

  @Override
  public HFileHistogram create(int binCount) {
    return new UniformSplitHFileHistogram(binCount);
  }
}
