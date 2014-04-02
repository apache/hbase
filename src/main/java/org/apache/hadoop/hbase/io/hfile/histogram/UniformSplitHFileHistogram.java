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
import java.util.Arrays;
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
  public static final int PADDING = 8;
  private static final byte[] INFINITY;
  // Infinity but padded with a zero at the start to avoid messing with 2's complement.
  private static final byte[] INFINITY_PADDED;
  private static final double INFINITY_DOUBLE;
  static {
    /**
     * Returns {0xff, 0xff ....  0xff}
     *          <----  padding  ---->
     */
    INFINITY = new byte[PADDING];
    INFINITY_PADDED = new byte[PADDING + 1];
    for (int i = 0; i < PADDING; i++) {
      INFINITY[i] = (byte)0xFF;
      INFINITY_PADDED[i + 1] = (byte)0xFF;
    }
    INFINITY_DOUBLE = (new BigInteger(getPaddedInfinityArr())).doubleValue();
  }

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
    double val = convertBytesToDouble(kv.getBuffer(),
        kv.getRowOffset(), kv.getRowLength());
    underlyingHistogram.add(val);
  }

  protected static double getInfinity() {
    return INFINITY_DOUBLE;
  }

  /**
   * This returns the maximum number that we can represent using padding bytes.
   * Returns {0xff, 0xff ....  0xff}
   *          <----  padding  ---->
   * @return
   */
  protected static byte[] getInfinityArr() {
    return Arrays.copyOf(INFINITY, PADDING);
  }

  /**
   * To use while converting to a BigInteger.
   * Contains a 0 in the 0'th index and 0xFF in the rest,
   * containing a total of PADDING + 1 bytes.
   */
  protected static byte[] getPaddedInfinityArr() {
    return Arrays.copyOf(INFINITY_PADDED, PADDING + 1);
  }

  protected static double getMinusInfinity() {
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
  protected static double convertBytesToDouble(byte[] row) {
    return convertBytesToDouble(row, 0, row.length);
  }

  protected static double convertBytesToDouble(byte[] rowbuffer, int offset,
      int length) {
    byte[] paddedRow = new byte[PADDING + 1];

    // To avoid messing with 2's complement.
    paddedRow[0] = 0;
    int minlength = Math.min(length, PADDING);
    System.arraycopy(rowbuffer, offset, paddedRow, 1, minlength);

    return new BigInteger(paddedRow).doubleValue();
  }

  /**
   * Double is converted to Bytes in a similar manner.
   *
   * @param d
   * @return
   */
  protected static byte[] convertDoubleToBytes(double d) {
    BigDecimal tmpDecimal = new BigDecimal(d);
    BigInteger tmp = tmpDecimal.toBigInteger();
    byte[] arr = tmp.toByteArray();
    if (arr[0] == 0) {
      // to represent {0xff, 0xff}, big integer uses {0x00, 0xff, 0xff}
      // due to the one's compliment representation.
      Preconditions.checkArgument(arr.length == 1 || arr[1] != 0);
      arr = Bytes.tail(arr, arr.length - 1);
    }
    if (arr.length > PADDING) {
      // {0x00, 0xff, ... , 0xff, 0xff}=>double=>{0x01, 0x00, ... , 0x00, 0x00}
      // might happen.
      arr = getInfinityArr();
    }
    return Bytes.padHead(arr, PADDING - arr.length);
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
        .setStartRow(convertDoubleToBytes(bucket.getStart()))
        .setEndRow(convertDoubleToBytes(bucket.getEnd()))
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
