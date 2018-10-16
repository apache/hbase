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
package org.apache.hadoop.hbase.types;

import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.OrderedBytes;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * An alternative to {@link OrderedBlob} for use by {@link Struct} fields that
 * do not terminate the fields list. Built on
 * {@link OrderedBytes#encodeBlobVar(PositionedByteRange, byte[], int, int, Order)}.
 */
@InterfaceAudience.Public
public class OrderedBlobVar extends OrderedBytesBase<byte[]> {

  public static final OrderedBlobVar ASCENDING = new OrderedBlobVar(Order.ASCENDING);
  public static final OrderedBlobVar DESCENDING = new OrderedBlobVar(Order.DESCENDING);

  protected OrderedBlobVar(Order order) {
    super(order);
  }

  @Override
  public int encodedLength(byte[] val) {
    return null == val ? 1 : OrderedBytes.blobVarEncodedLength(val.length);
  }

  @Override
  public Class<byte[]> encodedClass() {
    return byte[].class;
  }

  @Override
  public byte[] decode(PositionedByteRange src) {
    return OrderedBytes.decodeBlobVar(src);
  }

  @Override
  public int encode(PositionedByteRange dst, byte[] val) {
    return OrderedBytes.encodeBlobVar(dst, val, order);
  }

  /**
   * Write a subset of {@code val} to {@code buff}.
   */
  public int encode(PositionedByteRange dst, byte[] val, int voff, int vlen) {
    return OrderedBytes.encodeBlobVar(dst, val, voff, vlen, order);
  }
}
