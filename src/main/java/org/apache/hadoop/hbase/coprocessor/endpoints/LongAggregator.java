/**
 * Copyright 2014 The Apache Software Foundation
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
package org.apache.hadoop.hbase.coprocessor.endpoints;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegionIf;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Implementation of ILongAggregator for aggregation of sum/min/max
 */
public class LongAggregator implements ILongAggregator {

  /**
   * The factory of ILongAggregator
   */
  public static class Factory implements IEndpointFactory<ILongAggregator> {
    @Override
    public ILongAggregator create() {
      return new LongAggregator();
    }

    @Override
    public Class<ILongAggregator> getEndpointInterface() {
      return ILongAggregator.class;
    }
  }

  private HRegionIf region;
  private byte[] startRow;
  private byte[] stopRow;
  @Override
  public void setContext(IEndpointContext context) throws IOException {
    this.region = context.getRegion();
    this.startRow = context.getStartRow();
    this.stopRow = context.getStopRow();
  }

  private interface IUpdater {
    /**
     * Updates current with newValue and returns the result.
     */
    long update(long current, long newValue);
  }

  /**
   * Aggregates with a specified updater.
   *
   * @param offset the offset of the long in the value in bytes.
   * @param initValue the initial value for the aggregating variable.
   */
  private long cacluate(byte[] family, byte[] qualifier, int offset,
      IUpdater updater, long initValue) throws IOException {
    Scan.Builder builder = new Scan.Builder();
    if (family != null) {
      if (qualifier == null) {
        builder.addColumn(family, qualifier);
      } else {
        builder.addFamily(family);
      }
    }
    builder.setStartRow(startRow);
    builder.setStopRow(stopRow);

    try (InternalScanner scanner = this.region.getScanner(builder.create())) {

      long res = initValue;

      List<KeyValue> kvs = new ArrayList<>();
      boolean hasMore = true;
      while (hasMore) {
        // fetch key-values.
        kvs.clear();
        hasMore = scanner.next(kvs);

        for (KeyValue kv : kvs) {
          // check bound
          if (offset + Bytes.LONG_BYTES > kv.getValueLength()) {
            throw new IndexOutOfBoundsException();
          }
          // decode the value
          long vl = Bytes.toLong(kv.getBuffer(), kv.getValueOffset() + offset);
          // aggregate
          res = updater.update(res, vl);
        }
      }
      return res;
    }
  }

  private static final IUpdater SUM_UPDATER = new IUpdater() {
    @Override
    public long update(long current, long newValue) {
      return current + newValue;
    }
  };

  @Override
  public long sum(byte[] family, byte[] qualifier, int offset)
      throws IOException {
    return cacluate(family, qualifier, offset, SUM_UPDATER, 0L);
  }

  private static final IUpdater MIN_UPDATER = new IUpdater() {
    @Override
    public long update(long current, long newValue) {
      return Math.min(current, newValue);
    }
  };

  @Override
  public long min(byte[] family, byte[] qualifier, int offset)
      throws IOException {
    return cacluate(family, qualifier, offset, MIN_UPDATER, Long.MAX_VALUE);
  }

  private static final IUpdater MAX_UPDATER = new IUpdater() {
    @Override
    public long update(long current, long newValue) {
      return Math.max(current, newValue);
    }
  };

  @Override
  public long max(byte[] family, byte[] qualifier, int offset)
      throws IOException {
    return cacluate(family, qualifier, offset, MAX_UPDATER, Long.MIN_VALUE);
  }
}
