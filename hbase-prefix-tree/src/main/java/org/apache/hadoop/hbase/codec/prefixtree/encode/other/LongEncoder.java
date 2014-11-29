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

package org.apache.hadoop.hbase.codec.prefixtree.encode.other;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.ArrayUtils;
import org.apache.hadoop.hbase.util.CollectionUtils;
import org.apache.hadoop.hbase.util.vint.UFIntTool;

import com.google.common.base.Joiner;

/**
 * Used to de-duplicate, sort, minimize/diff, and serialize timestamps and mvccVersions from a
 * collection of Cells.
 *
 * 1. add longs to a HashSet for fast de-duplication
 * 2. keep track of the min and max
 * 3. copy all values to a new long[]
 * 4. Collections.sort the long[]
 * 5. calculate maxDelta = max - min
 * 6. determine FInt width based on maxDelta
 * 7. PrefixTreeEncoder binary searches to find index of each value
 */
@InterfaceAudience.Private
public class LongEncoder {

  /****************** fields ****************************/

  protected HashSet<Long> uniqueValues;
  protected long[] sortedUniqueValues;
  protected long min, max, maxDelta;

  protected int bytesPerDelta;
  protected int bytesPerIndex;
  protected int totalCompressedBytes;


  /****************** construct ****************************/

  public LongEncoder() {
    this.uniqueValues = new HashSet<Long>();
  }

  public void reset() {
    uniqueValues.clear();
    sortedUniqueValues = null;
    min = Long.MAX_VALUE;
    max = Long.MIN_VALUE;
    maxDelta = Long.MIN_VALUE;
    bytesPerIndex = 0;
    bytesPerDelta = 0;
    totalCompressedBytes = 0;
  }


  /************* methods ***************************/

  public void add(long timestamp) {
    uniqueValues.add(timestamp);
  }

  public LongEncoder compile() {
    int numUnique = uniqueValues.size();
    if (numUnique == 1) {
      min = CollectionUtils.getFirst(uniqueValues);
      sortedUniqueValues = new long[] { min };
      return this;
    }

    sortedUniqueValues = new long[numUnique];
    int lastIndex = -1;
    for (long value : uniqueValues) {
      sortedUniqueValues[++lastIndex] = value;
    }
    Arrays.sort(sortedUniqueValues);
    min = ArrayUtils.getFirst(sortedUniqueValues);
    max = ArrayUtils.getLast(sortedUniqueValues);
    maxDelta = max - min;
    if (maxDelta > 0) {
      bytesPerDelta = UFIntTool.numBytes(maxDelta);
    } else {
      bytesPerDelta = 0;
    }

    int maxIndex = numUnique - 1;
    bytesPerIndex = UFIntTool.numBytes(maxIndex);

    totalCompressedBytes = numUnique * bytesPerDelta;

    return this;
  }

  public long getDelta(int index) {
    if (sortedUniqueValues.length == 0) {
      return 0;
    }
    return sortedUniqueValues[index] - min;
  }

  public int getIndex(long value) {
    // should always find an exact match
    return Arrays.binarySearch(sortedUniqueValues, value);
  }

  public void writeBytes(OutputStream os) throws IOException {
    for (int i = 0; i < sortedUniqueValues.length; ++i) {
      long delta = sortedUniqueValues[i] - min;
      UFIntTool.writeBytes(bytesPerDelta, delta, os);
    }
  }

  //convenience method for tests
  public byte[] getByteArray() throws IOException{
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    writeBytes(baos);
    return baos.toByteArray();
  }

  public int getOutputArrayLength() {
    return sortedUniqueValues.length * bytesPerDelta;
  }

  public int getNumUniqueValues() {
    return sortedUniqueValues.length;
  }


  /******************* Object methods **********************/

  @Override
  public String toString() {
    if (ArrayUtils.isEmpty(sortedUniqueValues)) {
      return "[]";
    }
    return "[" + Joiner.on(",").join(ArrayUtils.toList(sortedUniqueValues)) + "]";
  }


  /******************** get/set **************************/

  public long getMin() {
    return min;
  }

  public int getBytesPerDelta() {
    return bytesPerDelta;
  }

  public int getBytesPerIndex() {
    return bytesPerIndex;
  }

  public int getTotalCompressedBytes() {
    return totalCompressedBytes;
  }

  public long[] getSortedUniqueTimestamps() {
    return sortedUniqueValues;
  }

}
