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

package org.apache.hadoop.hbase.util.byterange;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.ArrayUtils;
import org.apache.hadoop.hbase.util.ByteRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.SimpleMutableByteRange;

import com.google.common.collect.Lists;

/**
 * Performance oriented class for de-duping and storing arbitrary byte[]'s arriving in non-sorted
 * order. Appends individual byte[]'s to a single big byte[] to avoid overhead and garbage.
 * <p>
 * Current implementations are {@link org.apache.hadoop.hbase.util.byterange.impl.ByteRangeHashSet} and
 * {@link org.apache.hadoop.hbase.util.byterange.impl.ByteRangeTreeSet}, but other options might be a
 * trie-oriented ByteRangeTrieSet, etc
 */
@InterfaceAudience.Private
public abstract class ByteRangeSet {

  /******************** fields **********************/

  protected byte[] byteAppender;
  protected int numBytes;

  protected Map<ByteRange, Integer> uniqueIndexByUniqueRange;

  protected ArrayList<ByteRange> uniqueRanges;
  protected int numUniqueRanges = 0;

  protected int[] uniqueRangeIndexByInsertionId;
  protected int numInputs;

  protected List<Integer> sortedIndexByUniqueIndex;
  protected int[] sortedIndexByInsertionId;
  protected ArrayList<ByteRange> sortedRanges;


  /****************** construct **********************/

  protected ByteRangeSet() {
    this.byteAppender = new byte[0];
    this.uniqueRanges = Lists.newArrayList();
    this.uniqueRangeIndexByInsertionId = new int[0];
    this.sortedIndexByUniqueIndex = Lists.newArrayList();
    this.sortedIndexByInsertionId = new int[0];
    this.sortedRanges = Lists.newArrayList();
  }

  public void reset() {
    numBytes = 0;
    uniqueIndexByUniqueRange.clear();
    numUniqueRanges = 0;
    numInputs = 0;
    sortedIndexByUniqueIndex.clear();
    sortedRanges.clear();
  }


  /*************** abstract *************************/

  public abstract void addToSortedRanges();


  /**************** methods *************************/

  /**
   * Check if the incoming byte range exists.  If not, add it to the backing byteAppender[] and
   * insert it into the tracking Map uniqueIndexByUniqueRange.
   */
  public void add(ByteRange bytes) {
    Integer index = uniqueIndexByUniqueRange.get(bytes);
    if (index == null) {
      index = store(bytes);
    }
    int minLength = numInputs + 1;
    uniqueRangeIndexByInsertionId = ArrayUtils.growIfNecessary(uniqueRangeIndexByInsertionId,
        minLength, 2 * minLength);
    uniqueRangeIndexByInsertionId[numInputs] = index;
    ++numInputs;
  }

  protected int store(ByteRange bytes) {
    int indexOfNewElement = numUniqueRanges;
    if (uniqueRanges.size() <= numUniqueRanges) {
      uniqueRanges.add(new SimpleMutableByteRange());
    }
    ByteRange storedRange = uniqueRanges.get(numUniqueRanges);
    int neededBytes = numBytes + bytes.getLength();
    byteAppender = ArrayUtils.growIfNecessary(byteAppender, neededBytes, 2 * neededBytes);
    bytes.deepCopyTo(byteAppender, numBytes);
    storedRange.set(byteAppender, numBytes, bytes.getLength());// this isn't valid yet
    numBytes += bytes.getLength();
    uniqueIndexByUniqueRange.put(storedRange, indexOfNewElement);
    int newestUniqueIndex = numUniqueRanges;
    ++numUniqueRanges;
    return newestUniqueIndex;
  }

  public ByteRangeSet compile() {
    addToSortedRanges();
    for (int i = 0; i < sortedRanges.size(); ++i) {
      sortedIndexByUniqueIndex.add(null);// need to grow the size
    }
    // TODO move this to an invert(int[]) util method
    for (int i = 0; i < sortedIndexByUniqueIndex.size(); ++i) {
      int uniqueIndex = uniqueIndexByUniqueRange.get(sortedRanges.get(i));
      sortedIndexByUniqueIndex.set(uniqueIndex, i);
    }
    sortedIndexByInsertionId = ArrayUtils.growIfNecessary(sortedIndexByInsertionId, numInputs,
        numInputs);
    for (int i = 0; i < numInputs; ++i) {
      int uniqueRangeIndex = uniqueRangeIndexByInsertionId[i];
      int sortedIndex = sortedIndexByUniqueIndex.get(uniqueRangeIndex);
      sortedIndexByInsertionId[i] = sortedIndex;
    }
    return this;
  }

  public int getSortedIndexForInsertionId(int insertionId) {
    return sortedIndexByInsertionId[insertionId];
  }

  public int size() {
    return uniqueIndexByUniqueRange.size();
  }


  /***************** standard methods ************************/

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    int i = 0;
    for (ByteRange r : sortedRanges) {
      if (i > 0) {
        sb.append("\n");
      }
      sb.append(i + " " + Bytes.toStringBinary(r.deepCopyToNewArray()));
      ++i;
    }
    sb.append("\ntotalSize:" + numBytes);
    sb.append("\navgSize:" + getAvgSize());
    return sb.toString();
  }


  /**************** get/set *****************************/

  public ArrayList<ByteRange> getSortedRanges() {
    return sortedRanges;
  }

  public long getAvgSize() {
    return numBytes / numUniqueRanges;
  }

}
