/*
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

package org.apache.hadoop.hbase.filter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Preconditions;

public class CompoundRowPrefixFilter extends FilterBase {
  private List<byte[]> rowPrefixes;
  private byte[] curRowPrefix;
  private int curPrefixIndex;
  private boolean done = false;

  public CompoundRowPrefixFilter() {
    // Used for serialization and deserialization directly.
    // Not to be used otherwise.
  }

  /**
   * Constructor for CompoundRowPrefixFilter which takes a list of row prefixes
   * @param rowPrefixes : Assuming that one row prefix is not a prefix of
   * another. In which case, we can remove the later from the list.
   * TODO : Do this here and remove this assumption.
   */
  private CompoundRowPrefixFilter(List<byte[]> rowPrefixes) {
    this.rowPrefixes = rowPrefixes;
    Preconditions.checkArgument(rowPrefixes.size() > 0,
        "Requires atleast one row prefix to initialize.");
    // sorting the rowPrefixes to keep them in increasing order.
    Collections.sort(this.rowPrefixes, Bytes.BYTES_COMPARATOR);
    this.resetFilterProgress();
  }

  /**
   * Resets the filter by resetting the state of the filter to the starting
   * configuration.
   */
  public void resetFilterProgress() {
    this.curPrefixIndex = 0;
    this.curRowPrefix = rowPrefixes.get(this.curPrefixIndex);
    this.done = false;
  }

  @Override
  public boolean filterAllRemaining() {
    return this.done;
  }

  /**
   * Updates the state of the filter to the next rowPrefix.
   * @return false if we need to inspect the next prefix.
   * true if the next prefix that we need to use is updated and valid.
   */
  private boolean checkAndUpdateNextPrefix() {
    if ((this.curPrefixIndex + 1) >= this.rowPrefixes.size()) {
      done = true;
      return false;
    }
    this.curRowPrefix = this.rowPrefixes.get(++this.curPrefixIndex);
    return true;
  }

  private boolean passesRowPrefixBloomFilter(KeyValue kv,
      List<KeyValueScanner> scanners) {
    if (scanners == null) return true;
    for (KeyValueScanner scanner : scanners) {
      if (scanner.passesRowKeyPrefixBloomFilter(kv)) return true;
    }
    return false;
  }

  @Override
  public ReturnCode filterKeyValue(KeyValue currentKv, List<KeyValueScanner> scanners) {
    while (true) {
      if (Bytes.startsWith(currentKv.getRow(), this.curRowPrefix)) {
        return ReturnCode.INCLUDE;
      } else if (Bytes.compareTo(currentKv.getRow(), this.curRowPrefix) > 0) {
        // Covered this RowPrefix, hence we can proceed to the next rowPrefix
        // in the list
        if (!this.checkAndUpdateNextPrefix()) break;
      } else {
        // We need to check the bloom filter if we need to seek to the
        // current prefix.
        if (!passesRowPrefixBloomFilter(
            KeyValue.createFirstOnRow(this.curRowPrefix), scanners)) {
          if (!this.checkAndUpdateNextPrefix()) break;
          continue;
        }
        break;
      }
    }
    if (done) return ReturnCode.SKIP;
    return ReturnCode.SEEK_NEXT_USING_HINT;
  }

  @Override
  public KeyValue getNextKeyHint(KeyValue currentKV) {
    return KeyValue.createFirstOnRow(this.curRowPrefix);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("CompoundRowFilter : ");
    sb.append("Internal State : [ ");
    sb.append(" curRowPrefix : " + Bytes.toStringBinary(curRowPrefix));
    sb.append(" curPrefixIndex : " + this.curPrefixIndex);
    sb.append(" done : " + this.done + " ], rowPrefixes : [ ");
    for (byte[] arr : this.rowPrefixes) {
      sb.append(Bytes.toStringBinary(arr) + ", ");
    }
    sb.append(" ]");
    return sb.toString();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int numRowPrefixes = in.readInt();
    this.rowPrefixes = new ArrayList<byte[]>(numRowPrefixes);
    for (int i = 0; i < numRowPrefixes; i++) {
      rowPrefixes.add(Bytes.readByteArray(in));
    }
    Preconditions.checkArgument(rowPrefixes.size() > 0);
    Collections.sort(this.rowPrefixes, Bytes.BYTES_COMPARATOR);
    this.resetFilterProgress();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(this.rowPrefixes.size());
    for (byte[] row : this.rowPrefixes) {
      Bytes.writeByteArray(out, row);
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
        + ((rowPrefixes == null) ? 0 : rowPrefixes.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    CompoundRowPrefixFilter other = (CompoundRowPrefixFilter) obj;
    if (rowPrefixes == null) {
      if (other.rowPrefixes != null)
        return false;
    } else {
      TreeSet<byte[]> set = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
      for (byte[] arr : rowPrefixes) {
        set.add(arr);
      }
      for (byte[] arr : other.rowPrefixes) {
        if (!set.contains(arr)) return false;
      }
    }
    return true;
  }

  public static class Builder {
    private List<byte[]> underlyingArray;
    public Builder() {
      this.underlyingArray = new ArrayList<byte[]>();
    }

    /**
     * Adds a copy of arr to the internal list.
     * @param arr
     */
    public Builder addRowPrefix(byte[] arr) {
      this.underlyingArray.add(Arrays.copyOf(arr, arr.length));
      return this;
    }

    public CompoundRowPrefixFilter create() {
      return new CompoundRowPrefixFilter(this.underlyingArray);
    }
  }
}
