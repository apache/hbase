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

package org.apache.hadoop.hbase.util.byterange.impl;

import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.ByteRange;
import org.apache.hadoop.hbase.util.CollectionUtils;
import org.apache.hadoop.hbase.util.IterableUtils;
import org.apache.hadoop.hbase.util.byterange.ByteRangeSet;

/**
 * Not currently used in production, but here as a benchmark comparison against ByteRangeHashSet.
 */
@InterfaceAudience.Private
public class ByteRangeTreeSet extends ByteRangeSet {

  /************************ constructors *****************************/

  public ByteRangeTreeSet() {
    this.uniqueIndexByUniqueRange = new TreeMap<ByteRange, Integer>();
  }

  public ByteRangeTreeSet(List<ByteRange> rawByteArrays) {
    this();//needed to initialize the TreeSet
    for(ByteRange in : IterableUtils.nullSafe(rawByteArrays)){
      add(in);
    }
  }

  @Override
  public void addToSortedRanges() {
    sortedRanges.addAll(CollectionUtils.nullSafe(uniqueIndexByUniqueRange.keySet()));
  }

}
