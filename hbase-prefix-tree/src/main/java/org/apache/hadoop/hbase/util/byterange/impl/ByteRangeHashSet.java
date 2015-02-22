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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.ByteRange;
import org.apache.hadoop.hbase.util.CollectionUtils;
import org.apache.hadoop.hbase.util.IterableUtils;
import org.apache.hadoop.hbase.util.byterange.ByteRangeSet;

/**
 * This is probably the best implementation of ByteRangeSet at the moment, though a HashMap produces
 * garbage when adding a new element to it. We can probably create a tighter implementation without
 * pointers or garbage.
 */
@InterfaceAudience.Private
public class ByteRangeHashSet extends ByteRangeSet {

  /************************ constructors *****************************/

  public ByteRangeHashSet() {
    this.uniqueIndexByUniqueRange = new HashMap<ByteRange, Integer>();
  }

  public ByteRangeHashSet(List<ByteRange> rawByteArrays) {
    for (ByteRange in : IterableUtils.nullSafe(rawByteArrays)) {
      add(in);
    }
  }

  @Override
  public void addToSortedRanges() {
    sortedRanges.addAll(CollectionUtils.nullSafe(uniqueIndexByUniqueRange.keySet()));
    Collections.sort(sortedRanges);
  }

}
