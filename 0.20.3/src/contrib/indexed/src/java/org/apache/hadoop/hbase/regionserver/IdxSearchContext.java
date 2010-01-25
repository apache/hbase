/**
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.idx.support.Callback;
import org.apache.hadoop.hbase.regionserver.idx.support.arrays.ObjectArrayList;
import org.apache.hadoop.hbase.regionserver.idx.support.sets.IntSet;
import org.apache.hadoop.hbase.util.Pair;

import java.util.Map;

/**
 * The search context holds the context for a specific search request.
 * It takes a snapshot of the indexes at the time the search was requested.
 * <p/>
 * The search context is a transient object whose life spans only while the
 * search (typically a region scan) is in progress.
 */
public class IdxSearchContext {

  private ObjectArrayList<KeyValue> keys;
  private Map<Pair<byte[], byte[]>, IdxIndex> indexMap;

  /**
   * Construct a new search context.
   *
   * @param keys     the keys to use when searching.
   * @param indexMap the matching index map
   */
  public IdxSearchContext(ObjectArrayList<KeyValue> keys,
    Map<Pair<byte[], byte[]>, IdxIndex> indexMap) {
    this.keys = keys;
    this.indexMap = indexMap;
  }

  /**
   * Looks up an index based on the column and the qualifier. May return null if
   * no such index is found.
   *
   * @param column    the column
   * @param qualifier the column qualifier
   * @return the index for the column/qualifer
   */
  public IdxIndex getIndex(byte[] column, byte[] qualifier) {
    return indexMap.get(Pair.of(column, qualifier));
  }

  /**
   * Process a set of rows, typically to convert a query to a scan. Rows are
   * processed in sorted order.
   *
   * @param rowSet   the row set to process
   * @param callback the callback to use to process those rows
   */
  public void processRows(IntSet rowSet, Callback<KeyValue> callback) {
    IntSet.IntSetIterator iterator = rowSet.iterator();
    while (iterator.hasNext()) {
      int i = iterator.next();
      callback.call(keys.get(i));
    }
  }

  /**
   * Unmap a index key to a actual key.
   *
   * @param index the index offset
   * @return the byte
   */
  public KeyValue lookupRow(int index) {
    return keys.get(index);
  }

  /**
   * The number of rows indexed in this search context.
   *
   * @return the number of indexed rows.
   */
  public int rowCount() {
    return keys.size();
  }

  /**
   * close this search context
   */
  public void close(){
    keys = null;
    indexMap = null;
  }
}
