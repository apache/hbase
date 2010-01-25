/*
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

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.idx.IdxIndexDescriptor;
import org.apache.hadoop.hbase.regionserver.idx.support.arrays.BinarySearch;
import org.apache.hadoop.hbase.regionserver.idx.support.arrays.ObjectArrayList;
import org.apache.hadoop.hbase.regionserver.idx.support.arrays.BigDecimalArrayList;
import org.apache.hadoop.hbase.regionserver.idx.support.arrays.ByteArrayArrayList;
import org.apache.hadoop.hbase.regionserver.idx.support.arrays.ByteArrayList;
import org.apache.hadoop.hbase.regionserver.idx.support.arrays.CharArrayArrayList;
import org.apache.hadoop.hbase.regionserver.idx.support.arrays.CharArrayList;
import org.apache.hadoop.hbase.regionserver.idx.support.arrays.DoubleArrayList;
import org.apache.hadoop.hbase.regionserver.idx.support.arrays.FloatArrayList;
import org.apache.hadoop.hbase.regionserver.idx.support.arrays.IntegerArrayList;
import org.apache.hadoop.hbase.regionserver.idx.support.arrays.List;
import org.apache.hadoop.hbase.regionserver.idx.support.arrays.LongArrayList;
import org.apache.hadoop.hbase.regionserver.idx.support.arrays.ShortArrayList;
import org.apache.hadoop.hbase.regionserver.idx.support.sets.IntSet;
import org.apache.hadoop.hbase.regionserver.idx.support.sets.IntSetBuilder;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A builder class used to create complete indexes.
 */
public class CompleteIndexBuilder {

  private HColumnDescriptor columnDescriptor;
  private IdxIndexDescriptor indexDescriptor;

  /**
   * The target keystore.
   */
  private List<?> keyStore;
  /**
   * The value store set builders.
   */
  private ObjectArrayList<IntSetBuilder> valueStoreBuilders;

  /**
   * Construct a new complete index builder.
   *
   * @param columnDescriptor the column descriptor
   * @param indexDescriptor  the index descriptor
   */
  public CompleteIndexBuilder(HColumnDescriptor columnDescriptor,
    IdxIndexDescriptor indexDescriptor) {
    this.columnDescriptor = columnDescriptor;
    this.indexDescriptor = indexDescriptor;

    switch (this.indexDescriptor.getQualifierType()) {
      case BYTE_ARRAY:
        keyStore = new ByteArrayArrayList();
        break;
      case LONG:
        keyStore = new LongArrayList();
        break;
      case DOUBLE:
        keyStore = new DoubleArrayList();
        break;
      case BYTE:
        keyStore = new ByteArrayList();
        break;
      case CHAR:
        keyStore = new CharArrayList();
        break;
      case SHORT:
        keyStore = new ShortArrayList();
        break;
      case INT:
        keyStore = new IntegerArrayList();
        break;
      case FLOAT:
        keyStore = new FloatArrayList();
        break;
      case BIG_DECIMAL:
        keyStore = new BigDecimalArrayList();
        break;
      case CHAR_ARRAY:
        keyStore = new CharArrayArrayList();
        break;
      default:
        throw new IllegalStateException("Unsupported type " +
          this.indexDescriptor.getQualifierType());
    }
    valueStoreBuilders = new ObjectArrayList<IntSetBuilder>();

  }

  /**
   * Add a new key value to the index. The keyvalues are added in 'key' order.
   *
   * @param kv the keyvalue.
   * @param id the id of the keyvalue (e.g. its place in the sorted order)
   */
  public void addKeyValue(KeyValue kv, int id) {
    assert Bytes.equals(indexDescriptor.getQualifierName(), kv.getQualifier())
      && Bytes.equals(columnDescriptor.getName(), kv.getFamily());
    byte[] key = kv.getValue();
    int index = BinarySearch.search(keyStore, keyStore.size(), key);
    IntSetBuilder intsetBuilder;
    if (index < 0) {
      intsetBuilder = new IntSetBuilder().start();
      index = -(index + 1);
      keyStore.insert(index, key);
      valueStoreBuilders.insert(index, intsetBuilder);
    } else {
      intsetBuilder = valueStoreBuilders.get(index);
    }
    intsetBuilder.addNext(id);
  }

  /**
   * Finalized the index creation and creates the new index.
   *
   * @param numKeyValues the total number of keyvalues in the region
   * @return a new complete index
   */
  @SuppressWarnings({"unchecked"})
  public IdxIndex finalizeIndex(int numKeyValues) {
    if (valueStoreBuilders.size() > 0) {
      assert numKeyValues > 0;
      int indexSize = keyStore.size();

      IntSet[] valueStore = new IntSet[indexSize];
      for (int i = 0; i < indexSize; i++) {
        valueStore[i] = valueStoreBuilders.get(i).finish(numKeyValues);
      }
      int interval = (int) Math.round(Math.sqrt(indexSize));
      int precalcSize = indexSize / interval +
        Integer.signum(indexSize % interval);

      IntSet[] tails = new IntSet[precalcSize];
      IntSet currentTail = IntSetBuilder.newEmptyIntSet(numKeyValues);
      for (int i = indexSize - 1; i >= 0; i--) {
        currentTail = currentTail.unite(valueStore[i]);
        if (i % interval == 0) {
          tails[i / interval] = currentTail;
          currentTail = currentTail.clone();
        }
      }

      IntSet[] heads = new IntSet[precalcSize];
      IntSet currentHead = IntSetBuilder.newEmptyIntSet(numKeyValues);
      for (int i = 0; i < indexSize; i++) {
        currentHead = currentHead.unite(valueStore[i]);
        if (i % interval == 0) {
          heads[i / interval] = currentHead;
          currentHead = currentHead.clone();
        }
      }

      return new CompleteIndex(keyStore, valueStore, heads, tails,
        numKeyValues, interval);
    } else {
      return new EmptyIndex(keyStore, numKeyValues);
    }
  }

}
