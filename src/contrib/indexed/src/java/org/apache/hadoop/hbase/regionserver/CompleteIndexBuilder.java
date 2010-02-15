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
import org.apache.hadoop.hbase.regionserver.idx.support.arrays.BigDecimalArrayList;
import org.apache.hadoop.hbase.regionserver.idx.support.arrays.BinarySearch;
import org.apache.hadoop.hbase.regionserver.idx.support.arrays.ByteArrayArrayList;
import org.apache.hadoop.hbase.regionserver.idx.support.arrays.ByteArrayList;
import org.apache.hadoop.hbase.regionserver.idx.support.arrays.CharArrayArrayList;
import org.apache.hadoop.hbase.regionserver.idx.support.arrays.CharArrayList;
import org.apache.hadoop.hbase.regionserver.idx.support.arrays.DoubleArrayList;
import org.apache.hadoop.hbase.regionserver.idx.support.arrays.FloatArrayList;
import org.apache.hadoop.hbase.regionserver.idx.support.arrays.IntegerArrayList;
import org.apache.hadoop.hbase.regionserver.idx.support.arrays.List;
import org.apache.hadoop.hbase.regionserver.idx.support.arrays.LongArrayList;
import org.apache.hadoop.hbase.regionserver.idx.support.arrays.ObjectArrayList;
import org.apache.hadoop.hbase.regionserver.idx.support.arrays.ShortArrayList;
import org.apache.hadoop.hbase.regionserver.idx.support.sets.IntSet;
import org.apache.hadoop.hbase.regionserver.idx.support.sets.IntSetBuilder;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A builder class used to create complete indexes.
 */
public class CompleteIndexBuilder {

  private final HColumnDescriptor columnDescriptor;
  private final IdxIndexDescriptor indexDescriptor;
  /**
   * Offset extracted from the index descriptor.
   */
  private final int offset;

  /**
   * Length extracted from the index descriptor.
   */
  private final int length;

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
    this(columnDescriptor, indexDescriptor, 1);
  }

  /**
   * Construct a new complete index builder.
   *
   * @param columnDescriptor the column descriptor
   * @param indexDescriptor  the index descriptor
   * @param initialSize the initial arrays size, use -1 for defaults
   */
  public CompleteIndexBuilder(HColumnDescriptor columnDescriptor,
    IdxIndexDescriptor indexDescriptor, int initialSize) {
    this.columnDescriptor = columnDescriptor;
    this.indexDescriptor = indexDescriptor;
    this.offset = indexDescriptor.getOffset();
    this.length = indexDescriptor.getLength();

    switch (this.indexDescriptor.getQualifierType()) {
      case BYTE_ARRAY:
        keyStore = new ByteArrayArrayList(initialSize);
        break;
      case LONG:
        keyStore = new LongArrayList(initialSize);
        break;
      case DOUBLE:
        keyStore = new DoubleArrayList(initialSize);
        break;
      case BYTE:
        keyStore = new ByteArrayList(initialSize);
        break;
      case CHAR:
        keyStore = new CharArrayList(initialSize);
        break;
      case SHORT:
        keyStore = new ShortArrayList(initialSize);
        break;
      case INT:
        keyStore = new IntegerArrayList(initialSize);
        break;
      case FLOAT:
        keyStore = new FloatArrayList(initialSize);
        break;
      case BIG_DECIMAL:
        keyStore = new BigDecimalArrayList(initialSize);
        break;
      case CHAR_ARRAY:
        keyStore = new CharArrayArrayList(initialSize);
        break;
      default:
        throw new IllegalStateException("Unsupported type " +
          this.indexDescriptor.getQualifierType());
    }
    valueStoreBuilders = new ObjectArrayList<IntSetBuilder>(initialSize);
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
    byte[] key = extractKey(kv);
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
   * Extract the key from the KeyValue value.
   *
   * @param kv the key value from which to extract the key
   * @return the extracted keyvalue.
   */
  private byte[] extractKey(KeyValue kv) {
    int valueLength = kv.getValueLength();
    int l = length == -1 ? valueLength - offset : length;
    if (offset + l > valueLength) {
      throw new ArrayIndexOutOfBoundsException(String.format("Can't extract key: " +
        "Offset (%d) + Length (%d) > valueLength (%d)", offset, l, valueLength));
    }
    int o = kv.getValueOffset() + this.offset;
    byte[] result = new byte[l];
    System.arraycopy(kv.getBuffer(), o, result, 0, l);
    return result;
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
      int maxHeadIndex = -1;
      for (int i = 0; i < indexSize; i++) {
        currentHead = currentHead.unite(valueStore[i]);
        if (i % interval == 0) {
          maxHeadIndex = i;
          heads[i / interval] = currentHead;
          currentHead = currentHead.clone();
        }
      }
      
      IntSet allIds;
      if (maxHeadIndex < 0) {
        allIds = IntSetBuilder.newEmptyIntSet(numKeyValues);
      } else {
        allIds = currentHead.clone();
        // Add all remaning key values to the allKeys set
        for (int i = maxHeadIndex; i < indexSize; i++) {
          allIds = allIds.unite(valueStore[i]);
        }
      }

      return new CompleteIndex(keyStore, valueStore, heads, tails, allIds,
        numKeyValues, interval);
    } else {
      return new EmptyIndex(keyStore, numKeyValues);
    }
  }

}
