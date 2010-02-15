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

import org.apache.commons.lang.time.StopWatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.idx.IdxColumnDescriptor;
import org.apache.hadoop.hbase.client.idx.IdxIndexDescriptor;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.regionserver.idx.support.IdxClassSize;
import org.apache.hadoop.hbase.regionserver.idx.support.arrays.ObjectArrayList;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Manages the indexes for a single region.
 */
public class IdxRegionIndexManager implements HeapSize {
  private static final Log LOG = LogFactory.getLog(IdxRegionIndexManager.class);

  static final long FIXED_SIZE =
    ClassSize.align(ClassSize.OBJECT + 4 * ClassSize.REFERENCE +
      IdxClassSize.HASHMAP + IdxClassSize.OBJECT_ARRAY_LIST +
      Bytes.SIZEOF_LONG + ClassSize.REENTRANT_LOCK);


  private static final int DEFAULT_INITIAL_INDEX_SIZE = 1000;

  /**
   * The wrapping region.
   */
  private final IdxRegion region;
  /**
   * The index map. Each pair holds the column and qualifier.
   */
  private volatile Map<Pair<byte[], byte[]>, IdxIndex> indexMap;
  /**
   * The keys ordered by their id. The IntSet in the {@link IdxIndex} have
   * members corresponding to the indices of this array.
   */
  private volatile ObjectArrayList<KeyValue> keys;
  /**
   * The heap size.
   */
  private long heapSize;

  private final ReadWriteLock indexSwitchLock;
  private static final double INDEX_SIZE_GROWTH_FACTOR = 1.1;
  private static final double BYTES_IN_MB = 1024D * 1024D;

  /**
   * Create and initialize a new index manager.
   *
   * @param region the region to connect to
   */
  public IdxRegionIndexManager(IdxRegion region) {
    this.region = region;
    indexSwitchLock = new ReentrantReadWriteLock();
    heapSize = FIXED_SIZE;
  }

  /**
   * Creates and populates all indexes. Bruteforce scan fetching everything
   * into memory, creating indexes out of that.
   *
   * @return total time in millis to rebuild the indexes
   * @throws IOException in case scan throws
   */
  public long rebuildIndexes() throws IOException {
    long startMillis = System.currentTimeMillis();
    if (LOG.isInfoEnabled()) {
      LOG.info(String.format("Initializing index manager for region: %s", region.toString()));
    }
    heapSize = FIXED_SIZE;
    Map<Pair<byte[], byte[]>, CompleteIndexBuilder> builderTable = initIndexTable();
    // if the region is closing/closed then a fillIndex method will throw a
    // NotServingRegion exection when an attempt to obtain a scanner is made
    // NOTE: when the region is being created isClosing() returns true
    if (!(region.isClosing() || region.isClosed()) && !builderTable.isEmpty()) {
      try {
        ObjectArrayList<KeyValue> newKeys = fillIndex(builderTable);
        Map<Pair<byte[], byte[]>, IdxIndex> newIndexMap = finalizeIndex(builderTable, newKeys);
        switchIndex(newKeys, newIndexMap);
      } catch (NotServingRegionException e) {
        // the not serving exception may also be thrown during the scan if
        // the region was closed during the scan
        LOG.warn("Aborted index initialization", e);
      }
    } else {
      switchIndex(new ObjectArrayList<KeyValue>(), Collections.<Pair<byte[], byte[]>, IdxIndex>emptyMap());
    }
    return System.currentTimeMillis() - startMillis;
  }

  private void switchIndex(ObjectArrayList<KeyValue> newKeys,
    Map<Pair<byte[], byte[]>, IdxIndex> newIndexMap) {
    indexSwitchLock.writeLock().lock();
    try {
      this.keys = newKeys;
      this.indexMap = newIndexMap;
    } finally {
      indexSwitchLock.writeLock().unlock();
    }
  }

  /**
   * Initiate the index table. Read the column desciprtors, extract the index
   * descriptors from them and instantiate index builders for those columns.
   *
   * @return the initiated map of builders keyed by column:qualifer pair
   * @throws IOException thrown by {@link IdxColumnDescriptor#getIndexDescriptors(org.apache.hadoop.hbase.HColumnDescriptor)}
   */
  private Map<Pair<byte[], byte[]>, CompleteIndexBuilder> initIndexTable()
    throws IOException {
    Map<Pair<byte[], byte[]>, CompleteIndexBuilder> indexBuilders =
      new HashMap<Pair<byte[], byte[]>, CompleteIndexBuilder>();
    for (HColumnDescriptor columnDescriptor : region.getRegionInfo().getTableDesc().getColumnFamilies()) {
      Collection<IdxIndexDescriptor> indexDescriptors = IdxColumnDescriptor.getIndexDescriptors(columnDescriptor).values();

      for (IdxIndexDescriptor indexDescriptor : indexDescriptors) {
        LOG.info(String.format("Adding index for region: '%s' index: %s", region.getRegionNameAsString(), indexDescriptor.toString()));
        Pair<byte[], byte[]> key = Pair.of(columnDescriptor.getName(), indexDescriptor.getQualifierName());
        IdxIndex currentIndex = indexMap != null ? indexMap.get(key) : null;
        int initialSize = currentIndex == null ? DEFAULT_INITIAL_INDEX_SIZE : (int) Math.round(currentIndex.size() * INDEX_SIZE_GROWTH_FACTOR);
        indexBuilders.put(key, new CompleteIndexBuilder(columnDescriptor, indexDescriptor, initialSize));
      }
    }
    return indexBuilders;
  }

  /**
   * Fills the index. Scans the region for latest rows and sends key values
   * to the matching index builder
   *
   * @param builders the map of builders keyed by column:qualifer pair
   * @return the keyset (a fresh set)
   * @throws IOException may be thrown by the scan
   */
  private ObjectArrayList<KeyValue> fillIndex(Map<Pair<byte[], byte[]>,
    CompleteIndexBuilder> builders) throws IOException {
    ObjectArrayList<KeyValue> newKeys = this.keys == null ?
      new ObjectArrayList<KeyValue>() :
      // in case we already have keys in the store try to guess the new size
      new ObjectArrayList<KeyValue>(this.keys.size() + this.region.averageNumberOfMemStoreSKeys() * 2);

    StopWatch stopWatch = new StopWatch();
    stopWatch.start();

    InternalScanner scanner = region.getScanner(createScan(builders.keySet()));
    try {
      boolean moreRows;
      int id = 0;
      do {
        List<KeyValue> nextRow = new ArrayList<KeyValue>();
        moreRows = scanner.next(nextRow);
        if (nextRow.size() > 0) {
          KeyValue firstOnRow = KeyValue.createFirstOnRow(nextRow.get(0).getRow());
          newKeys.add(firstOnRow);
          // add keyvalue to the heapsize
          heapSize += firstOnRow.heapSize();
          for (KeyValue keyValue : nextRow) {
            try {
              CompleteIndexBuilder idx = builders.get(Pair.of(keyValue.getFamily(),
                keyValue.getQualifier()));
              // we must have an index since we've limited the
              // scan to include only indexed columns
              assert idx != null;
              idx.addKeyValue(keyValue, id);
            } catch (Exception e) {
              LOG.error("Failed to add " + keyValue + " to the index", e);
            }
          }
          id++;
        }
      } while (moreRows);
      stopWatch.stop();
      LOG.info("Filled indices for region: '" + region.getRegionNameAsString()
        + "' with " + id + " entries in " + stopWatch.toString());
      return newKeys;
    } finally {
      scanner.close();
    }
  }

  private Scan createScan(Set<Pair<byte[], byte[]>> columns) {
    Scan scan = new Scan();
    for (Pair<byte[], byte[]> column : columns) {
      scan.addColumn(column.getFirst(), column.getSecond());
    }
    return scan;
  }

  /**
   * Converts the map of builders into complete indexes, calling
   * {@link CompleteIndexBuilder#finalizeIndex(int)} on each builder.
   *
   * @param builders the map of builders
   * @param newKeys  the set of keys for the new index to be finalized
   * @return the new index map
   */
  private Map<Pair<byte[], byte[]>, IdxIndex> finalizeIndex(Map<Pair<byte[], byte[]>,
    CompleteIndexBuilder> builders, ObjectArrayList<KeyValue> newKeys) {
    Map<Pair<byte[], byte[]>, IdxIndex> newIndexes = new HashMap<Pair<byte[], byte[]>, IdxIndex>();
    for (Map.Entry<Pair<byte[], byte[]>, CompleteIndexBuilder> indexEntry :
      builders.entrySet()) {
      final IdxIndex index = indexEntry.getValue().finalizeIndex(newKeys.size());
      final Pair<byte[], byte[]> key = indexEntry.getKey();
      newIndexes.put(key, index);
      // adjust the heapsize
      long indexSize = ClassSize.align(ClassSize.MAP_ENTRY +
        ClassSize.align(ClassSize.OBJECT + 2 * ClassSize.ARRAY +
          key.getFirst().length +
          key.getSecond().length) + index.heapSize());
      LOG.info(String.format("Final index size: %f mb for region: '%s' index: %s",
        toMb(indexSize), Bytes.toString(key.getFirst()), Bytes.toString(key.getSecond())));
      heapSize += indexSize;
    }
    LOG.info(String.format("Total index heap overhead: %f mb for region: '%s'",
      toMb(heapSize), region.getRegionNameAsString()));
    return newIndexes;
  }

  private double toMb(long bytes) {
    return bytes / BYTES_IN_MB;
  }

  /**
   * Create a new search context.
   *
   * @return the new search context.
   */
  public IdxSearchContext newSearchContext() {
    indexSwitchLock.readLock().lock();
    try {
      return new IdxSearchContext(keys, indexMap);
    } finally {
      indexSwitchLock.readLock().unlock();
    }
  }

  @Override
  public long heapSize() {
    return heapSize;
  }

  /**
   * Exposes the number of keys in the index manager.
   *
   * @return the number of keys.
   */
  public int getNumberOfKeys() {
    indexSwitchLock.readLock().lock();
    try {
      return keys.size();
    } finally {
      indexSwitchLock.readLock().unlock();
    }
  }

  /**
   * A monitoring operation which returns the byte size of a given index.
   *
   * @param columnName in [family]:[qualifier] format
   * @return the byte size of the index
   */
  public long getIndexHeapSize(String columnName) {
    String[] familyAndQualifier = columnName.split(":");
    if (familyAndQualifier != null && familyAndQualifier.length == 2) {
      Pair fqPair = Pair.of(Bytes.toBytes(familyAndQualifier[0]),
        Bytes.toBytes(familyAndQualifier[1]));
      indexSwitchLock.readLock().lock();
      IdxIndex idx = null;
      try {
        idx = indexMap.get(fqPair);
      } finally {
        indexSwitchLock.readLock().unlock();
      }
      if (idx != null) {
        return idx.heapSize();
      }
    }
    throw new IllegalArgumentException("No index for " + columnName);
  }
}
