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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * MemStoreScanner implements the KeyValueScanner.
 * It lets the caller scan the contents of a memstore -- both current
 * map and snapshot.
 * <p/>
 * The memstore scanner keeps its own reference to the main and snapshot
 * key/value sets. Keeping those references allows the scanner to be indifferent
 * to memstore flushes. Calling the {@link #close()} method ensures that the
 * references to those classes are null'd allowing the GC to pick them up.
 */
class MemStoreScanner implements KeyValueScanner {
  private static final Log LOG = LogFactory.getLog(MemStoreScanner.class);

  private static final
  SortedSet<KeyValue> EMPTY_SET = new TreeSet<KeyValue>();
  private static final Iterator<KeyValue> EMPTY_ITERATOR =
    new Iterator<KeyValue>() {

      @Override
      public boolean hasNext() {
        return false;
      }
      @Override
      public KeyValue next() {
        return null;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };


  private SortedSet<KeyValue> kvsetRef;
  private SortedSet<KeyValue> snapshotRef;
  private KeyValue.KVComparator comparatorRef;
  private Iterator<KeyValue> kvsetIterator;
  private Iterator<KeyValue> snapshotIterator;

  private KeyValue currentKvsetKV;
  private KeyValue currentSnapshotKV;
  private KeyValue nextKV;

  /**
   * Create a new memstore scanner.
   *
   * @param kvset      the main key value set
   * @param snapshot   the snapshot set
   * @param comparator the comparator to use
   */
  MemStoreScanner(KeyValueSkipListSet kvset,
    KeyValueSkipListSet snapshot, KeyValue.KVComparator comparator) {
    super();
    this.kvsetRef = kvset;
    this.snapshotRef = snapshot != null ? snapshot : EMPTY_SET;
    this.comparatorRef = comparator;
    this.kvsetIterator = kvsetRef.iterator();
    this.snapshotIterator = snapshotRef.iterator();
    this.nextKV = currentKvsetKV = currentSnapshotKV = null;
  }

  private void fill() {
    if (nextKV == null) {
      if (currentSnapshotKV == null && snapshotIterator.hasNext()) {
        currentSnapshotKV = snapshotIterator.next();
      }

      if (currentKvsetKV == null && kvsetIterator.hasNext()) {
        currentKvsetKV = kvsetIterator.next();
      }

      if (currentSnapshotKV != null && currentKvsetKV != null) {
        int cmp = comparatorRef.compare(currentSnapshotKV, currentKvsetKV);
        if (cmp <= 0) {
          nextKV = currentSnapshotKV;
          currentSnapshotKV = null;
        } else {
          nextKV = currentKvsetKV;
          currentKvsetKV = null;
        }
      } else if (currentSnapshotKV != null) {
        nextKV = currentSnapshotKV;
        currentSnapshotKV = null;
      } else {
        nextKV = currentKvsetKV;
        currentKvsetKV = null;
      }
    }
  }

  @Override
  public synchronized boolean seek(KeyValue key) {
    if (key == null) {
      close();
      return false;
    }
    SortedSet<KeyValue> kvsetTail = kvsetRef.tailSet(key);
    SortedSet<KeyValue> snapshotTail = snapshotRef != null ?
      snapshotRef.tailSet(key) : EMPTY_SET;

    kvsetIterator = kvsetTail.iterator();
    snapshotIterator = snapshotTail.iterator();

    currentKvsetKV = null;
    currentSnapshotKV = null;
    nextKV = null;

    return kvsetIterator.hasNext() || snapshotIterator.hasNext();
  }

  @Override
  public synchronized KeyValue peek() {
    fill();
    return nextKV;
  }

  @Override
  public synchronized KeyValue next() {
    fill();
    KeyValue next = nextKV;
    nextKV = null;
    return next;
  }

  public synchronized void close() {
    this.kvsetRef = EMPTY_SET;
    this.snapshotRef = EMPTY_SET;
    this.kvsetIterator = EMPTY_ITERATOR;
    this.snapshotIterator = EMPTY_ITERATOR;
    this.currentKvsetKV = null;
    this.currentSnapshotKV = null;
    this.nextKV = null;
  }
}
