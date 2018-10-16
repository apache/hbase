/*
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

import java.util.Iterator;

import org.apache.hadoop.hbase.Cell;
import org.apache.yetus.audience.InterfaceAudience;


/**
 * A basic SegmentScanner used against an ImmutableScanner snapshot
 * Used flushing where we do a single pass, no reverse scanning or
 * inserts happening. Its a dumbed-down Scanner that can go fast.
 * Like {@link org.apache.hadoop.hbase.util.CollectionBackedScanner}
 * (but making it know about Segments was onerous).
 */
@InterfaceAudience.Private
public class SnapshotSegmentScanner extends NonReversedNonLazyKeyValueScanner {
  private final ImmutableSegment segment;
  private Iterator<Cell> iter;
  private Cell current;

  public SnapshotSegmentScanner(ImmutableSegment segment) {
    this.segment = segment;
    this.segment.incScannerCount();
    this.iter = createIterator(this.segment);
    if (this.iter.hasNext()){
      this.current = this.iter.next();
    }
  }

  private static Iterator<Cell> createIterator(Segment segment) {
    return segment.getCellSet().iterator();
  }

  @Override
  public Cell peek() {
    return current;
  }

  @Override
  public Cell next() {
    Cell oldCurrent = current;
    if(iter.hasNext()){
      current = iter.next();
    } else {
      current = null;
    }
    return oldCurrent;
  }

  @Override
  public boolean seek(Cell seekCell) {
    // restart iterator
    this.iter = createIterator(this.segment);
    return reseek(seekCell);
  }

  @Override
  public boolean reseek(Cell seekCell) {
    while (this.iter.hasNext()){
      Cell next = this.iter.next();
      int ret = this.segment.getComparator().compare(next, seekCell);
      if (ret >= 0) {
        this.current = next;
        return true;
      }
    }
    return false;
  }

  /**
   * @see KeyValueScanner#getScannerOrder()
   */
  @Override
  public long getScannerOrder() {
    return 0;
  }

  @Override
  public void close() {
    this.segment.decScannerCount();
  }
}
