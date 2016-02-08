/**
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

import org.apache.commons.logging.Log;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.CollectionBackedScanner;

/**
 * This segment is adapting a mutable segment making it into an immutable segment.
 * This is used when a mutable segment is moved to being a snapshot or pushed into a compaction
 * pipeline, that consists only of immutable segments.
 * The compaction may generate different type of immutable segment
 */
@InterfaceAudience.Private
public class ImmutableSegmentAdapter extends ImmutableSegment {

  final private MutableSegment adaptee;

  public ImmutableSegmentAdapter(MutableSegment segment) {
    super(segment);
    this.adaptee = segment;
  }

  @Override
  public KeyValueScanner getKeyValueScanner() {
    return new CollectionBackedScanner(adaptee.getCellSet(), adaptee.getComparator());
  }

  @Override
  public SegmentScanner getSegmentScanner(long readPoint) {
    return adaptee.getSegmentScanner(readPoint);
  }

  @Override
  public boolean isEmpty() {
    return adaptee.isEmpty();
  }

  @Override
  public int getCellsCount() {
    return adaptee.getCellsCount();
  }

  @Override
  public long add(Cell cell) {
    return adaptee.add(cell);
  }

  @Override
  public Cell getFirstAfter(Cell cell) {
    return adaptee.getFirstAfter(cell);
  }

  @Override
  public void close() {
    adaptee.close();
  }

  @Override
  public Cell maybeCloneWithAllocator(Cell cell) {
    return adaptee.maybeCloneWithAllocator(cell);
  }

  @Override
  public Segment setSize(long size) {
    adaptee.setSize(size);
    return this;
  }

  @Override
  public long getSize() {
    return adaptee.getSize();
  }

  @Override
  public long rollback(Cell cell) {
    return adaptee.rollback(cell);
  }

  @Override
  public CellSet getCellSet() {
    return adaptee.getCellSet();
  }

  @Override
  public void dump(Log log) {
    adaptee.dump(log);
  }
}
