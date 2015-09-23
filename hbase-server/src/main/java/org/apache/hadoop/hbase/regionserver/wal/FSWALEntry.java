/**
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
package org.apache.hadoop.hbase.regionserver.wal;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CollectionUtils;

import com.google.common.collect.Sets;

import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALKey;

/**
 * A WAL Entry for {@link FSHLog} implementation.  Immutable.
 * A subclass of {@link Entry} that carries extra info across the ring buffer such as
 * region sequence id (we want to use this later, just before we write the WAL to ensure region
 * edits maintain order).  The extra info added here is not 'serialized' as part of the WALEdit
 * hence marked 'transient' to underline this fact.  It also adds mechanism so we can wait on
 * the assign of the region sequence id.  See {@link #stampRegionSequenceId()}.
 */
@InterfaceAudience.Private
class FSWALEntry extends Entry {
  // The below data members are denoted 'transient' just to highlight these are not persisted;
  // they are only in memory and held here while passing over the ring buffer.
  private final transient long sequence;
  private final transient AtomicLong regionSequenceIdReference;
  private final transient boolean inMemstore;
  private final transient HTableDescriptor htd;
  private final transient HRegionInfo hri;
  private final transient List<Cell> memstoreCells;
  private final Set<byte[]> familyNames;

  FSWALEntry(final long sequence, final WALKey key, final WALEdit edit,
      final AtomicLong referenceToRegionSequenceId, final boolean inMemstore,
      final HTableDescriptor htd, final HRegionInfo hri, List<Cell> memstoreCells) {
    super(key, edit);
    this.regionSequenceIdReference = referenceToRegionSequenceId;
    this.inMemstore = inMemstore;
    this.htd = htd;
    this.hri = hri;
    this.sequence = sequence;
    this.memstoreCells = memstoreCells;
    if (inMemstore) {
      // construct familyNames here to reduce the work of log sinker.
      ArrayList<Cell> cells = this.getEdit().getCells();
      if (CollectionUtils.isEmpty(cells)) {
        this.familyNames = Collections.<byte[]> emptySet();
      } else {
        Set<byte[]> familySet = Sets.newTreeSet(Bytes.BYTES_COMPARATOR);
        for (Cell cell : cells) {
          if (!CellUtil.matchingFamily(cell, WALEdit.METAFAMILY)) {
            familySet.add(CellUtil.cloneFamily(cell));
          }
        }
        this.familyNames = Collections.unmodifiableSet(familySet);
      }
    } else {
      this.familyNames = Collections.<byte[]> emptySet();
    }
  }

  public String toString() {
    return "sequence=" + this.sequence + ", " + super.toString();
  };

  boolean isInMemstore() {
    return this.inMemstore;
  }

  HTableDescriptor getHTableDescriptor() {
    return this.htd;
  }

  HRegionInfo getHRegionInfo() {
    return this.hri;
  }

  /**
   * @return The sequence on the ring buffer when this edit was added.
   */
  long getSequence() {
    return this.sequence;
  }

  /**
   * Stamp this edit with a region edit/sequence id.
   * Call when safe to do so: i.e. the context is such that the increment on the passed in
   * {@link #regionSequenceIdReference} is guaranteed aligned w/ how appends are going into the
   * WAL.  This method works with {@link #getRegionSequenceId()}.  It will block waiting on this
   * method to be called.
   * @return The region edit/sequence id we set for this edit.
   * @throws IOException
   * @see #getRegionSequenceId()
   */
  long stampRegionSequenceId() throws IOException {
    long regionSequenceId = this.regionSequenceIdReference.incrementAndGet();
    if (!this.getEdit().isReplay() && !CollectionUtils.isEmpty(memstoreCells)) {
      for (Cell cell : this.memstoreCells) {
        CellUtil.setSequenceId(cell, regionSequenceId);
      }
    }
    WALKey key = getKey();
    key.setLogSeqNum(regionSequenceId);
    return regionSequenceId;
  }

  /**
   * @return the family names which are effected by this edit.
   */
  Set<byte[]> getFamilyNames() {
    return familyNames;
  }
}
