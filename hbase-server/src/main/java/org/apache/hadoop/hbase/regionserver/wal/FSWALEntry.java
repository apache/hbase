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
package org.apache.hadoop.hbase.regionserver.wal;

import static java.util.stream.Collectors.toCollection;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CollectionUtils;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * A WAL Entry for {@link AbstractFSWAL} implementation.  Immutable.
 * A subclass of {@link Entry} that carries extra info across the ring buffer such as
 * region sequence id (we want to use this later, just before we write the WAL to ensure region
 * edits maintain order).  The extra info added here is not 'serialized' as part of the WALEdit
 * hence marked 'transient' to underline this fact.  It also adds mechanism so we can wait on
 * the assign of the region sequence id.  See #stampRegionSequenceId().
 */
@InterfaceAudience.Private
class FSWALEntry extends Entry {
  // The below data members are denoted 'transient' just to highlight these are not persisted;
  // they are only in memory and held here while passing over the ring buffer.
  private final transient long txid;
  private final transient boolean inMemstore;
  private final transient RegionInfo regionInfo;
  private final transient Set<byte[]> familyNames;

  FSWALEntry(final long txid, final WALKeyImpl key, final WALEdit edit,
      final RegionInfo regionInfo, final boolean inMemstore) {
    super(key, edit);
    this.inMemstore = inMemstore;
    this.regionInfo = regionInfo;
    this.txid = txid;
    if (inMemstore) {
      // construct familyNames here to reduce the work of log sinker.
      Set<byte []> families = edit.getFamilies();
      this.familyNames = families != null? families: collectFamilies(edit.getCells());
    } else {
      this.familyNames = Collections.<byte[]>emptySet();
    }
  }

  @VisibleForTesting
  static Set<byte[]> collectFamilies(List<Cell> cells) {
    if (CollectionUtils.isEmpty(cells)) {
      return Collections.emptySet();
    } else {
      return cells.stream()
           .filter(v -> !CellUtil.matchingFamily(v, WALEdit.METAFAMILY))
           .collect(toCollection(() -> new TreeSet<>(CellComparator.getInstance()::compareFamilies)))
           .stream()
           .map(CellUtil::cloneFamily)
           .collect(toCollection(() -> new TreeSet<>(Bytes.BYTES_COMPARATOR)));
    }
  }

  @Override
  public String toString() {
    return "sequence=" + this.txid + ", " + super.toString();
  };

  boolean isInMemStore() {
    return this.inMemstore;
  }

  RegionInfo getRegionInfo() {
    return this.regionInfo;
  }

  /**
   * @return The transaction id of this edit.
   */
  long getTxid() {
    return this.txid;
  }

  /**
   * Here is where a WAL edit gets its sequenceid. SIDE-EFFECT is our stamping the sequenceid into
   * every Cell AND setting the sequenceid into the MVCC WriteEntry!!!!
   * @return The sequenceid we stamped on this edit.
   */
  long stampRegionSequenceId(MultiVersionConcurrencyControl.WriteEntry we) throws IOException {
    long regionSequenceId = we.getWriteNumber();
    if (!this.getEdit().isReplay() && inMemstore) {
      for (Cell c : getEdit().getCells()) {
        PrivateCellUtil.setSequenceId(c, regionSequenceId);
      }
    }

    getKey().setWriteEntry(we);
    return regionSequenceId;
  }

  /**
   * @return the family names which are effected by this edit.
   */
  Set<byte[]> getFamilyNames() {
    return familyNames;
  }
}
