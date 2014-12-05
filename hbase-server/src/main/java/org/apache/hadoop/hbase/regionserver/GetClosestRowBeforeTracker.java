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

import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * State and utility processing {@link HRegion#getClosestRowBefore(byte[], byte[])}.
 * Like {@link ScanQueryMatcher} and {@link ScanDeleteTracker} but does not
 * implement the {@link DeleteTracker} interface since state spans rows (There
 * is no update nor reset method).
 */
@InterfaceAudience.Private
class GetClosestRowBeforeTracker {
  private final KeyValue targetkey;
  // Any cell w/ a ts older than this is expired.
  private final long now;
  private final long oldestUnexpiredTs;
  private Cell candidate = null;
  private final KVComparator kvcomparator;
  // Flag for whether we're doing getclosest on a metaregion.
  private final boolean metaregion;
  // Offset and length into targetkey demarking table name (if in a metaregion).
  private final int rowoffset;
  private final int tablenamePlusDelimiterLength;

  // Deletes keyed by row.  Comparator compares on row portion of KeyValue only.
  private final NavigableMap<KeyValue, NavigableSet<KeyValue>> deletes;

  /**
   * @param c
   * @param kv Presume first on row: i.e. empty column, maximum timestamp and
   * a type of Type.Maximum
   * @param ttl Time to live in ms for this Store
   * @param metaregion True if this is hbase:meta or -ROOT- region.
   */
  GetClosestRowBeforeTracker(final KVComparator c, final KeyValue kv,
      final long ttl, final boolean metaregion) {
    super();
    this.metaregion = metaregion;
    this.targetkey = kv;
    // If we are in a metaregion, then our table name is the prefix on the
    // targetkey.
    this.rowoffset = kv.getRowOffset();
    int l = -1;
    if (metaregion) {
      l = KeyValue.getDelimiter(kv.getRowArray(), rowoffset, kv.getRowLength(),
        HConstants.DELIMITER) - this.rowoffset;
    }
    this.tablenamePlusDelimiterLength = metaregion? l + 1: -1;
    this.now = System.currentTimeMillis();
    this.oldestUnexpiredTs = now - ttl;
    this.kvcomparator = c;
    KeyValue.RowOnlyComparator rc = new KeyValue.RowOnlyComparator(this.kvcomparator);
    this.deletes = new TreeMap<KeyValue, NavigableSet<KeyValue>>(rc);
  }

  /*
   * Add the specified KeyValue to the list of deletes.
   * @param kv
   */
  private void addDelete(final Cell kv) {
    NavigableSet<KeyValue> rowdeletes = this.deletes.get(kv);
    if (rowdeletes == null) {
      rowdeletes = new TreeSet<KeyValue>(this.kvcomparator);
      this.deletes.put(KeyValueUtil.ensureKeyValue(kv), rowdeletes);
    }
    rowdeletes.add(KeyValueUtil.ensureKeyValue(kv));
  }

  /*
   * @param kv Adds candidate if nearer the target than previous candidate.
   * @return True if updated candidate.
   */
  private boolean addCandidate(final Cell kv) {
    if (!isDeleted(kv) && isBetterCandidate(kv)) {
      this.candidate = kv;
      return true;
    }
    return false;
  }

  boolean isBetterCandidate(final Cell contender) {
    return this.candidate == null ||
      (this.kvcomparator.compareRows(this.candidate, contender) < 0 &&
        this.kvcomparator.compareRows(contender, this.targetkey) <= 0);
  }

  /*
   * Check if specified KeyValue buffer has been deleted by a previously
   * seen delete.
   * @param kv
   * @return true is the specified KeyValue is deleted, false if not
   */
  private boolean isDeleted(final Cell kv) {
    if (this.deletes.isEmpty()) return false;
    NavigableSet<KeyValue> rowdeletes = this.deletes.get(kv);
    if (rowdeletes == null || rowdeletes.isEmpty()) return false;
    return isDeleted(kv, rowdeletes);
  }

  /**
   * Check if the specified KeyValue buffer has been deleted by a previously
   * seen delete.
   * @param kv
   * @param ds
   * @return True is the specified KeyValue is deleted, false if not
   */
  public boolean isDeleted(final Cell kv, final NavigableSet<KeyValue> ds) {
    if (deletes == null || deletes.isEmpty()) return false;
    for (KeyValue d: ds) {
      long kvts = kv.getTimestamp();
      long dts = d.getTimestamp();
      if (CellUtil.isDeleteFamily(d)) {
        if (kvts <= dts) return true;
        continue;
      }
      // Check column
      int ret = Bytes.compareTo(kv.getQualifierArray(), kv.getQualifierOffset(),
          kv.getQualifierLength(),
        d.getQualifierArray(), d.getQualifierOffset(), d.getQualifierLength());
      if (ret <= -1) {
        // This delete is for an earlier column.
        continue;
      } else if (ret >= 1) {
        // Beyond this kv.
        break;
      }
      // Check Timestamp
      if (kvts > dts) return false;

      // Check Type
      switch (KeyValue.Type.codeToType(d.getType())) {
        case Delete: return kvts == dts;
        case DeleteColumn: return true;
        default: continue;
      }
    }
    return false;
  }

  /**
   * @param cell
   * @return true if the cell is expired
   */
  public boolean isExpired(final Cell cell) {
    return cell.getTimestamp() < this.oldestUnexpiredTs ||
      HStore.isCellTTLExpired(cell, this.oldestUnexpiredTs, this.now);
  }

  /*
   * Handle keys whose values hold deletes.
   * Add to the set of deletes and then if the candidate keys contain any that
   * might match, then check for a match and remove it.  Implies candidates
   * is made with a Comparator that ignores key type.
   * @param kv
   * @return True if we removed <code>k</code> from <code>candidates</code>.
   */
  boolean handleDeletes(final Cell kv) {
    addDelete(kv);
    boolean deleted = false;
    if (!hasCandidate()) return deleted;
    if (isDeleted(this.candidate)) {
      this.candidate = null;
      deleted = true;
    }
    return deleted;
  }

  /**
   * Do right thing with passed key, add to deletes or add to candidates.
   * @param kv
   * @return True if we added a candidate
   */
  boolean handle(final Cell kv) {
    if (KeyValueUtil.ensureKeyValue(kv).isDelete()) {
      handleDeletes(kv);
      return false;
    }
    return addCandidate(kv);
  }

  /**
   * @return True if has candidate
   */
  public boolean hasCandidate() {
    return this.candidate != null;
  }

  /**
   * @return Best candidate or null.
   */
  public Cell getCandidate() {
    return this.candidate;
  }

  public KeyValue getTargetKey() {
    return this.targetkey;
  }

  /**
   * @param kv Current kv
   * @param firstOnRow on row kv.
   * @return True if we went too far, past the target key.
   */
  boolean isTooFar(final Cell kv, final Cell firstOnRow) {
    return this.kvcomparator.compareRows(kv, firstOnRow) > 0;
  }

  boolean isTargetTable(final Cell kv) {
    if (!metaregion) return true;
    // Compare start of keys row.  Compare including delimiter.  Saves having
    // to calculate where tablename ends in the candidate kv.
    return Bytes.compareTo(this.targetkey.getRowArray(), this.rowoffset,
        this.tablenamePlusDelimiterLength,
      kv.getRowArray(), kv.getRowOffset(), this.tablenamePlusDelimiterLength) == 0;
  }
}
