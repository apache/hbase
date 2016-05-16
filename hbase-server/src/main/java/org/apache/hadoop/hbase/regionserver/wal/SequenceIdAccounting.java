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

import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Accounting of sequence ids per region and then by column family. So we can our accounting
 * current, call startCacheFlush and then finishedCacheFlush or abortCacheFlush so this instance
 * can keep abreast of the state of sequence id persistence. Also call update per append.
 */
class SequenceIdAccounting {
  private static final Log LOG = LogFactory.getLog(SequenceIdAccounting.class);
  /**
   * This lock ties all operations on {@link SequenceIdAccounting#flushingSequenceIds} and
   * {@link #lowestUnflushedSequenceIds} Maps. {@link #lowestUnflushedSequenceIds} has the
   * lowest outstanding sequence ids EXCEPT when flushing. When we flush, the current
   * lowest set for the region/column family are moved (atomically because of this lock) to
   * {@link #flushingSequenceIds}.
   * 
   * <p>The two Maps are tied by this locking object EXCEPT when we go to update the lowest
   * entry; see {@link #lowest(byte[], Set, Long)}. In here is a putIfAbsent call on
   * {@link #lowestUnflushedSequenceIds}. In this latter case, we will add this lowest
   * sequence id if we find that there is no entry for the current column family. There will be no
   * entry only if we just came up OR we have moved aside current set of lowest sequence ids
   * because the current set are being flushed (by putting them into {@link #flushingSequenceIds}).
   * This is how we pick up the next 'lowest' sequence id per region per column family to be used
   * figuring what is in the next flush.
   */
  private final Object tieLock = new Object();

  /**
   * Map of encoded region names and family names to their OLDEST -- i.e. their first,
   * the longest-lived, their 'earliest', the 'lowest' -- sequence id.
   *
   * <p>When we flush, the current lowest sequence ids get cleared and added to
   * {@link #flushingSequenceIds}. The next append that comes in, is then added
   * here to {@link #lowestUnflushedSequenceIds} as the next lowest sequenceid.
   *
   * <p>If flush fails, currently server is aborted so no need to restore previous sequence ids.
   * <p>Needs to be concurrent Maps because we use putIfAbsent updating oldest.
   */
  private final ConcurrentMap<byte[], ConcurrentMap<byte[], Long>> lowestUnflushedSequenceIds
    = new ConcurrentSkipListMap<byte[], ConcurrentMap<byte[], Long>>(
      Bytes.BYTES_COMPARATOR);

  /**
   * Map of encoded region names and family names to their lowest or OLDEST sequence/edit id
   * currently being flushed out to hfiles. Entries are moved here from
   * {@link #lowestUnflushedSequenceIds} while the lock {@link #tieLock} is held
   * (so movement between the Maps is atomic).
   */
  private final Map<byte[], Map<byte[], Long>> flushingSequenceIds =
    new TreeMap<byte[], Map<byte[], Long>>(Bytes.BYTES_COMPARATOR);

 /**
  * Map of region encoded names to the latest/highest region sequence id.  Updated on each
  * call to append.
  * <p>
  * This map uses byte[] as the key, and uses reference equality. It works in our use case as we
  * use {@link HRegionInfo#getEncodedNameAsBytes()} as keys. For a given region, it always returns
  * the same array.
  */
  private Map<byte[], Long> highestSequenceIds = new HashMap<byte[], Long>();

  /**
   * Returns the lowest unflushed sequence id for the region.
   * @param encodedRegionName
   * @return Lowest outstanding unflushed sequenceid for <code>encodedRegionName</code>. Will
   * return {@link HConstants#NO_SEQNUM} when none.
   */
  long getLowestSequenceId(final byte [] encodedRegionName) {
    synchronized (this.tieLock)  {
      Map<byte[], Long> m = this.flushingSequenceIds.get(encodedRegionName);
      long flushingLowest = m != null? getLowestSequenceId(m): Long.MAX_VALUE;
      m = this.lowestUnflushedSequenceIds.get(encodedRegionName);
      long unflushedLowest = m != null? getLowestSequenceId(m): HConstants.NO_SEQNUM;
      return Math.min(flushingLowest, unflushedLowest);
    }
  }

  /**
   * @param encodedRegionName
   * @param familyName 
   * @return Lowest outstanding unflushed sequenceid for <code>encodedRegionname</code> and
   * <code>familyName</code>. Returned sequenceid may be for an edit currently being flushed.
   */
  long getLowestSequenceId(final byte [] encodedRegionName, final byte [] familyName) {
    synchronized (this.tieLock) {
      Map<byte[], Long> m = this.flushingSequenceIds.get(encodedRegionName);
      if (m != null) {
        Long lowest = m.get(familyName);
        if (lowest != null) return lowest;
      }
      m = this.lowestUnflushedSequenceIds.get(encodedRegionName);
      if (m != null) {
        Long lowest = m.get(familyName);
        if (lowest != null) return lowest;
      }
    }
    return HConstants.NO_SEQNUM;
  }

  /**
   * Reset the accounting of highest sequenceid by regionname.
   * @return Return the previous accounting Map of regions to the last sequence id written into
   * each.
   */
  Map<byte[], Long> resetHighest() {
    Map<byte[], Long> old = this.highestSequenceIds;
    this.highestSequenceIds = new HashMap<byte[], Long>();
    return old;
  }

  /**
   * We've been passed a new sequenceid for the region. Set it as highest seen for this region and
   * if we are to record oldest, or lowest sequenceids, save it as oldest seen if nothing
   * currently older.
   * @param encodedRegionName
   * @param families
   * @param sequenceid
   * @param lowest Whether to keep running account of oldest sequence id.
   */
  void update(byte[] encodedRegionName, Set<byte[]> families, long sequenceid,
      final boolean lowest) {
    Long l = Long.valueOf(sequenceid);
    this.highestSequenceIds.put(encodedRegionName, l);
    if (lowest) {
      ConcurrentMap<byte[], Long> m = getOrCreateLowestSequenceIds(encodedRegionName);
      for (byte[] familyName : families) {
        m.putIfAbsent(familyName, l);
      }
    }
  }

  /**
   * Update the store sequence id, e.g., upon executing in-memory compaction
   */
  void updateStore(byte[] encodedRegionName, byte[] familyName, Long sequenceId,
      boolean onlyIfGreater) {
    if(sequenceId == null) return;
    Long highest = this.highestSequenceIds.get(encodedRegionName);
    if(highest == null || sequenceId > highest) {
      this.highestSequenceIds.put(encodedRegionName,sequenceId);
    }
    synchronized (this.tieLock) {
      ConcurrentMap<byte[], Long> m = getOrCreateLowestSequenceIds(encodedRegionName);
      boolean replaced = false;
      while (!replaced) {
        Long oldSeqId = m.get(familyName);
        if (oldSeqId == null) {
          m.put(familyName, sequenceId);
          replaced = true;
        } else if (onlyIfGreater) {
          if (sequenceId > oldSeqId) {
            replaced = m.replace(familyName, oldSeqId, sequenceId);
          } else {
            return;
          }
        } else { // replace even if sequence id is not greater than oldSeqId
          m.put(familyName, sequenceId);
          return;
        }
      }
    }
  }

  ConcurrentMap<byte[], Long> getOrCreateLowestSequenceIds(byte[] encodedRegionName) {
    // Intentionally, this access is done outside of this.regionSequenceIdLock. Done per append.
    ConcurrentMap<byte[], Long> m = this.lowestUnflushedSequenceIds.get(encodedRegionName);
    if (m != null) return m;
    m = new ConcurrentSkipListMap<byte[], Long>(Bytes.BYTES_COMPARATOR);
    // Another thread may have added it ahead of us.
    ConcurrentMap<byte[], Long> alreadyPut =
        this.lowestUnflushedSequenceIds.putIfAbsent(encodedRegionName, m);
    return alreadyPut == null? m : alreadyPut;
  }

  /**
   * @param sequenceids Map to search for lowest value.
   * @return Lowest value found in <code>sequenceids</code>.
   */
  static long getLowestSequenceId(Map<byte[], Long> sequenceids) {
    long lowest = HConstants.NO_SEQNUM;
    for (Long sid: sequenceids.values()) {
      if (lowest == HConstants.NO_SEQNUM || sid.longValue() < lowest) {
        lowest = sid.longValue();
      }
    }
    return lowest;
  }

  /**
   * @param src
   * @return New Map that has same keys as <code>src</code> but instead of a Map for a value, it
   * instead has found the smallest sequence id and it returns that as the value instead.
   */
  private <T extends Map<byte[], Long>> Map<byte[], Long> flattenToLowestSequenceId(
      Map<byte[], T> src) {
    if (src == null || src.isEmpty()) return null;
    Map<byte[], Long> tgt = Maps.newHashMap();
    for (Map.Entry<byte[], T> entry: src.entrySet()) {
      long lowestSeqId = getLowestSequenceId(entry.getValue());
      if (lowestSeqId != HConstants.NO_SEQNUM) {
        tgt.put(entry.getKey(), lowestSeqId);
      }
    }
    return tgt;
  }

  /**
   * @param encodedRegionName Region to flush.
   * @param families Families to flush. May be a subset of all families in the region.
   * @return Returns {@link HConstants#NO_SEQNUM} if we are flushing the whole region OR if
   * we are flushing a subset of all families but there are no edits in those families not
   * being flushed; in other words, this is effectively same as a flush of all of the region
   * though we were passed a subset of regions. Otherwise, it returns the sequence id of the
   * oldest/lowest outstanding edit.
   */
  Long startCacheFlush(final byte[] encodedRegionName, final Set<byte[]> families) {
    Map<byte[], Long> oldSequenceIds = null;
    Long lowestUnflushedInRegion = HConstants.NO_SEQNUM;
    synchronized (tieLock) {
      Map<byte[], Long> m = this.lowestUnflushedSequenceIds.get(encodedRegionName);
      if (m != null) {
        // NOTE: Removal from this.lowestUnflushedSequenceIds must be done in controlled
        // circumstance because another concurrent thread now may add sequenceids for this family
        // (see above in getOrCreateLowestSequenceId). Make sure you are ok with this. Usually it
        // is fine because updates are blocked when this method is called. Make sure!!!
        for (byte[] familyName: families) {
          Long seqId = m.remove(familyName);
          if (seqId != null) {
            if (oldSequenceIds == null) oldSequenceIds = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
            oldSequenceIds.put(familyName, seqId);
          }
        }
        if (oldSequenceIds != null && !oldSequenceIds.isEmpty()) {
          if (this.flushingSequenceIds.put(encodedRegionName, oldSequenceIds) != null) {
            LOG.warn("Flushing Map not cleaned up for " + Bytes.toString(encodedRegionName) +
              ", sequenceid=" + oldSequenceIds);
          }
        }
        if (m.isEmpty()) {
          // Remove it otherwise it will be in oldestUnflushedStoreSequenceIds for ever
          // even if the region is already moved to other server.
          // Do not worry about data racing, we held write lock of region when calling
          // startCacheFlush, so no one can add value to the map we removed.
          this.lowestUnflushedSequenceIds.remove(encodedRegionName);
        } else {
          // Flushing a subset of the region families. Return the sequence id of the oldest entry.
          lowestUnflushedInRegion = Collections.min(m.values());
        }
      }
    }
    // Do this check outside lock.
    if (oldSequenceIds != null && oldSequenceIds.isEmpty()) {
      // TODO: if we have no oldStoreSeqNum, and WAL is not disabled, presumably either
      // the region is already flushing (which would make this call invalid), or there
      // were no appends after last flush, so why are we starting flush? Maybe we should
      // assert not empty. Less rigorous, but safer, alternative is telling the caller to stop.
      // For now preserve old logic.
      LOG.warn("Couldn't find oldest sequenceid for " + Bytes.toString(encodedRegionName));
    }
    return lowestUnflushedInRegion;
  }

  void completeCacheFlush(final byte [] encodedRegionName) {
    synchronized (tieLock) {
      this.flushingSequenceIds.remove(encodedRegionName);
    }
  }

  void abortCacheFlush(final byte[] encodedRegionName) {
    // Method is called when we are crashing down because failed write flush AND it is called
    // if we fail prepare. The below is for the fail prepare case; we restore the old sequence ids.
    Map<byte[], Long> flushing = null;
    Map<byte[], Long> tmpMap = new TreeMap<byte[], Long>(Bytes.BYTES_COMPARATOR);
    // Here we are moving sequenceids from flushing back to unflushed; doing opposite of what
    // happened in startCacheFlush. During prepare phase, we have update lock on the region so
    // no edits should be coming in via append.
    synchronized (tieLock) {
      flushing = this.flushingSequenceIds.remove(encodedRegionName);
      if (flushing != null) {
        Map<byte[], Long> unflushed = getOrCreateLowestSequenceIds(encodedRegionName);
        for (Map.Entry<byte[], Long> e: flushing.entrySet()) {
          // Set into unflushed the 'old' oldest sequenceid and if any value in flushed with this
          // value, it will now be in tmpMap.
          tmpMap.put(e.getKey(), unflushed.put(e.getKey(), e.getValue()));
        }
      }
    }

    // Here we are doing some 'test' to see if edits are going in out of order. What is it for?
    // Carried over from old code.
    if (flushing != null) {
      for (Map.Entry<byte[], Long> e : flushing.entrySet()) {
        Long currentId = tmpMap.get(e.getKey());
        if (currentId != null && currentId.longValue() <= e.getValue().longValue()) {
          String errorStr = Bytes.toString(encodedRegionName) + " family " +
            Bytes.toString(e.getKey()) + " acquired edits out of order current memstore seq=" +
              currentId + ", previous oldest unflushed id=" + e.getValue();
          LOG.error(errorStr);
          Runtime.getRuntime().halt(1);
        }
      }
    }
  }

  /**
   * See if passed <code>sequenceids</code> are lower -- i.e. earlier -- than any outstanding
   * sequenceids, sequenceids we are holding on to in this accounting instance.
   * @param sequenceids Keyed by encoded region name. Cannot be null (doesn't make
   * sense for it to be null).
   * @return true if all sequenceids are lower, older than, the old sequenceids in this instance.
   */
   boolean areAllLower(Map<byte[], Long> sequenceids) {
     Map<byte[], Long> flushing = null;
     Map<byte[], Long> unflushed = null;
     synchronized (this.tieLock) {
       // Get a flattened -- only the oldest sequenceid -- copy of current flushing and unflushed
       // data structures to use in tests below.
       flushing = flattenToLowestSequenceId(this.flushingSequenceIds);
       unflushed = flattenToLowestSequenceId(this.lowestUnflushedSequenceIds);
     }
    for (Map.Entry<byte[], Long> e : sequenceids.entrySet()) {
      long oldestFlushing = Long.MAX_VALUE;
      long oldestUnflushed = Long.MAX_VALUE;
      if (flushing != null) {
        if (flushing.containsKey(e.getKey())) oldestFlushing = flushing.get(e.getKey());
      }
      if (unflushed != null) {
        if (unflushed.containsKey(e.getKey())) oldestUnflushed = unflushed.get(e.getKey());
      }
      long min = Math.min(oldestFlushing, oldestUnflushed);
      if (min <= e.getValue()) return false;
    }
    return true;
  }

   /**
    * Iterates over the given Map and compares sequence ids with corresponding
    * entries in {@link #oldestUnflushedRegionSequenceIds}. If a region in
    * {@link #oldestUnflushedRegionSequenceIds} has a sequence id less than that passed
    * in <code>sequenceids</code> then return it.
    * @param sequenceids Sequenceids keyed by encoded region name.
    * @return regions found in this instance with sequence ids less than those passed in.
    */
   byte[][] findLower(Map<byte[], Long> sequenceids) {
     List<byte[]> toFlush = null;
     // Keeping the old behavior of iterating unflushedSeqNums under oldestSeqNumsLock.
     synchronized (tieLock) {
       for (Map.Entry<byte[], Long> e: sequenceids.entrySet()) {
         Map<byte[], Long> m = this.lowestUnflushedSequenceIds.get(e.getKey());
         if (m == null) continue;
         // The lowest sequence id outstanding for this region.
         long lowest = getLowestSequenceId(m);
         if (lowest != HConstants.NO_SEQNUM && lowest <= e.getValue()) {
           if (toFlush == null) toFlush = new ArrayList<byte[]>();
           toFlush.add(e.getKey());
         }
       }
     }
     return toFlush == null? null: toFlush.toArray(new byte[][] { HConstants.EMPTY_BYTE_ARRAY });
   }
}