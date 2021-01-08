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

import static org.apache.hadoop.hbase.util.ConcurrentMapUtils.computeIfAbsent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ImmutableByteArray;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Accounting of sequence ids per region and then by column family. So we can keep our accounting
 * current, call startCacheFlush and then finishedCacheFlush or abortCacheFlush so this instance can
 * keep abreast of the state of sequence id persistence. Also call update per append.
 * <p>
 * For the implementation, we assume that all the {@code encodedRegionName} passed in are gotten by
 * {@link org.apache.hadoop.hbase.client.RegionInfo#getEncodedNameAsBytes()}. So it is safe to use
 * it as a hash key. And for family name, we use {@link ImmutableByteArray} as key. This is because
 * hash based map is much faster than RBTree or CSLM and here we are on the critical write path. See
 * HBASE-16278 for more details.
 * </p>
 */
@InterfaceAudience.Private
class SequenceIdAccounting {
  private static final Logger LOG = LoggerFactory.getLogger(SequenceIdAccounting.class);

  /**
   * This lock ties all operations on {@link SequenceIdAccounting#flushingSequenceIds} and
   * {@link #lowestUnflushedSequenceIds} Maps. {@link #lowestUnflushedSequenceIds} has the
   * lowest outstanding sequence ids EXCEPT when flushing. When we flush, the current
   * lowest set for the region/column family are moved (atomically because of this lock) to
   * {@link #flushingSequenceIds}.
   * 
   * <p>The two Maps are tied by this locking object EXCEPT when we go to update the lowest
   * entry; see {@link #lowestUnflushedSequenceIds}. In here is a putIfAbsent call on
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
  private final ConcurrentMap<byte[], ConcurrentMap<ImmutableByteArray, Long>>
    lowestUnflushedSequenceIds = new ConcurrentHashMap<>();

  /**
   * Map of encoded region names and family names to their lowest or OLDEST sequence/edit id
   * currently being flushed out to hfiles. Entries are moved here from
   * {@link #lowestUnflushedSequenceIds} while the lock {@link #tieLock} is held
   * (so movement between the Maps is atomic).
   */
  private final Map<byte[], Map<ImmutableByteArray, Long>> flushingSequenceIds = new HashMap<>();

  /**
   * <p>
   * Map of region encoded names to the latest/highest region sequence id. Updated on each call to
   * append.
   * </p>
   * <p>
   * This map uses byte[] as the key, and uses reference equality. It works in our use case as we
   * use {@link org.apache.hadoop.hbase.client.RegionInfo#getEncodedNameAsBytes()} as keys. For a
   * given region, it always returns the same array.
   * </p>
   */
  private Map<byte[], Long> highestSequenceIds = new HashMap<>();

  /**
   * Returns the lowest unflushed sequence id for the region.
   * @return Lowest outstanding unflushed sequenceid for <code>encodedRegionName</code>. Will
   * return {@link HConstants#NO_SEQNUM} when none.
   */
  long getLowestSequenceId(final byte[] encodedRegionName) {
    synchronized (this.tieLock) {
      Map<?, Long> m = this.flushingSequenceIds.get(encodedRegionName);
      long flushingLowest = m != null ? getLowestSequenceId(m) : Long.MAX_VALUE;
      m = this.lowestUnflushedSequenceIds.get(encodedRegionName);
      long unflushedLowest = m != null ? getLowestSequenceId(m) : HConstants.NO_SEQNUM;
      return Math.min(flushingLowest, unflushedLowest);
    }
  }

  /**
   * @return Lowest outstanding unflushed sequenceid for <code>encodedRegionname</code> and
   *         <code>familyName</code>. Returned sequenceid may be for an edit currently being
   *         flushed.
   */
  long getLowestSequenceId(final byte[] encodedRegionName, final byte[] familyName) {
    ImmutableByteArray familyNameWrapper = ImmutableByteArray.wrap(familyName);
    synchronized (this.tieLock) {
      Map<ImmutableByteArray, Long> m = this.flushingSequenceIds.get(encodedRegionName);
      if (m != null) {
        Long lowest = m.get(familyNameWrapper);
        if (lowest != null) {
          return lowest;
        }
      }
      m = this.lowestUnflushedSequenceIds.get(encodedRegionName);
      if (m != null) {
        Long lowest = m.get(familyNameWrapper);
        if (lowest != null) {
          return lowest;
        }
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
    this.highestSequenceIds = new HashMap<>();
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
      ConcurrentMap<ImmutableByteArray, Long> m = getOrCreateLowestSequenceIds(encodedRegionName);
      for (byte[] familyName : families) {
        m.putIfAbsent(ImmutableByteArray.wrap(familyName), l);
      }
    }
  }

  /**
   * Clear all the records of the given region as it is going to be closed.
   * <p/>
   * We will call this once we get the region close marker. We need this because that, if we use
   * Durability.ASYNC_WAL, after calling startCacheFlush, we may still get some ongoing wal entries
   * that has not been processed yet, this will lead to orphan records in the
   * lowestUnflushedSequenceIds and then cause too many WAL files.
   * <p/>
   * See HBASE-23157 for more details.
   */
  void onRegionClose(byte[] encodedRegionName) {
    synchronized (tieLock) {
      this.lowestUnflushedSequenceIds.remove(encodedRegionName);
      Map<ImmutableByteArray, Long> flushing = this.flushingSequenceIds.remove(encodedRegionName);
      if (flushing != null) {
        LOG.warn("Still have flushing records when closing {}, {}",
          Bytes.toString(encodedRegionName),
          flushing.entrySet().stream().map(e -> e.getKey().toString() + "->" + e.getValue())
            .collect(Collectors.joining(",", "{", "}")));
      }
    }
    this.highestSequenceIds.remove(encodedRegionName);
  }

  /**
   * Update the store sequence id, e.g., upon executing in-memory compaction
   */
  void updateStore(byte[] encodedRegionName, byte[] familyName, Long sequenceId,
      boolean onlyIfGreater) {
    if (sequenceId == null) {
      return;
    }
    Long highest = this.highestSequenceIds.get(encodedRegionName);
    if (highest == null || sequenceId > highest) {
      this.highestSequenceIds.put(encodedRegionName, sequenceId);
    }
    ImmutableByteArray familyNameWrapper = ImmutableByteArray.wrap(familyName);
    synchronized (this.tieLock) {
      ConcurrentMap<ImmutableByteArray, Long> m = getOrCreateLowestSequenceIds(encodedRegionName);
      boolean replaced = false;
      while (!replaced) {
        Long oldSeqId = m.get(familyNameWrapper);
        if (oldSeqId == null) {
          m.put(familyNameWrapper, sequenceId);
          replaced = true;
        } else if (onlyIfGreater) {
          if (sequenceId > oldSeqId) {
            replaced = m.replace(familyNameWrapper, oldSeqId, sequenceId);
          } else {
            return;
          }
        } else { // replace even if sequence id is not greater than oldSeqId
          m.put(familyNameWrapper, sequenceId);
          return;
        }
      }
    }
  }

  ConcurrentMap<ImmutableByteArray, Long> getOrCreateLowestSequenceIds(byte[] encodedRegionName) {
    // Intentionally, this access is done outside of this.regionSequenceIdLock. Done per append.
    return computeIfAbsent(this.lowestUnflushedSequenceIds, encodedRegionName,
      ConcurrentHashMap::new);
  }

  /**
   * @param sequenceids Map to search for lowest value.
   * @return Lowest value found in <code>sequenceids</code>.
   */
  private static long getLowestSequenceId(Map<?, Long> sequenceids) {
    long lowest = HConstants.NO_SEQNUM;
    for (Map.Entry<? , Long> entry : sequenceids.entrySet()){
      if (entry.getKey().toString().equals("METAFAMILY")){
        continue;
      }
      Long sid = entry.getValue();
      if (lowest == HConstants.NO_SEQNUM || sid.longValue() < lowest) {
        lowest = sid.longValue();
      }
    }
    return lowest;
  }

  /**
   * @param src
   * @return New Map that has same keys as <code>src</code> but instead of a Map for a value, it
   *         instead has found the smallest sequence id and it returns that as the value instead.
   */
  private <T extends Map<?, Long>> Map<byte[], Long> flattenToLowestSequenceId(Map<byte[], T> src) {
    if (src == null || src.isEmpty()) {
      return null;
    }
    Map<byte[], Long> tgt = new HashMap<>();
    for (Map.Entry<byte[], T> entry : src.entrySet()) {
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
    Map<byte[],Long> familytoSeq = new HashMap<>();
    for (byte[] familyName : families){
      familytoSeq.put(familyName,HConstants.NO_SEQNUM);
    }
    return startCacheFlush(encodedRegionName,familytoSeq);
  }

  Long startCacheFlush(final byte[] encodedRegionName, final Map<byte[], Long> familyToSeq) {
    Map<ImmutableByteArray, Long> oldSequenceIds = null;
    Long lowestUnflushedInRegion = HConstants.NO_SEQNUM;
    synchronized (tieLock) {
      Map<ImmutableByteArray, Long> m = this.lowestUnflushedSequenceIds.get(encodedRegionName);
      if (m != null) {
        // NOTE: Removal from this.lowestUnflushedSequenceIds must be done in controlled
        // circumstance because another concurrent thread now may add sequenceids for this family
        // (see above in getOrCreateLowestSequenceId). Make sure you are ok with this. Usually it
        // is fine because updates are blocked when this method is called. Make sure!!!
        for (Map.Entry<byte[], Long> entry : familyToSeq.entrySet()) {
          ImmutableByteArray familyNameWrapper = ImmutableByteArray.wrap((byte[]) entry.getKey());
          Long seqId = null;
          if(entry.getValue() == HConstants.NO_SEQNUM) {
            seqId = m.remove(familyNameWrapper);
          } else {
            seqId = m.replace(familyNameWrapper, entry.getValue());
          }
          if (seqId != null) {
            if (oldSequenceIds == null) {
              oldSequenceIds = new HashMap<>();
            }
            oldSequenceIds.put(familyNameWrapper, seqId);
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

  void completeCacheFlush(byte[] encodedRegionName, long maxFlushedSeqId) {
    // This is a simple hack to avoid maxFlushedSeqId go backwards.
    // The system works fine normally, but if we make use of Durability.ASYNC_WAL and we are going
    // to flush all the stores, the maxFlushedSeqId will be next seq id of the region, but we may
    // still have some unsynced WAL entries in the ringbuffer after we call startCacheFlush, and
    // then it will be recorded as the lowestUnflushedSeqId by the above update method, which is
    // less than the current maxFlushedSeqId. And if next time we only flush the family with this
    // unusual lowestUnflushedSeqId, the maxFlushedSeqId will go backwards.
    // This is an unexpected behavior so we should fix it, otherwise it may cause unexpected
    // behavior in other area.
    // The solution here is a bit hack but fine. Just replace the lowestUnflushedSeqId with
    // maxFlushedSeqId + 1 if it is lesser. The meaning of maxFlushedSeqId is that, all edits less
    // than or equal to it have been flushed, i.e, persistent to HFile, so set
    // lowestUnflushedSequenceId to maxFlushedSeqId + 1 will not cause data loss.
    // And technically, using +1 is fine here. If the maxFlushesSeqId is just the flushOpSeqId, it
    // means we have flushed all the stores so the seq id for actual data should be at least plus 1.
    // And if we do not flush all the stores, then the maxFlushedSeqId is calculated by
    // lowestUnflushedSeqId - 1, so here let's plus the 1 back.
    Long wrappedSeqId = Long.valueOf(maxFlushedSeqId + 1);
    synchronized (tieLock) {
      this.flushingSequenceIds.remove(encodedRegionName);
      Map<ImmutableByteArray, Long> unflushed = lowestUnflushedSequenceIds.get(encodedRegionName);
      if (unflushed == null) {
        return;
      }
      for (Map.Entry<ImmutableByteArray, Long> e : unflushed.entrySet()) {
        if (e.getValue().longValue() <= maxFlushedSeqId) {
          e.setValue(wrappedSeqId);
        }
      }
    }
  }

  void abortCacheFlush(final byte[] encodedRegionName) {
    // Method is called when we are crashing down because failed write flush AND it is called
    // if we fail prepare. The below is for the fail prepare case; we restore the old sequence ids.
    Map<ImmutableByteArray, Long> flushing = null;
    Map<ImmutableByteArray, Long> tmpMap = new HashMap<>();
    // Here we are moving sequenceids from flushing back to unflushed; doing opposite of what
    // happened in startCacheFlush. During prepare phase, we have update lock on the region so
    // no edits should be coming in via append.
    synchronized (tieLock) {
      flushing = this.flushingSequenceIds.remove(encodedRegionName);
      if (flushing != null) {
        Map<ImmutableByteArray, Long> unflushed = getOrCreateLowestSequenceIds(encodedRegionName);
        for (Map.Entry<ImmutableByteArray, Long> e: flushing.entrySet()) {
          // Set into unflushed the 'old' oldest sequenceid and if any value in flushed with this
          // value, it will now be in tmpMap.
          tmpMap.put(e.getKey(), unflushed.put(e.getKey(), e.getValue()));
        }
      }
    }

    // Here we are doing some 'test' to see if edits are going in out of order. What is it for?
    // Carried over from old code.
    if (flushing != null) {
      for (Map.Entry<ImmutableByteArray, Long> e : flushing.entrySet()) {
        Long currentId = tmpMap.get(e.getKey());
        if (currentId != null && currentId.longValue() < e.getValue().longValue()) {
          String errorStr = Bytes.toString(encodedRegionName) + " family "
              + e.getKey().toString() + " acquired edits out of order current memstore seq="
              + currentId + ", previous oldest unflushed id=" + e.getValue();
          LOG.error(errorStr);
          Runtime.getRuntime().halt(1);
        }
      }
    }
  }

  /**
   * See if passed <code>sequenceids</code> are lower -- i.e. earlier -- than any outstanding
   * sequenceids, sequenceids we are holding on to in this accounting instance.
   * @param sequenceids Keyed by encoded region name. Cannot be null (doesn't make sense for it to
   *          be null).
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
      if (flushing != null && flushing.containsKey(e.getKey())) {
        oldestFlushing = flushing.get(e.getKey());
      }
      if (unflushed != null && unflushed.containsKey(e.getKey())) {
        oldestUnflushed = unflushed.get(e.getKey());
      }
      long min = Math.min(oldestFlushing, oldestUnflushed);
      if (min <= e.getValue()) {
        return false;
      }
    }
    return true;
  }

  /**
   * Iterates over the given Map and compares sequence ids with corresponding entries in
   * {@link #lowestUnflushedSequenceIds}. If a region in
   * {@link #lowestUnflushedSequenceIds} has a sequence id less than that passed in
   * <code>sequenceids</code> then return it.
   * @param sequenceids Sequenceids keyed by encoded region name.
   * @return stores of regions found in this instance with sequence ids less than those passed in.
   */
  Map<byte[], List<byte[]>> findLower(Map<byte[], Long> sequenceids) {
    Map<byte[], List<byte[]>> toFlush = null;
    // Keeping the old behavior of iterating unflushedSeqNums under oldestSeqNumsLock.
    synchronized (tieLock) {
      for (Map.Entry<byte[], Long> e : sequenceids.entrySet()) {
        Map<ImmutableByteArray, Long> m = this.lowestUnflushedSequenceIds.get(e.getKey());
        if (m == null) {
          continue;
        }
        for (Map.Entry<ImmutableByteArray, Long> me : m.entrySet()) {
          if (me.getValue() <= e.getValue()) {
            if (toFlush == null) {
              toFlush = new TreeMap(Bytes.BYTES_COMPARATOR);
            }
            toFlush.computeIfAbsent(e.getKey(), k -> new ArrayList<>())
              .add(Bytes.toBytes(me.getKey().toString()));
          }
        }
      }
    }
    return toFlush;
  }
}
