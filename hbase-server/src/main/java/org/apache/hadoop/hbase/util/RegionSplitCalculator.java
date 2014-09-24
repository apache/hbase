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
package org.apache.hadoop.hbase.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes.ByteArrayComparator;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;

/**
 * This is a generic region split calculator. It requires Ranges that provide
 * start, end, and a comparator. It works in two phases -- the first adds ranges
 * and rejects backwards ranges. Then one calls calcRegions to generate the
 * multimap that has a start split key as a key and possibly multiple Ranges as
 * members.
 * 
 * To traverse, one normally would get the split set, and iterate through the
 * calcRegions. Normal regions would have only one entry, holes would have zero,
 * and any overlaps would have multiple entries.
 * 
 * The interface is a bit cumbersome currently but is exposed this way so that
 * clients can choose how to iterate through the region splits.
 * 
 * @param <R>
 */
@InterfaceAudience.Private
public class RegionSplitCalculator<R extends KeyRange> {
  final static Log LOG = LogFactory.getLog(RegionSplitCalculator.class);

  private final Comparator<R> rangeCmp;
  /**
   * This contains a sorted set of all the possible split points
   * 
   * Invariant: once populated this has 0 entries if empty or at most n+1 values
   * where n == number of added ranges.
   */
  private final TreeSet<byte[]> splits = new TreeSet<byte[]>(BYTES_COMPARATOR);

  /**
   * This is a map from start key to regions with the same start key.
   * 
   * Invariant: This always have n values in total
   */
  private final Multimap<byte[], R> starts = ArrayListMultimap.create();

  /**
   * SPECIAL CASE
   */
  private final static byte[] ENDKEY = null;

  public RegionSplitCalculator(Comparator<R> cmp) {
    rangeCmp = cmp;
  }

  public final static Comparator<byte[]> BYTES_COMPARATOR = new ByteArrayComparator() {
    @Override
    public int compare(byte[] l, byte[] r) {
      if (l == null && r == null)
        return 0;
      if (l == null)
        return 1;
      if (r == null)
        return -1;
      return super.compare(l, r);
    }
  };

  /**
   * SPECIAL CASE wrapper for empty end key
   * 
   * @return ENDKEY if end key is empty, else normal endkey.
   */
  private static <R extends KeyRange> byte[] specialEndKey(R range) {
    byte[] end = range.getEndKey();
    if (end.length == 0) {
      return ENDKEY;
    }
    return end;
  }

  /**
   * Adds an edge to the split calculator
   * 
   * @return true if is included, false if backwards/invalid
   */
  public boolean add(R range) {
    byte[] start = range.getStartKey();
    byte[] end = specialEndKey(range);

    if (end != ENDKEY && Bytes.compareTo(start, end) > 0) {
      // don't allow backwards edges
      LOG.debug("attempted to add backwards edge: " + Bytes.toString(start)
          + " " + Bytes.toString(end));
      return false;
    }

    splits.add(start);
    splits.add(end);
    starts.put(start, range);
    return true;
  }

  /**
   * Generates a coverage multimap from split key to Regions that start with the
   * split key.
   * 
   * @return coverage multimap
   */
  public Multimap<byte[], R> calcCoverage() {
    // This needs to be sorted to force the use of the comparator on the values,
    // otherwise byte array comparison isn't used
    Multimap<byte[], R> regions = TreeMultimap.create(BYTES_COMPARATOR,
        rangeCmp);

    // march through all splits from the start points
    for (Entry<byte[], Collection<R>> start : starts.asMap().entrySet()) {
      byte[] key = start.getKey();
      for (R r : start.getValue()) {
        regions.put(key, r);

        for (byte[] coveredSplit : splits.subSet(r.getStartKey(),
            specialEndKey(r))) {
          regions.put(coveredSplit, r);
        }
      }
    }
    return regions;
  }

  public TreeSet<byte[]> getSplits() {
    return splits;
  }

  public Multimap<byte[], R> getStarts() {
    return starts;
  }

  /**
   * Find specified number of top ranges in a big overlap group.
   * It could return less if there are not that many top ranges.
   * Once these top ranges are excluded, the big overlap group will
   * be broken into ranges with no overlapping, or smaller overlapped
   * groups, and most likely some holes.
   *
   * @param bigOverlap a list of ranges that overlap with each other
   * @param count the max number of ranges to find
   * @return a list of ranges that overlap with most others
   */
  public static <R extends KeyRange> List<R>
      findBigRanges(Collection<R> bigOverlap, int count) {
    List<R> bigRanges = new ArrayList<R>();

    // The key is the count of overlaps,
    // The value is a list of ranges that have that many overlaps
    TreeMap<Integer, List<R>> overlapRangeMap = new TreeMap<Integer, List<R>>();
    for (R r: bigOverlap) {
      // Calculates the # of overlaps for each region
      // and populates rangeOverlapMap
      byte[] startKey = r.getStartKey();
      byte[] endKey = specialEndKey(r);

      int overlappedRegions = 0;
      for (R rr: bigOverlap) {
        byte[] start = rr.getStartKey();
        byte[] end = specialEndKey(rr);

        if (BYTES_COMPARATOR.compare(startKey, end) < 0
            && BYTES_COMPARATOR.compare(endKey, start) > 0) {
          overlappedRegions++;
        }
      }

      // One region always overlaps with itself,
      // so overlappedRegions should be more than 1
      // for actual overlaps.
      if (overlappedRegions > 1) {
        Integer key = Integer.valueOf(overlappedRegions);
        List<R> ranges = overlapRangeMap.get(key);
        if (ranges == null) {
          ranges = new ArrayList<R>();
          overlapRangeMap.put(key, ranges);
        }
        ranges.add(r);
      }
    }
    int toBeAdded = count;
    for (Integer key: overlapRangeMap.descendingKeySet()) {
      List<R> chunk = overlapRangeMap.get(key);
      int chunkSize = chunk.size();
      if (chunkSize <= toBeAdded) {
        bigRanges.addAll(chunk);
        toBeAdded -= chunkSize;
        if (toBeAdded > 0) continue;
      } else {
        // Try to use the middle chunk in case the overlapping is
        // chained, for example: [a, c), [b, e), [d, g), [f h)...
        // In such a case, sideline the middle chunk will break
        // the group efficiently.
        int start = (chunkSize - toBeAdded)/2;
        int end = start + toBeAdded;
        for (int i = start; i < end; i++) {
          bigRanges.add(chunk.get(i));
        }
      }
      break;
    }
    return bigRanges;
  }
}
