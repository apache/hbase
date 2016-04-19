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

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.compactions.StripeCompactionPolicy;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ConcatenatedLists;
import org.apache.hadoop.util.StringUtils;


/**
 * Stripe implementation of StoreFileManager.
 * Not thread safe - relies on external locking (in HStore). Collections that this class
 * returns are immutable or unique to the call, so they should be safe.
 * Stripe store splits the key space of the region into non-overlapping stripes, as well as
 * some recent files that have all the keys (level 0). Each stripe contains a set of files.
 * When L0 is compacted, it's split into the files corresponding to existing stripe boundaries,
 * that can thus be added to stripes.
 * When scan or get happens, it only has to read the files from the corresponding stripes.
 * See StripeCompationPolicy on how the stripes are determined; this class doesn't care.
 *
 * This class should work together with StripeCompactionPolicy and StripeCompactor.
 * With regard to how they work, we make at least the following (reasonable) assumptions:
 *  - Compaction produces one file per new stripe (if any); that is easy to change.
 *  - Compaction has one contiguous set of stripes both in and out, except if L0 is involved.
 */
@InterfaceAudience.Private
public class StripeStoreFileManager
  implements StoreFileManager, StripeCompactionPolicy.StripeInformationProvider {
  static final Log LOG = LogFactory.getLog(StripeStoreFileManager.class);

  /**
   * The file metadata fields that contain the stripe information.
   */
  public static final byte[] STRIPE_START_KEY = Bytes.toBytes("STRIPE_START_KEY");
  public static final byte[] STRIPE_END_KEY = Bytes.toBytes("STRIPE_END_KEY");

  private final static Bytes.RowEndKeyComparator MAP_COMPARATOR = new Bytes.RowEndKeyComparator();

  /**
   * The key value used for range boundary, indicating that the boundary is open (i.e. +-inf).
   */
  public final static byte[] OPEN_KEY = HConstants.EMPTY_BYTE_ARRAY;
  final static byte[] INVALID_KEY = null;

  /**
   * The state class. Used solely to replace results atomically during
   * compactions and avoid complicated error handling.
   */
  private static class State {
    /**
     * The end rows of each stripe. The last stripe end is always open-ended, so it's not stored
     * here. It is invariant that the start row of the stripe is the end row of the previous one
     * (and is an open boundary for the first one).
     */
    public byte[][] stripeEndRows = new byte[0][];

    /**
     * Files by stripe. Each element of the list corresponds to stripeEndRow element with the
     * same index, except the last one. Inside each list, the files are in reverse order by
     * seqNum. Note that the length of this is one higher than that of stripeEndKeys.
     */
    public ArrayList<ImmutableList<StoreFile>> stripeFiles
      = new ArrayList<ImmutableList<StoreFile>>();
    /** Level 0. The files are in reverse order by seqNum. */
    public ImmutableList<StoreFile> level0Files = ImmutableList.<StoreFile>of();

    /** Cached list of all files in the structure, to return from some calls */
    public ImmutableList<StoreFile> allFilesCached = ImmutableList.<StoreFile>of();
  }
  private State state = null;

  /** Cached file metadata (or overrides as the case may be) */
  private HashMap<StoreFile, byte[]> fileStarts = new HashMap<StoreFile, byte[]>();
  private HashMap<StoreFile, byte[]> fileEnds = new HashMap<StoreFile, byte[]>();
  /** Normally invalid key is null, but in the map null is the result for "no key"; so use
   * the following constant value in these maps instead. Note that this is a constant and
   * we use it to compare by reference when we read from the map. */
  private static final byte[] INVALID_KEY_IN_MAP = new byte[0];

  private final KVComparator kvComparator;
  private StripeStoreConfig config;

  private final int blockingFileCount;

  public StripeStoreFileManager(
      KVComparator kvComparator, Configuration conf, StripeStoreConfig config) {
    this.kvComparator = kvComparator;
    this.config = config;
    this.blockingFileCount = conf.getInt(
        HStore.BLOCKING_STOREFILES_KEY, HStore.DEFAULT_BLOCKING_STOREFILE_COUNT);
  }

  @Override
  public void loadFiles(List<StoreFile> storeFiles) {
    loadUnclassifiedStoreFiles(storeFiles);
  }

  @Override
  public Collection<StoreFile> getStorefiles() {
    return state.allFilesCached;
  }

  @Override
  public void insertNewFiles(Collection<StoreFile> sfs) throws IOException {
    CompactionOrFlushMergeCopy cmc = new CompactionOrFlushMergeCopy(true);
    cmc.mergeResults(null, sfs);
    debugDumpState("Added new files");
  }

  @Override
  public ImmutableCollection<StoreFile> clearFiles() {
    ImmutableCollection<StoreFile> result = state.allFilesCached;
    this.state = new State();
    this.fileStarts.clear();
    this.fileEnds.clear();
    return result;
  }

  @Override
  public int getStorefileCount() {
    return state.allFilesCached.size();
  }

  /** See {@link StoreFileManager#getCandidateFilesForRowKeyBefore(KeyValue)}
   * for details on this methods. */
  @Override
  public Iterator<StoreFile> getCandidateFilesForRowKeyBefore(final KeyValue targetKey) {
    KeyBeforeConcatenatedLists result = new KeyBeforeConcatenatedLists();
    // Order matters for this call.
    result.addSublist(state.level0Files);
    if (!state.stripeFiles.isEmpty()) {
      int lastStripeIndex = findStripeForRow(targetKey.getRow(), false);
      for (int stripeIndex = lastStripeIndex; stripeIndex >= 0; --stripeIndex) {
        result.addSublist(state.stripeFiles.get(stripeIndex));
      }
    }
    return result.iterator();
  }

  /** See {@link StoreFileManager#getCandidateFilesForRowKeyBefore(KeyValue)} and
   * {@link StoreFileManager#updateCandidateFilesForRowKeyBefore(Iterator, KeyValue, KeyValue)}
   * for details on this methods. */
  @Override
  public Iterator<StoreFile> updateCandidateFilesForRowKeyBefore(
      Iterator<StoreFile> candidateFiles, final KeyValue targetKey, final KeyValue candidate) {
    KeyBeforeConcatenatedLists.Iterator original =
        (KeyBeforeConcatenatedLists.Iterator)candidateFiles;
    assert original != null;
    ArrayList<List<StoreFile>> components = original.getComponents();
    for (int firstIrrelevant = 0; firstIrrelevant < components.size(); ++firstIrrelevant) {
      StoreFile sf = components.get(firstIrrelevant).get(0);
      byte[] endKey = endOf(sf);
      // Entries are ordered as such: L0, then stripes in reverse order. We never remove
      // level 0; we remove the stripe, and all subsequent ones, as soon as we find the
      // first one that cannot possibly have better candidates.
      if (!isInvalid(endKey) && !isOpen(endKey)
          && (nonOpenRowCompare(endKey, targetKey.getRow()) <= 0)) {
        original.removeComponents(firstIrrelevant);
        break;
      }
    }
    return original;
  }

  @Override
  /**
   * Override of getSplitPoint that determines the split point as the boundary between two
   * stripes, unless it causes significant imbalance between split sides' sizes. In that
   * case, the split boundary will be chosen from the middle of one of the stripes to
   * minimize imbalance.
   * @return The split point, or null if no split is possible.
   */
  public byte[] getSplitPoint() throws IOException {
    if (this.getStorefileCount() == 0) return null;
    if (state.stripeFiles.size() <= 1) {
      return getSplitPointFromAllFiles();
    }
    int leftIndex = -1, rightIndex = state.stripeFiles.size();
    long leftSize = 0, rightSize = 0;
    long lastLeftSize = 0, lastRightSize = 0;
    while (rightIndex - 1 != leftIndex) {
      if (leftSize >= rightSize) {
        --rightIndex;
        lastRightSize = getStripeFilesSize(rightIndex);
        rightSize += lastRightSize;
      } else {
        ++leftIndex;
        lastLeftSize = getStripeFilesSize(leftIndex);
        leftSize += lastLeftSize;
      }
    }
    if (leftSize == 0 || rightSize == 0) {
      String errMsg = String.format("Cannot split on a boundary - left index %d size %d, "
          + "right index %d size %d", leftIndex, leftSize, rightIndex, rightSize);
      debugDumpState(errMsg);
      LOG.warn(errMsg);
      return getSplitPointFromAllFiles();
    }
    double ratio = (double)rightSize / leftSize;
    if (ratio < 1) {
      ratio = 1 / ratio;
    }
    if (config.getMaxSplitImbalance() > ratio) return state.stripeEndRows[leftIndex];

    // If the difference between the sides is too large, we could get the proportional key on
    // the a stripe to equalize the difference, but there's no proportional key method at the
    // moment, and it's not extremely important.
    // See if we can achieve better ratio if we split the bigger side in half.
    boolean isRightLarger = rightSize >= leftSize;
    double newRatio = isRightLarger
        ? getMidStripeSplitRatio(leftSize, rightSize, lastRightSize)
        : getMidStripeSplitRatio(rightSize, leftSize, lastLeftSize);
    if (newRatio < 1) {
      newRatio = 1 / newRatio;
    }
    if (newRatio >= ratio)  return state.stripeEndRows[leftIndex];
    LOG.debug("Splitting the stripe - ratio w/o split " + ratio + ", ratio with split "
        + newRatio + " configured ratio " + config.getMaxSplitImbalance());
    // Ok, we may get better ratio, get it.
    return StoreUtils.getLargestFile(state.stripeFiles.get(
        isRightLarger ? rightIndex : leftIndex)).getFileSplitPoint(this.kvComparator);
  }

  private byte[] getSplitPointFromAllFiles() throws IOException {
    ConcatenatedLists<StoreFile> sfs = new ConcatenatedLists<StoreFile>();
    sfs.addSublist(state.level0Files);
    sfs.addAllSublists(state.stripeFiles);
    if (sfs.isEmpty()) return null;
    return StoreUtils.getLargestFile(sfs).getFileSplitPoint(this.kvComparator);
  }

  private double getMidStripeSplitRatio(long smallerSize, long largerSize, long lastLargerSize) {
    return (double)(largerSize - lastLargerSize / 2f) / (smallerSize + lastLargerSize / 2f);
  }

  @Override
  public Collection<StoreFile> getFilesForScanOrGet(
      boolean isGet, byte[] startRow, byte[] stopRow) {
    if (state.stripeFiles.isEmpty()) {
      return state.level0Files; // There's just L0.
    }

    int firstStripe = findStripeForRow(startRow, true);
    int lastStripe = findStripeForRow(stopRow, false);
    assert firstStripe <= lastStripe;
    if (firstStripe == lastStripe && state.level0Files.isEmpty()) {
      return state.stripeFiles.get(firstStripe); // There's just one stripe we need.
    }
    if (firstStripe == 0 && lastStripe == (state.stripeFiles.size() - 1)) {
      return state.allFilesCached; // We need to read all files.
    }

    ConcatenatedLists<StoreFile> result = new ConcatenatedLists<StoreFile>();
    result.addAllSublists(state.stripeFiles.subList(firstStripe, lastStripe + 1));
    result.addSublist(state.level0Files);
    return result;
  }

  @Override
  public void addCompactionResults(
    Collection<StoreFile> compactedFiles, Collection<StoreFile> results) throws IOException {
    // See class comment for the assumptions we make here.
    LOG.debug("Attempting to merge compaction results: " + compactedFiles.size()
        + " files replaced by " + results.size());
    // In order to be able to fail in the middle of the operation, we'll operate on lazy
    // copies and apply the result at the end.
    CompactionOrFlushMergeCopy cmc = new CompactionOrFlushMergeCopy(false);
    cmc.mergeResults(compactedFiles, results);
    debugDumpState("Merged compaction results");
  }

  @Override
  public int getStoreCompactionPriority() {
    // If there's only L0, do what the default store does.
    // If we are in critical priority, do the same - we don't want to trump all stores all
    // the time due to how many files we have.
    int fc = getStorefileCount();
    if (state.stripeFiles.isEmpty() || (this.blockingFileCount <= fc)) {
      return this.blockingFileCount - fc;
    }
    // If we are in good shape, we don't want to be trumped by all other stores due to how
    // many files we have, so do an approximate mapping to normal priority range; L0 counts
    // for all stripes.
    int l0 = state.level0Files.size(), sc = state.stripeFiles.size();
    int priority = (int)Math.ceil(((double)(this.blockingFileCount - fc + l0) / sc) - l0);
    return (priority <= HStore.PRIORITY_USER) ? (HStore.PRIORITY_USER + 1) : priority;
  }

  /**
   * Gets the total size of all files in the stripe.
   * @param stripeIndex Stripe index.
   * @return Size.
   */
  private long getStripeFilesSize(int stripeIndex) {
    long result = 0;
    for (StoreFile sf : state.stripeFiles.get(stripeIndex)) {
      result += sf.getReader().length();
    }
    return result;
  }

  /**
   * Loads initial store files that were picked up from some physical location pertaining to
   * this store (presumably). Unlike adding files after compaction, assumes empty initial
   * sets, and is forgiving with regard to stripe constraints - at worst, many/all files will
   * go to level 0.
   * @param storeFiles Store files to add.
   */
  private void loadUnclassifiedStoreFiles(List<StoreFile> storeFiles) {
    LOG.debug("Attempting to load " + storeFiles.size() + " store files.");
    TreeMap<byte[], ArrayList<StoreFile>> candidateStripes =
        new TreeMap<byte[], ArrayList<StoreFile>>(MAP_COMPARATOR);
    ArrayList<StoreFile> level0Files = new ArrayList<StoreFile>();
    // Separate the files into tentative stripes; then validate. Currently, we rely on metadata.
    // If needed, we could dynamically determine the stripes in future.
    for (StoreFile sf : storeFiles) {
      byte[] startRow = startOf(sf), endRow = endOf(sf);
      // Validate the range and put the files into place.
      if (isInvalid(startRow) || isInvalid(endRow)) {
        insertFileIntoStripe(level0Files, sf); // No metadata - goes to L0.
        ensureLevel0Metadata(sf);
      } else if (!isOpen(startRow) && !isOpen(endRow) &&
          nonOpenRowCompare(startRow, endRow) >= 0) {
        LOG.error("Unexpected metadata - start row [" + Bytes.toString(startRow) + "], end row ["
          + Bytes.toString(endRow) + "] in file [" + sf.getPath() + "], pushing to L0");
        insertFileIntoStripe(level0Files, sf); // Bad metadata - goes to L0 also.
        ensureLevel0Metadata(sf);
      } else {
        ArrayList<StoreFile> stripe = candidateStripes.get(endRow);
        if (stripe == null) {
          stripe = new ArrayList<StoreFile>();
          candidateStripes.put(endRow, stripe);
        }
        insertFileIntoStripe(stripe, sf);
      }
    }
    // Possible improvement - for variable-count stripes, if all the files are in L0, we can
    // instead create single, open-ended stripe with all files.

    boolean hasOverlaps = false;
    byte[] expectedStartRow = null; // first stripe can start wherever
    Iterator<Map.Entry<byte[], ArrayList<StoreFile>>> entryIter =
        candidateStripes.entrySet().iterator();
    while (entryIter.hasNext()) {
      Map.Entry<byte[], ArrayList<StoreFile>> entry = entryIter.next();
      ArrayList<StoreFile> files = entry.getValue();
      // Validate the file start rows, and remove the bad ones to level 0.
      for (int i = 0; i < files.size(); ++i) {
        StoreFile sf = files.get(i);
        byte[] startRow = startOf(sf);
        if (expectedStartRow == null) {
          expectedStartRow = startRow; // ensure that first stripe is still consistent
        } else if (!rowEquals(expectedStartRow, startRow)) {
          hasOverlaps = true;
          LOG.warn("Store file doesn't fit into the tentative stripes - expected to start at ["
              + Bytes.toString(expectedStartRow) + "], but starts at [" + Bytes.toString(startRow)
              + "], to L0 it goes");
          StoreFile badSf = files.remove(i);
          insertFileIntoStripe(level0Files, badSf);
          ensureLevel0Metadata(badSf);
          --i;
        }
      }
      // Check if any files from the candidate stripe are valid. If so, add a stripe.
      byte[] endRow = entry.getKey();
      if (!files.isEmpty()) {
        expectedStartRow = endRow; // Next stripe must start exactly at that key.
      } else {
        entryIter.remove();
      }
    }

    // In the end, there must be open ends on two sides. If not, and there were no errors i.e.
    // files are consistent, they might be coming from a split. We will treat the boundaries
    // as open keys anyway, and log the message.
    // If there were errors, we'll play it safe and dump everything into L0.
    if (!candidateStripes.isEmpty()) {
      StoreFile firstFile = candidateStripes.firstEntry().getValue().get(0);
      boolean isOpen = isOpen(startOf(firstFile)) && isOpen(candidateStripes.lastKey());
      if (!isOpen) {
        LOG.warn("The range of the loaded files does not cover full key space: from ["
            + Bytes.toString(startOf(firstFile)) + "], to ["
            + Bytes.toString(candidateStripes.lastKey()) + "]");
        if (!hasOverlaps) {
          ensureEdgeStripeMetadata(candidateStripes.firstEntry().getValue(), true);
          ensureEdgeStripeMetadata(candidateStripes.lastEntry().getValue(), false);
        } else {
          LOG.warn("Inconsistent files, everything goes to L0.");
          for (ArrayList<StoreFile> files : candidateStripes.values()) {
            for (StoreFile sf : files) {
              insertFileIntoStripe(level0Files, sf);
              ensureLevel0Metadata(sf);
            }
          }
          candidateStripes.clear();
        }
      }
    }

    // Copy the results into the fields.
    State state = new State();
    state.level0Files = ImmutableList.copyOf(level0Files);
    state.stripeFiles = new ArrayList<ImmutableList<StoreFile>>(candidateStripes.size());
    state.stripeEndRows = new byte[Math.max(0, candidateStripes.size() - 1)][];
    ArrayList<StoreFile> newAllFiles = new ArrayList<StoreFile>(level0Files);
    int i = candidateStripes.size() - 1;
    for (Map.Entry<byte[], ArrayList<StoreFile>> entry : candidateStripes.entrySet()) {
      state.stripeFiles.add(ImmutableList.copyOf(entry.getValue()));
      newAllFiles.addAll(entry.getValue());
      if (i > 0) {
        state.stripeEndRows[state.stripeFiles.size() - 1] = entry.getKey();
      }
      --i;
    }
    state.allFilesCached = ImmutableList.copyOf(newAllFiles);
    this.state = state;
    debugDumpState("Files loaded");
  }

  private void ensureEdgeStripeMetadata(ArrayList<StoreFile> stripe, boolean isFirst) {
    HashMap<StoreFile, byte[]> targetMap = isFirst ? fileStarts : fileEnds;
    for (StoreFile sf : stripe) {
      targetMap.put(sf, OPEN_KEY);
    }
  }

  private void ensureLevel0Metadata(StoreFile sf) {
    if (!isInvalid(startOf(sf))) this.fileStarts.put(sf, INVALID_KEY_IN_MAP);
    if (!isInvalid(endOf(sf))) this.fileEnds.put(sf, INVALID_KEY_IN_MAP);
  }

  private void debugDumpState(String string) {
    if (!LOG.isDebugEnabled()) return;
    StringBuilder sb = new StringBuilder();
    sb.append("\n" + string + "; current stripe state is as such:");
    sb.append("\n level 0 with ")
        .append(state.level0Files.size())
        .append(
          " files: "
              + StringUtils.humanReadableInt(
                StripeCompactionPolicy.getTotalFileSize(state.level0Files)) + ";");
    for (int i = 0; i < state.stripeFiles.size(); ++i) {
      String endRow = (i == state.stripeEndRows.length)
          ? "(end)" : "[" + Bytes.toString(state.stripeEndRows[i]) + "]";
      sb.append("\n stripe ending in ")
          .append(endRow)
          .append(" with ")
          .append(state.stripeFiles.get(i).size())
          .append(
            " files: "
                + StringUtils.humanReadableInt(
                  StripeCompactionPolicy.getTotalFileSize(state.stripeFiles.get(i))) + ";");
    }
    sb.append("\n").append(state.stripeFiles.size()).append(" stripes total.");
    sb.append("\n").append(getStorefileCount()).append(" files total.");
    LOG.debug(sb.toString());
  }

  /**
   * Checks whether the key indicates an open interval boundary (i.e. infinity).
   */
  private static final boolean isOpen(byte[] key) {
    return key != null && key.length == 0;
  }

  /**
   * Checks whether the key is invalid (e.g. from an L0 file, or non-stripe-compacted files).
   */
  private static final boolean isInvalid(byte[] key) {
    return key == INVALID_KEY;
  }

  /**
   * Compare two keys for equality.
   */
  private final boolean rowEquals(byte[] k1, byte[] k2) {
    return kvComparator.matchingRows(k1, 0, k1.length, k2, 0, k2.length);
  }

  /**
   * Compare two keys. Keys must not be open (isOpen(row) == false).
   */
  private final int nonOpenRowCompare(byte[] k1, byte[] k2) {
    assert !isOpen(k1) && !isOpen(k2);
    return kvComparator.compareRows(k1, 0, k1.length, k2, 0, k2.length);
  }

  /**
   * Finds the stripe index by end row.
   */
  private final int findStripeIndexByEndRow(byte[] endRow) {
    assert !isInvalid(endRow);
    if (isOpen(endRow)) return state.stripeEndRows.length;
    return Arrays.binarySearch(state.stripeEndRows, endRow, Bytes.BYTES_COMPARATOR);
  }

  /**
   * Finds the stripe index for the stripe containing a row provided externally for get/scan.
   */
  private final int findStripeForRow(byte[] row, boolean isStart) {
    if (isStart && row == HConstants.EMPTY_START_ROW) return 0;
    if (!isStart && row == HConstants.EMPTY_END_ROW) return state.stripeFiles.size() - 1;
    // If there's an exact match below, a stripe ends at "row". Stripe right boundary is
    // exclusive, so that means the row is in the next stripe; thus, we need to add one to index.
    // If there's no match, the return value of binarySearch is (-(insertion point) - 1), where
    // insertion point is the index of the next greater element, or list size if none. The
    // insertion point happens to be exactly what we need, so we need to add one to the result.
    return Math.abs(Arrays.binarySearch(state.stripeEndRows, row, Bytes.BYTES_COMPARATOR) + 1);
  }

  @Override
  public final byte[] getStartRow(int stripeIndex) {
    return (stripeIndex == 0  ? OPEN_KEY : state.stripeEndRows[stripeIndex - 1]);
  }

  @Override
  public final byte[] getEndRow(int stripeIndex) {
    return (stripeIndex == state.stripeEndRows.length
        ? OPEN_KEY : state.stripeEndRows[stripeIndex]);
  }


  private byte[] startOf(StoreFile sf) {
    byte[] result = this.fileStarts.get(sf);
    return result == null ? sf.getMetadataValue(STRIPE_START_KEY)
        : (result == INVALID_KEY_IN_MAP ? INVALID_KEY : result);
  }

  private byte[] endOf(StoreFile sf) {
    byte[] result = this.fileEnds.get(sf);
    return result == null ? sf.getMetadataValue(STRIPE_END_KEY)
        : (result == INVALID_KEY_IN_MAP ? INVALID_KEY : result);
  }

  /**
   * Inserts a file in the correct place (by seqnum) in a stripe copy.
   * @param stripe Stripe copy to insert into.
   * @param sf File to insert.
   */
  private static void insertFileIntoStripe(ArrayList<StoreFile> stripe, StoreFile sf) {
    // The only operation for which sorting of the files matters is KeyBefore. Therefore,
    // we will store the file in reverse order by seqNum from the outset.
    for (int insertBefore = 0; ; ++insertBefore) {
      if (insertBefore == stripe.size()
          || (StoreFile.Comparators.SEQ_ID.compare(sf, stripe.get(insertBefore)) >= 0)) {
        stripe.add(insertBefore, sf);
        break;
      }
    }
  }

  /**
   * An extension of ConcatenatedLists that has several peculiar properties.
   * First, one can cut the tail of the logical list by removing last several sub-lists.
   * Second, items can be removed thru iterator.
   * Third, if the sub-lists are immutable, they are replaced with mutable copies when needed.
   * On average KeyBefore operation will contain half the stripes as potential candidates,
   * but will quickly cut down on them as it finds something in the more likely ones; thus,
   * the above allow us to avoid unnecessary copying of a bunch of lists.
   */
  private static class KeyBeforeConcatenatedLists extends ConcatenatedLists<StoreFile> {
    @Override
    public java.util.Iterator<StoreFile> iterator() {
      return new Iterator();
    }

    public class Iterator extends ConcatenatedLists<StoreFile>.Iterator {
      public ArrayList<List<StoreFile>> getComponents() {
        return components;
      }

      public void removeComponents(int startIndex) {
        List<List<StoreFile>> subList = components.subList(startIndex, components.size());
        for (List<StoreFile> entry : subList) {
          size -= entry.size();
        }
        assert size >= 0;
        subList.clear();
      }

      @Override
      public void remove() {
        if (!this.nextWasCalled) {
          throw new IllegalStateException("No element to remove");
        }
        this.nextWasCalled = false;
        List<StoreFile> src = components.get(currentComponent);
        if (src instanceof ImmutableList<?>) {
          src = new ArrayList<StoreFile>(src);
          components.set(currentComponent, src);
        }
        src.remove(indexWithinComponent);
        --size;
        --indexWithinComponent;
        if (src.isEmpty()) {
          components.remove(currentComponent); // indexWithinComponent is already -1 here.
        }
      }
    }
  }

  /**
   * Non-static helper class for merging compaction or flush results.
   * Since we want to merge them atomically (more or less), it operates on lazy copies,
   * then creates a new state object and puts it in place.
   */
  private class CompactionOrFlushMergeCopy {
    private ArrayList<List<StoreFile>> stripeFiles = null;
    private ArrayList<StoreFile> level0Files = null;
    private ArrayList<byte[]> stripeEndRows = null;

    private Collection<StoreFile> compactedFiles = null;
    private Collection<StoreFile> results = null;

    private List<StoreFile> l0Results = new ArrayList<StoreFile>();
    private final boolean isFlush;

    public CompactionOrFlushMergeCopy(boolean isFlush) {
      // Create a lazy mutable copy (other fields are so lazy they start out as nulls).
      this.stripeFiles = new ArrayList<List<StoreFile>>(
          StripeStoreFileManager.this.state.stripeFiles);
      this.isFlush = isFlush;
    }

    public void mergeResults(Collection<StoreFile> compactedFiles, Collection<StoreFile> results)
        throws IOException {
      assert this.compactedFiles == null && this.results == null;
      this.compactedFiles = compactedFiles;
      this.results = results;
      // Do logical processing.
      if (!isFlush) removeCompactedFiles();
      TreeMap<byte[], StoreFile> newStripes = processResults();
      if (newStripes != null) {
        processNewCandidateStripes(newStripes);
      }
      // Create new state and update parent.
      State state = createNewState();
      StripeStoreFileManager.this.state = state;
      updateMetadataMaps();
    }

    private State createNewState() {
      State oldState = StripeStoreFileManager.this.state;
      // Stripe count should be the same unless the end rows changed.
      assert oldState.stripeFiles.size() == this.stripeFiles.size() || this.stripeEndRows != null;
      State newState = new State();
      newState.level0Files = (this.level0Files == null) ? oldState.level0Files
          : ImmutableList.copyOf(this.level0Files);
      newState.stripeEndRows = (this.stripeEndRows == null) ? oldState.stripeEndRows
          : this.stripeEndRows.toArray(new byte[this.stripeEndRows.size()][]);
      newState.stripeFiles = new ArrayList<ImmutableList<StoreFile>>(this.stripeFiles.size());
      for (List<StoreFile> newStripe : this.stripeFiles) {
        newState.stripeFiles.add(newStripe instanceof ImmutableList<?>
            ? (ImmutableList<StoreFile>)newStripe : ImmutableList.copyOf(newStripe));
      }

      List<StoreFile> newAllFiles = new ArrayList<StoreFile>(oldState.allFilesCached);
      if (!isFlush) newAllFiles.removeAll(compactedFiles);
      newAllFiles.addAll(results);
      newState.allFilesCached = ImmutableList.copyOf(newAllFiles);
      return newState;
    }

    private void updateMetadataMaps() {
      StripeStoreFileManager parent = StripeStoreFileManager.this;
      if (!isFlush) {
        for (StoreFile sf : this.compactedFiles) {
          parent.fileStarts.remove(sf);
          parent.fileEnds.remove(sf);
        }
      }
      if (this.l0Results != null) {
        for (StoreFile sf : this.l0Results) {
          parent.ensureLevel0Metadata(sf);
        }
      }
    }

    /**
     * @param index Index of the stripe we need.
     * @return A lazy stripe copy from current stripes.
     */
    private final ArrayList<StoreFile> getStripeCopy(int index) {
      List<StoreFile> stripeCopy = this.stripeFiles.get(index);
      ArrayList<StoreFile> result = null;
      if (stripeCopy instanceof ImmutableList<?>) {
        result = new ArrayList<StoreFile>(stripeCopy);
        this.stripeFiles.set(index, result);
      } else {
        result = (ArrayList<StoreFile>)stripeCopy;
      }
      return result;
    }

    /**
     * @return A lazy L0 copy from current state.
     */
    private final ArrayList<StoreFile> getLevel0Copy() {
      if (this.level0Files == null) {
        this.level0Files = new ArrayList<StoreFile>(StripeStoreFileManager.this.state.level0Files);
      }
      return this.level0Files;
    }

    /**
     * Process new files, and add them either to the structure of existing stripes,
     * or to the list of new candidate stripes.
     * @return New candidate stripes.
     */
    private TreeMap<byte[], StoreFile> processResults() throws IOException {
      TreeMap<byte[], StoreFile> newStripes = null;
      for (StoreFile sf : this.results) {
        byte[] startRow = startOf(sf), endRow = endOf(sf);
        if (isInvalid(endRow) || isInvalid(startRow)) {
          if (!isFlush) {
            LOG.warn("The newly compacted file doesn't have stripes set: " + sf.getPath());
          }
          insertFileIntoStripe(getLevel0Copy(), sf);
          this.l0Results.add(sf);
          continue;
        }
        if (!this.stripeFiles.isEmpty()) {
          int stripeIndex = findStripeIndexByEndRow(endRow);
          if ((stripeIndex >= 0) && rowEquals(getStartRow(stripeIndex), startRow)) {
            // Simple/common case - add file to an existing stripe.
            insertFileIntoStripe(getStripeCopy(stripeIndex), sf);
            continue;
          }
        }

        // Make a new candidate stripe.
        if (newStripes == null) {
          newStripes = new TreeMap<byte[], StoreFile>(MAP_COMPARATOR);
        }
        StoreFile oldSf = newStripes.put(endRow, sf);
        if (oldSf != null) {
          throw new IOException("Compactor has produced multiple files for the stripe ending in ["
              + Bytes.toString(endRow) + "], found " + sf.getPath() + " and " + oldSf.getPath());
        }
      }
      return newStripes;
    }

    /**
     * Remove compacted files.
     * @param compactedFiles Compacted files.
     */
    private void removeCompactedFiles() throws IOException {
      for (StoreFile oldFile : this.compactedFiles) {
        byte[] oldEndRow = endOf(oldFile);
        List<StoreFile> source = null;
        if (isInvalid(oldEndRow)) {
          source = getLevel0Copy();
        } else {
          int stripeIndex = findStripeIndexByEndRow(oldEndRow);
          if (stripeIndex < 0) {
            throw new IOException("An allegedly compacted file [" + oldFile + "] does not belong"
                + " to a known stripe (end row - [" + Bytes.toString(oldEndRow) + "])");
          }
          source = getStripeCopy(stripeIndex);
        }
        if (!source.remove(oldFile)) {
          throw new IOException("An allegedly compacted file [" + oldFile + "] was not found");
        }
      }
    }

    /**
     * See {@link #addCompactionResults(Collection, Collection)} - updates the stripe list with
     * new candidate stripes/removes old stripes; produces new set of stripe end rows.
     * @param newStripes  New stripes - files by end row.
     */
    private void processNewCandidateStripes(
        TreeMap<byte[], StoreFile> newStripes) throws IOException {
      // Validate that the removed and added aggregate ranges still make for a full key space.
      boolean hasStripes = !this.stripeFiles.isEmpty();
      this.stripeEndRows = new ArrayList<byte[]>(
          Arrays.asList(StripeStoreFileManager.this.state.stripeEndRows));
      int removeFrom = 0;
      byte[] firstStartRow = startOf(newStripes.firstEntry().getValue());
      byte[] lastEndRow = newStripes.lastKey();
      if (!hasStripes && (!isOpen(firstStartRow) || !isOpen(lastEndRow))) {
        throw new IOException("Newly created stripes do not cover the entire key space.");
      }

      boolean canAddNewStripes = true;
      Collection<StoreFile> filesForL0 = null;
      if (hasStripes) {
        // Determine which stripes will need to be removed because they conflict with new stripes.
        // The new boundaries should match old stripe boundaries, so we should get exact matches.
        if (isOpen(firstStartRow)) {
          removeFrom = 0;
        } else {
          removeFrom = findStripeIndexByEndRow(firstStartRow);
          if (removeFrom < 0) throw new IOException("Compaction is trying to add a bad range.");
          ++removeFrom;
        }
        int removeTo = findStripeIndexByEndRow(lastEndRow);
        if (removeTo < 0) throw new IOException("Compaction is trying to add a bad range.");
        // See if there are files in the stripes we are trying to replace.
        ArrayList<StoreFile> conflictingFiles = new ArrayList<StoreFile>();
        for (int removeIndex = removeTo; removeIndex >= removeFrom; --removeIndex) {
          conflictingFiles.addAll(this.stripeFiles.get(removeIndex));
        }
        if (!conflictingFiles.isEmpty()) {
          // This can be caused by two things - concurrent flush into stripes, or a bug.
          // Unfortunately, we cannot tell them apart without looking at timing or something
          // like that. We will assume we are dealing with a flush and dump it into L0.
          if (isFlush) {
            long newSize = StripeCompactionPolicy.getTotalFileSize(newStripes.values());
            LOG.warn("Stripes were created by a flush, but results of size " + newSize
                + " cannot be added because the stripes have changed");
            canAddNewStripes = false;
            filesForL0 = newStripes.values();
          } else {
            long oldSize = StripeCompactionPolicy.getTotalFileSize(conflictingFiles);
            LOG.info(conflictingFiles.size() + " conflicting files (likely created by a flush) "
                + " of size " + oldSize + " are moved to L0 due to concurrent stripe change");
            filesForL0 = conflictingFiles;
          }
          if (filesForL0 != null) {
            for (StoreFile sf : filesForL0) {
              insertFileIntoStripe(getLevel0Copy(), sf);
            }
            l0Results.addAll(filesForL0);
          }
        }

        if (canAddNewStripes) {
          // Remove old empty stripes.
          int originalCount = this.stripeFiles.size();
          for (int removeIndex = removeTo; removeIndex >= removeFrom; --removeIndex) {
            if (removeIndex != originalCount - 1) {
              this.stripeEndRows.remove(removeIndex);
            }
            this.stripeFiles.remove(removeIndex);
          }
        }
      }

      if (!canAddNewStripes) return; // Files were already put into L0.

      // Now, insert new stripes. The total ranges match, so we can insert where we removed.
      byte[] previousEndRow = null;
      int insertAt = removeFrom;
      for (Map.Entry<byte[], StoreFile> newStripe : newStripes.entrySet()) {
        if (previousEndRow != null) {
          // Validate that the ranges are contiguous.
          assert !isOpen(previousEndRow);
          byte[] startRow = startOf(newStripe.getValue());
          if (!rowEquals(previousEndRow, startRow)) {
            throw new IOException("The new stripes produced by "
                + (isFlush ? "flush" : "compaction") + " are not contiguous");
          }
        }
        // Add the new stripe.
        ArrayList<StoreFile> tmp = new ArrayList<StoreFile>();
        tmp.add(newStripe.getValue());
        stripeFiles.add(insertAt, tmp);
        previousEndRow = newStripe.getKey();
        if (!isOpen(previousEndRow)) {
          stripeEndRows.add(insertAt, previousEndRow);
        }
        ++insertAt;
      }
    }
  }

  @Override
  public List<StoreFile> getLevel0Files() {
    return this.state.level0Files;
  }

  @Override
  public List<byte[]> getStripeBoundaries() {
    if (this.state.stripeFiles.isEmpty()) return new ArrayList<byte[]>();
    ArrayList<byte[]> result = new ArrayList<byte[]>(this.state.stripeEndRows.length + 2);
    result.add(OPEN_KEY);
    Collections.addAll(result, this.state.stripeEndRows);
    result.add(OPEN_KEY);
    return result;
  }

  @Override
  public ArrayList<ImmutableList<StoreFile>> getStripes() {
    return this.state.stripeFiles;
  }

  @Override
  public int getStripeCount() {
    return this.state.stripeFiles.size();
  }

  @Override
  public Collection<StoreFile> getUnneededFiles(long maxTs, List<StoreFile> filesCompacting) {
    // 1) We can never get rid of the last file which has the maximum seqid in a stripe.
    // 2) Files that are not the latest can't become one due to (1), so the rest are fair game.
    State state = this.state;
    Collection<StoreFile> expiredStoreFiles = null;
    for (ImmutableList<StoreFile> stripe : state.stripeFiles) {
      expiredStoreFiles = findExpiredFiles(stripe, maxTs, filesCompacting, expiredStoreFiles);
    }
    return findExpiredFiles(state.level0Files, maxTs, filesCompacting, expiredStoreFiles);
  }

  private Collection<StoreFile> findExpiredFiles(ImmutableList<StoreFile> stripe, long maxTs,
      List<StoreFile> filesCompacting, Collection<StoreFile> expiredStoreFiles) {
    // Order by seqnum is reversed.
    for (int i = 1; i < stripe.size(); ++i) {
      StoreFile sf = stripe.get(i);
      long fileTs = sf.getReader().getMaxTimestamp();
      if (fileTs < maxTs && !filesCompacting.contains(sf)) {
        LOG.info("Found an expired store file: " + sf.getPath()
            + " whose maxTimeStamp is " + fileTs + ", which is below " + maxTs);
        if (expiredStoreFiles == null) {
          expiredStoreFiles = new ArrayList<StoreFile>();
        }
        expiredStoreFiles.add(sf);
      }
    }
    return expiredStoreFiles;
  }

  @Override
  public double getCompactionPressure() {
    State stateLocal = this.state;
    if (stateLocal.allFilesCached.size() > blockingFileCount) {
      // just a hit to tell others that we have reached the blocking file count.
      return 2.0;
    }
    if (stateLocal.stripeFiles.isEmpty()) {
      return 0.0;
    }
    int blockingFilePerStripe = blockingFileCount / stateLocal.stripeFiles.size();
    // do not calculate L0 separately because data will be moved to stripe quickly and in most cases
    // we flush data to stripe directly.
    int delta = stateLocal.level0Files.isEmpty() ? 0 : 1;
    double max = 0.0;
    for (ImmutableList<StoreFile> stripeFile : stateLocal.stripeFiles) {
      int stripeFileCount = stripeFile.size();
      double normCount =
          (double) (stripeFileCount + delta - config.getStripeCompactMinFiles())
              / (blockingFilePerStripe - config.getStripeCompactMinFiles());
      if (normCount >= 1.0) {
        // This could happen if stripe is not split evenly. Do not return values that larger than
        // 1.0 because we have not reached the blocking file count actually.
        return 1.0;
      }
      if (normCount > max) {
        max = normCount;
      }
    }
    return max;
  }

  @Override
  public Comparator<StoreFile> getStoreFileComparator() {
    return StoreFile.Comparators.SEQ_ID;
  }
}
