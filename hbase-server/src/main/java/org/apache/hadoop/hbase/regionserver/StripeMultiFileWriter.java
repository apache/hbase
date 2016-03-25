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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.StoreFile.Writer;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Base class for cell sink that separates the provided cells into multiple files for stripe
 * compaction.
 */
@InterfaceAudience.Private
public abstract class StripeMultiFileWriter extends AbstractMultiFileWriter {

  private static final Log LOG = LogFactory.getLog(StripeMultiFileWriter.class);

  protected final KVComparator comparator;
  protected List<StoreFile.Writer> existingWriters;
  protected List<byte[]> boundaries;

  /** Whether to write stripe metadata */
  private boolean doWriteStripeMetadata = true;

  public StripeMultiFileWriter(KVComparator comparator) {
    this.comparator = comparator;
  }

  public void setNoStripeMetadata() {
    this.doWriteStripeMetadata = false;
  }

  @Override
  protected Collection<Writer> writers() {
    return existingWriters;
  }

  @Override
  protected void preCloseWriter(Writer writer) throws IOException {
    if (doWriteStripeMetadata) {
      LOG.debug("Write stripe metadata for " + writer.getPath().toString());
      int index = existingWriters.indexOf(writer);
      writer.appendFileInfo(StripeStoreFileManager.STRIPE_START_KEY, boundaries.get(index));
      writer.appendFileInfo(StripeStoreFileManager.STRIPE_END_KEY, boundaries.get(index + 1));
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skip writing stripe metadata for " + writer.getPath().toString());
      }
    }
  }

  /**
   * Subclasses can call this method to make sure the first KV is within multi-writer range.
   * @param left The left boundary of the writer.
   * @param row The row to check.
   * @param rowOffset Offset for row.
   * @param rowLength Length for row.
   */
  protected void sanityCheckLeft(byte[] left, byte[] row, int rowOffset, int rowLength)
      throws IOException {
    if (StripeStoreFileManager.OPEN_KEY != left
        && comparator.compareRows(row, rowOffset, rowLength, left, 0, left.length) < 0) {
      String error = "The first row is lower than the left boundary of [" + Bytes.toString(left)
          + "]: [" + Bytes.toString(row, rowOffset, rowLength) + "]";
      LOG.error(error);
      throw new IOException(error);
    }
  }

  /**
   * Subclasses can call this method to make sure the last KV is within multi-writer range.
   * @param right The right boundary of the writer.
   * @param row The row to check.
   * @param rowOffset Offset for row.
   * @param rowLength Length for row.
   */
  protected void sanityCheckRight(byte[] right, byte[] row, int rowOffset, int rowLength)
      throws IOException {
    if (StripeStoreFileManager.OPEN_KEY != right
        && comparator.compareRows(row, rowOffset, rowLength, right, 0, right.length) >= 0) {
      String error = "The last row is higher or equal than the right boundary of ["
          + Bytes.toString(right) + "]: [" + Bytes.toString(row, rowOffset, rowLength) + "]";
      LOG.error(error);
      throw new IOException(error);
    }
  }

  /**
   * MultiWriter that separates the cells based on fixed row-key boundaries. All the KVs between
   * each pair of neighboring boundaries from the list supplied to ctor will end up in one file, and
   * separate from all other such pairs.
   */
  public static class BoundaryMultiWriter extends StripeMultiFileWriter {
    private StoreFile.Writer currentWriter;
    private byte[] currentWriterEndKey;

    private KeyValue lastKv;
    private long kvsInCurrentWriter = 0;
    private int majorRangeFromIndex = -1, majorRangeToIndex = -1;
    private boolean hasAnyWriter = false;

    /**
     * @param targetBoundaries The boundaries on which writers/files are separated.
     * @param majorRangeFrom Major range is the range for which at least one file should be written
     *          (because all files are included in compaction). majorRangeFrom is the left boundary.
     * @param majorRangeTo The right boundary of majorRange (see majorRangeFrom).
     */
    public BoundaryMultiWriter(KVComparator comparator, List<byte[]> targetBoundaries,
        byte[] majorRangeFrom, byte[] majorRangeTo) throws IOException {
      super(comparator);
      this.boundaries = targetBoundaries;
      this.existingWriters = new ArrayList<StoreFile.Writer>(this.boundaries.size() - 1);
      // "major" range (range for which all files are included) boundaries, if any,
      // must match some target boundaries, let's find them.
      assert (majorRangeFrom == null) == (majorRangeTo == null);
      if (majorRangeFrom != null) {
        majorRangeFromIndex = (majorRangeFrom == StripeStoreFileManager.OPEN_KEY) ? 0
            : Collections.binarySearch(this.boundaries, majorRangeFrom, Bytes.BYTES_COMPARATOR);
        majorRangeToIndex = (majorRangeTo == StripeStoreFileManager.OPEN_KEY) ? boundaries.size()
            : Collections.binarySearch(this.boundaries, majorRangeTo, Bytes.BYTES_COMPARATOR);
        if (this.majorRangeFromIndex < 0 || this.majorRangeToIndex < 0) {
          throw new IOException("Major range does not match writer boundaries: ["
              + Bytes.toString(majorRangeFrom) + "] [" + Bytes.toString(majorRangeTo) + "]; from "
              + majorRangeFromIndex + " to " + majorRangeToIndex);
        }
      }
    }

    @Override
    public void append(KeyValue kv) throws IOException {
      if (currentWriter == null && existingWriters.isEmpty()) {
        // First append ever, do a sanity check.
        sanityCheckLeft(this.boundaries.get(0), kv.getRowArray(), kv.getRowOffset(),
          kv.getRowLength());
      }
      prepareWriterFor(kv);
      currentWriter.append(kv);
      lastKv = kv; // for the sanity check
      ++kvsInCurrentWriter;
    }

    private boolean isKvAfterCurrentWriter(KeyValue kv) {
      return ((currentWriterEndKey != StripeStoreFileManager.OPEN_KEY)
          && (comparator.compareRows(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(),
            currentWriterEndKey, 0, currentWriterEndKey.length) >= 0));
    }

    @Override
    protected void preCommitWriters() throws IOException {
      stopUsingCurrentWriter();
      while (existingWriters.size() < boundaries.size() - 1) {
        createEmptyWriter();
      }
      if (lastKv != null) {
        sanityCheckRight(boundaries.get(boundaries.size() - 1), lastKv.getRowArray(),
          lastKv.getRowOffset(), lastKv.getRowLength());
      }
    }

    private void prepareWriterFor(KeyValue kv) throws IOException {
      if (currentWriter != null && !isKvAfterCurrentWriter(kv)) return; // Use same writer.

      stopUsingCurrentWriter();
      // See if KV will be past the writer we are about to create; need to add another one.
      while (isKvAfterCurrentWriter(kv)) {
        checkCanCreateWriter();
        createEmptyWriter();
      }
      checkCanCreateWriter();
      hasAnyWriter = true;
      currentWriter = writerFactory.createWriter();
      existingWriters.add(currentWriter);
    }

    /**
     * Called if there are no cells for some stripe. We need to have something in the writer list
     * for this stripe, so that writer-boundary list indices correspond to each other. We can insert
     * null in the writer list for that purpose, except in the following cases where we actually
     * need a file: 1) If we are in range for which we are compacting all the files, we need to
     * create an empty file to preserve stripe metadata. 2) If we have not produced any file at all
     * for this compactions, and this is the last chance (the last stripe), we need to preserve last
     * seqNum (see also HBASE-6059).
     */
    private void createEmptyWriter() throws IOException {
      int index = existingWriters.size();
      boolean isInMajorRange = (index >= majorRangeFromIndex) && (index < majorRangeToIndex);
      // Stripe boundary count = stripe count + 1, so last stripe index is (#boundaries minus 2)
      boolean isLastWriter = !hasAnyWriter && (index == (boundaries.size() - 2));
      boolean needEmptyFile = isInMajorRange || isLastWriter;
      existingWriters.add(needEmptyFile ? writerFactory.createWriter() : null);
      hasAnyWriter |= needEmptyFile;
      currentWriterEndKey = (existingWriters.size() + 1 == boundaries.size()) ? null
          : boundaries.get(existingWriters.size() + 1);
    }

    private void checkCanCreateWriter() throws IOException {
      int maxWriterCount = boundaries.size() - 1;
      assert existingWriters.size() <= maxWriterCount;
      if (existingWriters.size() >= maxWriterCount) {
        throw new IOException("Cannot create any more writers (created " + existingWriters.size()
            + " out of " + maxWriterCount + " - row might be out of range of all valid writers");
      }
    }

    private void stopUsingCurrentWriter() {
      if (currentWriter != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Stopping to use a writer after [" + Bytes.toString(currentWriterEndKey)
              + "] row; wrote out " + kvsInCurrentWriter + " kvs");
        }
        kvsInCurrentWriter = 0;
      }
      currentWriter = null;
      currentWriterEndKey = (existingWriters.size() + 1 == boundaries.size()) ? null
          : boundaries.get(existingWriters.size() + 1);
    }
  }

  /**
   * MultiWriter that separates the cells based on target cell number per file and file count. New
   * file is started every time the target number of KVs is reached, unless the fixed count of
   * writers has already been created (in that case all the remaining KVs go into the last writer).
   */
  public static class SizeMultiWriter extends StripeMultiFileWriter {
    private int targetCount;
    private long targetKvs;
    private byte[] left;
    private byte[] right;

    private KeyValue lastKv;
    private StoreFile.Writer currentWriter;
    protected byte[] lastRowInCurrentWriter = null;
    private long kvsInCurrentWriter = 0;
    private long kvsSeen = 0;
    private long kvsSeenInPrevious = 0;

    /**
     * @param targetCount The maximum count of writers that can be created.
     * @param targetKvs The number of KVs to read from source before starting each new writer.
     * @param left The left boundary of the first writer.
     * @param right The right boundary of the last writer.
     */
    public SizeMultiWriter(KVComparator comparator, int targetCount, long targetKvs, byte[] left,
        byte[] right) {
      super(comparator);
      this.targetCount = targetCount;
      this.targetKvs = targetKvs;
      this.left = left;
      this.right = right;
      int preallocate = Math.min(this.targetCount, 64);
      this.existingWriters = new ArrayList<StoreFile.Writer>(preallocate);
      this.boundaries = new ArrayList<byte[]>(preallocate + 1);
    }

    @Override
    public void append(KeyValue kv) throws IOException {
      // If we are waiting for opportunity to close and we started writing different row,
      // discard the writer and stop waiting.
      boolean doCreateWriter = false;
      if (currentWriter == null) {
        // First append ever, do a sanity check.
        sanityCheckLeft(left, kv.getRowArray(), kv.getRowOffset(), kv.getRowLength());
        doCreateWriter = true;
      } else if (lastRowInCurrentWriter != null
          && !comparator.matchingRows(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength(),
            lastRowInCurrentWriter, 0, lastRowInCurrentWriter.length)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Stopping to use a writer after [" + Bytes.toString(lastRowInCurrentWriter)
              + "] row; wrote out " + kvsInCurrentWriter + " kvs");
        }
        lastRowInCurrentWriter = null;
        kvsInCurrentWriter = 0;
        kvsSeenInPrevious += kvsSeen;
        doCreateWriter = true;
      }
      if (doCreateWriter) {
        byte[] boundary = existingWriters.isEmpty() ? left : kv.getRow(); // make a copy
        if (LOG.isDebugEnabled()) {
          LOG.debug("Creating new writer starting at [" + Bytes.toString(boundary) + "]");
        }
        currentWriter = writerFactory.createWriter();
        boundaries.add(boundary);
        existingWriters.add(currentWriter);
      }

      currentWriter.append(kv);
      lastKv = kv; // for the sanity check
      ++kvsInCurrentWriter;
      kvsSeen = kvsInCurrentWriter;
      if (this.sourceScanner != null) {
        kvsSeen = Math.max(kvsSeen,
          this.sourceScanner.getEstimatedNumberOfKvsScanned() - kvsSeenInPrevious);
      }

      // If we are not already waiting for opportunity to close, start waiting if we can
      // create any more writers and if the current one is too big.
      if (lastRowInCurrentWriter == null && existingWriters.size() < targetCount
          && kvsSeen >= targetKvs) {
        lastRowInCurrentWriter = kv.getRow(); // make a copy
        if (LOG.isDebugEnabled()) {
          LOG.debug("Preparing to start a new writer after ["
              + Bytes.toString(lastRowInCurrentWriter) + "] row; observed " + kvsSeen
              + " kvs and wrote out " + kvsInCurrentWriter + " kvs");
        }
      }
    }

    @Override
    protected void preCommitWriters() throws IOException {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Stopping with " + kvsInCurrentWriter + " kvs in last writer"
            + ((this.sourceScanner == null) ? ""
                : ("; observed estimated " + this.sourceScanner.getEstimatedNumberOfKvsScanned()
                    + " KVs total")));
      }
      if (lastKv != null) {
        sanityCheckRight(right, lastKv.getRowArray(), lastKv.getRowOffset(), lastKv.getRowLength());
      }

      // When expired stripes were going to be merged into one, and if no writer was created during
      // the compaction, we need to create an empty file to preserve metadata.
      if (existingWriters.isEmpty() && 1 == targetCount) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Merge expired stripes into one, create an empty file to preserve metadata.");
        }
        boundaries.add(left);
        existingWriters.add(writerFactory.createWriter());
      }

      this.boundaries.add(right);
    }
  }
}
