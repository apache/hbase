/**
 * Copyright 2010 The Apache Software Foundation
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
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.regionserver.StoreFile.Reader;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;

/**
 * KeyValueScanner adaptor over the Reader.  It also provides hooks into
 * bloom filter things.
 */
class StoreFileScanner implements KeyValueScanner {
  static final Log LOG = LogFactory.getLog(Store.class);

  // the reader it comes from:
  private final StoreFile.Reader reader;
  private final HFileScanner hfs;
  private KeyValue cur = null;

  private boolean realSeekDone;
  private boolean delayedReseek;
  private KeyValue delayedSeekKV;

  private static final AtomicLong seekCount = new AtomicLong();

  /**
   * Implements a {@link KeyValueScanner} on top of the specified {@link HFileScanner}
   * @param hfs HFile scanner
   */
  public StoreFileScanner(StoreFile.Reader reader, HFileScanner hfs) {
    this.reader = reader;
    this.hfs = hfs;
  }

  /**
   * Return an array of scanners corresponding to the given
   * set of store files.
   */
  public static List<StoreFileScanner> getScannersForStoreFiles(
      Collection<StoreFile> filesToCompact,
      boolean cacheBlocks,
      boolean usePread) throws IOException {
    List<StoreFileScanner> scanners =
      new ArrayList<StoreFileScanner>(filesToCompact.size());
    for (StoreFile file : filesToCompact) {
      StoreFile.Reader r = file.createReader();
      scanners.add(r.getStoreFileScanner(cacheBlocks, usePread));
    }
    return scanners;
  }

  public String toString() {
    return "StoreFileScanner[" + hfs.toString() + ", cur=" + cur + "]";
  }

  public KeyValue peek() {
    return cur;
  }

  public KeyValue next() throws IOException {
    KeyValue retKey = cur;
    try {
      // only seek if we aren't at the end. cur == null implies 'end'.
      if (cur != null) {
        hfs.next();
        cur = hfs.getKeyValue();
      }
    } catch(IOException e) {
      throw new IOException("Could not iterate " + this, e);
    }
    return retKey;
  }

  public boolean seek(KeyValue key) throws IOException {
    seekCount.incrementAndGet();
    try {
      try {
        if(!seekAtOrAfter(hfs, key)) {
          close();
          return false;
        }
        cur = hfs.getKeyValue();
        return true;
      } finally {
        realSeekDone = true;
      }
    } catch(IOException ioe) {
      throw new IOException("Could not seek " + this, ioe);
    }
  }

  public boolean reseek(KeyValue key) throws IOException {
    seekCount.incrementAndGet();
    try {
      try {
        if (!reseekAtOrAfter(hfs, key)) {
          close();
          return false;
        }
        cur = hfs.getKeyValue();
        return true;
      } finally {
        realSeekDone = true;
      }
    } catch (IOException ioe) {
      throw new IOException("Could not seek " + this, ioe);
    }
  }

  public void close() {
    // Nothing to close on HFileScanner?
    cur = null;
  }

  /**
   *
   * @param s
   * @param k
   * @return
   * @throws IOException
   */
  public static boolean seekAtOrAfter(HFileScanner s, KeyValue k)
  throws IOException {
    int result = s.seekTo(k.getBuffer(), k.getKeyOffset(), k.getKeyLength());
    if(result < 0) {
      // Passed KV is smaller than first KV in file, work from start of file
      return s.seekTo();
    } else if(result > 0) {
      // Passed KV is larger than current KV in file, if there is a next
      // it is the "after", if not then this scanner is done.
      return s.next();
    }
    // Seeked to the exact key
    return true;
  }

  static boolean reseekAtOrAfter(HFileScanner s, KeyValue k)
  throws IOException {
    //This function is similar to seekAtOrAfter function
    int result = s.reseekTo(k.getBuffer(), k.getKeyOffset(), k.getKeyLength());
    if (result <= 0) {
      return true;
    } else {
      // passed KV is larger than current KV in file, if there is a next
      // it is after, if not then this scanner is done.
      return s.next();
    }
  }

  // StoreFile filter hook.
  public boolean shouldSeek(Scan scan, final SortedSet<byte[]> columns) {
    return reader.shouldSeek(scan, columns);
  }

  @Override
  public long getSequenceID() {
    return reader.getSequenceID();
  }

  /**
   * Pretend we have done a seek but don't do it yet, if possible. The hope is
   * that we find requested columns in more recent files and won't have to seek
   * in older files. Creates a fake key/value with the given row/column and the
   * highest (most recent) possible timestamp we might get from this file. When
   * users of such "lazy scanner" need to know the next KV precisely (e.g. when
   * this scanner is at the top of the heap), they run {@link #enforceSeek()}.
   * <p>
   * Note that this function does guarantee that the current KV of this scanner
   * will be advanced to at least the given KV. Because of this, it does have
   * to do a real seek in cases when the seek timestamp is older than the
   * highest timestamp of the file, e.g. when we are trying to seek to the next
   * row/column and use OLDEST_TIMESTAMP in the seek key.
   */
  @Override
  public boolean requestSeek(KeyValue kv, boolean forward, boolean useBloom)
      throws IOException {
    if (reader.getBloomFilterType() != StoreFile.BloomType.ROWCOL ||
        kv.getFamilyLength() == 0) {
      useBloom = false;
    }

    boolean haveToSeek = true;
    if (useBloom) {
      haveToSeek = reader.passesBloomFilter(kv.getBuffer(),
          kv.getRowOffset(), kv.getRowLength(), kv.getBuffer(),
          kv.getQualifierOffset(), kv.getQualifierLength());
    }

    delayedReseek = forward;
    delayedSeekKV = kv;

    if (haveToSeek) {
      // This row/column might be in this store file (or we did not use the
      // Bloom filter), so we still need to seek.
      realSeekDone = false;
      long maxTimestampInFile = reader.getMaxTimestamp();
      long seekTimestamp = kv.getTimestamp();
      if (seekTimestamp > maxTimestampInFile) {
        // Create a fake key that is not greater than the real next key.
        // (Lower timestamps correspond to higher KVs.)
        // To understand this better, consider that we are asked to seek to
        // a higher timestamp than the max timestamp in this file. We know that
        // the next point when we have to consider this file again is when we
        // pass the max timestamp of this file (with the same row/column).
        cur = kv.createFirstOnRowColTS(maxTimestampInFile);
      } else {
        // This will be the case e.g. when we need to seek to the next
        // row/column, and we don't know exactly what they are, so we set the
        // seek key's timestamp to OLDEST_TIMESTAMP to skip the rest of this
        // row/column.
        enforceSeek();
      }
      return cur != null;
    }

    // Multi-column Bloom filter optimization.
    // Create a fake key/value, so that this scanner only bubbles up to the top
    // of the KeyValueHeap in StoreScanner after we scanned this row/column in
    // all other store files. The query matcher will then just skip this fake
    // key/value and the store scanner will progress to the next column. This
    // is obviously not a "real real" seek, but unlike the fake KV earlier in
    // this method, we want this to be propagated to ScanQueryMatcher.
    cur = kv.createLastOnRowCol();

    realSeekDone = true;
    return true;
  }

  Reader getReaderForTesting() {
    return reader;
  }

  @Override
  public boolean realSeekDone() {
    return realSeekDone;
  }

  @Override
  public void enforceSeek() throws IOException {
    if (realSeekDone)
      return;

    if (delayedReseek) {
      reseek(delayedSeekKV);
    } else {
      seek(delayedSeekKV);
    }
  }

  // Test methods

  static final long getSeekCount() {
    return seekCount.get();
  }

}
