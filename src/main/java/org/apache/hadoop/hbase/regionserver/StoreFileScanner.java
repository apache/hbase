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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.regionserver.StoreFile.Reader;

/**
 * KeyValueScanner adaptor over the Reader.  It also provides hooks into
 * bloom filter things.
 */
public class StoreFileScanner implements KeyValueScanner {
  static final Log LOG = LogFactory.getLog(Store.class);

  // the reader it comes from:
  private final StoreFile.Reader reader;
  private final HFileScanner hfs;
  private KeyValue cur = null;

  private boolean realSeekDone;
  private boolean delayedReseek;
  private KeyValue delayedSeekKV;

  private boolean enforceMVCC = false;
  private boolean hasMVCCInfo = false;

  private static AtomicLong seekCount;

  private ScanQueryMatcher matcher;

  /**
   * Implements a {@link KeyValueScanner} on top of the specified {@link HFileScanner}
   * @param hfs HFile scanner
   */
  public StoreFileScanner(StoreFile.Reader reader, HFileScanner hfs, boolean useMVCC, boolean hasMVCC) {
    this.reader = reader;
    this.hfs = hfs;
    this.enforceMVCC = useMVCC;
    this.hasMVCCInfo = hasMVCC;
  }

  /**
   * Return an array of scanners corresponding to the given
   * set of store files.
   */
  public static List<StoreFileScanner> getScannersForStoreFiles(
      Collection<StoreFile> files,
      boolean cacheBlocks,
      boolean usePread) throws IOException {
    return getScannersForStoreFiles(files, cacheBlocks,
                                   usePread, false);
  }

  /**
   * Return an array of scanners corresponding to the given set of store files.
   */
  public static List<StoreFileScanner> getScannersForStoreFiles(
      Collection<StoreFile> files, boolean cacheBlocks, boolean usePread,
      boolean isCompaction) throws IOException {
    return getScannersForStoreFiles(files, cacheBlocks, usePread, isCompaction,
        null);
  }

  /**
   * Return an array of scanners corresponding to the given set of store files,
   * And set the ScanQueryMatcher for each store file scanner for further
   * optimization
   */
  public static List<StoreFileScanner> getScannersForStoreFiles(
      Collection<StoreFile> files, boolean cacheBlocks, boolean usePread,
      boolean isCompaction, ScanQueryMatcher matcher) throws IOException {
    List<StoreFileScanner> scanners = new ArrayList<StoreFileScanner>(
        files.size());
    for (StoreFile file : files) {
      StoreFile.Reader r = file.createReader();
      StoreFileScanner scanner = r.getStoreFileScanner(cacheBlocks, usePread,
          isCompaction);
      scanner.setScanQueryMatcher(matcher);
      scanners.add(scanner);
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
        if (hasMVCCInfo)
          skipKVsNewerThanReadpoint();
      }
    } catch (FileNotFoundException e) {
      throw e;
    } catch(IOException e) {
      throw new IOException("Could not iterate " + this, e);
    }
    return retKey;
  }

  public boolean seek(KeyValue key) throws IOException {
    if (seekCount != null) seekCount.incrementAndGet();

    try {
      try {
        if(!seekAtOrAfter(hfs, key)) {
          close();
          return false;
        }

        cur = hfs.getKeyValue();

        return !hasMVCCInfo ? true : skipKVsNewerThanReadpoint();
      } finally {
        realSeekDone = true;
      }
    } catch (FileNotFoundException e) {
      throw e;
    } catch (IOException ioe) {
      throw new IOException("Could not seek " + this + " to key " + key, ioe);
    }
  }

  public boolean reseek(KeyValue key) throws IOException {
    if (seekCount != null) seekCount.incrementAndGet();

    try {
      try {
        if (!reseekAtOrAfter(hfs, key)) {
          close();
          return false;
        }
        cur = hfs.getKeyValue();

        return !hasMVCCInfo ? true : skipKVsNewerThanReadpoint();
      } finally {
        realSeekDone = true;
      }
    } catch (FileNotFoundException e) {
      throw e;
    } catch (IOException ioe) {
      throw new IOException("Could not reseek " + this + " to key " + key,
          ioe);
    }
  }

  protected boolean skipKVsNewerThanReadpoint() throws IOException {
    long readPoint = MultiVersionConsistencyControl.getThreadReadPoint();

    // We want to ignore all key-values that are newer than our current
    // readPoint
    while(enforceMVCC
        && cur != null
        && (cur.getMemstoreTS() > readPoint)) {
      hfs.next();
      cur = hfs.getKeyValue();
    }

    if (cur == null) {
      close();
      return false;
    }

    // For the optimisation in HBASE-4346, we set the KV's memstoreTS to
    // 0, if it is older than all the scanners' read points. It is possible
    // that a newer KV's memstoreTS was reset to 0. But, there is an
    // older KV which was not reset to 0 (because it was
    // not old enough during flush). Make sure that we set it correctly now,
    // so that the comparision order does not change.
    if (cur.getMemstoreTS() <= readPoint) {
      cur.setMemstoreTS(0);
    }
    return true;
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
      // If up to now scanner is not seeked yet, this means passed KV is smaller
      // than first KV in file, and it is the first time we seek on this file.
      // So we also need to work from the start of file.
      if (!s.isSeeked()) {
        return  s.seekTo();
      }
      return true;
    } else {
      // passed KV is larger than current KV in file, if there is a next
      // it is after, if not then this scanner is done.
      return s.next();
    }
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
    if (kv.getFamilyLength() == 0) {
      useBloom = false;
    }

    boolean haveToSeek = true;
    if (useBloom) {
      // check ROWCOL Bloom filter first.
      if (reader.getBloomFilterType() == StoreFile.BloomType.ROWCOL) {
        haveToSeek = reader.passesGeneralBloomFilter(kv.getBuffer(),
            kv.getRowOffset(), kv.getRowLength(), kv.getBuffer(),
            kv.getQualifierOffset(), kv.getQualifierLength());
      } else if (this.matcher != null && !matcher.hasNullColumnInQuery() &&
          kv.isDeleteFamily()) {
        // if there is no such delete family kv in the store file,
        // then no need to seek.
        haveToSeek = reader.passesDeleteFamilyBloomFilter(kv.getBuffer(),
            kv.getRowOffset(), kv.getRowLength());
      }
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

  public void setScanQueryMatcher(ScanQueryMatcher matcher) {
    this.matcher = matcher;
  }

  @Override
  public boolean isFileScanner() {
    return true;
  }

  // Test methods

  static final long getSeekCount() {
    return seekCount.get();
  }
  static final void instrument() {
    seekCount = new AtomicLong();
  }

  @Override
  public boolean shouldUseScanner(Scan scan, SortedSet<byte[]> columns, long oldestUnexpiredTS) {
    return reader.passesTimerangeFilter(scan, oldestUnexpiredTS)
        && reader.passesKeyRangeFilter(scan) && reader.passesBloomFilter(scan, columns);
  }
}
