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
  private boolean enforceMVCC = false;

  /**
   * Implements a {@link KeyValueScanner} on top of the specified {@link HFileScanner}
   * @param hfs HFile scanner
   */
  public StoreFileScanner(StoreFile.Reader reader, HFileScanner hfs, boolean useMVCC) {
    this.reader = reader;
    this.hfs = hfs;
    this.enforceMVCC = useMVCC;
  }

  /**
   * Return an array of scanners corresponding to the given
   * set of store files.
   */
  public static List<StoreFileScanner> getScannersForStoreFiles(
      Collection<StoreFile> filesToCompact,
      boolean cacheBlocks,
      boolean usePread) throws IOException {
    return getScannersForStoreFiles(filesToCompact, cacheBlocks, usePread, false);
  }

  /**
   * Return an array of scanners corresponding to the given set of store files.
   */
  public static List<StoreFileScanner> getScannersForStoreFiles(
      Collection<StoreFile> files, boolean cacheBlocks, boolean usePread,
      boolean isCompaction) throws IOException {
    List<StoreFileScanner> scanners = new ArrayList<StoreFileScanner>(
        files.size());
    for (StoreFile file : files) {
      StoreFile.Reader r = file.createReader();
      scanners.add(r.getStoreFileScanner(cacheBlocks, usePread, isCompaction));
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
        skipKVsNewerThanReadpoint();
      }
    } catch(IOException e) {
      throw new IOException("Could not iterate " + this, e);
    }
    return retKey;
  }

  public boolean seek(KeyValue key) throws IOException {
    try {
      if(!seekAtOrAfter(hfs, key)) {
        close();
        return false;
      }
      cur = hfs.getKeyValue();
      return skipKVsNewerThanReadpoint();
    } catch(IOException ioe) {
      throw new IOException("Could not seek " + this, ioe);
    }
  }

  public boolean reseek(KeyValue key) throws IOException {
    try {
      if (!reseekAtOrAfter(hfs, key)) {
        close();
        return false;
      }
      cur = hfs.getKeyValue();
      return skipKVsNewerThanReadpoint();
    } catch (IOException ioe) {
      throw new IOException("Could not seek " + this, ioe);
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

  @Override
  public boolean seekExactly(KeyValue kv, boolean forward)
      throws IOException {
    if (reader.getBloomFilterType() != StoreFile.BloomType.ROWCOL ||
        kv.getRowLength() == 0 || kv.getQualifierLength() == 0) {
      return forward ? reseek(kv) : seek(kv);
    }

    boolean isInBloom = reader.passesBloomFilter(kv.getBuffer(),
        kv.getRowOffset(), kv.getRowLength(), kv.getBuffer(),
        kv.getQualifierOffset(), kv.getQualifierLength());
    if (isInBloom) {
      // This row/column might be in this store file. Do a normal seek.
      return forward ? reseek(kv) : seek(kv);
    }

    // Create a fake key/value, so that this scanner only bubbles up to the top
    // of the KeyValueHeap in StoreScanner after we scanned this row/column in
    // all other store files. The query matcher will then just skip this fake
    // key/value and the store scanner will progress to the next column.
    cur = kv.createLastOnRowCol();
    return true;
  }

  Reader getReaderForTesting() {
    return reader;
  }
}
