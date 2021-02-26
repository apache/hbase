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
package org.apache.hadoop.hbase.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileInfo;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.io.hfile.ReaderContext;
import org.apache.hadoop.hbase.regionserver.StoreFileReader;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A facade for a {@link org.apache.hadoop.hbase.io.hfile.HFile.Reader} that serves up
 * either the top or bottom half of a HFile where 'bottom' is the first half
 * of the file containing the keys that sort lowest and 'top' is the second half
 * of the file with keys that sort greater than those of the bottom half.
 * The top includes the split files midkey, of the key that follows if it does
 * not exist in the file.
 *
 * <p>This type works in tandem with the {@link Reference} type.  This class
 * is used reading while Reference is used writing.
 *
 * <p>This file is not splitable.  Calls to {@link #midKey()} return null.
 */
@InterfaceAudience.Private
public class HalfStoreFileReader extends StoreFileReader {
  private static final Logger LOG = LoggerFactory.getLogger(HalfStoreFileReader.class);
  final boolean top;
  // This is the key we split around.  Its the first possible entry on a row:
  // i.e. empty column and a timestamp of LATEST_TIMESTAMP.
  protected final byte [] splitkey;

  private final Cell splitCell;

  private Optional<Cell> firstKey = Optional.empty();

  private boolean firstKeySeeked = false;

  /**
   * Creates a half file reader for a hfile referred to by an hfilelink.
   * @param context Reader context info
   * @param fileInfo HFile info
   * @param cacheConf CacheConfig
   * @param r original reference file (contains top or bottom)
   * @param refCount reference count
   * @param conf Configuration
   */
  public HalfStoreFileReader(final ReaderContext context, final HFileInfo fileInfo,
      final CacheConfig cacheConf, final Reference r,
      AtomicInteger refCount, final Configuration conf) throws IOException {
    super(context, fileInfo, cacheConf, refCount, conf);
    // This is not actual midkey for this half-file; its just border
    // around which we split top and bottom.  Have to look in files to find
    // actual last and first keys for bottom and top halves.  Half-files don't
    // have an actual midkey themselves. No midkey is how we indicate file is
    // not splittable.
    this.splitkey = r.getSplitKey();
    this.splitCell = new KeyValue.KeyOnlyKeyValue(this.splitkey, 0, this.splitkey.length);
    // Is it top or bottom half?
    this.top = Reference.isTopFileRegion(r.getFileRegion());
  }

  protected boolean isTop() {
    return this.top;
  }

  @Override
  public HFileScanner getScanner(final boolean cacheBlocks,
      final boolean pread, final boolean isCompaction) {
    final HFileScanner s = super.getScanner(cacheBlocks, pread, isCompaction);
    return new HFileScanner() {
      final HFileScanner delegate = s;
      public boolean atEnd = false;

      @Override
      public Cell getKey() {
        if (atEnd) return null;
        return delegate.getKey();
      }

      @Override
      public String getKeyString() {
        if (atEnd) return null;

        return delegate.getKeyString();
      }

      @Override
      public ByteBuffer getValue() {
        if (atEnd) return null;

        return delegate.getValue();
      }

      @Override
      public String getValueString() {
        if (atEnd) return null;

        return delegate.getValueString();
      }

      @Override
      public Cell getCell() {
        if (atEnd) return null;

        return delegate.getCell();
      }

      @Override
      public boolean next() throws IOException {
        if (atEnd) return false;

        boolean b = delegate.next();
        if (!b) {
          return b;
        }
        // constrain the bottom.
        if (!top) {
          if (getComparator().compare(splitCell, getKey()) <= 0) {
            atEnd = true;
            return false;
          }
        }
        return true;
      }

      @Override
      public boolean seekTo() throws IOException {
        if (top) {
          int r = this.delegate.seekTo(splitCell);
          if (r == HConstants.INDEX_KEY_MAGIC) {
            return true;
          }
          if (r < 0) {
            // midkey is < first key in file
            return this.delegate.seekTo();
          }
          if (r > 0) {
            return this.delegate.next();
          }
          return true;
        }

        boolean b = delegate.seekTo();
        if (!b) {
          return b;
        }
        // Check key.
        return (this.delegate.getReader().getComparator().compare(splitCell, getKey())) > 0;
      }

      @Override
      public org.apache.hadoop.hbase.io.hfile.HFile.Reader getReader() {
        return this.delegate.getReader();
      }

      @Override
      public boolean isSeeked() {
        return this.delegate.isSeeked();
      }

      @Override
      public int seekTo(Cell key) throws IOException {
        if (top) {
          if (PrivateCellUtil.compareKeyIgnoresMvcc(getComparator(), key, splitCell) < 0) {
            return -1;
          }
        } else {
          if (PrivateCellUtil.compareKeyIgnoresMvcc(getComparator(), key, splitCell) >= 0) {
            // we would place the scanner in the second half.
            // it might be an error to return false here ever...
            boolean res = delegate.seekBefore(splitCell);
            if (!res) {
              throw new IOException(
                  "Seeking for a key in bottom of file, but key exists in top of file, " +
                  "failed on seekBefore(midkey)");
            }
            return 1;
          }
        }
        return delegate.seekTo(key);
      }

      @Override
      public int reseekTo(Cell key) throws IOException {
        // This function is identical to the corresponding seekTo function
        // except
        // that we call reseekTo (and not seekTo) on the delegate.
        if (top) {
          if (PrivateCellUtil.compareKeyIgnoresMvcc(getComparator(), key, splitCell) < 0) {
            return -1;
          }
        } else {
          if (PrivateCellUtil.compareKeyIgnoresMvcc(getComparator(), key, splitCell) >= 0) {
            // we would place the scanner in the second half.
            // it might be an error to return false here ever...
            boolean res = delegate.seekBefore(splitCell);
            if (!res) {
              throw new IOException("Seeking for a key in bottom of file, but"
                  + " key exists in top of file, failed on seekBefore(midkey)");
            }
            return 1;
          }
        }
        if (atEnd) {
          // skip the 'reseek' and just return 1.
          return 1;
        }
        return delegate.reseekTo(key);
      }

      @Override
      public boolean seekBefore(Cell key) throws IOException {
        if (top) {
          Optional<Cell> fk = getFirstKey();
          if (fk.isPresent() &&
                  PrivateCellUtil.compareKeyIgnoresMvcc(getComparator(), key, fk.get()) <= 0) {
            return false;
          }
        } else {
          // The equals sign isn't strictly necessary just here to be consistent
          // with seekTo
          if (PrivateCellUtil.compareKeyIgnoresMvcc(getComparator(), key, splitCell) >= 0) {
            boolean ret = this.delegate.seekBefore(splitCell);
            if (ret) {
              atEnd = false;
            }
            return ret;
          }
        }
        boolean ret = this.delegate.seekBefore(key);
        if (ret) {
          atEnd = false;
        }
        return ret;
      }

      @Override
      public Cell getNextIndexedKey() {
        return null;
      }

      @Override
      public void close() {
        this.delegate.close();
      }

      @Override
      public void shipped() throws IOException {
        this.delegate.shipped();
      }
    };
  }
  
  @Override
  public boolean passesKeyRangeFilter(Scan scan) {
    return true;
  }
  
  @Override
  public Optional<Cell> getLastKey() {
    if (top) {
      return super.getLastKey();
    }
    // Get a scanner that caches the block and that uses pread.
    HFileScanner scanner = getScanner(true, true);
    try {
      if (scanner.seekBefore(this.splitCell)) {
        return Optional.ofNullable(scanner.getKey());
      }
    } catch (IOException e) {
      LOG.warn("Failed seekBefore " + Bytes.toStringBinary(this.splitkey), e);
    } finally {
      if (scanner != null) {
        scanner.close();
      }
    }
    return Optional.empty();
  }

  @Override
  public Optional<Cell> midKey() throws IOException {
    // Returns null to indicate file is not splitable.
    return Optional.empty();
  }

  @Override
  public Optional<Cell> getFirstKey() {
    if (!firstKeySeeked) {
      HFileScanner scanner = getScanner(true, true, false);
      try {
        if (scanner.seekTo()) {
          this.firstKey = Optional.ofNullable(scanner.getKey());
        }
        firstKeySeeked = true;
      } catch (IOException e) {
        LOG.warn("Failed seekTo first KV in the file", e);
      } finally {
        if(scanner != null) {
          scanner.close();
        }
      }
    }
    return this.firstKey;
  }

  @Override
  public long getEntries() {
    // Estimate the number of entries as half the original file; this may be wildly inaccurate.
    return super.getEntries() / 2;
  }

  @Override
  public long getFilterEntries() {
    // Estimate the number of entries as half the original file; this may be wildly inaccurate.
    return super.getFilterEntries() / 2;
  }
}
