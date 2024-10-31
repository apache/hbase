/*
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
import java.util.function.IntConsumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileInfo;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.io.hfile.ReaderContext;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.regionserver.StoreFileReader;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class RangeStoreFileReader extends StoreFileReader {

  private static final Logger LOG = LoggerFactory.getLogger(RangeStoreFileReader.class);

  protected final byte[] startKey;
  protected final byte[] endKey;
  private final ExtendedCell startCell;
  private final ExtendedCell endCell;
  private Optional<ExtendedCell> firstKey = Optional.empty();

  private boolean firstKeySeeked = false;

  public RangeStoreFileReader(final ReaderContext context, final HFileInfo fileInfo,
    final CacheConfig cacheConf, RangeReference rangeReference, StoreFileInfo storeFileInfo,
    final Configuration conf) throws IOException {
    super(context, fileInfo, cacheConf, storeFileInfo, conf);
    this.startKey = rangeReference.getStartKey();
    assert this.startKey != null;
    this.endKey = rangeReference.getEndKey();
    assert this.endKey != null;
    this.startCell = new KeyValue.KeyOnlyKeyValue(this.startKey, 0, this.startKey.length);
    this.endCell = new KeyValue.KeyOnlyKeyValue(this.endKey, 0, this.endKey.length);
  }

  public HFileScanner getScanner(final boolean cacheBlocks, final boolean pread,
    final boolean isCompaction) {
    final HFileScanner s = super.getScanner(cacheBlocks, pread, isCompaction);
    return new HFileScanner() {
      final HFileScanner delegate = s;
      public boolean atEnd = false;

      @Override
      public ExtendedCell getKey() {
        if (atEnd) return null;
        return delegate.getKey();
      }

      @Override
      public ByteBuffer getValue() {
        if (atEnd) return null;

        return delegate.getValue();
      }

      @Override
      public ExtendedCell getCell() {
        if (atEnd) return null;

        return delegate.getCell();
      }

      @Override
      public boolean next() throws IOException {
        if (atEnd) return false;

        boolean b = delegate.next();
        if (!b) {
          return false;
        }

        if (getComparator().compare(endCell, getKey()) <= 0) {
          atEnd = true;
          return false;
        }
        return true;
      }

      @Override
      public boolean seekTo() throws IOException {
        int r = this.delegate.seekTo(startCell);
        if (r == HConstants.INDEX_KEY_MAGIC) {
          return true;
        }
        if (r < 0) {
          // start range is < first key in file
          boolean seekTo = this.delegate.seekTo();
          if (seekTo) {
            // first key in file is less than end range return true otherwise return false.
            return this.delegate.getReader().getComparator().compare(getKey(), endCell) < 0;
          }
          return false;
        } else if (r > 0) {
          // TODO: Need to check anything move forward by skipping a cell.
          boolean next = this.delegate.next();
          if (!next) {
            return false;
          }
          // next available key in file is less than end range return true otherwise return false.
          return this.delegate.getReader().getComparator().compare(getKey(), endCell) < 0;
        }
        return true;
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
      public int seekTo(ExtendedCell key) throws IOException {
        if (PrivateCellUtil.compareKeyIgnoresMvcc(getComparator(), key, startCell) < 0) {
          return this.delegate.seekTo(startCell);
        }
        if (PrivateCellUtil.compareKeyIgnoresMvcc(getComparator(), key, endCell) >= 0) {
          // we would place the scanner in the within the range of a file.
          // it might be an error to return false here ever...
          boolean res = delegate.seekBefore(endCell);
          if (!res) {
            throw new IOException(
              "Seeking for a key in range of file, but key exists in outside range of file, "
                + "failed on seekBefore(endCell)");
          }
          return 1;
        }
        return this.delegate.seekTo(key);
      }

      @Override
      public int reseekTo(ExtendedCell key) throws IOException {
        // This function is identical to the corresponding seekTo function
        // except
        // that we call reseekTo (and not seekTo) on the delegate.
        if (PrivateCellUtil.compareKeyIgnoresMvcc(getComparator(), key, startCell) < 0) {
          return this.delegate.reseekTo(startCell);
        }
        if (PrivateCellUtil.compareKeyIgnoresMvcc(getComparator(), key, endCell) >= 0) {
          // we would place the scanner in the within the range of a file.
          // it might be an error to return false here ever...
          boolean res = delegate.seekBefore(endCell);
          if (!res) {
            throw new IOException(
              "Seeking for a key in range of file, but key exists in outside range of file, "
                + "failed on seekBefore(endCell)");
          }
          return 1;
        }
        if (atEnd) {
          // skip the 'reseek' and just return 1.
          return 1;
        }
        return this.delegate.reseekTo(key);
      }

      @Override
      public boolean seekBefore(ExtendedCell key) throws IOException {
        boolean ret = false;
        if (PrivateCellUtil.compareKeyIgnoresMvcc(getComparator(), key, startCell) <= 0) {
          return false;
        } else if (PrivateCellUtil.compareKeyIgnoresMvcc(getComparator(), key, endCell) >= 0) {
          ret = this.delegate.seekBefore(endCell);
        } else {
          ret = this.delegate.seekBefore(key);
        }

        if (ret) {
          atEnd = false;
        }
        return ret;
      }

      @Override
      public ExtendedCell getNextIndexedKey() {
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

      @Override
      public void recordBlockSize(IntConsumer blockSizeConsumer) {
        this.delegate.recordBlockSize(blockSizeConsumer);
      }
    };
  }

  @Override
  public boolean passesKeyRangeFilter(Scan scan) {
    return true;
  }

  @Override
  public Optional<ExtendedCell> getLastKey() {
    // Get a scanner that caches the block and that uses pread.
    HFileScanner scanner = getScanner(true, true, false);
    try {
      if (scanner.seekBefore(this.endCell)) {
        return Optional.ofNullable(scanner.getKey());
      }
    } catch (IOException e) {
      LOG.warn("Failed seekBefore " + Bytes.toStringBinary(this.endKey), e);
    } finally {
      if (scanner != null) {
        scanner.close();
      }
    }
    return Optional.empty();
  }

  @Override
  public Optional<ExtendedCell> midKey() throws IOException {
    // Returns null to indicate file is not splitable.
    return Optional.empty();
  }

  @Override
  public Optional<ExtendedCell> getFirstKey() {
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
        if (scanner != null) {
          scanner.close();
        }
      }
    }
    return this.firstKey;
  }
}
