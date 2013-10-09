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
package org.apache.hadoop.hbase.io.hfile;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;

/**
 * {@link HFile} reader for version 3.
 * This Reader is aware of Tags.
 */
@InterfaceAudience.Private
public class HFileReaderV3 extends HFileReaderV2 {

  public static final int MAX_MINOR_VERSION = 0;
  /**
   * Opens a HFile. You must load the index before you can use it by calling
   * {@link #loadFileInfo()}.
   * @param path
   *          Path to HFile.
   * @param trailer
   *          File trailer.
   * @param fsdis
   *          input stream.
   * @param size
   *          Length of the stream.
   * @param cacheConf
   *          Cache configuration.
   * @param preferredEncodingInCache
   *          the encoding to use in cache in case we have a choice. If the file
   *          is already encoded on disk, we will still use its on-disk encoding
   *          in cache.
   */
  public HFileReaderV3(Path path, FixedFileTrailer trailer, final FSDataInputStreamWrapper fsdis,
      final long size, final CacheConfig cacheConf, DataBlockEncoding preferredEncodingInCache,
      final HFileSystem hfs) throws IOException {
    super(path, trailer, fsdis, size, cacheConf, preferredEncodingInCache, hfs);

  }

  @Override
  protected HFileContext createHFileContext(FixedFileTrailer trailer) {
    HFileContext hfileContext = new HFileContextBuilder()
                                .withIncludesMvcc(this.includesMemstoreTS)
                                .withHBaseCheckSum(true)
                                .withCompressionAlgo(this.compressAlgo)
                                .withIncludesTags(true)
                                .build();
    return hfileContext;
  }

  /**
   * Create a Scanner on this file. No seeks or reads are done on creation. Call
   * {@link HFileScanner#seekTo(byte[])} to position an start the read. There is
   * nothing to clean up in a Scanner. Letting go of your references to the
   * scanner is sufficient.
   * @param cacheBlocks
   *          True if we should cache blocks read in by this scanner.
   * @param pread
   *          Use positional read rather than seek+read if true (pread is better
   *          for random reads, seek+read is better scanning).
   * @param isCompaction
   *          is scanner being used for a compaction?
   * @return Scanner on this file.
   */
  @Override
  public HFileScanner getScanner(boolean cacheBlocks, final boolean pread,
      final boolean isCompaction) {
    // check if we want to use data block encoding in memory
    if (dataBlockEncoder.useEncodedScanner(isCompaction)) {
      return new EncodedScannerV3(this, cacheBlocks, pread, isCompaction, this.hfileContext);
    }
    return new ScannerV3(this, cacheBlocks, pread, isCompaction);
  }

  /**
   * Implementation of {@link HFileScanner} interface.
   */
  protected static class ScannerV3 extends ScannerV2 {

    private HFileReaderV3 reader;
    private int currTagsLen;

    public ScannerV3(HFileReaderV3 r, boolean cacheBlocks, final boolean pread,
        final boolean isCompaction) {
      super(r, cacheBlocks, pread, isCompaction);
      this.reader = r;
    }

    @Override
    protected int getKvBufSize() {
      int kvBufSize = super.getKvBufSize();
      if (reader.hfileContext.shouldIncludeTags()) {
        kvBufSize += Bytes.SIZEOF_SHORT + currTagsLen;
      }
      return kvBufSize;
    }

    protected void setNonSeekedState() {
      super.setNonSeekedState();
      currTagsLen = 0;
    }

    @Override
    protected int getNextKVStartPosition() {
      int nextKvPos = super.getNextKVStartPosition();
      if (reader.hfileContext.shouldIncludeTags()) {
        nextKvPos += Bytes.SIZEOF_SHORT + currTagsLen;
      }
      return nextKvPos;
    }

    protected void readKeyValueLen() {
      blockBuffer.mark();
      currKeyLen = blockBuffer.getInt();
      currValueLen = blockBuffer.getInt();
      ByteBufferUtils.skip(blockBuffer, currKeyLen + currValueLen);
      if (reader.hfileContext.shouldIncludeTags()) {
        currTagsLen = blockBuffer.getShort();
        ByteBufferUtils.skip(blockBuffer, currTagsLen);
      }
      readMvccVersion();
      if (currKeyLen < 0 || currValueLen < 0 || currTagsLen < 0 || currKeyLen > blockBuffer.limit()
          || currValueLen > blockBuffer.limit() || currTagsLen > blockBuffer.limit()) {
        throw new IllegalStateException("Invalid currKeyLen " + currKeyLen + " or currValueLen "
            + currValueLen + " or currTagLen " + currTagsLen + ". Block offset: "
            + block.getOffset() + ", block length: " + blockBuffer.limit() + ", position: "
            + blockBuffer.position() + " (without header).");
      }
      blockBuffer.reset();
    }

    /**
     * Within a loaded block, seek looking for the last key that is smaller than
     * (or equal to?) the key we are interested in.
     * A note on the seekBefore: if you have seekBefore = true, AND the first
     * key in the block = key, then you'll get thrown exceptions. The caller has
     * to check for that case and load the previous block as appropriate.
     * @param key
     *          the key to find
     * @param seekBefore
     *          find the key before the given key in case of exact match.
     * @param offset
     *          Offset to find the key in the given bytebuffer
     * @param length
     *          Length of the key to be found
     * @return 0 in case of an exact key match, 1 in case of an inexact match,
     *         -2 in case of an inexact match and furthermore, the input key
     *         less than the first key of current block(e.g. using a faked index
     *         key)
     */
    protected int blockSeek(byte[] key, int offset, int length, boolean seekBefore) {
      int klen, vlen, tlen = 0;
      long memstoreTS = 0;
      int memstoreTSLen = 0;
      int lastKeyValueSize = -1;
      do {
        blockBuffer.mark();
        klen = blockBuffer.getInt();
        vlen = blockBuffer.getInt();
        ByteBufferUtils.skip(blockBuffer, klen + vlen);
        if (reader.hfileContext.shouldIncludeTags()) {
          tlen = blockBuffer.getShort();
          ByteBufferUtils.skip(blockBuffer, tlen);
        }
        if (this.reader.shouldIncludeMemstoreTS()) {
          if (this.reader.decodeMemstoreTS) {
            try {
              memstoreTS = Bytes.readVLong(blockBuffer.array(), blockBuffer.arrayOffset()
                  + blockBuffer.position());
              memstoreTSLen = WritableUtils.getVIntSize(memstoreTS);
            } catch (Exception e) {
              throw new RuntimeException("Error reading memstore timestamp", e);
            }
          } else {
            memstoreTS = 0;
            memstoreTSLen = 1;
          }
        }
        blockBuffer.reset();
        int keyOffset = blockBuffer.arrayOffset() + blockBuffer.position() + (Bytes.SIZEOF_INT * 2);
        int comp = reader.getComparator().compare(key, offset, length, blockBuffer.array(),
            keyOffset, klen);

        if (comp == 0) {
          if (seekBefore) {
            if (lastKeyValueSize < 0) {
              throw new IllegalStateException("blockSeek with seekBefore "
                  + "at the first key of the block: key=" + Bytes.toStringBinary(key)
                  + ", blockOffset=" + block.getOffset() + ", onDiskSize="
                  + block.getOnDiskSizeWithHeader());
            }
            blockBuffer.position(blockBuffer.position() - lastKeyValueSize);
            readKeyValueLen();
            return 1; // non exact match.
          }
          currKeyLen = klen;
          currValueLen = vlen;
          currTagsLen = tlen;
          if (this.reader.shouldIncludeMemstoreTS()) {
            currMemstoreTS = memstoreTS;
            currMemstoreTSLen = memstoreTSLen;
          }
          return 0; // indicate exact match
        } else if (comp < 0) {
          if (lastKeyValueSize > 0)
            blockBuffer.position(blockBuffer.position() - lastKeyValueSize);
          readKeyValueLen();
          if (lastKeyValueSize == -1 && blockBuffer.position() == 0) {
            return HConstants.INDEX_KEY_MAGIC;
          }
          return 1;
        }

        // The size of this key/value tuple, including key/value length fields.
        lastKeyValueSize = klen + vlen + memstoreTSLen + KEY_VALUE_LEN_SIZE;
        // include tag length also if tags included with KV
        if (reader.hfileContext.shouldIncludeTags()) {
          lastKeyValueSize += tlen + Bytes.SIZEOF_SHORT;
        }
        blockBuffer.position(blockBuffer.position() + lastKeyValueSize);
      } while (blockBuffer.remaining() > 0);

      // Seek to the last key we successfully read. This will happen if this is
      // the last key/value pair in the file, in which case the following call
      // to next() has to return false.
      blockBuffer.position(blockBuffer.position() - lastKeyValueSize);
      readKeyValueLen();
      return 1; // didn't exactly find it.
    }

  }

  /**
   * ScannerV3 that operates on encoded data blocks.
   */
  protected static class EncodedScannerV3 extends EncodedScannerV2 {
    public EncodedScannerV3(HFileReaderV3 reader, boolean cacheBlocks, boolean pread,
        boolean isCompaction, HFileContext meta) {
      super(reader, cacheBlocks, pread, isCompaction, meta);
    }
  }

  @Override
  public int getMajorVersion() {
    return 3;
  }

  @Override
  protected HFileBlock diskToCacheFormat(HFileBlock hfileBlock, final boolean isCompaction) {
    return dataBlockEncoder.diskToCacheFormat(hfileBlock, isCompaction);
  }
}
