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
import java.security.Key;
import java.security.KeyException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.NoTagsKeyValue;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.crypto.Cipher;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.hfile.HFile.FileInfo;
import org.apache.hadoop.hbase.security.EncryptionUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;

/**
 * {@link HFile} reader for version 3.
 */
@InterfaceAudience.Private
public class HFileReaderV3 extends HFileReaderV2 {

  private static final Log LOG = LogFactory.getLog(HFileReaderV3.class);

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
   * @param hfs
   *          The file system.
   * @param conf
   *          Configuration
   */
  public HFileReaderV3(final Path path, FixedFileTrailer trailer,
      final FSDataInputStreamWrapper fsdis,
      final long size, final CacheConfig cacheConf, final HFileSystem hfs,
      final Configuration conf) throws IOException {
    super(path, trailer, fsdis, size, cacheConf, hfs, conf);
    byte[] tmp = fileInfo.get(FileInfo.MAX_TAGS_LEN);
    // max tag length is not present in the HFile means tags were not at all written to file.
    if (tmp != null) {
      hfileContext.setIncludesTags(true);
      tmp = fileInfo.get(FileInfo.TAGS_COMPRESSED);
      if (tmp != null && Bytes.toBoolean(tmp)) {
        hfileContext.setCompressTags(true);
      }
    }
  }

  @Override
  protected HFileContext createHFileContext(FSDataInputStreamWrapper fsdis, long fileSize,
      HFileSystem hfs, Path path, FixedFileTrailer trailer) throws IOException {
    trailer.expectMajorVersion(3);
    HFileContextBuilder builder = new HFileContextBuilder()
      .withIncludesMvcc(shouldIncludeMemstoreTS())
      .withHBaseCheckSum(true)
      .withCompression(this.compressAlgo);

    // Check for any key material available
    byte[] keyBytes = trailer.getEncryptionKey();
    if (keyBytes != null) {
      Encryption.Context cryptoContext = Encryption.newContext(conf);
      Key key;
      String masterKeyName = conf.get(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY,
        User.getCurrent().getShortName());
      try {
        // First try the master key
        key = EncryptionUtil.unwrapKey(conf, masterKeyName, keyBytes);
      } catch (KeyException e) {
        // If the current master key fails to unwrap, try the alternate, if
        // one is configured
        if (LOG.isDebugEnabled()) {
          LOG.debug("Unable to unwrap key with current master key '" + masterKeyName + "'");
        }
        String alternateKeyName =
          conf.get(HConstants.CRYPTO_MASTERKEY_ALTERNATE_NAME_CONF_KEY);
        if (alternateKeyName != null) {
          try {
            key = EncryptionUtil.unwrapKey(conf, alternateKeyName, keyBytes);
          } catch (KeyException ex) {
            throw new IOException(ex);
          }
        } else {
          throw new IOException(e);
        }
      }
      // Use the algorithm the key wants
      Cipher cipher = Encryption.getCipher(conf, key.getAlgorithm());
      if (cipher == null) {
        throw new IOException("Cipher '" + key.getAlgorithm() + "' is not available");
      }
      cryptoContext.setCipher(cipher);
      cryptoContext.setKey(key);
      builder.withEncryptionContext(cryptoContext);
    }

    HFileContext context = builder.build();

    if (LOG.isTraceEnabled()) {
      LOG.trace("Reader" + (path != null ? " for " + path : "" ) +
        " initialized with cacheConf: " + cacheConf +
        " comparator: " + comparator.getClass().getSimpleName() +
        " fileContext: " + context);
    }

    return context;
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
    if (dataBlockEncoder.useEncodedScanner()) {
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
    protected int getCellBufSize() {
      int kvBufSize = super.getCellBufSize();
      if (reader.hfileContext.isIncludesTags()) {
        kvBufSize += Bytes.SIZEOF_SHORT + currTagsLen;
      }
      return kvBufSize;
    }

    @Override
    public Cell getKeyValue() {
      if (!isSeeked())
        return null;
      if (currTagsLen > 0) {
        KeyValue ret = new KeyValue(blockBuffer.array(), blockBuffer.arrayOffset()
            + blockBuffer.position(), getCellBufSize());
        if (this.reader.shouldIncludeMemstoreTS()) {
          ret.setSequenceId(currMemstoreTS);
        }
        return ret;
      } else {
        return formNoTagsKeyValue();
      }
    }

    protected void setNonSeekedState() {
      super.setNonSeekedState();
      currTagsLen = 0;
    }

    @Override
    protected int getNextCellStartPosition() {
      int nextKvPos = super.getNextCellStartPosition();
      if (reader.hfileContext.isIncludesTags()) {
        nextKvPos += Bytes.SIZEOF_SHORT + currTagsLen;
      }
      return nextKvPos;
    }

    private final void checkTagsLen() {
      if (checkLen(this.currTagsLen)) {
        throw new IllegalStateException("Invalid currTagsLen " + this.currTagsLen +
          ". Block offset: " + block.getOffset() + ", block length: " + this.blockBuffer.limit() +
          ", position: " + this.blockBuffer.position() + " (without header).");
      }
    }

    protected final void readKeyValueLen() {
      // TODO: METHOD (mostly) DUPLICATED IN V2!!!! FIXED in master branch by collapsing v3 and v2.
      // This is a hot method. We go out of our way to make this method short so it can be
      // inlined and is not too big to compile. We also manage position in ByteBuffer ourselves
      // because it is faster than going via range-checked ByteBuffer methods or going through a
      // byte buffer array a byte at a time.
      int p = blockBuffer.position() + blockBuffer.arrayOffset();
      // Get a long at a time rather than read two individual ints. In micro-benchmarking, even
      // with the extra bit-fiddling, this is order-of-magnitude faster than getting two ints.
      long ll = Bytes.toLong(blockBuffer.array(), p);
      // Read top half as an int of key length and bottom int as value length
      this.currKeyLen = (int)(ll >> Integer.SIZE);
      this.currValueLen = (int)(Bytes.MASK_FOR_LOWER_INT_IN_LONG ^ ll);
      checkKeyValueLen();
      // Move position past the key and value lengths and then beyond the key and value
      p += (Bytes.SIZEOF_LONG + currKeyLen + currValueLen);
      if (reader.hfileContext.isIncludesTags()) {
        // Tags length is a short.
        this.currTagsLen = Bytes.toShort(blockBuffer.array(), p);
        checkTagsLen();
        p += (Bytes.SIZEOF_SHORT + currTagsLen);
      }
      readMvccVersion(p);
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
     * @return 0 in case of an exact key match, 1 in case of an inexact match,
     *         -2 in case of an inexact match and furthermore, the input key
     *         less than the first key of current block(e.g. using a faked index
     *         key)
     */
    @Override
    protected int blockSeek(Cell key, boolean seekBefore) {
      int klen, vlen, tlen = 0;
      long memstoreTS = 0;
      int memstoreTSLen = 0;
      int lastKeyValueSize = -1;
      KeyValue.KeyOnlyKeyValue keyOnlyKv = new KeyValue.KeyOnlyKeyValue();
      do {
        blockBuffer.mark();
        klen = blockBuffer.getInt();
        vlen = blockBuffer.getInt();
        if (klen < 0 || vlen < 0 || klen > blockBuffer.limit()
            || vlen > blockBuffer.limit()) {
          throw new IllegalStateException("Invalid klen " + klen + " or vlen "
              + vlen + ". Block offset: "
              + block.getOffset() + ", block length: " + blockBuffer.limit() + ", position: "
              + blockBuffer.position() + " (without header).");
        }
        ByteBufferUtils.skip(blockBuffer, klen + vlen);
        if (reader.hfileContext.isIncludesTags()) {
          // Read short as unsigned, high byte first
          tlen = ((blockBuffer.get() & 0xff) << 8) ^ (blockBuffer.get() & 0xff);
          if (tlen < 0 || tlen > blockBuffer.limit()) {
            throw new IllegalStateException("Invalid tlen " + tlen + ". Block offset: "
                + block.getOffset() + ", block length: " + blockBuffer.limit() + ", position: "
                + blockBuffer.position() + " (without header).");
          }
          ByteBufferUtils.skip(blockBuffer, tlen);
        }
        if (this.reader.shouldIncludeMemstoreTS()) {
          if (this.reader.decodeMemstoreTS) {
            memstoreTS = Bytes.readAsVLong(blockBuffer.array(), blockBuffer.arrayOffset()
                + blockBuffer.position());
            memstoreTSLen = WritableUtils.getVIntSize(memstoreTS);
          } else {
            memstoreTS = 0;
            memstoreTSLen = 1;
          }
        }
        blockBuffer.reset();
        int keyOffset =
          blockBuffer.arrayOffset() + blockBuffer.position() + (Bytes.SIZEOF_INT * 2);
        keyOnlyKv.setKey(blockBuffer.array(), keyOffset, klen);
        int comp = reader.getComparator().compareOnlyKeyPortion(key, keyOnlyKv);

        if (comp == 0) {
          if (seekBefore) {
            if (lastKeyValueSize < 0) {
              throw new IllegalStateException("blockSeek with seekBefore "
                  + "at the first key of the block: key="
                  + CellUtil.getCellKeyAsString(key)
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
        if (reader.hfileContext.isIncludesTags()) {
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
        boolean isCompaction, HFileContext context) {
      super(reader, cacheBlocks, pread, isCompaction, context);
    }
  }

  @Override
  public int getMajorVersion() {
    return 3;
  }
}
