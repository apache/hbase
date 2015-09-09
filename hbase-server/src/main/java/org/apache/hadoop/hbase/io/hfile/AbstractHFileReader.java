/*
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
package org.apache.hadoop.hbase.io.hfile;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.HFile.FileInfo;

/**
 * Common functionality needed by all versions of {@link HFile} readers.
 */
@InterfaceAudience.Private
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value="URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
public abstract class AbstractHFileReader
    implements HFile.Reader, Configurable {
  /** Stream to read from. Does checksum verifications in file system */
  protected FSDataInputStream istream; // UUF_UNUSED_PUBLIC_OR_PROTECTED_FIELD

  /** The file system stream of the underlying {@link HFile} that
   * does not do checksum verification in the file system */
  protected FSDataInputStream istreamNoFsChecksum;  // UUF_UNUSED_PUBLIC_OR_PROTECTED_FIELD

  /** Data block index reader keeping the root data index in memory */
  protected HFileBlockIndex.BlockIndexReader dataBlockIndexReader;

  /** Meta block index reader -- always single level */
  protected HFileBlockIndex.BlockIndexReader metaBlockIndexReader;

  protected final FixedFileTrailer trailer;

  /** Filled when we read in the trailer. */
  protected final Compression.Algorithm compressAlgo;

  private boolean isPrimaryReplicaReader;

  /**
   * What kind of data block encoding should be used while reading, writing,
   * and handling cache.
   */
  protected HFileDataBlockEncoder dataBlockEncoder =
      NoOpDataBlockEncoder.INSTANCE;

  /** Last key in the file. Filled in when we read in the file info */
  protected byte [] lastKey = null;

  /** Average key length read from file info */
  protected int avgKeyLen = -1;

  /** Average value length read from file info */
  protected int avgValueLen = -1;

  /** Key comparator */
  protected KVComparator comparator = new KVComparator();

  /** Size of this file. */
  protected final long fileSize;

  /** Block cache configuration. */
  protected final CacheConfig cacheConf;

  /** Path of file */
  protected final Path path;

  /** File name to be used for block names */
  protected final String name;

  protected FileInfo fileInfo;

  /** The filesystem used for accesing data */
  protected HFileSystem hfs;

  protected Configuration conf;

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
  protected AbstractHFileReader(Path path, FixedFileTrailer trailer,
      final long fileSize, final CacheConfig cacheConf, final HFileSystem hfs,
      final Configuration conf) {
    this.trailer = trailer;
    this.compressAlgo = trailer.getCompressionCodec();
    this.cacheConf = cacheConf;
    this.fileSize = fileSize;
    this.path = path;
    this.name = path.getName();
    this.hfs = hfs; // URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD
    this.conf = conf;
  }

  @SuppressWarnings("serial")
  public static class BlockIndexNotLoadedException
      extends IllegalStateException {
    public BlockIndexNotLoadedException() {
      // Add a message in case anyone relies on it as opposed to class name.
      super("Block index not loaded");
    }
  }

  protected String toStringFirstKey() {
    return KeyValue.keyToString(getFirstKey());
  }

  protected String toStringLastKey() {
    return KeyValue.keyToString(getLastKey());
  }

  public abstract boolean isFileInfoLoaded();

  @Override
  public String toString() {
    return "reader=" + path.toString() +
        (!isFileInfoLoaded()? "":
          ", compression=" + compressAlgo.getName() +
          ", cacheConf=" + cacheConf +
          ", firstKey=" + toStringFirstKey() +
          ", lastKey=" + toStringLastKey()) +
          ", avgKeyLen=" + avgKeyLen +
          ", avgValueLen=" + avgValueLen +
          ", entries=" + trailer.getEntryCount() +
          ", length=" + fileSize;
  }

  @Override
  public long length() {
    return fileSize;
  }

  /**
   * Create a Scanner on this file. No seeks or reads are done on creation. Call
   * {@link HFileScanner#seekTo(byte[])} to position an start the read. There is
   * nothing to clean up in a Scanner. Letting go of your references to the
   * scanner is sufficient. NOTE: Do not use this overload of getScanner for
   * compactions.
   *
   * @param cacheBlocks True if we should cache blocks read in by this scanner.
   * @param pread Use positional read rather than seek+read if true (pread is
   *          better for random reads, seek+read is better scanning).
   * @return Scanner on this file.
   */
  @Override
  public HFileScanner getScanner(boolean cacheBlocks, final boolean pread) {
    return getScanner(cacheBlocks, pread, false);
  }

  /**
   * @return the first key in the file. May be null if file has no entries. Note
   *         that this is not the first row key, but rather the byte form of the
   *         first KeyValue.
   */
  @Override
  public byte [] getFirstKey() {
    if (dataBlockIndexReader == null) {
      throw new BlockIndexNotLoadedException();
    }
    return dataBlockIndexReader.isEmpty() ? null
        : dataBlockIndexReader.getRootBlockKey(0);
  }

  /**
   * TODO left from {@link HFile} version 1: move this to StoreFile after Ryan's
   * patch goes in to eliminate {@link KeyValue} here.
   *
   * @return the first row key, or null if the file is empty.
   */
  @Override
  public byte[] getFirstRowKey() {
    byte[] firstKey = getFirstKey();
    if (firstKey == null)
      return null;
    return KeyValue.createKeyValueFromKey(firstKey).getRow();
  }

  /**
   * TODO left from {@link HFile} version 1: move this to StoreFile after
   * Ryan's patch goes in to eliminate {@link KeyValue} here.
   *
   * @return the last row key, or null if the file is empty.
   */
  @Override
  public byte[] getLastRowKey() {
    byte[] lastKey = getLastKey();
    if (lastKey == null)
      return null;
    return KeyValue.createKeyValueFromKey(lastKey).getRow();
  }

  /** @return number of KV entries in this HFile */
  @Override
  public long getEntries() {
    return trailer.getEntryCount();
  }

  /** @return comparator */
  @Override
  public KVComparator getComparator() {
    return comparator;
  }

  /** @return compression algorithm */
  @Override
  public Compression.Algorithm getCompressionAlgorithm() {
    return compressAlgo;
  }

  /**
   * @return the total heap size of data and meta block indexes in bytes. Does
   *         not take into account non-root blocks of a multilevel data index.
   */
  public long indexSize() {
    return (dataBlockIndexReader != null ? dataBlockIndexReader.heapSize() : 0)
        + ((metaBlockIndexReader != null) ? metaBlockIndexReader.heapSize()
            : 0);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public HFileBlockIndex.BlockIndexReader getDataBlockIndexReader() {
    return dataBlockIndexReader;
  }

  @Override
  public FixedFileTrailer getTrailer() {
    return trailer;
  }

  @Override
  public boolean isPrimaryReplicaReader() {
    return isPrimaryReplicaReader;
  }

  @Override
  public void setPrimaryReplicaReader(boolean isPrimaryReplicaReader) {
    this.isPrimaryReplicaReader = isPrimaryReplicaReader;
  }

  public FileInfo loadFileInfo() throws IOException {
    return fileInfo;
  }

  /**
   * An exception thrown when an operation requiring a scanner to be seeked
   * is invoked on a scanner that is not seeked.
   */
  @SuppressWarnings("serial")
  public static class NotSeekedException extends IllegalStateException {
    public NotSeekedException() {
      super("Not seeked to a key/value");
    }
  }

  protected static abstract class Scanner implements HFileScanner {
    protected ByteBuffer blockBuffer;

    protected boolean cacheBlocks;
    protected final boolean pread;
    protected final boolean isCompaction;

    protected int currKeyLen;
    protected int currValueLen;
    protected int currMemstoreTSLen;
    protected long currMemstoreTS;

    protected int blockFetches;

    protected final HFile.Reader reader;

    public Scanner(final HFile.Reader reader, final boolean cacheBlocks,
        final boolean pread, final boolean isCompaction) {
      this.reader = reader;
      this.cacheBlocks = cacheBlocks;
      this.pread = pread;
      this.isCompaction = isCompaction;
    }

    @Override
    public boolean isSeeked(){
      return blockBuffer != null;
    }

    @Override
    public String toString() {
      return "HFileScanner for reader " + String.valueOf(getReader());
    }

    protected void assertSeeked() {
      if (!isSeeked())
        throw new NotSeekedException();
    }

    @Override
    public int seekTo(byte[] key) throws IOException {
      return seekTo(key, 0, key.length);
    }
    
    @Override
    public boolean seekBefore(byte[] key) throws IOException {
      return seekBefore(key, 0, key.length);
    }
    
    @Override
    public int reseekTo(byte[] key) throws IOException {
      return reseekTo(key, 0, key.length);
    }

    @Override
    public HFile.Reader getReader() {
      return reader;
    }
  }

  /** For testing */
  abstract HFileBlock.FSReader getUncachedBlockReader();

  public Path getPath() {
    return path;
  }

  @Override
  public DataBlockEncoding getDataBlockEncoding() {
    return dataBlockEncoder.getDataBlockEncoding();
  }

  public abstract int getMajorVersion();

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }
}
