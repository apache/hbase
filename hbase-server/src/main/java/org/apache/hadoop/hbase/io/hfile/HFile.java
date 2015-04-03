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
package org.apache.hadoop.hbase.io.hfile;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.SequenceInputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.BytesBytesPair;
import org.apache.hadoop.hbase.protobuf.generated.HFileProtos;
import org.apache.hadoop.hbase.util.BloomFilterWriter;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ChecksumType;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.io.Writable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * File format for hbase.
 * A file of sorted key/value pairs. Both keys and values are byte arrays.
 * <p>
 * The memory footprint of a HFile includes the following (below is taken from the
 * <a
 * href=https://issues.apache.org/jira/browse/HADOOP-3315>TFile</a> documentation
 * but applies also to HFile):
 * <ul>
 * <li>Some constant overhead of reading or writing a compressed block.
 * <ul>
 * <li>Each compressed block requires one compression/decompression codec for
 * I/O.
 * <li>Temporary space to buffer the key.
 * <li>Temporary space to buffer the value.
 * </ul>
 * <li>HFile index, which is proportional to the total number of Data Blocks.
 * The total amount of memory needed to hold the index can be estimated as
 * (56+AvgKeySize)*NumBlocks.
 * </ul>
 * Suggestions on performance optimization.
 * <ul>
 * <li>Minimum block size. We recommend a setting of minimum block size between
 * 8KB to 1MB for general usage. Larger block size is preferred if files are
 * primarily for sequential access. However, it would lead to inefficient random
 * access (because there are more data to decompress). Smaller blocks are good
 * for random access, but require more memory to hold the block index, and may
 * be slower to create (because we must flush the compressor stream at the
 * conclusion of each data block, which leads to an FS I/O flush). Further, due
 * to the internal caching in Compression codec, the smallest possible block
 * size would be around 20KB-30KB.
 * <li>The current implementation does not offer true multi-threading for
 * reading. The implementation uses FSDataInputStream seek()+read(), which is
 * shown to be much faster than positioned-read call in single thread mode.
 * However, it also means that if multiple threads attempt to access the same
 * HFile (using multiple scanners) simultaneously, the actual I/O is carried out
 * sequentially even if they access different DFS blocks (Reexamine! pread seems
 * to be 10% faster than seek+read in my testing -- stack).
 * <li>Compression codec. Use "none" if the data is not very compressable (by
 * compressable, I mean a compression ratio at least 2:1). Generally, use "lzo"
 * as the starting point for experimenting. "gz" overs slightly better
 * compression ratio over "lzo" but requires 4x CPU to compress and 2x CPU to
 * decompress, comparing to "lzo".
 * </ul>
 *
 * For more on the background behind HFile, see <a
 * href=https://issues.apache.org/jira/browse/HBASE-61>HBASE-61</a>.
 * <p>
 * File is made of data blocks followed by meta data blocks (if any), a fileinfo
 * block, data block index, meta data block index, and a fixed size trailer
 * which records the offsets at which file changes content type.
 * <pre>&lt;data blocks>&lt;meta blocks>&lt;fileinfo>&lt;data index>&lt;meta index>&lt;trailer></pre>
 * Each block has a bit of magic at its start.  Block are comprised of
 * key/values.  In data blocks, they are both byte arrays.  Metadata blocks are
 * a String key and a byte array value.  An empty file looks like this:
 * <pre>&lt;fileinfo>&lt;trailer></pre>.  That is, there are not data nor meta
 * blocks present.
 * <p>
 * TODO: Do scanners need to be able to take a start and end row?
 * TODO: Should BlockIndex know the name of its file?  Should it have a Path
 * that points at its file say for the case where an index lives apart from
 * an HFile instance?
 */
@InterfaceAudience.Private
public class HFile {
  static final Log LOG = LogFactory.getLog(HFile.class);

  /**
   * Maximum length of key in HFile.
   */
  public final static int MAXIMUM_KEY_LENGTH = Integer.MAX_VALUE;

  /**
   * Default compression: none.
   */
  public final static Compression.Algorithm DEFAULT_COMPRESSION_ALGORITHM =
    Compression.Algorithm.NONE;

  /** Minimum supported HFile format version */
  public static final int MIN_FORMAT_VERSION = 2;

  /** Maximum supported HFile format version
   */
  public static final int MAX_FORMAT_VERSION = 3;

  /**
   * Minimum HFile format version with support for persisting cell tags
   */
  public static final int MIN_FORMAT_VERSION_WITH_TAGS = 3;

  /** Default compression name: none. */
  public final static String DEFAULT_COMPRESSION =
    DEFAULT_COMPRESSION_ALGORITHM.getName();

  /** Meta data block name for bloom filter bits. */
  public static final String BLOOM_FILTER_DATA_KEY = "BLOOM_FILTER_DATA";

  /**
   * We assume that HFile path ends with
   * ROOT_DIR/TABLE_NAME/REGION_NAME/CF_NAME/HFILE, so it has at least this
   * many levels of nesting. This is needed for identifying table and CF name
   * from an HFile path.
   */
  public final static int MIN_NUM_HFILE_PATH_LEVELS = 5;

  /**
   * The number of bytes per checksum.
   */
  public static final int DEFAULT_BYTES_PER_CHECKSUM = 16 * 1024;
  // TODO: This define is done in three places.  Fix.
  public static final ChecksumType DEFAULT_CHECKSUM_TYPE = ChecksumType.CRC32;

  // For measuring number of checksum failures
  static final AtomicLong checksumFailures = new AtomicLong();

  // for test purpose
  public static final AtomicLong dataBlockReadCnt = new AtomicLong(0);

  /**
   * Number of checksum verification failures. It also
   * clears the counter.
   */
  public static final long getChecksumFailuresCount() {
    return checksumFailures.getAndSet(0);
  }

  /** API required to write an {@link HFile} */
  public interface Writer extends Closeable {
    /** Max memstore (mvcc) timestamp in FileInfo */
    public static final byte [] MAX_MEMSTORE_TS_KEY = Bytes.toBytes("MAX_MEMSTORE_TS_KEY");

    /** Add an element to the file info map. */
    void appendFileInfo(byte[] key, byte[] value) throws IOException;

    void append(Cell cell) throws IOException;

    /** @return the path to this {@link HFile} */
    Path getPath();

    /**
     * Adds an inline block writer such as a multi-level block index writer or
     * a compound Bloom filter writer.
     */
    void addInlineBlockWriter(InlineBlockWriter bloomWriter);

    // The below three methods take Writables.  We'd like to undo Writables but undoing the below would be pretty
    // painful.  Could take a byte [] or a Message but we want to be backward compatible around hfiles so would need
    // to map between Message and Writable or byte [] and current Writable serialization.  This would be a bit of work
    // to little gain.  Thats my thinking at moment.  St.Ack 20121129

    void appendMetaBlock(String bloomFilterMetaKey, Writable metaWriter);

    /**
     * Store general Bloom filter in the file. This does not deal with Bloom filter
     * internals but is necessary, since Bloom filters are stored differently
     * in HFile version 1 and version 2.
     */
    void addGeneralBloomFilter(BloomFilterWriter bfw);

    /**
     * Store delete family Bloom filter in the file, which is only supported in
     * HFile V2.
     */
    void addDeleteFamilyBloomFilter(BloomFilterWriter bfw) throws IOException;

    /**
     * Return the file context for the HFile this writer belongs to
     */
    HFileContext getFileContext();
  }

  /**
   * This variety of ways to construct writers is used throughout the code, and
   * we want to be able to swap writer implementations.
   */
  public static abstract class WriterFactory {
    protected final Configuration conf;
    protected final CacheConfig cacheConf;
    protected FileSystem fs;
    protected Path path;
    protected FSDataOutputStream ostream;
    protected KVComparator comparator = KeyValue.COMPARATOR;
    protected InetSocketAddress[] favoredNodes;
    private HFileContext fileContext;

    WriterFactory(Configuration conf, CacheConfig cacheConf) {
      this.conf = conf;
      this.cacheConf = cacheConf;
    }

    public WriterFactory withPath(FileSystem fs, Path path) {
      Preconditions.checkNotNull(fs);
      Preconditions.checkNotNull(path);
      this.fs = fs;
      this.path = path;
      return this;
    }

    public WriterFactory withOutputStream(FSDataOutputStream ostream) {
      Preconditions.checkNotNull(ostream);
      this.ostream = ostream;
      return this;
    }

    public WriterFactory withComparator(KVComparator comparator) {
      Preconditions.checkNotNull(comparator);
      this.comparator = comparator;
      return this;
    }

    public WriterFactory withFavoredNodes(InetSocketAddress[] favoredNodes) {
      // Deliberately not checking for null here.
      this.favoredNodes = favoredNodes;
      return this;
    }

    public WriterFactory withFileContext(HFileContext fileContext) {
      this.fileContext = fileContext;
      return this;
    }

    public Writer create() throws IOException {
      if ((path != null ? 1 : 0) + (ostream != null ? 1 : 0) != 1) {
        throw new AssertionError("Please specify exactly one of " +
            "filesystem/path or path");
      }
      if (path != null) {
        ostream = HFileWriterImpl.createOutputStream(conf, fs, path, favoredNodes);
      }
      return createWriter(fs, path, ostream,
                   comparator, fileContext);
    }

    protected abstract Writer createWriter(FileSystem fs, Path path, FSDataOutputStream ostream,
        KVComparator comparator, HFileContext fileContext) throws IOException;
  }

  /** The configuration key for HFile version to use for new files */
  public static final String FORMAT_VERSION_KEY = "hfile.format.version";

  public static int getFormatVersion(Configuration conf) {
    int version = conf.getInt(FORMAT_VERSION_KEY, MAX_FORMAT_VERSION);
    checkFormatVersion(version);
    return version;
  }

  /**
   * Returns the factory to be used to create {@link HFile} writers.
   * Disables block cache access for all writers created through the
   * returned factory.
   */
  public static final WriterFactory getWriterFactoryNoCache(Configuration
       conf) {
    Configuration tempConf = new Configuration(conf);
    tempConf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0.0f);
    return HFile.getWriterFactory(conf, new CacheConfig(tempConf));
  }

  /**
   * Returns the factory to be used to create {@link HFile} writers
   */
  public static final WriterFactory getWriterFactory(Configuration conf,
      CacheConfig cacheConf) {
    int version = getFormatVersion(conf);
    switch (version) {
    case 2:
      throw new IllegalArgumentException("This should never happen. " +
        "Did you change hfile.format.version to read v2? This version of the software writes v3" +
        " hfiles only (but it can read v2 files without having to update hfile.format.version " +
        "in hbase-site.xml)");
    case 3:
      return new HFileWriterFactory(conf, cacheConf);
    default:
      throw new IllegalArgumentException("Cannot create writer for HFile " +
          "format version " + version);
    }
  }

  /**
   * An abstraction used by the block index.
   * Implementations will check cache for any asked-for block and return cached block if found.
   * Otherwise, after reading from fs, will try and put block into cache before returning.
   */
  public interface CachingBlockReader {
    /**
     * Read in a file block.
     * @param offset offset to read.
     * @param onDiskBlockSize size of the block
     * @param cacheBlock
     * @param pread
     * @param isCompaction is this block being read as part of a compaction
     * @param expectedBlockType the block type we are expecting to read with this read operation,
     *  or null to read whatever block type is available and avoid checking (that might reduce
     *  caching efficiency of encoded data blocks)
     * @param expectedDataBlockEncoding the data block encoding the caller is expecting data blocks
     *  to be in, or null to not perform this check and return the block irrespective of the
     *  encoding. This check only applies to data blocks and can be set to null when the caller is
     *  expecting to read a non-data block and has set expectedBlockType accordingly.
     * @return Block wrapped in a ByteBuffer.
     * @throws IOException
     */
    HFileBlock readBlock(long offset, long onDiskBlockSize,
        boolean cacheBlock, final boolean pread, final boolean isCompaction,
        final boolean updateCacheMetrics, BlockType expectedBlockType,
        DataBlockEncoding expectedDataBlockEncoding)
        throws IOException;
  }

  /** An interface used by clients to open and iterate an {@link HFile}. */
  public interface Reader extends Closeable, CachingBlockReader {
    /**
     * Returns this reader's "name". Usually the last component of the path.
     * Needs to be constant as the file is being moved to support caching on
     * write.
     */
    String getName();

    KVComparator getComparator();

    HFileScanner getScanner(boolean cacheBlocks, final boolean pread, final boolean isCompaction);

    ByteBuffer getMetaBlock(String metaBlockName, boolean cacheBlock) throws IOException;

    Map<byte[], byte[]> loadFileInfo() throws IOException;

    byte[] getLastKey();

    byte[] midkey() throws IOException;

    long length();

    long getEntries();

    byte[] getFirstKey();

    long indexSize();

    byte[] getFirstRowKey();

    byte[] getLastRowKey();

    FixedFileTrailer getTrailer();

    HFileBlockIndex.BlockIndexReader getDataBlockIndexReader();

    HFileScanner getScanner(boolean cacheBlocks, boolean pread);

    Compression.Algorithm getCompressionAlgorithm();

    /**
     * Retrieves general Bloom filter metadata as appropriate for each
     * {@link HFile} version.
     * Knows nothing about how that metadata is structured.
     */
    DataInput getGeneralBloomFilterMetadata() throws IOException;

    /**
     * Retrieves delete family Bloom filter metadata as appropriate for each
     * {@link HFile}  version.
     * Knows nothing about how that metadata is structured.
     */
    DataInput getDeleteBloomFilterMetadata() throws IOException;

    Path getPath();

    /** Close method with optional evictOnClose */
    void close(boolean evictOnClose) throws IOException;

    DataBlockEncoding getDataBlockEncoding();

    boolean hasMVCCInfo();

    /**
     * Return the file context of the HFile this reader belongs to
     */
    HFileContext getFileContext();

    boolean shouldIncludeMemstoreTS();

    boolean isDecodeMemstoreTS();

    DataBlockEncoding getEffectiveEncodingInCache(boolean isCompaction);

    @VisibleForTesting
    HFileBlock.FSReader getUncachedBlockReader();

    @VisibleForTesting
    boolean prefetchComplete();
  }

  /**
   * Method returns the reader given the specified arguments.
   * TODO This is a bad abstraction.  See HBASE-6635.
   *
   * @param path hfile's path
   * @param fsdis stream of path's file
   * @param size max size of the trailer.
   * @param cacheConf Cache configuation values, cannot be null.
   * @param hfs
   * @return an appropriate instance of HFileReader
   * @throws IOException If file is invalid, will throw CorruptHFileException flavored IOException
   */
  private static Reader pickReaderVersion(Path path, FSDataInputStreamWrapper fsdis,
      long size, CacheConfig cacheConf, HFileSystem hfs, Configuration conf) throws IOException {
    FixedFileTrailer trailer = null;
    try {
      boolean isHBaseChecksum = fsdis.shouldUseHBaseChecksum();
      assert !isHBaseChecksum; // Initially we must read with FS checksum.
      trailer = FixedFileTrailer.readFromStream(fsdis.getStream(isHBaseChecksum), size);
      switch (trailer.getMajorVersion()) {
      case 2:
        LOG.debug("Opening HFile v2 with v3 reader");
        // Fall through.
      case 3 :
        return new HFileReaderImpl(path, trailer, fsdis, size, cacheConf, hfs, conf);
      default:
        throw new IllegalArgumentException("Invalid HFile version " + trailer.getMajorVersion());
      }
    } catch (Throwable t) {
      try {
        fsdis.close();
      } catch (Throwable t2) {
        LOG.warn("Error closing fsdis FSDataInputStreamWrapper", t2);
      }
      throw new CorruptHFileException("Problem reading HFile Trailer from file " + path, t);
    }
  }

  /**
   * @param fs A file system
   * @param path Path to HFile
   * @param fsdis a stream of path's file
   * @param size max size of the trailer.
   * @param cacheConf Cache configuration for hfile's contents
   * @param conf Configuration
   * @return A version specific Hfile Reader
   * @throws IOException If file is invalid, will throw CorruptHFileException flavored IOException
   */
  @SuppressWarnings("resource")
  public static Reader createReader(FileSystem fs, Path path,
      FSDataInputStreamWrapper fsdis, long size, CacheConfig cacheConf, Configuration conf)
      throws IOException {
    HFileSystem hfs = null;

    // If the fs is not an instance of HFileSystem, then create an
    // instance of HFileSystem that wraps over the specified fs.
    // In this case, we will not be able to avoid checksumming inside
    // the filesystem.
    if (!(fs instanceof HFileSystem)) {
      hfs = new HFileSystem(fs);
    } else {
      hfs = (HFileSystem)fs;
    }
    return pickReaderVersion(path, fsdis, size, cacheConf, hfs, conf);
  }

  /**
   *
   * @param fs filesystem
   * @param path Path to file to read
   * @param cacheConf This must not be null.  @see {@link org.apache.hadoop.hbase.io.hfile.CacheConfig#CacheConfig(Configuration)}
   * @return an active Reader instance
   * @throws IOException Will throw a CorruptHFileException (DoNotRetryIOException subtype) if hfile is corrupt/invalid.
   */
  public static Reader createReader(
      FileSystem fs, Path path, CacheConfig cacheConf, Configuration conf) throws IOException {
    Preconditions.checkNotNull(cacheConf, "Cannot create Reader with null CacheConf");
    FSDataInputStreamWrapper stream = new FSDataInputStreamWrapper(fs, path);
    return pickReaderVersion(path, stream, fs.getFileStatus(path).getLen(),
      cacheConf, stream.getHfs(), conf);
  }

  /**
   * This factory method is used only by unit tests
   */
  static Reader createReaderFromStream(Path path,
      FSDataInputStream fsdis, long size, CacheConfig cacheConf, Configuration conf)
      throws IOException {
    FSDataInputStreamWrapper wrapper = new FSDataInputStreamWrapper(fsdis);
    return pickReaderVersion(path, wrapper, size, cacheConf, null, conf);
  }

  /**
   * Returns true if the specified file has a valid HFile Trailer.
   * @param fs filesystem
   * @param path Path to file to verify
   * @return true if the file has a valid HFile Trailer, otherwise false
   * @throws IOException if failed to read from the underlying stream
   */
  public static boolean isHFileFormat(final FileSystem fs, final Path path) throws IOException {
    return isHFileFormat(fs, fs.getFileStatus(path));
  }

  /**
   * Returns true if the specified file has a valid HFile Trailer.
   * @param fs filesystem
   * @param fileStatus the file to verify
   * @return true if the file has a valid HFile Trailer, otherwise false
   * @throws IOException if failed to read from the underlying stream
   */
  public static boolean isHFileFormat(final FileSystem fs, final FileStatus fileStatus)
      throws IOException {
    final Path path = fileStatus.getPath();
    final long size = fileStatus.getLen();
    FSDataInputStreamWrapper fsdis = new FSDataInputStreamWrapper(fs, path);
    try {
      boolean isHBaseChecksum = fsdis.shouldUseHBaseChecksum();
      assert !isHBaseChecksum; // Initially we must read with FS checksum.
      FixedFileTrailer.readFromStream(fsdis.getStream(isHBaseChecksum), size);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    } catch (IOException e) {
      throw e;
    } finally {
      try {
        fsdis.close();
      } catch (Throwable t) {
        LOG.warn("Error closing fsdis FSDataInputStreamWrapper: " + path, t);
      }
    }
  }

  /**
   * Metadata for this file. Conjured by the writer. Read in by the reader.
   */
  public static class FileInfo implements SortedMap<byte[], byte[]> {
    static final String RESERVED_PREFIX = "hfile.";
    static final byte[] RESERVED_PREFIX_BYTES = Bytes.toBytes(RESERVED_PREFIX);
    static final byte [] LASTKEY = Bytes.toBytes(RESERVED_PREFIX + "LASTKEY");
    static final byte [] AVG_KEY_LEN = Bytes.toBytes(RESERVED_PREFIX + "AVG_KEY_LEN");
    static final byte [] AVG_VALUE_LEN = Bytes.toBytes(RESERVED_PREFIX + "AVG_VALUE_LEN");
    static final byte [] CREATE_TIME_TS = Bytes.toBytes(RESERVED_PREFIX + "CREATE_TIME_TS");
    static final byte [] COMPARATOR = Bytes.toBytes(RESERVED_PREFIX + "COMPARATOR");
    static final byte [] TAGS_COMPRESSED = Bytes.toBytes(RESERVED_PREFIX + "TAGS_COMPRESSED");
    public static final byte [] MAX_TAGS_LEN = Bytes.toBytes(RESERVED_PREFIX + "MAX_TAGS_LEN");
    private final SortedMap<byte [], byte []> map = new TreeMap<byte [], byte []>(Bytes.BYTES_COMPARATOR);

    public FileInfo() {
      super();
    }

    /**
     * Append the given key/value pair to the file info, optionally checking the
     * key prefix.
     *
     * @param k key to add
     * @param v value to add
     * @param checkPrefix whether to check that the provided key does not start
     *          with the reserved prefix
     * @return this file info object
     * @throws IOException if the key or value is invalid
     */
    public FileInfo append(final byte[] k, final byte[] v,
        final boolean checkPrefix) throws IOException {
      if (k == null || v == null) {
        throw new NullPointerException("Key nor value may be null");
      }
      if (checkPrefix && isReservedFileInfoKey(k)) {
        throw new IOException("Keys with a " + FileInfo.RESERVED_PREFIX
            + " are reserved");
      }
      put(k, v);
      return this;
    }

    public void clear() {
      this.map.clear();
    }

    public Comparator<? super byte[]> comparator() {
      return map.comparator();
    }

    public boolean containsKey(Object key) {
      return map.containsKey(key);
    }

    public boolean containsValue(Object value) {
      return map.containsValue(value);
    }

    public Set<java.util.Map.Entry<byte[], byte[]>> entrySet() {
      return map.entrySet();
    }

    public boolean equals(Object o) {
      return map.equals(o);
    }

    public byte[] firstKey() {
      return map.firstKey();
    }

    public byte[] get(Object key) {
      return map.get(key);
    }

    public int hashCode() {
      return map.hashCode();
    }

    public SortedMap<byte[], byte[]> headMap(byte[] toKey) {
      return this.map.headMap(toKey);
    }

    public boolean isEmpty() {
      return map.isEmpty();
    }

    public Set<byte[]> keySet() {
      return map.keySet();
    }

    public byte[] lastKey() {
      return map.lastKey();
    }

    public byte[] put(byte[] key, byte[] value) {
      return this.map.put(key, value);
    }

    public void putAll(Map<? extends byte[], ? extends byte[]> m) {
      this.map.putAll(m);
    }

    public byte[] remove(Object key) {
      return this.map.remove(key);
    }

    public int size() {
      return map.size();
    }

    public SortedMap<byte[], byte[]> subMap(byte[] fromKey, byte[] toKey) {
      return this.map.subMap(fromKey, toKey);
    }

    public SortedMap<byte[], byte[]> tailMap(byte[] fromKey) {
      return this.map.tailMap(fromKey);
    }

    public Collection<byte[]> values() {
      return map.values();
    }

    /**
     * Write out this instance on the passed in <code>out</code> stream.
     * We write it as a protobuf.
     * @param out
     * @throws IOException
     * @see #read(DataInputStream)
     */
    void write(final DataOutputStream out) throws IOException {
      HFileProtos.FileInfoProto.Builder builder = HFileProtos.FileInfoProto.newBuilder();
      for (Map.Entry<byte [], byte[]> e: this.map.entrySet()) {
        HBaseProtos.BytesBytesPair.Builder bbpBuilder = HBaseProtos.BytesBytesPair.newBuilder();
        bbpBuilder.setFirst(ByteStringer.wrap(e.getKey()));
        bbpBuilder.setSecond(ByteStringer.wrap(e.getValue()));
        builder.addMapEntry(bbpBuilder.build());
      }
      out.write(ProtobufUtil.PB_MAGIC);
      builder.build().writeDelimitedTo(out);
    }

    /**
     * Populate this instance with what we find on the passed in <code>in</code> stream.
     * Can deserialize protobuf of old Writables format.
     * @param in
     * @throws IOException
     * @see #write(DataOutputStream)
     */
    void read(final DataInputStream in) throws IOException {
      // This code is tested over in TestHFileReaderV1 where we read an old hfile w/ this new code.
      int pblen = ProtobufUtil.lengthOfPBMagic();
      byte [] pbuf = new byte[pblen];
      if (in.markSupported()) in.mark(pblen);
      int read = in.read(pbuf);
      if (read != pblen) throw new IOException("read=" + read + ", wanted=" + pblen);
      if (ProtobufUtil.isPBMagicPrefix(pbuf)) {
        parsePB(HFileProtos.FileInfoProto.parseDelimitedFrom(in));
      } else {
        if (in.markSupported()) {
          in.reset();
          parseWritable(in);
        } else {
          // We cannot use BufferedInputStream, it consumes more than we read from the underlying IS
          ByteArrayInputStream bais = new ByteArrayInputStream(pbuf);
          SequenceInputStream sis = new SequenceInputStream(bais, in); // Concatenate input streams
          // TODO: Am I leaking anything here wrapping the passed in stream?  We are not calling close on the wrapped
          // streams but they should be let go after we leave this context?  I see that we keep a reference to the
          // passed in inputstream but since we no longer have a reference to this after we leave, we should be ok.
          parseWritable(new DataInputStream(sis));
        }
      }
    }

    /** Now parse the old Writable format.  It was a list of Map entries.  Each map entry was a key and a value of
     * a byte [].  The old map format had a byte before each entry that held a code which was short for the key or
     * value type.  We know it was a byte [] so in below we just read and dump it.
     * @throws IOException
     */
    void parseWritable(final DataInputStream in) throws IOException {
      // First clear the map.  Otherwise we will just accumulate entries every time this method is called.
      this.map.clear();
      // Read the number of entries in the map
      int entries = in.readInt();
      // Then read each key/value pair
      for (int i = 0; i < entries; i++) {
        byte [] key = Bytes.readByteArray(in);
        // We used to read a byte that encoded the class type.  Read and ignore it because it is always byte [] in hfile
        in.readByte();
        byte [] value = Bytes.readByteArray(in);
        this.map.put(key, value);
      }
    }

    /**
     * Fill our map with content of the pb we read off disk
     * @param fip protobuf message to read
     */
    void parsePB(final HFileProtos.FileInfoProto fip) {
      this.map.clear();
      for (BytesBytesPair pair: fip.getMapEntryList()) {
        this.map.put(pair.getFirst().toByteArray(), pair.getSecond().toByteArray());
      }
    }
  }

  /** Return true if the given file info key is reserved for internal use. */
  public static boolean isReservedFileInfoKey(byte[] key) {
    return Bytes.startsWith(key, FileInfo.RESERVED_PREFIX_BYTES);
  }

  /**
   * Get names of supported compression algorithms. The names are acceptable by
   * HFile.Writer.
   *
   * @return Array of strings, each represents a supported compression
   *         algorithm. Currently, the following compression algorithms are
   *         supported.
   *         <ul>
   *         <li>"none" - No compression.
   *         <li>"gz" - GZIP compression.
   *         </ul>
   */
  public static String[] getSupportedCompressionAlgorithms() {
    return Compression.getSupportedAlgorithms();
  }

  // Utility methods.
  /*
   * @param l Long to convert to an int.
   * @return <code>l</code> cast as an int.
   */
  static int longToInt(final long l) {
    // Expecting the size() of a block not exceeding 4GB. Assuming the
    // size() will wrap to negative integer if it exceeds 2GB (From tfile).
    return (int)(l & 0x00000000ffffffffL);
  }

  /**
   * Returns all files belonging to the given region directory. Could return an
   * empty list.
   *
   * @param fs  The file system reference.
   * @param regionDir  The region directory to scan.
   * @return The list of files found.
   * @throws IOException When scanning the files fails.
   */
  static List<Path> getStoreFiles(FileSystem fs, Path regionDir)
      throws IOException {
    List<Path> res = new ArrayList<Path>();
    PathFilter dirFilter = new FSUtils.DirFilter(fs);
    FileStatus[] familyDirs = fs.listStatus(regionDir, dirFilter);
    for(FileStatus dir : familyDirs) {
      FileStatus[] files = fs.listStatus(dir.getPath());
      for (FileStatus file : files) {
        if (!file.isDirectory()) {
          res.add(file.getPath());
        }
      }
    }
    return res;
  }

  /**
   * Checks the given {@link HFile} format version, and throws an exception if
   * invalid. Note that if the version number comes from an input file and has
   * not been verified, the caller needs to re-throw an {@link IOException} to
   * indicate that this is not a software error, but corrupted input.
   *
   * @param version an HFile version
   * @throws IllegalArgumentException if the version is invalid
   */
  public static void checkFormatVersion(int version)
      throws IllegalArgumentException {
    if (version < MIN_FORMAT_VERSION || version > MAX_FORMAT_VERSION) {
      throw new IllegalArgumentException("Invalid HFile version: " + version
          + " (expected to be " + "between " + MIN_FORMAT_VERSION + " and "
          + MAX_FORMAT_VERSION + ")");
    }
  }


  public static void checkHFileVersion(final Configuration c) {
    int version = c.getInt(FORMAT_VERSION_KEY, MAX_FORMAT_VERSION);
    if (version < MAX_FORMAT_VERSION || version > MAX_FORMAT_VERSION) {
      throw new IllegalArgumentException("The setting for " + FORMAT_VERSION_KEY +
        " (in your hbase-*.xml files) is " + version + " which does not match " +
        MAX_FORMAT_VERSION +
        "; are you running with a configuration from an older or newer hbase install (an " +
        "incompatible hbase-default.xml or hbase-site.xml on your CLASSPATH)?");
    }
  }

  public static void main(String[] args) throws Exception {
    // delegate to preserve old behavior
    HFilePrettyPrinter.main(args);
  }
}
