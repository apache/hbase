/**
 * Copyright 2009 The Apache Software Foundation
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

import java.io.Closeable;
import java.io.DataInput;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.WriteOptions;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue.KeyComparator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueContext;
import org.apache.hadoop.hbase.io.HbaseMapWritable;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.metrics.PercentileMetric;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics.SchemaAware;
import org.apache.hadoop.hbase.util.BloomFilterWriter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Histogram;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;

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
public class HFile {
  static final Log LOG = LogFactory.getLog(HFile.class);

  /**
   * Maximum length of key in HFile.
   */
  public final static int MAXIMUM_KEY_LENGTH = Integer.MAX_VALUE;

  /**
   * Default block size for an HFile.
   */
  public final static int DEFAULT_BLOCKSIZE = 64 * 1024;

  /**
   * Default byes per checksum for hfile
   */
  public final static int DEFAULT_BYTES_PER_CHECKSUM = 512;

  /**
   * Default compression: none.
   */
  public final static Compression.Algorithm DEFAULT_COMPRESSION_ALGORITHM =
    Compression.Algorithm.NONE;

  /** Minimum supported HFile format version */
  public static final int MIN_FORMAT_VERSION = 1;

  /** Maximum supported HFile format version */
  public static final int MAX_FORMAT_VERSION = 2;

  /** Default compression name: none. */
  public final static String DEFAULT_COMPRESSION =
    DEFAULT_COMPRESSION_ALGORITHM.getName();

  /**
   * We assume that HFile path ends with
   * ROOT_DIR/TABLE_NAME/REGION_NAME/CF_NAME/HFILE, so it has at least this
   * many levels of nesting. This is needed for identifying table and CF name
   * from an HFile path.
   */
  public final static int MIN_NUM_HFILE_PATH_LEVELS = 5;

  static final AtomicInteger writeOps = new AtomicInteger();
  static final AtomicLong writeTimeNano = new AtomicLong();
  static final AtomicInteger blocksCachedDuringCompaction = new AtomicInteger();

  // For measuring latency of pread
  static final AtomicInteger preadOps = new AtomicInteger();
  static final AtomicLong preadTimeNano = new AtomicLong();

  //For measuring latency of pread during compactions
  static final AtomicInteger preadCompactionOps = new AtomicInteger();
  static final AtomicLong preadCompactionTimeNano = new AtomicLong();
  // These outliers measure in nano seconds.
  public static final Histogram preadHistogram = new Histogram(
    PercentileMetric.HISTOGRAM_NUM_BUCKETS_DEFAULT,
    PercentileMetric.HISTOGRAM_MINVALUE_DEFAULT,
    PercentileMetric.HISTOGRAM_MAXVALUE_DEFAULT);
  public static final Histogram preadCompactionHistogram = new Histogram(
    PercentileMetric.HISTOGRAM_NUM_BUCKETS_DEFAULT,
    PercentileMetric.HISTOGRAM_MINVALUE_DEFAULT,
    PercentileMetric.HISTOGRAM_MAXVALUE_DEFAULT);
  /**
   * Get the number of positional read operations during compaction
   * and reset it to zero.
   */
  public static final int getPreadCompactionOpsAndReset() {
    return preadCompactionOps.getAndSet(0);
  }

  /**
   * Get number of blocks cached during compaction and reset the counter
   */
  public static final int getBlocksCachedDuringCompactionAndReset() {
    return blocksCachedDuringCompaction.getAndSet(0);
  }

  /**
   * Get the total time of positional reads during compaction in milliseconds and reset it to zero.
   */
  public static final long getPreadCompactionTimeMsAndReset() {
    return preadCompactionTimeNano.getAndSet(0) / 1000000;
  }

  /**
   * Get the number of positional read operations and reset it to zero.
   */
  public static final int getPreadOpsAndReset() {
    return preadOps.getAndSet(0);
  }

  /**
   * Get the total time of positional reads in milliseconds and reset it to zero.
   */
  public static final long getPreadTimeMsAndReset() {
    return preadTimeNano.getAndSet(0) / 1000000;
  }

  /**
   * Get the configured bytes per checksum for HFile
   * if not configured, return the default value
   * @param hconf hbase configuration
   * @param fsconf dfs configuration
   * @return bytes per checksum for HFile
   */
  public static int getBytesPerChecksum(Configuration hconf,
      Configuration fsconf) {
    int bytesPerChecksum = HFile.DEFAULT_BYTES_PER_CHECKSUM;
    if (hconf != null) {
      bytesPerChecksum =  hconf.getInt("hfile.io.bytes.per.checksum",
          fsconf.getInt("io.bytes.per.checksum",
              HFile.DEFAULT_BYTES_PER_CHECKSUM));
    }
    return bytesPerChecksum;
  }

  /** API required to write an {@link HFile} */
  public interface Writer extends Closeable {

    /** Add an element to the file info map. */
    void appendFileInfo(byte[] key, byte[] value) throws IOException;

    void append(KeyValue kv) throws IOException;

    void append(KeyValue kv, KeyValueContext vc) throws IOException;

    void append(byte[] key, byte[] value) throws IOException;

    /** @return the path to this {@link HFile} */
    Path getPath();

    String getColumnFamilyName();

    void appendMetaBlock(String bloomFilterMetaKey, Writable metaWriter);

    /**
     * Adds an inline block writer such as a multi-level block index writer or
     * a compound Bloom filter writer.
     */
    void addInlineBlockWriter(InlineBlockWriter bloomWriter);

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
     * Store delete column Bloom filter in the file, which is only supported in
     * HFile V2.
     * @throws IOException
     */
    void addDeleteColumnBloomFilter(BloomFilterWriter bfw) throws IOException;

    /**
     * Set whether compaction is in progress or not
     */
    void setCompactionWriter(boolean isCompaction);

    /**
     * Returns the number of blocks cached during compaction
     */
    int getNumBlocksCachedPerCompaction();
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
    protected int blockSize = HColumnDescriptor.DEFAULT_BLOCKSIZE;
    protected Compression.Algorithm compression =
        HFile.DEFAULT_COMPRESSION_ALGORITHM;
    protected HFileDataBlockEncoder encoder = NoOpDataBlockEncoder.INSTANCE;
    protected KeyComparator comparator;
    protected InetSocketAddress[] favoredNodes;
    protected WriteOptions options = new WriteOptions();

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

    public WriterFactory withBlockSize(int blockSize) {
      this.blockSize = blockSize;
      return this;
    }

    public WriterFactory withCompression(Compression.Algorithm compression) {
      Preconditions.checkNotNull(compression);
      this.compression = compression;
      return this;
    }

    public WriterFactory withCompression(String compressAlgo) {
      Preconditions.checkNotNull(compression);
      this.compression = AbstractHFileWriter.compressionByName(compressAlgo);
      return this;
    }

    public WriterFactory withDataBlockEncoder(HFileDataBlockEncoder encoder) {
      Preconditions.checkNotNull(encoder);
      this.encoder = encoder;
      return this;
    }

    public WriterFactory withComparator(KeyComparator comparator) {
      Preconditions.checkNotNull(comparator);
      this.comparator = comparator;
      return this;
    }

    public WriterFactory withWriteOptions(WriteOptions options) {
      Preconditions.checkNotNull(options);
      this.options = options;
      return this;
    }

    public WriterFactory withFavoredNodes(InetSocketAddress[] favoredNodes) {
      // Deliberately not checking for null here.
      this.favoredNodes = favoredNodes;
      return this;
    }

    public Writer create() throws IOException {
      if ((path != null ? 1 : 0) + (ostream != null ? 1 : 0) != 1) {
        throw new AssertionError("Please specify exactly one of " +
            "filesystem/path or path");
      }
      if (path != null) {
        ostream = AbstractHFileWriter.createOutputStream(conf, fs, path,
            HFile.getBytesPerChecksum(conf, fs.getConf()), favoredNodes,
            options);
      }
      return createWriter();
    }

    protected abstract Writer createWriter() throws IOException;
  }

  /** The configuration key for HFile version to use for new files */
  public static final String FORMAT_VERSION_KEY = "hfile.format.version";

  public static final String HFILEHISTOGRAM_METABLOCK = "hfile.histogram.metaentry";

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
   * Returns the factory to be used to create {@link HFile} writers. Should always be an instance of
   * {@link HFileWriterV2.WriterFactoryV2} in production, but can also be
   * {@link HFileWriterV1.WriterFactoryV1} in testing.
   */
  public static final WriterFactory getWriterFactory(Configuration conf,
      CacheConfig cacheConf) {
    int version = getFormatVersion(conf);
    LOG.debug("Using HFile format version " + version);
    switch (version) {
    case 1:
      return new HFileWriterV1.WriterFactoryV1(conf, cacheConf);
    case 2:
      return new HFileWriterV2.WriterFactoryV2(conf, cacheConf);
    default:
      throw new IllegalArgumentException("Cannot create writer for HFile " +
          "format version " + version);
    }
  }

  /** An abstraction used by the block index */
  public interface CachingBlockReader {
    /**
     * Read in a file block.
     * @param dataBlockOffset offset to read.
     * @param onDiskBlockSize size of the block
     * @param cacheBlock
     * @param isCompaction is this block being read as part of a compaction
     * @param cacheOnPreload should we put this block into cache because we are preloading it
     * @param expectedBlockType the block type we are expecting to read with this read operation, or
     *          null to read whatever block type is available and avoid checking (that might reduce
     *          caching efficiency of encoded data blocks)
     * @param obtainedFromCache
     * @return Block wrapped in a ByteBuffer.
     * @throws IOException
     */
    HFileBlock readBlock(long offset, long onDiskBlockSize,
        boolean cacheBlock, final boolean isCompaction, boolean cacheOnPreload,
        BlockType expectedBlockType, KeyValueContext kvContext)
        throws IOException;
  }

  /** An interface used by clients to open and iterate an {@link HFile}. */
  public interface Reader extends Closeable, CachingBlockReader, SchemaAware {
    /**
     * Returns this reader's "name". Usually the last component of the path.
     * Needs to be constant as the file is being moved to support caching on
     * write.
     */
    String getName();

    String getColumnFamilyName();

    RawComparator<byte []> getComparator();

    HFileScanner getScanner(boolean cacheBlocks, final boolean isCompaction,
        boolean preloadBlocks);

    ByteBuffer getMetaBlock(String metaBlockName,
       boolean cacheBlock) throws IOException;

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

    HFileScanner getScanner(boolean cacheBlocks);

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

    /**
     * Retrieves delete column Bloom filter metadata as appropriate for each
     * {@link HFile} version.
     * Knows nothing about how metadata is structured.
     * @throws IOException
     */
    DataInput getDeleteColumnBloomFilterMetadata() throws IOException;

    Path getPath();

    /** Close method with optional evictOnClose */
    void close(boolean evictOnClose) throws IOException;

    /** Close method with optional evictOnClose for L1 and L2 caches */
    void close(boolean evictL1OnClose, boolean evictL2OnClose) throws IOException;

    DataBlockEncoding getEncodingOnDisk();

    String toShortString();
  }

  private static Reader pickReaderVersion(Path path, FSDataInputStream fsdis,
      long size, boolean closeIStream, CacheConfig cacheConf,
      DataBlockEncoding preferredEncodingInCache, Configuration conf)
      throws IOException {
    FixedFileTrailer trailer = FixedFileTrailer.readFromStream(fsdis, size);
    switch (trailer.getVersion()) {
    case 1:
      return new HFileReaderV1(path, trailer, fsdis, size, closeIStream,
          cacheConf, conf);
    case 2:
      return new HFileReaderV2(path, trailer, fsdis, size, closeIStream,
          cacheConf, preferredEncodingInCache, conf);
    default:
      throw new IOException("Cannot instantiate reader for HFile version " +
          trailer.getVersion());
    }
  }

  public static Reader createReaderWithEncoding(FileSystem fs, Path path,
      CacheConfig cacheConf, DataBlockEncoding preferredEncodingInCache)
      throws IOException {
    final boolean closeIStream = true;
    return pickReaderVersion(path, fs.open(path),
        fs.getFileStatus(path).getLen(), closeIStream, cacheConf,
        preferredEncodingInCache, fs.getConf());
  }

  public static Reader createReader(
      FileSystem fs, Path path, CacheConfig cacheConf) throws IOException {
    return createReaderWithEncoding(fs, path, cacheConf,
        DataBlockEncoding.NONE);
  }

  /**
   * Currently used only for testing.
   */
  public static Reader createReaderFromStream(Path path,
      FSDataInputStream fsdis, long size, CacheConfig cacheConf)
      throws IOException {
    final boolean closeIStream = false;
    return pickReaderVersion(path, fsdis, size, closeIStream, cacheConf,
        DataBlockEncoding.NONE, HBaseConfiguration.create());
  }

  /*
   * Metadata for this file.  Conjured by the writer.  Read in by the reader.
   */
  static class FileInfo extends HbaseMapWritable<byte [], byte []> {
    static final String RESERVED_PREFIX = "hfile.";
    static final byte[] RESERVED_PREFIX_BYTES = Bytes.toBytes(RESERVED_PREFIX);
    static final byte [] LASTKEY = Bytes.toBytes(RESERVED_PREFIX + "LASTKEY");
    static final byte [] AVG_KEY_LEN =
      Bytes.toBytes(RESERVED_PREFIX + "AVG_KEY_LEN");
    static final byte [] AVG_VALUE_LEN =
      Bytes.toBytes(RESERVED_PREFIX + "AVG_VALUE_LEN");
    static final byte [] COMPARATOR =
      Bytes.toBytes(RESERVED_PREFIX + "COMPARATOR");

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
      if (checkPrefix && Bytes.startsWith(k, FileInfo.RESERVED_PREFIX_BYTES)) {
        throw new IOException("Keys with a " + FileInfo.RESERVED_PREFIX
            + " are reserved");
      }
      put(k, v);
      return this;
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
        if (!file.isDir()) {
          res.add(file.getPath());
        }
      }
    }
    return res;
  }

  public static void main(String[] args) throws IOException {
    HFilePrettyPrinter prettyPrinter = new HFilePrettyPrinter();
    System.exit(prettyPrinter.run(args));
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

}
