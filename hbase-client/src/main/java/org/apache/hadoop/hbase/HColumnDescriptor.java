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
package org.apache.hadoop.hbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.BytesBytesPair;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.ColumnFamilySchema;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.NameStringPair;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.PrettyPrinter;
import org.apache.hadoop.hbase.util.PrettyPrinter.Unit;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.util.ByteStringer;

/**
 * An HColumnDescriptor contains information about a column family such as the
 * number of versions, compression settings, etc.
 *
 * It is used as input when creating a table or adding a column.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class HColumnDescriptor implements WritableComparable<HColumnDescriptor> {
  // For future backward compatibility

  // Version  3 was when column names become byte arrays and when we picked up
  // Time-to-live feature.  Version 4 was when we moved to byte arrays, HBASE-82.
  // Version  5 was when bloom filter descriptors were removed.
  // Version  6 adds metadata as a map where keys and values are byte[].
  // Version  7 -- add new compression and hfile blocksize to HColumnDescriptor (HBASE-1217)
  // Version  8 -- reintroduction of bloom filters, changed from boolean to enum
  // Version  9 -- add data block encoding
  // Version 10 -- change metadata to standard type.
  // Version 11 -- add column family level configuration.
  private static final byte COLUMN_DESCRIPTOR_VERSION = (byte) 11;

  // These constants are used as FileInfo keys
  public static final String COMPRESSION = "COMPRESSION";
  public static final String COMPRESSION_COMPACT = "COMPRESSION_COMPACT";
  public static final String ENCODE_ON_DISK = // To be removed, it is not used anymore
      "ENCODE_ON_DISK";
  public static final String DATA_BLOCK_ENCODING =
      "DATA_BLOCK_ENCODING";
  public static final String BLOCKCACHE = "BLOCKCACHE";
  public static final String CACHE_DATA_ON_WRITE = "CACHE_DATA_ON_WRITE";
  public static final String CACHE_INDEX_ON_WRITE = "CACHE_INDEX_ON_WRITE";
  public static final String CACHE_BLOOMS_ON_WRITE = "CACHE_BLOOMS_ON_WRITE";
  public static final String EVICT_BLOCKS_ON_CLOSE = "EVICT_BLOCKS_ON_CLOSE";
  /**
   * Key for the PREFETCH_BLOCKS_ON_OPEN attribute.
   * If set, all INDEX, BLOOM, and DATA blocks of HFiles belonging to this
   * family will be loaded into the cache as soon as the file is opened. These
   * loads will not count as cache misses.
   */
  public static final String PREFETCH_BLOCKS_ON_OPEN = "PREFETCH_BLOCKS_ON_OPEN";

  /**
   * Size of storefile/hfile 'blocks'.  Default is {@link #DEFAULT_BLOCKSIZE}.
   * Use smaller block sizes for faster random-access at expense of larger
   * indices (more memory consumption).
   */
  public static final String BLOCKSIZE = "BLOCKSIZE";

  public static final String LENGTH = "LENGTH";
  public static final String TTL = "TTL";
  public static final String BLOOMFILTER = "BLOOMFILTER";
  public static final String FOREVER = "FOREVER";
  public static final String REPLICATION_SCOPE = "REPLICATION_SCOPE";
  public static final byte[] REPLICATION_SCOPE_BYTES = Bytes.toBytes(REPLICATION_SCOPE);
  public static final String MIN_VERSIONS = "MIN_VERSIONS";
  public static final String KEEP_DELETED_CELLS = "KEEP_DELETED_CELLS";
  public static final String COMPRESS_TAGS = "COMPRESS_TAGS";

  @InterfaceStability.Unstable
  public static final String ENCRYPTION = "ENCRYPTION";
  @InterfaceStability.Unstable
  public static final String ENCRYPTION_KEY = "ENCRYPTION_KEY";

  public static final String DFS_REPLICATION = "DFS_REPLICATION";
  public static final short DEFAULT_DFS_REPLICATION = 0;

  /**
   * Default compression type.
   */
  public static final String DEFAULT_COMPRESSION =
    Compression.Algorithm.NONE.getName();

  /**
   * Default value of the flag that enables data block encoding on disk, as
   * opposed to encoding in cache only. We encode blocks everywhere by default,
   * as long as {@link #DATA_BLOCK_ENCODING} is not NONE.
   */
  public static final boolean DEFAULT_ENCODE_ON_DISK = true;

  /** Default data block encoding algorithm. */
  public static final String DEFAULT_DATA_BLOCK_ENCODING =
      DataBlockEncoding.NONE.toString();

  /**
   * Default number of versions of a record to keep.
   */
  public static final int DEFAULT_VERSIONS = HBaseConfiguration.create().getInt(
    "hbase.column.max.version", 1);

  /**
   * Default is not to keep a minimum of versions.
   */
  public static final int DEFAULT_MIN_VERSIONS = 0;

  /*
   * Cache here the HCD value.
   * Question: its OK to cache since when we're reenable, we create a new HCD?
   */
  private volatile Integer blocksize = null;

  /**
   * Default setting for whether to serve from memory or not.
   */
  public static final boolean DEFAULT_IN_MEMORY = false;

  /**
   * Default setting for preventing deleted from being collected immediately.
   */
  public static final KeepDeletedCells DEFAULT_KEEP_DELETED = KeepDeletedCells.FALSE;

  /**
   * Default setting for whether to use a block cache or not.
   */
  public static final boolean DEFAULT_BLOCKCACHE = true;

  /**
   * Default setting for whether to cache data blocks on write if block caching
   * is enabled.
   */
  public static final boolean DEFAULT_CACHE_DATA_ON_WRITE = false;

  /**
   * Default setting for whether to cache index blocks on write if block
   * caching is enabled.
   */
  public static final boolean DEFAULT_CACHE_INDEX_ON_WRITE = false;

  /**
   * Default size of blocks in files stored to the filesytem (hfiles).
   */
  public static final int DEFAULT_BLOCKSIZE = HConstants.DEFAULT_BLOCKSIZE;

  /**
   * Default setting for whether or not to use bloomfilters.
   */
  public static final String DEFAULT_BLOOMFILTER = BloomType.ROW.toString();

  /**
   * Default setting for whether to cache bloom filter blocks on write if block
   * caching is enabled.
   */
  public static final boolean DEFAULT_CACHE_BLOOMS_ON_WRITE = false;

  /**
   * Default time to live of cell contents.
   */
  public static final int DEFAULT_TTL = HConstants.FOREVER;

  /**
   * Default scope.
   */
  public static final int DEFAULT_REPLICATION_SCOPE = HConstants.REPLICATION_SCOPE_LOCAL;

  /**
   * Default setting for whether to evict cached blocks from the blockcache on
   * close.
   */
  public static final boolean DEFAULT_EVICT_BLOCKS_ON_CLOSE = false;

  /**
   * Default compress tags along with any type of DataBlockEncoding.
   */
  public static final boolean DEFAULT_COMPRESS_TAGS = true;

  /*
   * Default setting for whether to prefetch blocks into the blockcache on open.
   */
  public static final boolean DEFAULT_PREFETCH_BLOCKS_ON_OPEN = false;

  private final static Map<String, String> DEFAULT_VALUES
    = new HashMap<String, String>();
  private final static Set<ImmutableBytesWritable> RESERVED_KEYWORDS
    = new HashSet<ImmutableBytesWritable>();
  static {
      DEFAULT_VALUES.put(BLOOMFILTER, DEFAULT_BLOOMFILTER);
      DEFAULT_VALUES.put(REPLICATION_SCOPE, String.valueOf(DEFAULT_REPLICATION_SCOPE));
      DEFAULT_VALUES.put(HConstants.VERSIONS, String.valueOf(DEFAULT_VERSIONS));
      DEFAULT_VALUES.put(MIN_VERSIONS, String.valueOf(DEFAULT_MIN_VERSIONS));
      DEFAULT_VALUES.put(COMPRESSION, DEFAULT_COMPRESSION);
      DEFAULT_VALUES.put(TTL, String.valueOf(DEFAULT_TTL));
      DEFAULT_VALUES.put(BLOCKSIZE, String.valueOf(DEFAULT_BLOCKSIZE));
      DEFAULT_VALUES.put(HConstants.IN_MEMORY, String.valueOf(DEFAULT_IN_MEMORY));
      DEFAULT_VALUES.put(BLOCKCACHE, String.valueOf(DEFAULT_BLOCKCACHE));
      DEFAULT_VALUES.put(KEEP_DELETED_CELLS, String.valueOf(DEFAULT_KEEP_DELETED));
      DEFAULT_VALUES.put(DATA_BLOCK_ENCODING, String.valueOf(DEFAULT_DATA_BLOCK_ENCODING));
      DEFAULT_VALUES.put(CACHE_DATA_ON_WRITE, String.valueOf(DEFAULT_CACHE_DATA_ON_WRITE));
      DEFAULT_VALUES.put(CACHE_INDEX_ON_WRITE, String.valueOf(DEFAULT_CACHE_INDEX_ON_WRITE));
      DEFAULT_VALUES.put(CACHE_BLOOMS_ON_WRITE, String.valueOf(DEFAULT_CACHE_BLOOMS_ON_WRITE));
      DEFAULT_VALUES.put(EVICT_BLOCKS_ON_CLOSE, String.valueOf(DEFAULT_EVICT_BLOCKS_ON_CLOSE));
      DEFAULT_VALUES.put(PREFETCH_BLOCKS_ON_OPEN, String.valueOf(DEFAULT_PREFETCH_BLOCKS_ON_OPEN));
      for (String s : DEFAULT_VALUES.keySet()) {
        RESERVED_KEYWORDS.add(new ImmutableBytesWritable(Bytes.toBytes(s)));
      }
      RESERVED_KEYWORDS.add(new ImmutableBytesWritable(Bytes.toBytes(ENCRYPTION)));
      RESERVED_KEYWORDS.add(new ImmutableBytesWritable(Bytes.toBytes(ENCRYPTION_KEY)));
  }

  private static final int UNINITIALIZED = -1;

  // Column family name
  private byte [] name;

  // Column metadata
  private final Map<ImmutableBytesWritable, ImmutableBytesWritable> values =
    new HashMap<ImmutableBytesWritable,ImmutableBytesWritable>();

  /**
   * A map which holds the configuration specific to the column family.
   * The keys of the map have the same names as config keys and override the defaults with
   * cf-specific settings. Example usage may be for compactions, etc.
   */
  private final Map<String, String> configuration = new HashMap<String, String>();

  /*
   * Cache the max versions rather than calculate it every time.
   */
  private int cachedMaxVersions = UNINITIALIZED;

  /**
   * Default constructor. Must be present for Writable.
   * @deprecated Used by Writables and Writables are going away.
   */
  @Deprecated
  // Make this private rather than remove after deprecation period elapses.  Its needed by pb
  // deserializations.
  public HColumnDescriptor() {
    this.name = null;
  }

  /**
   * Construct a column descriptor specifying only the family name
   * The other attributes are defaulted.
   *
   * @param familyName Column family name. Must be 'printable' -- digit or
   * letter -- and may not contain a <code>:<code>
   */
  public HColumnDescriptor(final String familyName) {
    this(Bytes.toBytes(familyName));
  }

  /**
   * Construct a column descriptor specifying only the family name
   * The other attributes are defaulted.
   *
   * @param familyName Column family name. Must be 'printable' -- digit or
   * letter -- and may not contain a <code>:<code>
   */
  public HColumnDescriptor(final byte [] familyName) {
    this (familyName == null || familyName.length <= 0?
      HConstants.EMPTY_BYTE_ARRAY: familyName, DEFAULT_VERSIONS,
      DEFAULT_COMPRESSION, DEFAULT_IN_MEMORY, DEFAULT_BLOCKCACHE,
      DEFAULT_TTL, DEFAULT_BLOOMFILTER);
  }

  /**
   * Constructor.
   * Makes a deep copy of the supplied descriptor.
   * Can make a modifiable descriptor from an UnmodifyableHColumnDescriptor.
   * @param desc The descriptor.
   */
  public HColumnDescriptor(HColumnDescriptor desc) {
    super();
    this.name = desc.name.clone();
    for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> e:
        desc.values.entrySet()) {
      this.values.put(e.getKey(), e.getValue());
    }
    for (Map.Entry<String, String> e : desc.configuration.entrySet()) {
      this.configuration.put(e.getKey(), e.getValue());
    }
    setMaxVersions(desc.getMaxVersions());
  }

  /**
   * Constructor
   * @param familyName Column family name. Must be 'printable' -- digit or
   * letter -- and may not contain a <code>:<code>
   * @param maxVersions Maximum number of versions to keep
   * @param compression Compression type
   * @param inMemory If true, column data should be kept in an HRegionServer's
   * cache
   * @param blockCacheEnabled If true, MapFile blocks should be cached
   * @param timeToLive Time-to-live of cell contents, in seconds
   * (use HConstants.FOREVER for unlimited TTL)
   * @param bloomFilter Bloom filter type for this column
   *
   * @throws IllegalArgumentException if passed a family name that is made of
   * other than 'word' characters: i.e. <code>[a-zA-Z_0-9]</code> or contains
   * a <code>:</code>
   * @throws IllegalArgumentException if the number of versions is &lt;= 0
   * @deprecated use {@link #HColumnDescriptor(String)} and setters
   */
  @Deprecated
  public HColumnDescriptor(final byte [] familyName, final int maxVersions,
      final String compression, final boolean inMemory,
      final boolean blockCacheEnabled,
      final int timeToLive, final String bloomFilter) {
    this(familyName, maxVersions, compression, inMemory, blockCacheEnabled,
      DEFAULT_BLOCKSIZE, timeToLive, bloomFilter, DEFAULT_REPLICATION_SCOPE);
  }

  /**
   * Constructor
   * @param familyName Column family name. Must be 'printable' -- digit or
   * letter -- and may not contain a <code>:<code>
   * @param maxVersions Maximum number of versions to keep
   * @param compression Compression type
   * @param inMemory If true, column data should be kept in an HRegionServer's
   * cache
   * @param blockCacheEnabled If true, MapFile blocks should be cached
   * @param blocksize Block size to use when writing out storefiles.  Use
   * smaller block sizes for faster random-access at expense of larger indices
   * (more memory consumption).  Default is usually 64k.
   * @param timeToLive Time-to-live of cell contents, in seconds
   * (use HConstants.FOREVER for unlimited TTL)
   * @param bloomFilter Bloom filter type for this column
   * @param scope The scope tag for this column
   *
   * @throws IllegalArgumentException if passed a family name that is made of
   * other than 'word' characters: i.e. <code>[a-zA-Z_0-9]</code> or contains
   * a <code>:</code>
   * @throws IllegalArgumentException if the number of versions is &lt;= 0
   * @deprecated use {@link #HColumnDescriptor(String)} and setters
   */
  @Deprecated
  public HColumnDescriptor(final byte [] familyName, final int maxVersions,
      final String compression, final boolean inMemory,
      final boolean blockCacheEnabled, final int blocksize,
      final int timeToLive, final String bloomFilter, final int scope) {
    this(familyName, DEFAULT_MIN_VERSIONS, maxVersions, DEFAULT_KEEP_DELETED,
        compression, DEFAULT_ENCODE_ON_DISK, DEFAULT_DATA_BLOCK_ENCODING,
        inMemory, blockCacheEnabled, blocksize, timeToLive, bloomFilter,
        scope);
  }

  /**
   * Constructor
   * @param familyName Column family name. Must be 'printable' -- digit or
   * letter -- and may not contain a <code>:<code>
   * @param minVersions Minimum number of versions to keep
   * @param maxVersions Maximum number of versions to keep
   * @param keepDeletedCells Whether to retain deleted cells until they expire
   *        up to maxVersions versions.
   * @param compression Compression type
   * @param encodeOnDisk whether to use the specified data block encoding
   *        on disk. If false, the encoding will be used in cache only.
   * @param dataBlockEncoding data block encoding
   * @param inMemory If true, column data should be kept in an HRegionServer's
   * cache
   * @param blockCacheEnabled If true, MapFile blocks should be cached
   * @param blocksize Block size to use when writing out storefiles.  Use
   * smaller blocksizes for faster random-access at expense of larger indices
   * (more memory consumption).  Default is usually 64k.
   * @param timeToLive Time-to-live of cell contents, in seconds
   * (use HConstants.FOREVER for unlimited TTL)
   * @param bloomFilter Bloom filter type for this column
   * @param scope The scope tag for this column
   *
   * @throws IllegalArgumentException if passed a family name that is made of
   * other than 'word' characters: i.e. <code>[a-zA-Z_0-9]</code> or contains
   * a <code>:</code>
   * @throws IllegalArgumentException if the number of versions is &lt;= 0
   * @deprecated use {@link #HColumnDescriptor(String)} and setters
   */
  @Deprecated
  public HColumnDescriptor(final byte[] familyName, final int minVersions,
      final int maxVersions, final KeepDeletedCells keepDeletedCells,
      final String compression, final boolean encodeOnDisk,
      final String dataBlockEncoding, final boolean inMemory,
      final boolean blockCacheEnabled, final int blocksize,
      final int timeToLive, final String bloomFilter, final int scope) {
    isLegalFamilyName(familyName);
    this.name = familyName;

    if (maxVersions <= 0) {
      // TODO: Allow maxVersion of 0 to be the way you say "Keep all versions".
      // Until there is support, consider 0 or < 0 -- a configuration error.
      throw new IllegalArgumentException("Maximum versions must be positive");
    }

    if (minVersions > 0) {
      if (timeToLive == HConstants.FOREVER) {
        throw new IllegalArgumentException("Minimum versions requires TTL.");
      }
      if (minVersions >= maxVersions) {
        throw new IllegalArgumentException("Minimum versions must be < "
            + "maximum versions.");
      }
    }

    setMaxVersions(maxVersions);
    setMinVersions(minVersions);
    setKeepDeletedCells(keepDeletedCells);
    setInMemory(inMemory);
    setBlockCacheEnabled(blockCacheEnabled);
    setTimeToLive(timeToLive);
    setCompressionType(Compression.Algorithm.
      valueOf(compression.toUpperCase(Locale.ROOT)));
    setDataBlockEncoding(DataBlockEncoding.
        valueOf(dataBlockEncoding.toUpperCase(Locale.ROOT)));
    setBloomFilterType(BloomType.
      valueOf(bloomFilter.toUpperCase(Locale.ROOT)));
    setBlocksize(blocksize);
    setScope(scope);
  }

  /**
   * @param b Family name.
   * @return <code>b</code>
   * @throws IllegalArgumentException If not null and not a legitimate family
   * name: i.e. 'printable' and ends in a ':' (Null passes are allowed because
   * <code>b</code> can be null when deserializing).  Cannot start with a '.'
   * either. Also Family can not be an empty value or equal "recovered.edits".
   */
  public static byte [] isLegalFamilyName(final byte [] b) {
    if (b == null) {
      return b;
    }
    Preconditions.checkArgument(b.length != 0, "Family name can not be empty");
    if (b[0] == '.') {
      throw new IllegalArgumentException("Family names cannot start with a " +
        "period: " + Bytes.toString(b));
    }
    for (int i = 0; i < b.length; i++) {
      if (Character.isISOControl(b[i]) || b[i] == ':' || b[i] == '\\' || b[i] == '/') {
        throw new IllegalArgumentException("Illegal character <" + b[i] +
          ">. Family names cannot contain control characters or colons: " +
          Bytes.toString(b));
      }
    }
    byte[] recoveredEdit = Bytes.toBytes(HConstants.RECOVERED_EDITS_DIR);
    if (Bytes.equals(recoveredEdit, b)) {
      throw new IllegalArgumentException("Family name cannot be: " +
          HConstants.RECOVERED_EDITS_DIR);
    }
    return b;
  }

  /**
   * @return Name of this column family
   */
  public byte [] getName() {
    return name;
  }

  /**
   * @return Name of this column family
   */
  public String getNameAsString() {
    return Bytes.toString(this.name);
  }

  /**
   * @param key The key.
   * @return The value.
   */
  public byte[] getValue(byte[] key) {
    ImmutableBytesWritable ibw = values.get(new ImmutableBytesWritable(key));
    if (ibw == null)
      return null;
    return ibw.get();
  }

  /**
   * @param key The key.
   * @return The value as a string.
   */
  public String getValue(String key) {
    byte[] value = getValue(Bytes.toBytes(key));
    if (value == null)
      return null;
    return Bytes.toString(value);
  }

  /**
   * @return All values.
   */
  public Map<ImmutableBytesWritable,ImmutableBytesWritable> getValues() {
    // shallow pointer copy
    return Collections.unmodifiableMap(values);
  }

  /**
   * @param key The key.
   * @param value The value.
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setValue(byte[] key, byte[] value) {
    values.put(new ImmutableBytesWritable(key),
      new ImmutableBytesWritable(value));
    return this;
  }

  /**
   * @param key Key whose key and value we're to remove from HCD parameters.
   */
  public void remove(final byte [] key) {
    values.remove(new ImmutableBytesWritable(key));
  }

  /**
   * @param key The key.
   * @param value The value.
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setValue(String key, String value) {
    if (value == null) {
      remove(Bytes.toBytes(key));
    } else {
      setValue(Bytes.toBytes(key), Bytes.toBytes(value));
    }
    return this;
  }

  /** @return compression type being used for the column family */
  public Compression.Algorithm getCompression() {
    String n = getValue(COMPRESSION);
    if (n == null) {
      return Compression.Algorithm.NONE;
    }
    return Compression.Algorithm.valueOf(n.toUpperCase(Locale.ROOT));
  }

  /** @return compression type being used for the column family for major
      compression */
  public Compression.Algorithm getCompactionCompression() {
    String n = getValue(COMPRESSION_COMPACT);
    if (n == null) {
      return getCompression();
    }
    return Compression.Algorithm.valueOf(n.toUpperCase(Locale.ROOT));
  }

  /** @return maximum number of versions */
  public int getMaxVersions() {
    if (this.cachedMaxVersions == UNINITIALIZED) {
      String v = getValue(HConstants.VERSIONS);
      this.cachedMaxVersions = Integer.parseInt(v);
    }
    return this.cachedMaxVersions;
  }

  /**
   * @param maxVersions maximum number of versions
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setMaxVersions(int maxVersions) {
    if (maxVersions <= 0) {
      // TODO: Allow maxVersion of 0 to be the way you say "Keep all versions".
      // Until there is support, consider 0 or < 0 -- a configuration error.
      throw new IllegalArgumentException("Maximum versions must be positive");
    }    
    if (maxVersions < this.getMinVersions()) {      
        throw new IllegalArgumentException("Set MaxVersion to " + maxVersions
            + " while minVersion is " + this.getMinVersions()
            + ". Maximum versions must be >= minimum versions ");      
    }
    setValue(HConstants.VERSIONS, Integer.toString(maxVersions));
    cachedMaxVersions = maxVersions;
    return this;
  }

  /**
   * Set minimum and maximum versions to keep
   *
   * @param minVersions minimal number of versions
   * @param maxVersions maximum number of versions
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setVersions(int minVersions, int maxVersions) {
    if (minVersions <= 0) {
      // TODO: Allow minVersion and maxVersion of 0 to be the way you say "Keep all versions".
      // Until there is support, consider 0 or < 0 -- a configuration error.
      throw new IllegalArgumentException("Minimum versions must be positive");
    }

    if (maxVersions < minVersions) {
      throw new IllegalArgumentException("Unable to set MaxVersion to " + maxVersions
        + " and set MinVersion to " + minVersions
        + ", as maximum versions must be >= minimum versions.");
    }
    setMinVersions(minVersions);
    setMaxVersions(maxVersions);
    return this;
  }

  /**
   * @return The storefile/hfile blocksize for this column family.
   */
  public synchronized int getBlocksize() {
    if (this.blocksize == null) {
      String value = getValue(BLOCKSIZE);
      this.blocksize = (value != null)?
        Integer.decode(value): Integer.valueOf(DEFAULT_BLOCKSIZE);
    }
    return this.blocksize.intValue();
  }

  /**
   * @param s Blocksize to use when writing out storefiles/hfiles on this
   * column family.
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setBlocksize(int s) {
    setValue(BLOCKSIZE, Integer.toString(s));
    this.blocksize = null;
    return this;
  }

  /**
   * @return Compression type setting.
   */
  public Compression.Algorithm getCompressionType() {
    return getCompression();
  }

  /**
   * Compression types supported in hbase.
   * LZO is not bundled as part of the hbase distribution.
   * See <a href="http://wiki.apache.org/hadoop/UsingLzoCompression">LZO Compression</a>
   * for how to enable it.
   * @param type Compression type setting.
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setCompressionType(Compression.Algorithm type) {
    return setValue(COMPRESSION, type.getName().toUpperCase(Locale.ROOT));
  }

  /** @return data block encoding algorithm used on disk */
  @Deprecated
  public DataBlockEncoding getDataBlockEncodingOnDisk() {
    return getDataBlockEncoding();
  }

  /**
   * This method does nothing now. Flag ENCODE_ON_DISK is not used
   * any more. Data blocks have the same encoding in cache as on disk.
   * @return this (for chained invocation)
   */
  @Deprecated
  public HColumnDescriptor setEncodeOnDisk(boolean encodeOnDisk) {
    return this;
  }

  /**
   * @return the data block encoding algorithm used in block cache and
   *         optionally on disk
   */
  public DataBlockEncoding getDataBlockEncoding() {
    String type = getValue(DATA_BLOCK_ENCODING);
    if (type == null) {
      type = DEFAULT_DATA_BLOCK_ENCODING;
    }
    return DataBlockEncoding.valueOf(type);
  }

  /**
   * Set data block encoding algorithm used in block cache.
   * @param type What kind of data block encoding will be used.
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setDataBlockEncoding(DataBlockEncoding type) {
    String name;
    if (type != null) {
      name = type.toString();
    } else {
      name = DataBlockEncoding.NONE.toString();
    }
    return setValue(DATA_BLOCK_ENCODING, name);
  }

  /**
   * Set whether the tags should be compressed along with DataBlockEncoding. When no
   * DataBlockEncoding is been used, this is having no effect.
   * 
   * @param compressTags
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setCompressTags(boolean compressTags) {
    return setValue(COMPRESS_TAGS, String.valueOf(compressTags));
  }

  /**
   * @return Whether KV tags should be compressed along with DataBlockEncoding. When no
   *         DataBlockEncoding is been used, this is having no effect.
   */
  public boolean shouldCompressTags() {
    String compressTagsStr = getValue(COMPRESS_TAGS);
    boolean compressTags = DEFAULT_COMPRESS_TAGS;
    if (compressTagsStr != null) {
      compressTags = Boolean.valueOf(compressTagsStr);
    }
    return compressTags;
  }

  /**
   * @return Compression type setting.
   */
  public Compression.Algorithm getCompactionCompressionType() {
    return getCompactionCompression();
  }

  /**
   * Compression types supported in hbase.
   * LZO is not bundled as part of the hbase distribution.
   * See <a href="http://wiki.apache.org/hadoop/UsingLzoCompression">LZO Compression</a>
   * for how to enable it.
   * @param type Compression type setting.
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setCompactionCompressionType(
      Compression.Algorithm type) {
    return setValue(COMPRESSION_COMPACT, type.getName().toUpperCase(Locale.ROOT));
  }

  /**
   * @return True if we are to keep all in use HRegionServer cache.
   */
  public boolean isInMemory() {
    String value = getValue(HConstants.IN_MEMORY);
    if (value != null)
      return Boolean.valueOf(value).booleanValue();
    return DEFAULT_IN_MEMORY;
  }

  /**
   * @param inMemory True if we are to keep all values in the HRegionServer
   * cache
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setInMemory(boolean inMemory) {
    return setValue(HConstants.IN_MEMORY, Boolean.toString(inMemory));
  }

  public KeepDeletedCells getKeepDeletedCells() {
    String value = getValue(KEEP_DELETED_CELLS);
    if (value != null) {
      // toUpperCase for backwards compatibility
      return KeepDeletedCells.valueOf(value.toUpperCase(Locale.ROOT));
    }
    return DEFAULT_KEEP_DELETED;
  }

  /**
   * @param keepDeletedCells True if deleted rows should not be collected
   * immediately.
   * @return this (for chained invocation)
   * @deprecated use {@link #setKeepDeletedCells(KeepDeletedCells)}
   */
  @Deprecated
  public HColumnDescriptor setKeepDeletedCells(boolean keepDeletedCells) {
    return setValue(KEEP_DELETED_CELLS, (keepDeletedCells ? KeepDeletedCells.TRUE
        : KeepDeletedCells.FALSE).toString());
  }

  /**
   * @param keepDeletedCells True if deleted rows should not be collected
   * immediately.
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setKeepDeletedCells(KeepDeletedCells keepDeletedCells) {
    return setValue(KEEP_DELETED_CELLS, keepDeletedCells.toString());
  }

  /**
   * @return Time-to-live of cell contents, in seconds.
   */
  public int getTimeToLive() {
    String value = getValue(TTL);
    return (value != null)? Integer.valueOf(value).intValue(): DEFAULT_TTL;
  }

  /**
   * @param timeToLive Time-to-live of cell contents, in seconds.
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setTimeToLive(int timeToLive) {
    return setValue(TTL, Integer.toString(timeToLive));
  }

  /**
   * @return The minimum number of versions to keep.
   */
  public int getMinVersions() {
    String value = getValue(MIN_VERSIONS);
    return (value != null)? Integer.valueOf(value).intValue(): 0;
  }

  /**
   * @param minVersions The minimum number of versions to keep.
   * (used when timeToLive is set)
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setMinVersions(int minVersions) {
    return setValue(MIN_VERSIONS, Integer.toString(minVersions));
  }

  /**
   * @return True if MapFile blocks should be cached.
   */
  public boolean isBlockCacheEnabled() {
    String value = getValue(BLOCKCACHE);
    if (value != null)
      return Boolean.valueOf(value).booleanValue();
    return DEFAULT_BLOCKCACHE;
  }

  /**
   * @param blockCacheEnabled True if MapFile blocks should be cached.
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setBlockCacheEnabled(boolean blockCacheEnabled) {
    return setValue(BLOCKCACHE, Boolean.toString(blockCacheEnabled));
  }

  /**
   * @return bloom filter type used for new StoreFiles in ColumnFamily
   */
  public BloomType getBloomFilterType() {
    String n = getValue(BLOOMFILTER);
    if (n == null) {
      n = DEFAULT_BLOOMFILTER;
    }
    return BloomType.valueOf(n.toUpperCase(Locale.ROOT));
  }

  /**
   * @param bt bloom filter type
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setBloomFilterType(final BloomType bt) {
    return setValue(BLOOMFILTER, bt.toString());
  }

   /**
    * @return the scope tag
    */
  public int getScope() {
    byte[] value = getValue(REPLICATION_SCOPE_BYTES);
    if (value != null) {
      return Integer.valueOf(Bytes.toString(value));
    }
    return DEFAULT_REPLICATION_SCOPE;
  }

 /**
  * @param scope the scope tag
  * @return this (for chained invocation)
  */
  public HColumnDescriptor setScope(int scope) {
    return setValue(REPLICATION_SCOPE, Integer.toString(scope));
  }

  /**
   * @return true if we should cache data blocks on write
   */
  public boolean shouldCacheDataOnWrite() {
    String value = getValue(CACHE_DATA_ON_WRITE);
    if (value != null) {
      return Boolean.valueOf(value).booleanValue();
    }
    return DEFAULT_CACHE_DATA_ON_WRITE;
  }

  /**
   * @param value true if we should cache data blocks on write
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setCacheDataOnWrite(boolean value) {
    return setValue(CACHE_DATA_ON_WRITE, Boolean.toString(value));
  }

  /**
   * @return true if we should cache index blocks on write
   */
  public boolean shouldCacheIndexesOnWrite() {
    String value = getValue(CACHE_INDEX_ON_WRITE);
    if (value != null) {
      return Boolean.valueOf(value).booleanValue();
    }
    return DEFAULT_CACHE_INDEX_ON_WRITE;
  }

  /**
   * @param value true if we should cache index blocks on write
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setCacheIndexesOnWrite(boolean value) {
    return setValue(CACHE_INDEX_ON_WRITE, Boolean.toString(value));
  }

  /**
   * @return true if we should cache bloomfilter blocks on write
   */
  public boolean shouldCacheBloomsOnWrite() {
    String value = getValue(CACHE_BLOOMS_ON_WRITE);
    if (value != null) {
      return Boolean.valueOf(value).booleanValue();
    }
    return DEFAULT_CACHE_BLOOMS_ON_WRITE;
  }

  /**
   * @param value true if we should cache bloomfilter blocks on write
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setCacheBloomsOnWrite(boolean value) {
    return setValue(CACHE_BLOOMS_ON_WRITE, Boolean.toString(value));
  }

  /**
   * @return true if we should evict cached blocks from the blockcache on
   * close
   */
  public boolean shouldEvictBlocksOnClose() {
    String value = getValue(EVICT_BLOCKS_ON_CLOSE);
    if (value != null) {
      return Boolean.valueOf(value).booleanValue();
    }
    return DEFAULT_EVICT_BLOCKS_ON_CLOSE;
  }

  /**
   * @param value true if we should evict cached blocks from the blockcache on
   * close
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setEvictBlocksOnClose(boolean value) {
    return setValue(EVICT_BLOCKS_ON_CLOSE, Boolean.toString(value));
  }

  /**
   * @return true if we should prefetch blocks into the blockcache on open
   */
  public boolean shouldPrefetchBlocksOnOpen() {
    String value = getValue(PREFETCH_BLOCKS_ON_OPEN);
   if (value != null) {
      return Boolean.valueOf(value).booleanValue();
    }
    return DEFAULT_PREFETCH_BLOCKS_ON_OPEN;
  }

  /**
   * @param value true if we should prefetch blocks into the blockcache on open
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setPrefetchBlocksOnOpen(boolean value) {
    return setValue(PREFETCH_BLOCKS_ON_OPEN, Boolean.toString(value));
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    StringBuilder s = new StringBuilder();

    s.append('{');
    s.append(HConstants.NAME);
    s.append(" => '");
    s.append(Bytes.toString(name));
    s.append("'");
    s.append(getValues(true));
    s.append('}');
    return s.toString();
  }

  /**
   * @return Column family descriptor with only the customized attributes.
   */
  public String toStringCustomizedValues() {
    StringBuilder s = new StringBuilder();
    s.append('{');
    s.append(HConstants.NAME);
    s.append(" => '");
    s.append(Bytes.toString(name));
    s.append("'");
    s.append(getValues(false));
    s.append('}');
    return s.toString();
  }

  private StringBuilder getValues(boolean printDefaults) {
    StringBuilder s = new StringBuilder();

    boolean hasConfigKeys = false;

    // print all reserved keys first
    for (ImmutableBytesWritable k : values.keySet()) {
      if (!RESERVED_KEYWORDS.contains(k)) {
        hasConfigKeys = true;
        continue;
      }
      String key = Bytes.toString(k.get());
      String value = Bytes.toStringBinary(values.get(k).get());
      if (printDefaults
          || !DEFAULT_VALUES.containsKey(key)
          || !DEFAULT_VALUES.get(key).equalsIgnoreCase(value)) {
        s.append(", ");
        s.append(key);
        s.append(" => ");
        s.append('\'').append(PrettyPrinter.format(value, getUnit(key))).append('\'');
      }
    }

    // print all non-reserved, advanced config keys as a separate subset
    if (hasConfigKeys) {
      s.append(", ");
      s.append(HConstants.METADATA).append(" => ");
      s.append('{');
      boolean printComma = false;
      for (ImmutableBytesWritable k : values.keySet()) {
        if (RESERVED_KEYWORDS.contains(k)) {
          continue;
        }
        String key = Bytes.toString(k.get());
        String value = Bytes.toStringBinary(values.get(k).get());
        if (printComma) {
          s.append(", ");
        }
        printComma = true;
        s.append('\'').append(key).append('\'');
        s.append(" => ");
        s.append('\'').append(PrettyPrinter.format(value, getUnit(key))).append('\'');
      }
      s.append('}');
    }

    if (!configuration.isEmpty()) {
      s.append(", ");
      s.append(HConstants.CONFIGURATION).append(" => ");
      s.append('{');
      boolean printCommaForConfiguration = false;
      for (Map.Entry<String, String> e : configuration.entrySet()) {
        if (printCommaForConfiguration) s.append(", ");
        printCommaForConfiguration = true;
        s.append('\'').append(e.getKey()).append('\'');
        s.append(" => ");
        s.append('\'').append(PrettyPrinter.format(e.getValue(), getUnit(e.getKey()))).append('\'');
      }
      s.append("}");
    }
    return s;
  }

  public static Unit getUnit(String key) {
    Unit unit;
      /* TTL for now, we can add more as we neeed */
    if (key.equals(HColumnDescriptor.TTL)) {
      unit = Unit.TIME_INTERVAL;
    } else {
      unit = Unit.NONE;
    }
    return unit;
  }

  public static Map<String, String> getDefaultValues() {
    return Collections.unmodifiableMap(DEFAULT_VALUES);
  }

  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof HColumnDescriptor)) {
      return false;
    }
    return compareTo((HColumnDescriptor)obj) == 0;
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    int result = Bytes.hashCode(this.name);
    result ^= Byte.valueOf(COLUMN_DESCRIPTOR_VERSION).hashCode();
    result ^= values.hashCode();
    result ^= configuration.hashCode();
    return result;
  }

  /**
   * @deprecated Writables are going away.  Use pb {@link #parseFrom(byte[])} instead.
   */
  @Deprecated
  public void readFields(DataInput in) throws IOException {
    int version = in.readByte();
    if (version < 6) {
      if (version <= 2) {
        Text t = new Text();
        t.readFields(in);
        this.name = t.getBytes();
//        if(KeyValue.getFamilyDelimiterIndex(this.name, 0, this.name.length)
//            > 0) {
//          this.name = stripColon(this.name);
//        }
      } else {
        this.name = Bytes.readByteArray(in);
      }
      this.values.clear();
      setMaxVersions(in.readInt());
      int ordinal = in.readInt();
      setCompressionType(Compression.Algorithm.values()[ordinal]);
      setInMemory(in.readBoolean());
      setBloomFilterType(in.readBoolean() ? BloomType.ROW : BloomType.NONE);
      if (getBloomFilterType() != BloomType.NONE && version < 5) {
        // If a bloomFilter is enabled and the column descriptor is less than
        // version 5, we need to skip over it to read the rest of the column
        // descriptor. There are no BloomFilterDescriptors written to disk for
        // column descriptors with a version number >= 5
        throw new UnsupportedClassVersionError(this.getClass().getName() +
            " does not support backward compatibility with versions older " +
            "than version 5");
      }
      if (version > 1) {
        setBlockCacheEnabled(in.readBoolean());
      }
      if (version > 2) {
       setTimeToLive(in.readInt());
      }
    } else {
      // version 6+
      this.name = Bytes.readByteArray(in);
      this.values.clear();
      int numValues = in.readInt();
      for (int i = 0; i < numValues; i++) {
        ImmutableBytesWritable key = new ImmutableBytesWritable();
        ImmutableBytesWritable value = new ImmutableBytesWritable();
        key.readFields(in);
        value.readFields(in);

        // in version 8, the BloomFilter setting changed from bool to enum
        if (version < 8 && Bytes.toString(key.get()).equals(BLOOMFILTER)) {
          value.set(Bytes.toBytes(
              Boolean.getBoolean(Bytes.toString(value.get()))
                ? BloomType.ROW.toString()
                : BloomType.NONE.toString()));
        }

        values.put(key, value);
      }
      if (version == 6) {
        // Convert old values.
        setValue(COMPRESSION, Compression.Algorithm.NONE.getName());
      }
      String value = getValue(HConstants.VERSIONS);
      this.cachedMaxVersions = (value != null)?
          Integer.valueOf(value).intValue(): DEFAULT_VERSIONS;
      if (version > 10) {
        configuration.clear();
        int numConfigs = in.readInt();
        for (int i = 0; i < numConfigs; i++) {
          ImmutableBytesWritable key = new ImmutableBytesWritable();
          ImmutableBytesWritable val = new ImmutableBytesWritable();
          key.readFields(in);
          val.readFields(in);
          configuration.put(
            Bytes.toString(key.get(), key.getOffset(), key.getLength()),
            Bytes.toString(val.get(), val.getOffset(), val.getLength()));
        }
      }
    }
  }

  /**
   * @deprecated Writables are going away.  Use {@link #toByteArray()} instead.
   */
  @Deprecated
  public void write(DataOutput out) throws IOException {
    out.writeByte(COLUMN_DESCRIPTOR_VERSION);
    Bytes.writeByteArray(out, this.name);
    out.writeInt(values.size());
    for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> e:
        values.entrySet()) {
      e.getKey().write(out);
      e.getValue().write(out);
    }
    out.writeInt(configuration.size());
    for (Map.Entry<String, String> e : configuration.entrySet()) {
      new ImmutableBytesWritable(Bytes.toBytes(e.getKey())).write(out);
      new ImmutableBytesWritable(Bytes.toBytes(e.getValue())).write(out);
    }
  }

  // Comparable

  public int compareTo(HColumnDescriptor o) {
    int result = Bytes.compareTo(this.name, o.getName());
    if (result == 0) {
      // punt on comparison for ordering, just calculate difference
      result = this.values.hashCode() - o.values.hashCode();
      if (result < 0)
        result = -1;
      else if (result > 0)
        result = 1;
    }
    if (result == 0) {
      result = this.configuration.hashCode() - o.configuration.hashCode();
      if (result < 0)
        result = -1;
      else if (result > 0)
        result = 1;
    }
    return result;
  }

  /**
   * @return This instance serialized with pb with pb magic prefix
   * @see #parseFrom(byte[])
   */
  public byte [] toByteArray() {
    return ProtobufUtil.prependPBMagic(convert().toByteArray());
  }

  /**
   * @param bytes A pb serialized {@link HColumnDescriptor} instance with pb magic prefix
   * @return An instance of {@link HColumnDescriptor} made from <code>bytes</code>
   * @throws DeserializationException
   * @see #toByteArray()
   */
  public static HColumnDescriptor parseFrom(final byte [] bytes) throws DeserializationException {
    if (!ProtobufUtil.isPBMagicPrefix(bytes)) throw new DeserializationException("No magic");
    int pblen = ProtobufUtil.lengthOfPBMagic();
    ColumnFamilySchema.Builder builder = ColumnFamilySchema.newBuilder();
    ColumnFamilySchema cfs = null;
    try {
      ProtobufUtil.mergeFrom(builder, bytes, pblen, bytes.length - pblen);
      cfs = builder.build();
    } catch (IOException e) {
      throw new DeserializationException(e);
    }
    return convert(cfs);
  }

  /**
   * @param cfs
   * @return An {@link HColumnDescriptor} made from the passed in <code>cfs</code>
   */
  public static HColumnDescriptor convert(final ColumnFamilySchema cfs) {
    // Use the empty constructor so we preserve the initial values set on construction for things
    // like maxVersion.  Otherwise, we pick up wrong values on deserialization which makes for
    // unrelated-looking test failures that are hard to trace back to here.
    HColumnDescriptor hcd = new HColumnDescriptor();
    hcd.name = cfs.getName().toByteArray();
    for (BytesBytesPair a: cfs.getAttributesList()) {
      hcd.setValue(a.getFirst().toByteArray(), a.getSecond().toByteArray());
    }
    for (NameStringPair a: cfs.getConfigurationList()) {
      hcd.setConfiguration(a.getName(), a.getValue());
    }
    return hcd;
  }

  /**
   * @return Convert this instance to a the pb column family type
   */
  public ColumnFamilySchema convert() {
    ColumnFamilySchema.Builder builder = ColumnFamilySchema.newBuilder();
    builder.setName(ByteStringer.wrap(getName()));
    for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> e: this.values.entrySet()) {
      BytesBytesPair.Builder aBuilder = BytesBytesPair.newBuilder();
      aBuilder.setFirst(ByteStringer.wrap(e.getKey().get()));
      aBuilder.setSecond(ByteStringer.wrap(e.getValue().get()));
      builder.addAttributes(aBuilder.build());
    }
    for (Map.Entry<String, String> e : this.configuration.entrySet()) {
      NameStringPair.Builder aBuilder = NameStringPair.newBuilder();
      aBuilder.setName(e.getKey());
      aBuilder.setValue(e.getValue());
      builder.addConfiguration(aBuilder.build());
    }
    return builder.build();
  }

  /**
   * Getter for accessing the configuration value by key.
   */
  public String getConfigurationValue(String key) {
    return configuration.get(key);
  }

  /**
   * Getter for fetching an unmodifiable {@link #configuration} map.
   */
  public Map<String, String> getConfiguration() {
    // shallow pointer copy
    return Collections.unmodifiableMap(configuration);
  }

  /**
   * Setter for storing a configuration setting in {@link #configuration} map.
   * @param key Config key. Same as XML config key e.g. hbase.something.or.other.
   * @param value String value. If null, removes the configuration.
   */
  public void setConfiguration(String key, String value) {
    if (value == null) {
      removeConfiguration(key);
    } else {
      configuration.put(key, value);
    }
  }

  /**
   * Remove a configuration setting represented by the key from the {@link #configuration} map.
   */
  public void removeConfiguration(final String key) {
    configuration.remove(key);
  }

  /**
   * Return the encryption algorithm in use by this family
   */
  @InterfaceStability.Unstable
  public String getEncryptionType() {
    return getValue(ENCRYPTION);
  }

  /**
   * Set the encryption algorithm for use with this family
   * @param algorithm
   */
  @InterfaceStability.Unstable
  public HColumnDescriptor setEncryptionType(String algorithm) {
    setValue(ENCRYPTION, algorithm);
    return this;
  }

  /** Return the raw crypto key attribute for the family, or null if not set  */
  @InterfaceStability.Unstable
  public byte[] getEncryptionKey() {
    return getValue(Bytes.toBytes(ENCRYPTION_KEY));
  }

  /** Set the raw crypto key attribute for the family */
  @InterfaceStability.Unstable
  public HColumnDescriptor setEncryptionKey(byte[] keyBytes) {
    setValue(Bytes.toBytes(ENCRYPTION_KEY), keyBytes);
    return this;
  }

  /**
   * @return replication factor set for this CF or {@link #DEFAULT_DFS_REPLICATION} if not set.
   *         <p>
   *         {@link #DEFAULT_DFS_REPLICATION} value indicates that user has explicitly not set any
   *         block replication factor for this CF, hence use the default replication factor set in
   *         the file system.
   */
  public short getDFSReplication() {
    String rf = getValue(DFS_REPLICATION);
    return rf == null ? DEFAULT_DFS_REPLICATION : Short.valueOf(rf);
  }

  /**
   * Set the replication factor to hfile(s) belonging to this family
   * @param replication number of replicas the blocks(s) belonging to this CF should have, or
   *          {@link #DEFAULT_DFS_REPLICATION} for the default replication factor set in the
   *          filesystem
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setDFSReplication(short replication) {
    if (replication < 1 && replication != DEFAULT_DFS_REPLICATION) {
      throw new IllegalArgumentException(
          "DFS replication factor cannot be less than 1 if explicitly set.");
    }
    setValue(DFS_REPLICATION, Short.toString(replication));
    return this;
  }
}
