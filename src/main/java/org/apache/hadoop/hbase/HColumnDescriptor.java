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
package org.apache.hadoop.hbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * An HColumnDescriptor contains information about a column family such as the
 * number of versions, compression settings, etc.
 *
 * It is used as input when creating a table or adding a column. Once set, the
 * parameters that specify a column cannot be changed without deleting the
 * column and recreating it. If there is data stored in the column, it will be
 * deleted when the column is deleted.
 */
public class HColumnDescriptor implements WritableComparable<HColumnDescriptor> {
  // For future backward compatibility

  // Version 3 was when column names become byte arrays and when we picked up
  // Time-to-live feature.  Version 4 was when we moved to byte arrays, HBASE-82.
  // Version 5 was when bloom filter descriptors were removed.
  // Version 6 adds metadata as a map where keys and values are byte[].
  // Version 7 -- add new compression and hfile blocksize to HColumnDescriptor (HBASE-1217)
  // Version 8 -- reintroduction of bloom filters, changed from boolean to enum
  // Version 9 -- add data block encoding
  private static final byte COLUMN_DESCRIPTOR_VERSION = (byte) 9;

  // These constants are used as FileInfo keys
  public static final String COMPRESSION = "COMPRESSION";
  public static final String COMPRESSION_COMPACT = "COMPRESSION_COMPACT";
  public static final String ENCODE_ON_DISK =
      "ENCODE_ON_DISK";
  public static final String DATA_BLOCK_ENCODING =
      "DATA_BLOCK_ENCODING";
  public static final String BLOCKCACHE = "BLOCKCACHE";
  public static final String CACHE_DATA_ON_WRITE = "CACHE_DATA_ON_WRITE";
  public static final String CACHE_INDEX_ON_WRITE = "CACHE_INDEX_ON_WRITE";
  public static final String CACHE_BLOOMS_ON_WRITE = "CACHE_BLOOMS_ON_WRITE";
  public static final String EVICT_BLOCKS_ON_CLOSE = "EVICT_BLOCKS_ON_CLOSE";

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
  public static final String MIN_VERSIONS = "MIN_VERSIONS";
  public static final String KEEP_DELETED_CELLS = "KEEP_DELETED_CELLS";

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
  public static final int DEFAULT_VERSIONS = 3;

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
  public static final boolean DEFAULT_KEEP_DELETED = false;

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
  public static final int DEFAULT_BLOCKSIZE = HFile.DEFAULT_BLOCKSIZE;

  /**
   * Default setting for whether or not to use bloomfilters.
   */
  public static final String DEFAULT_BLOOMFILTER = StoreFile.BloomType.NONE.toString();

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

  private final static Map<String, String> DEFAULT_VALUES = new HashMap<String, String>();
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
      DEFAULT_VALUES.put(ENCODE_ON_DISK,
          String.valueOf(DEFAULT_ENCODE_ON_DISK));
      DEFAULT_VALUES.put(DATA_BLOCK_ENCODING,
          String.valueOf(DEFAULT_DATA_BLOCK_ENCODING));
      DEFAULT_VALUES.put(CACHE_DATA_ON_WRITE,
          String.valueOf(DEFAULT_CACHE_DATA_ON_WRITE));
      DEFAULT_VALUES.put(CACHE_INDEX_ON_WRITE,
          String.valueOf(DEFAULT_CACHE_INDEX_ON_WRITE));
      DEFAULT_VALUES.put(CACHE_BLOOMS_ON_WRITE,
          String.valueOf(DEFAULT_CACHE_BLOOMS_ON_WRITE));
      DEFAULT_VALUES.put(EVICT_BLOCKS_ON_CLOSE,
          String.valueOf(DEFAULT_EVICT_BLOCKS_ON_CLOSE));
      for (String s : DEFAULT_VALUES.keySet()) {
        RESERVED_KEYWORDS.add(new ImmutableBytesWritable(Bytes.toBytes(s)));
      }
  }

  // Column family name
  private byte [] name;

  // Column metadata
  protected final Map<ImmutableBytesWritable,ImmutableBytesWritable> values =
    new HashMap<ImmutableBytesWritable,ImmutableBytesWritable>();

  /*
   * Cache the max versions rather than calculate it every time.
   */
  private int cachedMaxVersions = -1;

  /**
   * Default constructor. Must be present for Writable.
   */
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
      final int maxVersions, final boolean keepDeletedCells,
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
      valueOf(compression.toUpperCase()));
    setEncodeOnDisk(encodeOnDisk);
    setDataBlockEncoding(DataBlockEncoding.
        valueOf(dataBlockEncoding.toUpperCase()));
    setBloomFilterType(StoreFile.BloomType.
      valueOf(bloomFilter.toUpperCase()));
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
    byte[] recoveredEdit = Bytes.toBytes(HLog.RECOVERED_EDITS_DIR);
    if (Bytes.equals(recoveredEdit, b)) {
      throw new IllegalArgumentException("Family name cannot be: " +
          HLog.RECOVERED_EDITS_DIR);
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
    return Compression.Algorithm.valueOf(n.toUpperCase());
  }

  /** @return compression type being used for the column family for major 
      compression */
  public Compression.Algorithm getCompactionCompression() {
    String n = getValue(COMPRESSION_COMPACT);
    if (n == null) {
      return getCompression();
    }
    return Compression.Algorithm.valueOf(n.toUpperCase());
  }

  /** @return maximum number of versions */
  public int getMaxVersions() {
    return this.cachedMaxVersions;
  }

  /**
   * @param maxVersions maximum number of versions
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setMaxVersions(int maxVersions) {
    setValue(HConstants.VERSIONS, Integer.toString(maxVersions));
    cachedMaxVersions = maxVersions;
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
    return setValue(COMPRESSION, type.getName().toUpperCase());
  }

  /** @return data block encoding algorithm used on disk */
  public DataBlockEncoding getDataBlockEncodingOnDisk() {
    String encodeOnDiskStr = getValue(ENCODE_ON_DISK);
    boolean encodeOnDisk;
    if (encodeOnDiskStr == null) {
      encodeOnDisk = DEFAULT_ENCODE_ON_DISK;
    } else {
      encodeOnDisk = Boolean.valueOf(encodeOnDiskStr);
    }

    if (!encodeOnDisk) {
      // No encoding on disk.
      return DataBlockEncoding.NONE;
    }
    return getDataBlockEncoding();
  }

  /**
   * Set the flag indicating that we only want to encode data block in cache
   * but not on disk.
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setEncodeOnDisk(boolean encodeOnDisk) {
    return setValue(ENCODE_ON_DISK, String.valueOf(encodeOnDisk));
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
    return setValue(COMPRESSION_COMPACT, type.getName().toUpperCase());
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

  public boolean getKeepDeletedCells() {
    String value = getValue(KEEP_DELETED_CELLS);
    if (value != null) {
      return Boolean.valueOf(value).booleanValue();
    }
    return DEFAULT_KEEP_DELETED;
  }

  /**
   * @param keepDeletedCells True if deleted rows should not be collected
   * immediately.
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setKeepDeletedCells(boolean keepDeletedCells) {
    return setValue(KEEP_DELETED_CELLS, Boolean.toString(keepDeletedCells));
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
  public StoreFile.BloomType getBloomFilterType() {
    String n = getValue(BLOOMFILTER);
    if (n == null) {
      n = DEFAULT_BLOOMFILTER;
    }
    return StoreFile.BloomType.valueOf(n.toUpperCase());
  }

  /**
   * @param bt bloom filter type
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setBloomFilterType(final StoreFile.BloomType bt) {
    return setValue(BLOOMFILTER, bt.toString());
  }

   /**
    * @return the scope tag
    */
  public int getScope() {
    String value = getValue(REPLICATION_SCOPE);
    if (value != null) {
      return Integer.valueOf(value).intValue();
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
      String value = Bytes.toString(values.get(k).get());
      if (printDefaults
          || !DEFAULT_VALUES.containsKey(key)
          || !DEFAULT_VALUES.get(key).equalsIgnoreCase(value)) {
        s.append(", ");
        s.append(key);

        s.append(" => ");
        s.append('\'').append(value).append('\'');
      }
    }

    // print all non-reserved, advanced config keys as a separate subset
    if (hasConfigKeys) {
      s.append(", ");
      s.append(HConstants.CONFIG).append(" => ");
      s.append('{');
      boolean printComma = false;
      for (ImmutableBytesWritable k : values.keySet()) {
        if (RESERVED_KEYWORDS.contains(k)) {
          continue;
        }
        String key = Bytes.toString(k.get());
        String value = Bytes.toString(values.get(k).get());
        if (printComma) {
          s.append(", ");
        }
        printComma = true;
        s.append('\'').append(key).append('\'');
        s.append(" => ");
        s.append('\'').append(value).append('\'');
      }
      s.append('}');
    }
    return s;
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
    return result;
  }

  // Writable

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
    }
  }

  public void write(DataOutput out) throws IOException {
    out.writeByte(COLUMN_DESCRIPTOR_VERSION);
    Bytes.writeByteArray(out, this.name);
    out.writeInt(values.size());
    for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> e:
        values.entrySet()) {
      e.getKey().write(out);
      e.getValue().write(out);
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
    return result;
  }
}
