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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.MobCompactPartitionPolicy;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.exceptions.HBaseException;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.ColumnFamilySchema;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.PrettyPrinter;
import org.apache.hadoop.hbase.util.PrettyPrinter.Unit;

import com.google.common.base.Preconditions;

/**
 * An HColumnDescriptor contains information about a column family such as the
 * number of versions, compression settings, etc.
 *
 * It is used as input when creating a table or adding a column.
 */
@InterfaceAudience.Public
public class HColumnDescriptor implements Comparable<HColumnDescriptor> {
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

  public static final String IN_MEMORY_COMPACTION = "IN_MEMORY_COMPACTION";

  // These constants are used as FileInfo keys
  public static final String COMPRESSION = "COMPRESSION";
  public static final String COMPRESSION_COMPACT = "COMPRESSION_COMPACT";
  public static final String ENCODE_ON_DISK = // To be removed, it is not used anymore
      "ENCODE_ON_DISK";
  public static final String DATA_BLOCK_ENCODING =
      "DATA_BLOCK_ENCODING";
  /**
   * Key for the BLOCKCACHE attribute.
   * A more exact name would be CACHE_DATA_ON_READ because this flag sets whether or not we
   * cache DATA blocks.  We always cache INDEX and BLOOM blocks; caching these blocks cannot be
   * disabled.
   */
  public static final String BLOCKCACHE = "BLOCKCACHE";
  public static final String CACHE_DATA_ON_WRITE = "CACHE_DATA_ON_WRITE";
  public static final String CACHE_INDEX_ON_WRITE = "CACHE_INDEX_ON_WRITE";
  public static final String CACHE_BLOOMS_ON_WRITE = "CACHE_BLOOMS_ON_WRITE";
  public static final String EVICT_BLOCKS_ON_CLOSE = "EVICT_BLOCKS_ON_CLOSE";
  /**
   * Key for cache data into L1 if cache is set up with more than one tier.
   * To set in the shell, do something like this:
   * <code>hbase(main):003:0&gt; create 't',
   *    {NAME =&gt; 't', CONFIGURATION =&gt; {CACHE_DATA_IN_L1 =&gt; 'true'}}</code>
   */
  public static final String CACHE_DATA_IN_L1 = "CACHE_DATA_IN_L1";

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
   * indices (more memory consumption). Note that this is a soft limit and that
   * blocks have overhead (metadata, CRCs) so blocks will tend to be the size
   * specified here and then some; i.e. don't expect that setting BLOCKSIZE=4k
   * means hbase data will align with an SSDs 4k page accesses (TODO).
   */
  public static final String BLOCKSIZE = "BLOCKSIZE";

  public static final String LENGTH = "LENGTH";
  public static final String TTL = "TTL";
  public static final String BLOOMFILTER = "BLOOMFILTER";
  public static final String FOREVER = "FOREVER";
  public static final String REPLICATION_SCOPE = "REPLICATION_SCOPE";
  public static final byte[] REPLICATION_SCOPE_BYTES = Bytes.toBytes(REPLICATION_SCOPE);
  public static final String MIN_VERSIONS = "MIN_VERSIONS";
  /**
   * Retain all cells across flushes and compactions even if they fall behind
   * a delete tombstone. To see all retained cells, do a 'raw' scan; see
   * Scan#setRaw or pass RAW =&gt; true attribute in the shell.
   */
  public static final String KEEP_DELETED_CELLS = "KEEP_DELETED_CELLS";
  public static final String COMPRESS_TAGS = "COMPRESS_TAGS";

  public static final String ENCRYPTION = "ENCRYPTION";
  public static final String ENCRYPTION_KEY = "ENCRYPTION_KEY";

  public static final String IS_MOB = "IS_MOB";
  public static final byte[] IS_MOB_BYTES = Bytes.toBytes(IS_MOB);
  public static final String MOB_THRESHOLD = "MOB_THRESHOLD";
  public static final byte[] MOB_THRESHOLD_BYTES = Bytes.toBytes(MOB_THRESHOLD);
  public static final long DEFAULT_MOB_THRESHOLD = 100 * 1024; // 100k
  public static final String MOB_COMPACT_PARTITION_POLICY = "MOB_COMPACT_PARTITION_POLICY";
  public static final byte[] MOB_COMPACT_PARTITION_POLICY_BYTES =
      Bytes.toBytes(MOB_COMPACT_PARTITION_POLICY);
  public static final MobCompactPartitionPolicy DEFAULT_MOB_COMPACT_PARTITION_POLICY =
      MobCompactPartitionPolicy.DAILY;

  public static final String DFS_REPLICATION = "DFS_REPLICATION";
  public static final short DEFAULT_DFS_REPLICATION = 0;

  public static final String STORAGE_POLICY = "STORAGE_POLICY";

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
   * Default setting for whether to try and serve this column family from memory or not.
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
   * Default setting for whether to cache data blocks in L1 tier.  Only makes sense if more than
   * one tier in operations: i.e. if we have an L1 and a L2.  This will be the cases if we are
   * using BucketCache.
   */
  public static final boolean DEFAULT_CACHE_DATA_IN_L1 = false;

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

  private final static Map<String, String> DEFAULT_VALUES = new HashMap<>();
  private final static Set<Bytes> RESERVED_KEYWORDS = new HashSet<>();

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
    DEFAULT_VALUES.put(CACHE_DATA_IN_L1, String.valueOf(DEFAULT_CACHE_DATA_IN_L1));
    DEFAULT_VALUES.put(CACHE_INDEX_ON_WRITE, String.valueOf(DEFAULT_CACHE_INDEX_ON_WRITE));
    DEFAULT_VALUES.put(CACHE_BLOOMS_ON_WRITE, String.valueOf(DEFAULT_CACHE_BLOOMS_ON_WRITE));
    DEFAULT_VALUES.put(EVICT_BLOCKS_ON_CLOSE, String.valueOf(DEFAULT_EVICT_BLOCKS_ON_CLOSE));
    DEFAULT_VALUES.put(PREFETCH_BLOCKS_ON_OPEN, String.valueOf(DEFAULT_PREFETCH_BLOCKS_ON_OPEN));
    for (String s : DEFAULT_VALUES.keySet()) {
      RESERVED_KEYWORDS.add(new Bytes(Bytes.toBytes(s)));
    }
    RESERVED_KEYWORDS.add(new Bytes(Bytes.toBytes(ENCRYPTION)));
    RESERVED_KEYWORDS.add(new Bytes(Bytes.toBytes(ENCRYPTION_KEY)));
    RESERVED_KEYWORDS.add(new Bytes(IS_MOB_BYTES));
    RESERVED_KEYWORDS.add(new Bytes(MOB_THRESHOLD_BYTES));
    RESERVED_KEYWORDS.add(new Bytes(MOB_COMPACT_PARTITION_POLICY_BYTES));
  }

  private static final int UNINITIALIZED = -1;

  // Column family name
  private byte [] name;

  // Column metadata
  private final Map<Bytes, Bytes> values = new HashMap<>();

  /**
   * A map which holds the configuration specific to the column family.
   * The keys of the map have the same names as config keys and override the defaults with
   * cf-specific settings. Example usage may be for compactions, etc.
   */
  private final Map<String, String> configuration = new HashMap<>();

  /*
   * Cache the max versions rather than calculate it every time.
   */
  private int cachedMaxVersions = UNINITIALIZED;

  /**
   * Construct a column descriptor specifying only the family name
   * The other attributes are defaulted.
   *
   * @param familyName Column family name. Must be 'printable' -- digit or
   * letter -- and may not contain a <code>:</code>
   */
  public HColumnDescriptor(final String familyName) {
    this(Bytes.toBytes(familyName));
  }

  /**
   * Construct a column descriptor specifying only the family name
   * The other attributes are defaulted.
   *
   * @param familyName Column family name. Must be 'printable' -- digit or
   * letter -- and may not contain a <code>:</code>
   */
  public HColumnDescriptor(final byte [] familyName) {
    isLegalFamilyName(familyName);
    this.name = familyName;

    setMaxVersions(DEFAULT_VERSIONS);
    setMinVersions(DEFAULT_MIN_VERSIONS);
    setKeepDeletedCells(DEFAULT_KEEP_DELETED);
    setInMemory(DEFAULT_IN_MEMORY);
    setBlockCacheEnabled(DEFAULT_BLOCKCACHE);
    setTimeToLive(DEFAULT_TTL);
    setCompressionType(Compression.Algorithm.valueOf(DEFAULT_COMPRESSION.toUpperCase(Locale.ROOT)));
    setDataBlockEncoding(DataBlockEncoding.valueOf(DEFAULT_DATA_BLOCK_ENCODING.toUpperCase(Locale.ROOT)));
    setBloomFilterType(BloomType.valueOf(DEFAULT_BLOOMFILTER.toUpperCase(Locale.ROOT)));
    setBlocksize(DEFAULT_BLOCKSIZE);
    setScope(DEFAULT_REPLICATION_SCOPE);
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
    for (Map.Entry<Bytes, Bytes> e :
        desc.values.entrySet()) {
      this.values.put(e.getKey(), e.getValue());
    }
    for (Map.Entry<String, String> e : desc.configuration.entrySet()) {
      this.configuration.put(e.getKey(), e.getValue());
    }
    setMaxVersions(desc.getMaxVersions());
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
    Bytes ibw = values.get(new Bytes(key));
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
  public Map<Bytes, Bytes> getValues() {
    // shallow pointer copy
    return Collections.unmodifiableMap(values);
  }

  /**
   * @param key The key.
   * @param value The value.
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setValue(byte[] key, byte[] value) {
    if (Bytes.compareTo(Bytes.toBytes(HConstants.VERSIONS), key) == 0) {
      cachedMaxVersions = UNINITIALIZED;
    }
    values.put(new Bytes(key), new Bytes(value));
    return this;
  }

  /**
   * @param key Key whose key and value we're to remove from HCD parameters.
   */
  public void remove(final byte [] key) {
    values.remove(new Bytes(key));
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

  /**
   * @return compression type being used for the column family
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             (<a href="https://issues.apache.org/jira/browse/HBASE-13655">HBASE-13655</a>).
   *             Use {@link #getCompressionType()}.
   */
  @Deprecated
  public Compression.Algorithm getCompression() {
    return getCompressionType();
  }

  /**
   *  @return compression type being used for the column family for major compaction
   *  @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             (<a href="https://issues.apache.org/jira/browse/HBASE-13655">HBASE-13655</a>).
   *             Use {@link #getCompactionCompressionType()}.
   */
  @Deprecated
  public Compression.Algorithm getCompactionCompression() {
    return getCompactionCompressionType();
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
    String n = getValue(COMPRESSION);
    if (n == null) {
      return Compression.Algorithm.NONE;
    }
    return Compression.Algorithm.valueOf(n.toUpperCase(Locale.ROOT));
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
  public boolean isCompressTags() {
    String compressTagsStr = getValue(COMPRESS_TAGS);
    boolean compressTags = DEFAULT_COMPRESS_TAGS;
    if (compressTagsStr != null) {
      compressTags = Boolean.parseBoolean(compressTagsStr);
    }
    return compressTags;
  }

  /**
   * @return Compression type setting.
   */
  public Compression.Algorithm getCompactionCompressionType() {
    String n = getValue(COMPRESSION_COMPACT);
    if (n == null) {
      return getCompressionType();
    }
    return Compression.Algorithm.valueOf(n.toUpperCase(Locale.ROOT));
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
   * @return True if we are to favor keeping all values for this column family in the
   * HRegionServer cache.
   */
  public boolean isInMemory() {
    String value = getValue(HConstants.IN_MEMORY);
    if (value != null) {
      return Boolean.parseBoolean(value);
    }
    return DEFAULT_IN_MEMORY;
  }

  /**
   * @param inMemory True if we are to favor keeping all values for this column family in the
   * HRegionServer cache
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setInMemory(boolean inMemory) {
    return setValue(HConstants.IN_MEMORY, Boolean.toString(inMemory));
  }

  /**
   * @return in-memory compaction policy if set for the cf. Returns null if no policy is set for
   *          for this column family
   */
  public MemoryCompactionPolicy getInMemoryCompaction() {
    String value = getValue(IN_MEMORY_COMPACTION);
    if (value != null) {
      return MemoryCompactionPolicy.valueOf(value);
    }
    return null;
  }

  /**
   * @param inMemoryCompaction the prefered in-memory compaction policy
   *                  for this column family
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setInMemoryCompaction(MemoryCompactionPolicy inMemoryCompaction) {
    return setValue(IN_MEMORY_COMPACTION, inMemoryCompaction.toString());
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
   */
  public HColumnDescriptor setKeepDeletedCells(KeepDeletedCells keepDeletedCells) {
    return setValue(KEEP_DELETED_CELLS, keepDeletedCells.toString());
  }

  /**
   * @return Time-to-live of cell contents, in seconds.
   */
  public int getTimeToLive() {
    String value = getValue(TTL);
    return (value != null)? Integer.parseInt(value) : DEFAULT_TTL;
  }

  /**
   * @param timeToLive Time-to-live of cell contents, in seconds.
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setTimeToLive(int timeToLive) {
    return setValue(TTL, Integer.toString(timeToLive));
  }

  /**
   * @param timeToLive Time to live of cell contents, in human readable format
   *                   @see org.apache.hadoop.hbase.util.PrettyPrinter#format(String, Unit)
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setTimeToLive(String timeToLive) throws HBaseException {
    return setValue(TTL, PrettyPrinter.valueOf(timeToLive, Unit.TIME_INTERVAL));
  }

  /**
   * @return The minimum number of versions to keep.
   */
  public int getMinVersions() {
    String value = getValue(MIN_VERSIONS);
    return (value != null)? Integer.parseInt(value) : 0;
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
   * @return True if hfile DATA type blocks should be cached (You cannot disable caching of INDEX
   * and BLOOM type blocks).
   */
  public boolean isBlockCacheEnabled() {
    String value = getValue(BLOCKCACHE);
    if (value != null) {
      return Boolean.parseBoolean(value);
    }
    return DEFAULT_BLOCKCACHE;
  }

  /**
   * @param blockCacheEnabled True if hfile DATA type blocks should be cached (We always cache
   * INDEX and BLOOM blocks; you cannot turn this off).
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
      return Integer.parseInt(Bytes.toString(value));
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
  public boolean isCacheDataOnWrite() {
    return setAndGetBoolean(CACHE_DATA_ON_WRITE, DEFAULT_CACHE_DATA_ON_WRITE);
  }

  /**
   * @param value true if we should cache data blocks on write
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setCacheDataOnWrite(boolean value) {
    return setValue(CACHE_DATA_ON_WRITE, Boolean.toString(value));
  }

  /**
   * @return true if we should cache data blocks in the L1 cache (if block cache deploy has more
   *         than one tier; e.g. we are using CombinedBlockCache).
   */
  public boolean isCacheDataInL1() {
    return setAndGetBoolean(CACHE_DATA_IN_L1, DEFAULT_CACHE_DATA_IN_L1);
  }

  /**
   * @param value true if we should cache data blocks in the L1 cache (if block cache deploy
   * has more than one tier; e.g. we are using CombinedBlockCache).
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setCacheDataInL1(boolean value) {
    return setValue(CACHE_DATA_IN_L1, Boolean.toString(value));
  }

  private boolean setAndGetBoolean(final String key, final boolean defaultSetting) {
    String value = getValue(key);
    if (value != null) {
      return Boolean.parseBoolean(value);
    }
    return defaultSetting;
  }

  /**
   * @return true if we should cache index blocks on write
   */
  public boolean isCacheIndexesOnWrite() {
    return setAndGetBoolean(CACHE_INDEX_ON_WRITE, DEFAULT_CACHE_INDEX_ON_WRITE);
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
  public boolean isCacheBloomsOnWrite() {
    return setAndGetBoolean(CACHE_BLOOMS_ON_WRITE, DEFAULT_CACHE_BLOOMS_ON_WRITE);
  }

  /**
   * @param value true if we should cache bloomfilter blocks on write
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setCacheBloomsOnWrite(boolean value) {
    return setValue(CACHE_BLOOMS_ON_WRITE, Boolean.toString(value));
  }

  /**
   * @return true if we should evict cached blocks from the blockcache on close
   */
  public boolean isEvictBlocksOnClose() {
    return setAndGetBoolean(EVICT_BLOCKS_ON_CLOSE, DEFAULT_EVICT_BLOCKS_ON_CLOSE);
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
  public boolean isPrefetchBlocksOnOpen() {
    return setAndGetBoolean(PREFETCH_BLOCKS_ON_OPEN, DEFAULT_PREFETCH_BLOCKS_ON_OPEN);
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
    for (Map.Entry<Bytes, Bytes> entry : values.entrySet()) {
      if (!RESERVED_KEYWORDS.contains(entry.getKey())) {
        hasConfigKeys = true;
        continue;
      }
      String key = Bytes.toString(entry.getKey().get());
      String value = Bytes.toStringBinary(entry.getValue().get());
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
      for (Bytes k : values.keySet()) {
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
      /* TTL for now, we can add more as we need */
    if (key.equals(HColumnDescriptor.TTL)) {
      unit = Unit.TIME_INTERVAL;
    } else if (key.equals(HColumnDescriptor.MOB_THRESHOLD)) {
      unit = Unit.LONG;
    } else if (key.equals(HColumnDescriptor.IS_MOB)) {
      unit = Unit.BOOLEAN;
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
    result ^= (int) COLUMN_DESCRIPTOR_VERSION;
    result ^= values.hashCode();
    result ^= configuration.hashCode();
    return result;
  }

  // Comparable
  @Override
  public int compareTo(HColumnDescriptor o) {
    int result = Bytes.compareTo(this.name, o.getName());
    if (result == 0) {
      // punt on comparison for ordering, just calculate difference.
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
  public byte[] toByteArray() {
    return ProtobufUtil
        .prependPBMagic(ProtobufUtil.convertToColumnFamilySchema(this).toByteArray());
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
    return ProtobufUtil.convertToHColumnDesc(cfs);
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
  public HColumnDescriptor setConfiguration(String key, String value) {
    if (value == null) {
      removeConfiguration(key);
    } else {
      configuration.put(key, value);
    }
    return this;
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
  public String getEncryptionType() {
    return getValue(ENCRYPTION);
  }

  /**
   * Set the encryption algorithm for use with this family
   * @param algorithm
   */
  public HColumnDescriptor setEncryptionType(String algorithm) {
    setValue(ENCRYPTION, algorithm);
    return this;
  }

  /** Return the raw crypto key attribute for the family, or null if not set  */
  public byte[] getEncryptionKey() {
    return getValue(Bytes.toBytes(ENCRYPTION_KEY));
  }

  /** Set the raw crypto key attribute for the family */
  public HColumnDescriptor setEncryptionKey(byte[] keyBytes) {
    setValue(Bytes.toBytes(ENCRYPTION_KEY), keyBytes);
    return this;
  }

  /**
   * Gets the mob threshold of the family.
   * If the size of a cell value is larger than this threshold, it's regarded as a mob.
   * The default threshold is 1024*100(100K)B.
   * @return The mob threshold.
   */
  public long getMobThreshold() {
    byte[] threshold = getValue(MOB_THRESHOLD_BYTES);
    return threshold != null && threshold.length == Bytes.SIZEOF_LONG ? Bytes.toLong(threshold)
        : DEFAULT_MOB_THRESHOLD;
  }

  /**
   * Sets the mob threshold of the family.
   * @param threshold The mob threshold.
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setMobThreshold(long threshold) {
    setValue(MOB_THRESHOLD_BYTES, Bytes.toBytes(threshold));
    return this;
  }

  /**
   * Gets whether the mob is enabled for the family.
   * @return True if the mob is enabled for the family.
   */
  public boolean isMobEnabled() {
    byte[] isMobEnabled = getValue(IS_MOB_BYTES);
    return isMobEnabled != null && isMobEnabled.length == Bytes.SIZEOF_BOOLEAN
        && Bytes.toBoolean(isMobEnabled);
  }

  /**
   * Enables the mob for the family.
   * @param isMobEnabled Whether to enable the mob for the family.
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setMobEnabled(boolean isMobEnabled) {
    setValue(IS_MOB_BYTES, Bytes.toBytes(isMobEnabled));
    return this;
  }

  /**
   * Get the mob compact partition policy for this family
   * @return MobCompactPartitionPolicy
   */
  public MobCompactPartitionPolicy getMobCompactPartitionPolicy() {
    String policy = getValue(MOB_COMPACT_PARTITION_POLICY);
    if (policy == null) {
      return DEFAULT_MOB_COMPACT_PARTITION_POLICY;
    }

    return MobCompactPartitionPolicy.valueOf(policy.toUpperCase(Locale.ROOT));
  }

  /**
   * Set the mob compact partition policy for the family.
   * @param policy policy type
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setMobCompactPartitionPolicy(MobCompactPartitionPolicy policy) {
    return setValue(MOB_COMPACT_PARTITION_POLICY, policy.toString().toUpperCase(Locale.ROOT));
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

  /**
   * Return the storage policy in use by this family
   * <p/>
   * Not using {@code enum} here because HDFS is not using {@code enum} for storage policy, see
   * org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite for more details
   */
  public String getStoragePolicy() {
    return getValue(STORAGE_POLICY);
  }

  /**
   * Set the storage policy for use with this family
   * @param policy the policy to set, valid setting includes: <i>"LAZY_PERSIST"</i>,
   *          <i>"ALL_SSD"</i>, <i>"ONE_SSD"</i>, <i>"HOT"</i>, <i>"WARM"</i>, <i>"COLD"</i>
   */
  public HColumnDescriptor setStoragePolicy(String policy) {
    setValue(STORAGE_POLICY, policy);
    return this;
  }
}
