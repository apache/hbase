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

import java.util.Map;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.MobCompactPartitionPolicy;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.exceptions.HBaseException;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.PrettyPrinter.Unit;

/**
 * An HColumnDescriptor contains information about a column family such as the
 * number of versions, compression settings, etc.
 *
 * It is used as input when creating a table or adding a column.
 */
@InterfaceAudience.Public
@Deprecated // remove it in 3.0
public class HColumnDescriptor implements ColumnFamilyDescriptor, Comparable<HColumnDescriptor> {
  public static final String IN_MEMORY_COMPACTION = ColumnFamilyDescriptorBuilder.IN_MEMORY_COMPACTION;
  public static final String COMPRESSION = ColumnFamilyDescriptorBuilder.COMPRESSION;
  public static final String COMPRESSION_COMPACT = ColumnFamilyDescriptorBuilder.COMPRESSION_COMPACT;
  public static final String ENCODE_ON_DISK = "ENCODE_ON_DISK";
  public static final String DATA_BLOCK_ENCODING = ColumnFamilyDescriptorBuilder.DATA_BLOCK_ENCODING;
  public static final String BLOCKCACHE = ColumnFamilyDescriptorBuilder.BLOCKCACHE;
  public static final String CACHE_DATA_ON_WRITE = ColumnFamilyDescriptorBuilder.CACHE_DATA_ON_WRITE;
  public static final String CACHE_INDEX_ON_WRITE = ColumnFamilyDescriptorBuilder.CACHE_INDEX_ON_WRITE;
  public static final String CACHE_BLOOMS_ON_WRITE = ColumnFamilyDescriptorBuilder.CACHE_BLOOMS_ON_WRITE;
  public static final String EVICT_BLOCKS_ON_CLOSE = ColumnFamilyDescriptorBuilder.EVICT_BLOCKS_ON_CLOSE;
  public static final String CACHE_DATA_IN_L1 = "CACHE_DATA_IN_L1";
  public static final String PREFETCH_BLOCKS_ON_OPEN = ColumnFamilyDescriptorBuilder.PREFETCH_BLOCKS_ON_OPEN;
  public static final String BLOCKSIZE = ColumnFamilyDescriptorBuilder.BLOCKSIZE;
  public static final String LENGTH = "LENGTH";
  public static final String TTL = ColumnFamilyDescriptorBuilder.TTL;
  public static final String BLOOMFILTER = ColumnFamilyDescriptorBuilder.BLOOMFILTER;
  public static final String FOREVER = "FOREVER";
  public static final String REPLICATION_SCOPE = ColumnFamilyDescriptorBuilder.REPLICATION_SCOPE;
  public static final byte[] REPLICATION_SCOPE_BYTES = Bytes.toBytes(REPLICATION_SCOPE);
  public static final String MIN_VERSIONS = ColumnFamilyDescriptorBuilder.MIN_VERSIONS;
  public static final String KEEP_DELETED_CELLS = ColumnFamilyDescriptorBuilder.KEEP_DELETED_CELLS;
  public static final String COMPRESS_TAGS = ColumnFamilyDescriptorBuilder.COMPRESS_TAGS;
  public static final String ENCRYPTION = ColumnFamilyDescriptorBuilder.ENCRYPTION;
  public static final String ENCRYPTION_KEY = ColumnFamilyDescriptorBuilder.ENCRYPTION_KEY;
  public static final String IS_MOB = ColumnFamilyDescriptorBuilder.IS_MOB;
  public static final byte[] IS_MOB_BYTES = Bytes.toBytes(IS_MOB);
  public static final String MOB_THRESHOLD = ColumnFamilyDescriptorBuilder.MOB_THRESHOLD;
  public static final byte[] MOB_THRESHOLD_BYTES = Bytes.toBytes(MOB_THRESHOLD);
  public static final long DEFAULT_MOB_THRESHOLD = ColumnFamilyDescriptorBuilder.DEFAULT_MOB_THRESHOLD;
  public static final String MOB_COMPACT_PARTITION_POLICY = ColumnFamilyDescriptorBuilder.MOB_COMPACT_PARTITION_POLICY;
  public static final byte[] MOB_COMPACT_PARTITION_POLICY_BYTES = Bytes.toBytes(MOB_COMPACT_PARTITION_POLICY);
  public static final MobCompactPartitionPolicy DEFAULT_MOB_COMPACT_PARTITION_POLICY
        = ColumnFamilyDescriptorBuilder.DEFAULT_MOB_COMPACT_PARTITION_POLICY;
  public static final String DFS_REPLICATION = ColumnFamilyDescriptorBuilder.DFS_REPLICATION;
  public static final short DEFAULT_DFS_REPLICATION = ColumnFamilyDescriptorBuilder.DEFAULT_DFS_REPLICATION;
  public static final String STORAGE_POLICY = ColumnFamilyDescriptorBuilder.STORAGE_POLICY;
  public static final String DEFAULT_COMPRESSION = ColumnFamilyDescriptorBuilder.DEFAULT_COMPRESSION.name();
  public static final boolean DEFAULT_ENCODE_ON_DISK = true;
  public static final String DEFAULT_DATA_BLOCK_ENCODING = ColumnFamilyDescriptorBuilder.DEFAULT_DATA_BLOCK_ENCODING.name();
  public static final int DEFAULT_VERSIONS = ColumnFamilyDescriptorBuilder.DEFAULT_MAX_VERSIONS;
  public static final int DEFAULT_MIN_VERSIONS = ColumnFamilyDescriptorBuilder.DEFAULT_MIN_VERSIONS;
  public static final boolean DEFAULT_IN_MEMORY = ColumnFamilyDescriptorBuilder.DEFAULT_IN_MEMORY;
  public static final KeepDeletedCells DEFAULT_KEEP_DELETED = ColumnFamilyDescriptorBuilder.DEFAULT_KEEP_DELETED;
  public static final boolean DEFAULT_BLOCKCACHE = ColumnFamilyDescriptorBuilder.DEFAULT_BLOCKCACHE;
  public static final boolean DEFAULT_CACHE_DATA_ON_WRITE = ColumnFamilyDescriptorBuilder.DEFAULT_CACHE_DATA_ON_WRITE;
  public static final boolean DEFAULT_CACHE_DATA_IN_L1 = false;
  public static final boolean DEFAULT_CACHE_INDEX_ON_WRITE = ColumnFamilyDescriptorBuilder.DEFAULT_CACHE_INDEX_ON_WRITE;
  public static final int DEFAULT_BLOCKSIZE = ColumnFamilyDescriptorBuilder.DEFAULT_BLOCKSIZE;
  public static final String DEFAULT_BLOOMFILTER =  ColumnFamilyDescriptorBuilder.DEFAULT_BLOOMFILTER.name();
  public static final boolean DEFAULT_CACHE_BLOOMS_ON_WRITE = ColumnFamilyDescriptorBuilder.DEFAULT_CACHE_BLOOMS_ON_WRITE;
  public static final int DEFAULT_TTL = ColumnFamilyDescriptorBuilder.DEFAULT_TTL;
  public static final int DEFAULT_REPLICATION_SCOPE = ColumnFamilyDescriptorBuilder.DEFAULT_REPLICATION_SCOPE;
  public static final boolean DEFAULT_EVICT_BLOCKS_ON_CLOSE = ColumnFamilyDescriptorBuilder.DEFAULT_EVICT_BLOCKS_ON_CLOSE;
  public static final boolean DEFAULT_COMPRESS_TAGS = ColumnFamilyDescriptorBuilder.DEFAULT_COMPRESS_TAGS;
  public static final boolean DEFAULT_PREFETCH_BLOCKS_ON_OPEN = ColumnFamilyDescriptorBuilder.DEFAULT_PREFETCH_BLOCKS_ON_OPEN;
  public static final String NEW_VERSION_BEHAVIOR = ColumnFamilyDescriptorBuilder.NEW_VERSION_BEHAVIOR;
  public static final boolean DEFAULT_NEW_VERSION_BEHAVIOR = ColumnFamilyDescriptorBuilder.DEFAULT_NEW_VERSION_BEHAVIOR;
  protected final ModifyableColumnFamilyDescriptor delegatee;

  /**
   * Construct a column descriptor specifying only the family name
   * The other attributes are defaulted.
   *
   * @param familyName Column family name. Must be 'printable' -- digit or
   * letter -- and may not contain a <code>:</code>
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             (<a href="https://issues.apache.org/jira/browse/HBASE-18433">HBASE-18433</a>).
   *             Use {@link ColumnFamilyDescriptorBuilder#of(String)}.
   */
  @Deprecated
  public HColumnDescriptor(final String familyName) {
    this(Bytes.toBytes(familyName));
  }

  /**
   * Construct a column descriptor specifying only the family name
   * The other attributes are defaulted.
   *
   * @param familyName Column family name. Must be 'printable' -- digit or
   * letter -- and may not contain a <code>:</code>
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             (<a href="https://issues.apache.org/jira/browse/HBASE-18433">HBASE-18433</a>).
   *             Use {@link ColumnFamilyDescriptorBuilder#of(byte[])}.
   */
  @Deprecated
  public HColumnDescriptor(final byte [] familyName) {
    this(new ModifyableColumnFamilyDescriptor(familyName));
  }

  /**
   * Constructor.
   * Makes a deep copy of the supplied descriptor.
   * Can make a modifiable descriptor from an UnmodifyableHColumnDescriptor.
   *
   * @param desc The descriptor.
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             (<a href="https://issues.apache.org/jira/browse/HBASE-18433">HBASE-18433</a>).
   *             Use {@link ColumnFamilyDescriptorBuilder#copy(ColumnFamilyDescriptor)}.
   */
  @Deprecated
  public HColumnDescriptor(HColumnDescriptor desc) {
    this(desc, true);
  }

  protected HColumnDescriptor(HColumnDescriptor desc, boolean deepClone) {
    this(deepClone ? new ModifyableColumnFamilyDescriptor(desc)
            : desc.delegatee);
  }

  protected HColumnDescriptor(ModifyableColumnFamilyDescriptor delegate) {
    this.delegatee = delegate;
  }

  /**
   * @param b Family name.
   * @return <code>b</code>
   * @throws IllegalArgumentException If not null and not a legitimate family
   * name: i.e. 'printable' and ends in a ':' (Null passes are allowed because
   * <code>b</code> can be null when deserializing).  Cannot start with a '.'
   * either. Also Family can not be an empty value or equal "recovered.edits".
   * @deprecated since 2.0.0 and will be removed in 3.0.0. Use
   *   {@link ColumnFamilyDescriptorBuilder#isLegalColumnFamilyName(byte[])} instead.
   * @see ColumnFamilyDescriptorBuilder#isLegalColumnFamilyName(byte[])
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-18008">HBASE-18008</a>
   */
  @Deprecated
  public static byte [] isLegalFamilyName(final byte [] b) {
    return ColumnFamilyDescriptorBuilder.isLegalColumnFamilyName(b);
  }

  /**
   * @return Name of this column family
   */
  @Override
  public byte [] getName() {
    return delegatee.getName();
  }

  /**
   * @return The name string of this column family
   */
  @Override
  public String getNameAsString() {
    return delegatee.getNameAsString();
  }

  /**
   * @param key The key.
   * @return The value.
   */
  @Override
  public byte[] getValue(byte[] key) {
    return delegatee.getValue(key);
  }

  /**
   * @param key The key.
   * @return The value as a string.
   */
  public String getValue(String key) {
    byte[] value = getValue(Bytes.toBytes(key));
    return value == null ? null : Bytes.toString(value);
  }

  @Override
  public Map<Bytes, Bytes> getValues() {
    return delegatee.getValues();
  }

  /**
   * @param key The key.
   * @param value The value.
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setValue(byte[] key, byte[] value) {
    getDelegateeForModification().setValue(key, value);
    return this;
  }

  /**
   * @param key Key whose key and value we're to remove from HCD parameters.
   */
  public void remove(final byte [] key) {
    getDelegateeForModification().removeValue(new Bytes(key));
  }

  /**
   * @param key The key.
   * @param value The value.
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setValue(String key, String value) {
    getDelegateeForModification().setValue(key, value);
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

  @Override
  public int getMaxVersions() {
    return delegatee.getMaxVersions();
  }

  /**
   * @param value maximum number of versions
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setMaxVersions(int value) {
    getDelegateeForModification().setMaxVersions(value);
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

  @Override
  public int getBlocksize() {
    return delegatee.getBlocksize();
  }

  /**
   * @param value Blocksize to use when writing out storefiles/hfiles on this
   * column family.
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setBlocksize(int value) {
    getDelegateeForModification().setBlocksize(value);
    return this;
  }

  @Override
  public Compression.Algorithm getCompressionType() {
    return delegatee.getCompressionType();
  }

  /**
   * Compression types supported in hbase.
   * LZO is not bundled as part of the hbase distribution.
   * See <a href="http://wiki.apache.org/hadoop/UsingLzoCompression">LZO Compression</a>
   * for how to enable it.
   * @param value Compression type setting.
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setCompressionType(Compression.Algorithm value) {
    getDelegateeForModification().setCompressionType(value);
    return this;
  }

  @Override
  public DataBlockEncoding getDataBlockEncoding() {
    return delegatee.getDataBlockEncoding();
  }

  /**
   * Set data block encoding algorithm used in block cache.
   * @param value What kind of data block encoding will be used.
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setDataBlockEncoding(DataBlockEncoding value) {
    getDelegateeForModification().setDataBlockEncoding(value);
    return this;
  }

  /**
   * Set whether the tags should be compressed along with DataBlockEncoding. When no
   * DataBlockEncoding is been used, this is having no effect.
   *
   * @param value
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setCompressTags(boolean value) {
    getDelegateeForModification().setCompressTags(value);
    return this;
  }

  @Override
  public boolean isCompressTags() {
    return delegatee.isCompressTags();
  }

  @Override
  public Compression.Algorithm getCompactionCompressionType() {
    return delegatee.getCompactionCompressionType();
  }

  /**
   * Compression types supported in hbase.
   * LZO is not bundled as part of the hbase distribution.
   * See <a href="http://wiki.apache.org/hadoop/UsingLzoCompression">LZO Compression</a>
   * for how to enable it.
   * @param value Compression type setting.
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setCompactionCompressionType(Compression.Algorithm value) {
    getDelegateeForModification().setCompactionCompressionType(value);
    return this;
  }

  @Override
  public boolean isInMemory() {
    return delegatee.isInMemory();
  }

  /**
   * @param value True if we are to favor keeping all values for this column family in the
   * HRegionServer cache
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setInMemory(boolean value) {
    getDelegateeForModification().setInMemory(value);
    return this;
  }

  @Override
  public MemoryCompactionPolicy getInMemoryCompaction() {
    return delegatee.getInMemoryCompaction();
  }

  /**
   * @param value the prefered in-memory compaction policy
   *                  for this column family
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setInMemoryCompaction(MemoryCompactionPolicy value) {
    getDelegateeForModification().setInMemoryCompaction(value);
    return this;
  }

  @Override
  public KeepDeletedCells getKeepDeletedCells() {
    return delegatee.getKeepDeletedCells();
  }

  /**
   * @param value True if deleted rows should not be collected
   * immediately.
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setKeepDeletedCells(KeepDeletedCells value) {
    getDelegateeForModification().setKeepDeletedCells(value);
    return this;
  }

  /**
   * By default, HBase only consider timestamp in versions. So a previous Delete with higher ts
   * will mask a later Put with lower ts. Set this to true to enable new semantics of versions.
   * We will also consider mvcc in versions. See HBASE-15968 for details.
   */
  @Override
  public boolean isNewVersionBehavior() {
    return delegatee.isNewVersionBehavior();
  }

  public HColumnDescriptor setNewVersionBehavior(boolean newVersionBehavior) {
    getDelegateeForModification().setNewVersionBehavior(newVersionBehavior);
    return this;
  }


  @Override
  public int getTimeToLive() {
    return delegatee.getTimeToLive();
  }

  /**
   * @param value Time-to-live of cell contents, in seconds.
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setTimeToLive(int value) {
    getDelegateeForModification().setTimeToLive(value);
    return this;
  }

  /**
   * @param value Time to live of cell contents, in human readable format
   *                   @see org.apache.hadoop.hbase.util.PrettyPrinter#format(String, Unit)
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setTimeToLive(String value) throws HBaseException {
    getDelegateeForModification().setTimeToLive(value);
    return this;
  }

  @Override
  public int getMinVersions() {
    return delegatee.getMinVersions();
  }

  /**
   * @param value The minimum number of versions to keep.
   * (used when timeToLive is set)
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setMinVersions(int value) {
    getDelegateeForModification().setMinVersions(value);
    return this;
  }

  @Override
  public boolean isBlockCacheEnabled() {
    return delegatee.isBlockCacheEnabled();
  }

  /**
   * @param value True if hfile DATA type blocks should be cached (We always cache
   * INDEX and BLOOM blocks; you cannot turn this off).
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setBlockCacheEnabled(boolean value) {
    getDelegateeForModification().setBlockCacheEnabled(value);
    return this;
  }

  @Override
  public BloomType getBloomFilterType() {
    return delegatee.getBloomFilterType();
  }

  /**
   * @param value bloom filter type
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setBloomFilterType(final BloomType value) {
    getDelegateeForModification().setBloomFilterType(value);
    return this;
  }

  @Override
  public int getScope() {
    return delegatee.getScope();
  }

 /**
  * @param value the scope tag
  * @return this (for chained invocation)
  */
  public HColumnDescriptor setScope(int value) {
    getDelegateeForModification().setScope(value);
    return this;
  }

  @Override
  public boolean isCacheDataOnWrite() {
    return delegatee.isCacheDataOnWrite();
  }

  /**
   * @param value true if we should cache data blocks on write
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setCacheDataOnWrite(boolean value) {
    getDelegateeForModification().setCacheDataOnWrite(value);
    return this;
  }

  /**
   * This is a noop call from HBase 2.0 onwards
   *
   * @return this (for chained invocation)
   * @deprecated Since 2.0 and will be removed in 3.0 with out any replacement. Caching data in on
   *             heap Cache, when there are both on heap LRU Cache and Bucket Cache will no longer
   *             be supported from 2.0.
   */
  @Deprecated
  public HColumnDescriptor setCacheDataInL1(boolean value) {
    return this;
  }

  @Override
  public boolean isCacheIndexesOnWrite() {
    return delegatee.isCacheIndexesOnWrite();
  }

  /**
   * @param value true if we should cache index blocks on write
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setCacheIndexesOnWrite(boolean value) {
    getDelegateeForModification().setCacheIndexesOnWrite(value);
    return this;
  }

  @Override
  public boolean isCacheBloomsOnWrite() {
    return delegatee.isCacheBloomsOnWrite();
  }

  /**
   * @param value true if we should cache bloomfilter blocks on write
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setCacheBloomsOnWrite(boolean value) {
    getDelegateeForModification().setCacheBloomsOnWrite(value);
    return this;
  }

  @Override
  public boolean isEvictBlocksOnClose() {
    return delegatee.isEvictBlocksOnClose();
  }

  /**
   * @param value true if we should evict cached blocks from the blockcache on
   * close
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setEvictBlocksOnClose(boolean value) {
    getDelegateeForModification().setEvictBlocksOnClose(value);
    return this;
  }

  @Override
  public boolean isPrefetchBlocksOnOpen() {
    return delegatee.isPrefetchBlocksOnOpen();
  }

  /**
   * @param value true if we should prefetch blocks into the blockcache on open
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setPrefetchBlocksOnOpen(boolean value) {
    getDelegateeForModification().setPrefetchBlocksOnOpen(value);
    return this;
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return delegatee.toString();
  }

  /**
   * @return Column family descriptor with only the customized attributes.
   */
  @Override
  public String toStringCustomizedValues() {
    return delegatee.toStringCustomizedValues();
  }

  public static Unit getUnit(String key) {
    return ColumnFamilyDescriptorBuilder.getUnit(key);
  }

  public static Map<String, String> getDefaultValues() {
    return ColumnFamilyDescriptorBuilder.getDefaultValues();
  }

  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof HColumnDescriptor) {
      return delegatee.equals(((HColumnDescriptor) obj).delegatee);
    }
    return false;
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    return delegatee.hashCode();
  }

  @Override
  public int compareTo(HColumnDescriptor other) {
    return COMPARATOR.compare(this, other);
  }

  /**
   * @return This instance serialized with pb with pb magic prefix
   * @see #parseFrom(byte[])
   */
  public byte[] toByteArray() {
    return ColumnFamilyDescriptorBuilder.toByteArray(delegatee);
  }

  /**
   * @param bytes A pb serialized {@link HColumnDescriptor} instance with pb magic prefix
   * @return An instance of {@link HColumnDescriptor} made from <code>bytes</code>
   * @throws DeserializationException
   * @see #toByteArray()
   */
  public static HColumnDescriptor parseFrom(final byte [] bytes) throws DeserializationException {
    ColumnFamilyDescriptor desc = ColumnFamilyDescriptorBuilder.parseFrom(bytes);
    if (desc instanceof ModifyableColumnFamilyDescriptor) {
      return new HColumnDescriptor((ModifyableColumnFamilyDescriptor) desc);
    } else {
      return new HColumnDescriptor(new ModifyableColumnFamilyDescriptor(desc));
    }
  }

  @Override
  public String getConfigurationValue(String key) {
    return delegatee.getConfigurationValue(key);
  }

  @Override
  public Map<String, String> getConfiguration() {
    return delegatee.getConfiguration();
  }

  /**
   * Setter for storing a configuration setting.
   * @param key Config key. Same as XML config key e.g. hbase.something.or.other.
   * @param value String value. If null, removes the configuration.
   */
  public HColumnDescriptor setConfiguration(String key, String value) {
    getDelegateeForModification().setConfiguration(key, value);
    return this;
  }

  /**
   * Remove a configuration setting represented by the key.
   */
  public void removeConfiguration(final String key) {
    getDelegateeForModification().removeConfiguration(key);
  }

  @Override
  public String getEncryptionType() {
    return delegatee.getEncryptionType();
  }

  /**
   * Set the encryption algorithm for use with this family
   * @param value
   */
  public HColumnDescriptor setEncryptionType(String value) {
    getDelegateeForModification().setEncryptionType(value);
    return this;
  }

  @Override
  public byte[] getEncryptionKey() {
    return delegatee.getEncryptionKey();
  }

  /** Set the raw crypto key attribute for the family */
  public HColumnDescriptor setEncryptionKey(byte[] value) {
    getDelegateeForModification().setEncryptionKey(value);
    return this;
  }

  @Override
  public long getMobThreshold() {
    return delegatee.getMobThreshold();
  }

  /**
   * Sets the mob threshold of the family.
   * @param value The mob threshold.
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setMobThreshold(long value) {
    getDelegateeForModification().setMobThreshold(value);
    return this;
  }

  @Override
  public boolean isMobEnabled() {
    return delegatee.isMobEnabled();
  }

  /**
   * Enables the mob for the family.
   * @param value Whether to enable the mob for the family.
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setMobEnabled(boolean value) {
    getDelegateeForModification().setMobEnabled(value);
    return this;
  }

  @Override
  public MobCompactPartitionPolicy getMobCompactPartitionPolicy() {
    return delegatee.getMobCompactPartitionPolicy();
  }

  /**
   * Set the mob compact partition policy for the family.
   * @param value policy type
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setMobCompactPartitionPolicy(MobCompactPartitionPolicy value) {
    getDelegateeForModification().setMobCompactPartitionPolicy(value);
    return this;
  }

  @Override
  public short getDFSReplication() {
    return delegatee.getDFSReplication();
  }

  /**
   * Set the replication factor to hfile(s) belonging to this family
   * @param value number of replicas the blocks(s) belonging to this CF should have, or
   *          {@link #DEFAULT_DFS_REPLICATION} for the default replication factor set in the
   *          filesystem
   * @return this (for chained invocation)
   */
  public HColumnDescriptor setDFSReplication(short value) {
    getDelegateeForModification().setDFSReplication(value);
    return this;
  }

  @Override
  public String getStoragePolicy() {
    return delegatee.getStoragePolicy();
  }

  /**
   * Set the storage policy for use with this family
   * @param value the policy to set, valid setting includes: <i>"LAZY_PERSIST"</i>,
   *          <i>"ALL_SSD"</i>, <i>"ONE_SSD"</i>, <i>"HOT"</i>, <i>"WARM"</i>, <i>"COLD"</i>
   */
  public HColumnDescriptor setStoragePolicy(String value) {
    getDelegateeForModification().setStoragePolicy(value);
    return this;
  }

  @Override
  public Bytes getValue(Bytes key) {
    return delegatee.getValue(key);
  }

  protected ModifyableColumnFamilyDescriptor getDelegateeForModification() {
    return delegatee;
  }
}
