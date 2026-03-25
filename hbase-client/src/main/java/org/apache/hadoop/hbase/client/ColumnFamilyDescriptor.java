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
package org.apache.hadoop.hbase.client;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.MemoryCompactionPolicy;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.encoding.IndexBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * An ColumnFamilyDescriptor contains information about a column family such as the number of
 * versions, compression settings, etc. It is used as input when creating a table or adding a
 * column. To construct a new instance, use the {@link ColumnFamilyDescriptorBuilder} methods
 * @since 2.0.0
 */
@InterfaceAudience.Public
public interface ColumnFamilyDescriptor {

  @InterfaceAudience.Private
  static final Comparator<ColumnFamilyDescriptor> COMPARATOR =
    (ColumnFamilyDescriptor lhs, ColumnFamilyDescriptor rhs) -> {
      int result = Bytes.compareTo(lhs.getName(), rhs.getName());
      if (result != 0) {
        return result;
      }
      // punt on comparison for ordering, just calculate difference.
      result = lhs.getValues().hashCode() - rhs.getValues().hashCode();
      if (result != 0) {
        return result;
      }
      return lhs.getConfiguration().hashCode() - rhs.getConfiguration().hashCode();
    };

  static final Bytes REPLICATION_SCOPE_BYTES =
    new Bytes(Bytes.toBytes(ColumnFamilyDescriptorBuilder.REPLICATION_SCOPE));

  @InterfaceAudience.Private
  static final Comparator<ColumnFamilyDescriptor> COMPARATOR_IGNORE_REPLICATION =
    (ColumnFamilyDescriptor lcf, ColumnFamilyDescriptor rcf) -> {
      int result = Bytes.compareTo(lcf.getName(), rcf.getName());
      if (result != 0) {
        return result;
      }
      // ColumnFamilyDescriptor.getValues is a immutable map, so copy it and remove
      // REPLICATION_SCOPE_BYTES
      Map<Bytes, Bytes> lValues = new HashMap<>();
      lValues.putAll(lcf.getValues());
      lValues.remove(REPLICATION_SCOPE_BYTES);
      Map<Bytes, Bytes> rValues = new HashMap<>();
      rValues.putAll(rcf.getValues());
      rValues.remove(REPLICATION_SCOPE_BYTES);
      result = lValues.hashCode() - rValues.hashCode();
      if (result != 0) {
        return result;
      }
      return lcf.getConfiguration().hashCode() - rcf.getConfiguration().hashCode();
    };

  /** Returns The storefile/hfile blocksize for this column family. */
  int getBlocksize();

  /** Returns bloom filter type used for new StoreFiles in ColumnFamily */
  BloomType getBloomFilterType();

  /** Returns Compression type setting. */
  Compression.Algorithm getCompactionCompressionType();

  /** Returns Compression type setting for major compactions. */
  Compression.Algorithm getMajorCompactionCompressionType();

  /** Returns Compression type setting for minor compactions. */
  Compression.Algorithm getMinorCompactionCompressionType();

  /** Returns Compression type setting. */
  Compression.Algorithm getCompressionType();

  /** Returns an unmodifiable map. */
  Map<String, String> getConfiguration();

  /** Returns accessing the configuration value by key. */
  String getConfigurationValue(String key);

  /** Returns replication factor set for this CF */
  short getDFSReplication();

  /** Returns the data block encoding algorithm used in block cache and optionally on disk */
  DataBlockEncoding getDataBlockEncoding();

  /** Return the index block encoding algorithm used in block cache and optionally on disk */
  IndexBlockEncoding getIndexBlockEncoding();

  /** Returns Return the raw crypto key attribute for the family, or null if not set */
  byte[] getEncryptionKey();

  /** Returns the encryption key namespace for this family */
  String getEncryptionKeyNamespace();

  /** Returns Return the encryption algorithm in use by this family */
  String getEncryptionType();

  /**
   * Returns in-memory compaction policy if set for the cf. Returns null if no policy is set for for
   * this column family
   */
  MemoryCompactionPolicy getInMemoryCompaction();

  /** Returns return the KeepDeletedCells */
  KeepDeletedCells getKeepDeletedCells();

  /** Returns maximum number of versions */
  int getMaxVersions();

  /** Returns The minimum number of versions to keep. */
  int getMinVersions();

  /**
   * Get the mob compact partition policy for this family
   */
  MobCompactPartitionPolicy getMobCompactPartitionPolicy();

  /**
   * Gets the mob threshold of the family. If the size of a cell value is larger than this
   * threshold, it's regarded as a mob. The default threshold is 1024*100(100K)B.
   * @return The mob threshold.
   */
  long getMobThreshold();

  /** Returns a copy of Name of this column family */
  byte[] getName();

  /** Returns Name of this column family */
  String getNameAsString();

  /** Returns the scope tag */
  int getScope();

  /**
   * Not using {@code enum} here because HDFS is not using {@code enum} for storage policy, see
   * org.apache.hadoop.hdfs.server.blockmanagement.BlockStoragePolicySuite for more details.
   * @return Return the storage policy in use by this family
   */
  String getStoragePolicy();

  /** Returns Time-to-live of cell contents, in seconds. */
  int getTimeToLive();

  /**
   * Get a configuration value.
   * @param key The key.
   * @return A clone value. Null if no mapping for the key
   */
  Bytes getValue(Bytes key);

  /**
   * Get a configuration value.
   * @param key The key.
   * @return A clone value. Null if no mapping for the key
   */
  String getValue(String key);

  /**
   * Get a configuration value.
   * @param key The key.
   * @return A clone value. Null if no mapping for the key
   */
  byte[] getValue(byte[] key);

  /**
   * Get all configuration values. It clone all bytes of all elements.
   * @return All values
   */
  Map<Bytes, Bytes> getValues();

  /**
   * Returns True if hfile DATA type blocks should be cached (You cannot disable caching of INDEX
   * and BLOOM type blocks).
   */
  boolean isBlockCacheEnabled();

  /** Returns true if we should cache bloomfilter blocks on write */
  boolean isCacheBloomsOnWrite();

  /** Returns true if we should cache data blocks on write */
  boolean isCacheDataOnWrite();

  /** Returns true if we should cache index blocks on write */
  boolean isCacheIndexesOnWrite();

  /**
   * Returns Whether KV tags should be compressed along with DataBlockEncoding. When no
   * DataBlockEncoding is been used, this is having no effect.
   */
  boolean isCompressTags();

  /** Returns true if we should evict cached blocks from the blockcache on close */
  boolean isEvictBlocksOnClose();

  /**
   * Returns True if we are to favor keeping all values for this column family in the HRegionServer
   * cache.
   */
  boolean isInMemory();

  /**
   * Gets whether the mob is enabled for the family.
   * @return True if the mob is enabled for the family.
   */
  boolean isMobEnabled();

  /** Returns true if we should prefetch blocks into the blockcache on open */
  boolean isPrefetchBlocksOnOpen();

  /** Returns Column family descriptor with only the customized attributes. */
  String toStringCustomizedValues();

  /**
   * By default, HBase only consider timestamp in versions. So a previous Delete with higher ts will
   * mask a later Put with lower ts. Set this to true to enable new semantics of versions. We will
   * also consider mvcc in versions. See HBASE-15968 for details.
   */
  boolean isNewVersionBehavior();
}
