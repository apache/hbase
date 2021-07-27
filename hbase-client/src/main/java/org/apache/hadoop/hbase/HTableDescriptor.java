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
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.CoprocessorDescriptor;
import org.apache.hadoop.hbase.client.CoprocessorDescriptorBuilder;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder.ModifyableTableDescriptor;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * HTableDescriptor contains the details about an HBase table  such as the descriptors of
 * all the column families, is the table a catalog table, <code> hbase:meta </code>,
 * if the table is read only, the maximum size of the memstore,
 * when the region split should occur, coprocessors associated with it etc...
 * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
 *             Use {@link TableDescriptorBuilder} to build {@link HTableDescriptor}.
 */
@Deprecated
@InterfaceAudience.Public
public class HTableDescriptor implements TableDescriptor, Comparable<HTableDescriptor> {
  public static final String SPLIT_POLICY = TableDescriptorBuilder.SPLIT_POLICY;
  public static final String MAX_FILESIZE = TableDescriptorBuilder.MAX_FILESIZE;
  public static final String OWNER = TableDescriptorBuilder.OWNER;
  public static final Bytes OWNER_KEY = TableDescriptorBuilder.OWNER_KEY;
  public static final String READONLY = TableDescriptorBuilder.READONLY;
  public static final String COMPACTION_ENABLED = TableDescriptorBuilder.COMPACTION_ENABLED;
  public static final boolean DEFAULT_NORMALIZATION_ENABLED = TableDescriptorBuilder.DEFAULT_NORMALIZATION_ENABLED;
  public static final String SPLIT_ENABLED = TableDescriptorBuilder.SPLIT_ENABLED;
  public static final String MERGE_ENABLED = TableDescriptorBuilder.MERGE_ENABLED;
  public static final String MEMSTORE_FLUSHSIZE = TableDescriptorBuilder.MEMSTORE_FLUSHSIZE;
  public static final String FLUSH_POLICY = TableDescriptorBuilder.FLUSH_POLICY;
  public static final String IS_ROOT = "IS_ROOT";
  public static final String IS_META = TableDescriptorBuilder.IS_META;
  public static final String DURABILITY = TableDescriptorBuilder.DURABILITY;
  public static final String REGION_REPLICATION = TableDescriptorBuilder.REGION_REPLICATION;
  public static final String REGION_MEMSTORE_REPLICATION = TableDescriptorBuilder.REGION_MEMSTORE_REPLICATION;
  public static final String NORMALIZATION_ENABLED = TableDescriptorBuilder.NORMALIZATION_ENABLED;
  public static final String NORMALIZER_TARGET_REGION_COUNT =
      TableDescriptorBuilder.NORMALIZER_TARGET_REGION_COUNT;
  public static final String NORMALIZER_TARGET_REGION_SIZE =
      TableDescriptorBuilder.NORMALIZER_TARGET_REGION_SIZE;
  public static final String PRIORITY = TableDescriptorBuilder.PRIORITY;
  public static final boolean DEFAULT_READONLY = TableDescriptorBuilder.DEFAULT_READONLY;
  public static final boolean DEFAULT_COMPACTION_ENABLED = TableDescriptorBuilder.DEFAULT_COMPACTION_ENABLED;
  public static final long DEFAULT_MEMSTORE_FLUSH_SIZE = TableDescriptorBuilder.DEFAULT_MEMSTORE_FLUSH_SIZE;
  public static final int DEFAULT_REGION_REPLICATION = TableDescriptorBuilder.DEFAULT_REGION_REPLICATION;
  public static final boolean DEFAULT_REGION_MEMSTORE_REPLICATION = TableDescriptorBuilder.DEFAULT_REGION_MEMSTORE_REPLICATION;
  protected final ModifyableTableDescriptor delegatee;

  /**
   * Construct a table descriptor specifying a TableName object
   * @param name Table name.
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-174">HADOOP-1581 HBASE: (HBASE-174) Un-openable tablename bug</a>
   */
  public HTableDescriptor(final TableName name) {
    this(new ModifyableTableDescriptor(name));
  }

  /**
   * Construct a table descriptor by cloning the descriptor passed as a parameter.
   * <p>
   * Makes a deep copy of the supplied descriptor.
   * Can make a modifiable descriptor from an ImmutableHTableDescriptor.
   * @param desc The descriptor.
   */
  public HTableDescriptor(final HTableDescriptor desc) {
    this(desc, true);
  }

  protected HTableDescriptor(final HTableDescriptor desc, boolean deepClone) {
    this(deepClone ? new ModifyableTableDescriptor(desc.getTableName(), desc)
      : desc.delegatee);
  }

  public HTableDescriptor(final TableDescriptor desc) {
    this(new ModifyableTableDescriptor(desc.getTableName(), desc));
  }

  /**
   * Construct a table descriptor by cloning the descriptor passed as a parameter
   * but using a different table name.
   * <p>
   * Makes a deep copy of the supplied descriptor.
   * Can make a modifiable descriptor from an ImmutableHTableDescriptor.
   * @param name Table name.
   * @param desc The descriptor.
   */
  public HTableDescriptor(final TableName name, final HTableDescriptor desc) {
    this(new ModifyableTableDescriptor(name, desc));
  }

  protected HTableDescriptor(ModifyableTableDescriptor delegatee) {
    this.delegatee = delegatee;
  }

  /**
   * This is vestigial API. It will be removed in 3.0.
   *
   * @return always return the false
   */
  public boolean isRootRegion() {
    return false;
  }

  /**
   * Checks if this table is <code> hbase:meta </code>
   * region.
   *
   * @return true if this table is <code> hbase:meta </code>
   * region
   */
  @Override
  public boolean isMetaRegion() {
    return delegatee.isMetaRegion();
  }

  /**
   * Checks if the table is a <code>hbase:meta</code> table
   *
   * @return true if table is <code> hbase:meta </code> region.
   */
  @Override
  public boolean isMetaTable() {
    return delegatee.isMetaTable();
  }

  /**
   * @return Getter for fetching an unmodifiable map.
   */
  @Override
  public Map<Bytes, Bytes> getValues() {
    return delegatee.getValues();
  }

  /**
   * Setter for storing metadata as a (key, value) pair in map
   *
   * @param key The key.
   * @param value The value. If null, removes the setting.
   */
  public HTableDescriptor setValue(byte[] key, byte[] value) {
    getDelegateeForModification().setValue(key, value);
    return this;
  }

  /*
   * Setter for storing metadata as a (key, value) pair in map
   *
   * @param key The key.
   * @param value The value. If null, removes the setting.
   */
  public HTableDescriptor setValue(final Bytes key, final Bytes value) {
    getDelegateeForModification().setValue(key, value);
    return this;
  }

  /**
   * Setter for storing metadata as a (key, value) pair in map
   *
   * @param key The key.
   * @param value The value. If null, removes the setting.
   */
  public HTableDescriptor setValue(String key, String value) {
    getDelegateeForModification().setValue(key, value);
    return this;
  }

  /**
   * Remove metadata represented by the key from the map
   *
   * @param key Key whose key and value we're to remove from HTableDescriptor
   * parameters.
   */
  public void remove(final String key) {
    getDelegateeForModification().removeValue(Bytes.toBytes(key));
  }

  /**
   * Remove metadata represented by the key from the map
   *
   * @param key Key whose key and value we're to remove from HTableDescriptor
   * parameters.
   */
  public void remove(Bytes key) {
    getDelegateeForModification().removeValue(key);
  }

  /**
   * Remove metadata represented by the key from the map
   *
   * @param key Key whose key and value we're to remove from HTableDescriptor
   * parameters.
   */
  public void remove(final byte [] key) {
    getDelegateeForModification().removeValue(key);
  }

  /**
   * Check if the readOnly flag of the table is set. If the readOnly flag is
   * set then the contents of the table can only be read from but not modified.
   *
   * @return true if all columns in the table should be read only
   */
  @Override
  public boolean isReadOnly() {
    return delegatee.isReadOnly();
  }

  /**
   * Setting the table as read only sets all the columns in the table as read
   * only. By default all tables are modifiable, but if the readOnly flag is
   * set to true then the contents of the table can only be read but not modified.
   *
   * @param readOnly True if all of the columns in the table should be read
   * only.
   */
  public HTableDescriptor setReadOnly(final boolean readOnly) {
    getDelegateeForModification().setReadOnly(readOnly);
    return this;
  }

  /**
   * Check if the compaction enable flag of the table is true. If flag is
   * false then no minor/major compactions will be done in real.
   *
   * @return true if table compaction enabled
   */
  @Override
  public boolean isCompactionEnabled() {
    return delegatee.isCompactionEnabled();
  }

  /**
   * Setting the table compaction enable flag.
   *
   * @param isEnable True if enable compaction.
   */
  public HTableDescriptor setCompactionEnabled(final boolean isEnable) {
    getDelegateeForModification().setCompactionEnabled(isEnable);
    return this;
  }

  /**
   * Check if the region split enable flag of the table is true. If flag is
   * false then no split will be done.
   *
   * @return true if table region split enabled
   */
  @Override
  public boolean isSplitEnabled() {
    return delegatee.isSplitEnabled();
  }

  /**
   * Setting the table region split enable flag.
   *
   * @param isEnable True if enable split.
   */
  public HTableDescriptor setSplitEnabled(final boolean isEnable) {
    getDelegateeForModification().setSplitEnabled(isEnable);
    return this;
  }


  /**
   * Check if the region merge enable flag of the table is true. If flag is
   * false then no merge will be done.
   *
   * @return true if table region merge enabled
   */
  @Override
  public boolean isMergeEnabled() {
    return delegatee.isMergeEnabled();
  }

  /**
   * Setting the table region merge enable flag.
   *
   * @param isEnable True if enable merge.
   */
  public HTableDescriptor setMergeEnabled(final boolean isEnable) {
    getDelegateeForModification().setMergeEnabled(isEnable);
    return this;
  }

  /**
   * Check if normalization enable flag of the table is true. If flag is
   * false then no region normalizer won't attempt to normalize this table.
   *
   * @return true if region normalization is enabled for this table
   */
  @Override
  public boolean isNormalizationEnabled() {
    return delegatee.isNormalizationEnabled();
  }

  /**
   * Setting the table normalization enable flag.
   *
   * @param isEnable True if enable normalization.
   */
  public HTableDescriptor setNormalizationEnabled(final boolean isEnable) {
    getDelegateeForModification().setNormalizationEnabled(isEnable);
    return this;
  }

  @Override
  public int getNormalizerTargetRegionCount() {
    return getDelegateeForModification().getNormalizerTargetRegionCount();
  }

  public HTableDescriptor setNormalizerTargetRegionCount(final int regionCount) {
    getDelegateeForModification().setNormalizerTargetRegionCount(regionCount);
    return this;
  }

  @Override
  public long getNormalizerTargetRegionSize() {
    return getDelegateeForModification().getNormalizerTargetRegionSize();
  }

  public HTableDescriptor setNormalizerTargetRegionSize(final long regionSize) {
    getDelegateeForModification().setNormalizerTargetRegionSize(regionSize);
    return this;
  }

  /**
   * Sets the {@link Durability} setting for the table. This defaults to Durability.USE_DEFAULT.
   * @param durability enum value
   */
  public HTableDescriptor setDurability(Durability durability) {
    getDelegateeForModification().setDurability(durability);
    return this;
  }

  /**
   * Returns the durability setting for the table.
   * @return durability setting for the table.
   */
  @Override
  public Durability getDurability() {
    return delegatee.getDurability();
  }

  /**
   * Get the name of the table
   *
   * @return TableName
   */
  @Override
  public TableName getTableName() {
    return delegatee.getTableName();
  }

  /**
   * Get the name of the table as a String
   *
   * @return name of table as a String
   */
  public String getNameAsString() {
    return delegatee.getTableName().getNameAsString();
  }

  /**
   * This sets the class associated with the region split policy which
   * determines when a region split should occur.  The class used by
   * default is defined in org.apache.hadoop.hbase.regionserver.RegionSplitPolicy
   * @param clazz the class name
   */
  public HTableDescriptor setRegionSplitPolicyClassName(String clazz) {
    getDelegateeForModification().setRegionSplitPolicyClassName(clazz);
    return this;
  }

  /**
   * This gets the class associated with the region split policy which
   * determines when a region split should occur.  The class used by
   * default is defined in org.apache.hadoop.hbase.regionserver.RegionSplitPolicy
   *
   * @return the class name of the region split policy for this table.
   * If this returns null, the default split policy is used.
   */
  @Override
   public String getRegionSplitPolicyClassName() {
    return delegatee.getRegionSplitPolicyClassName();
  }

  /**
   * Returns the maximum size upto which a region can grow to after which a region
   * split is triggered. The region size is represented by the size of the biggest
   * store file in that region.
   *
   * @return max hregion size for table, -1 if not set.
   *
   * @see #setMaxFileSize(long)
   */
   @Override
  public long getMaxFileSize() {
    return delegatee.getMaxFileSize();
  }

  /**
   * Sets the maximum size upto which a region can grow to after which a region
   * split is triggered. The region size is represented by the size of the biggest
   * store file in that region, i.e. If the biggest store file grows beyond the
   * maxFileSize, then the region split is triggered. This defaults to a value of
   * 256 MB.
   * <p>
   * This is not an absolute value and might vary. Assume that a single row exceeds
   * the maxFileSize then the storeFileSize will be greater than maxFileSize since
   * a single row cannot be split across multiple regions
   * </p>
   *
   * @param maxFileSize The maximum file size that a store file can grow to
   * before a split is triggered.
   */
  public HTableDescriptor setMaxFileSize(long maxFileSize) {
    getDelegateeForModification().setMaxFileSize(maxFileSize);
    return this;
  }

  /**
   * Returns the size of the memstore after which a flush to filesystem is triggered.
   *
   * @return memory cache flush size for each hregion, -1 if not set.
   *
   * @see #setMemStoreFlushSize(long)
   */
  @Override
  public long getMemStoreFlushSize() {
    return delegatee.getMemStoreFlushSize();
  }

  /**
   * Represents the maximum size of the memstore after which the contents of the
   * memstore are flushed to the filesystem. This defaults to a size of 64 MB.
   *
   * @param memstoreFlushSize memory cache flush size for each hregion
   */
  public HTableDescriptor setMemStoreFlushSize(long memstoreFlushSize) {
    getDelegateeForModification().setMemStoreFlushSize(memstoreFlushSize);
    return this;
  }

  /**
   * This sets the class associated with the flush policy which determines determines the stores
   * need to be flushed when flushing a region. The class used by default is defined in
   * org.apache.hadoop.hbase.regionserver.FlushPolicy.
   * @param clazz the class name
   */
  public HTableDescriptor setFlushPolicyClassName(String clazz) {
    getDelegateeForModification().setFlushPolicyClassName(clazz);
    return this;
  }

  /**
   * This gets the class associated with the flush policy which determines the stores need to be
   * flushed when flushing a region. The class used by default is defined in
   * org.apache.hadoop.hbase.regionserver.FlushPolicy.
   * @return the class name of the flush policy for this table. If this returns null, the default
   *         flush policy is used.
   */
  @Override
  public String getFlushPolicyClassName() {
    return delegatee.getFlushPolicyClassName();
  }

  /**
   * Adds a column family.
   * For the updating purpose please use {@link #modifyFamily(HColumnDescriptor)} instead.
   * @param family HColumnDescriptor of family to add.
   */
  public HTableDescriptor addFamily(final HColumnDescriptor family) {
    getDelegateeForModification().setColumnFamily(family);
    return this;
  }

  /**
   * Modifies the existing column family.
   * @param family HColumnDescriptor of family to update
   * @return this (for chained invocation)
   */
  public HTableDescriptor modifyFamily(final HColumnDescriptor family) {
    getDelegateeForModification().modifyColumnFamily(family);
    return this;
  }

  /**
   * Checks to see if this table contains the given column family
   * @param familyName Family name or column name.
   * @return true if the table contains the specified family name
   */
  public boolean hasFamily(final byte [] familyName) {
    return delegatee.hasColumnFamily(familyName);
  }

  /**
   * @return Name of this table and then a map of all of the column family
   * descriptors.
   * @see #getNameAsString()
   */
  @Override
  public String toString() {
    return delegatee.toString();
  }

  /**
   * @return Name of this table and then a map of all of the column family
   * descriptors (with only the non-default column family attributes)
   */
  @Override
  public String toStringCustomizedValues() {
    return delegatee.toStringCustomizedValues();
  }

  /**
   * @return map of all table attributes formatted into string.
   */
  public String toStringTableAttributes() {
   return delegatee.toStringTableAttributes();
  }

  /**
   * Compare the contents of the descriptor with another one passed as a parameter.
   * Checks if the obj passed is an instance of HTableDescriptor, if yes then the
   * contents of the descriptors are compared.
   *
   * @return true if the contents of the the two descriptors exactly match
   *
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof HTableDescriptor) {
      return delegatee.equals(((HTableDescriptor) obj).delegatee);
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

  // Comparable

  /**
   * Compares the descriptor with another descriptor which is passed as a parameter.
   * This compares the content of the two descriptors and not the reference.
   *
   * @return 0 if the contents of the descriptors are exactly matching,
   *         1 if there is a mismatch in the contents
   */
  @Override
  public int compareTo(final HTableDescriptor other) {
    return TableDescriptor.COMPARATOR.compare(this, other);
  }

  /**
   * Returns an unmodifiable collection of all the {@link HColumnDescriptor}
   * of all the column families of the table.
   * @deprecated since 2.0.0 and will be removed in 3.0.0. Use {@link #getColumnFamilies()} instead.
   * @return Immutable collection of {@link HColumnDescriptor} of all the
   * column families.
   * @see #getColumnFamilies()
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-18008">HBASE-18008</a>
   */
  @Deprecated
  public Collection<HColumnDescriptor> getFamilies() {
    return Stream.of(delegatee.getColumnFamilies())
            .map(this::toHColumnDescriptor)
            .collect(Collectors.toList());
  }

  /**
   * Returns the configured replicas per region
   */
  @Override
  public int getRegionReplication() {
    return delegatee.getRegionReplication();
  }

  /**
   * Sets the number of replicas per region.
   * @param regionReplication the replication factor per region
   */
  public HTableDescriptor setRegionReplication(int regionReplication) {
    getDelegateeForModification().setRegionReplication(regionReplication);
    return this;
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   *             Use {@link #hasRegionMemStoreReplication()} instead
   */
  @Deprecated
  public boolean hasRegionMemstoreReplication() {
    return hasRegionMemStoreReplication();
  }

  /**
   * @return true if the read-replicas memstore replication is enabled.
   */
  @Override
  public boolean hasRegionMemStoreReplication() {
    return delegatee.hasRegionMemStoreReplication();
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   *             Use {@link #setRegionMemStoreReplication(boolean)} instead
   */
  @Deprecated
  public HTableDescriptor setRegionMemstoreReplication(boolean memstoreReplication) {
    return setRegionMemStoreReplication(memstoreReplication);
  }

  /**
   * Enable or Disable the memstore replication from the primary region to the replicas.
   * The replication will be used only for meta operations (e.g. flush, compaction, ...)
   *
   * @param memstoreReplication true if the new data written to the primary region
   *                                 should be replicated.
   *                            false if the secondaries can tollerate to have new
   *                                  data only when the primary flushes the memstore.
   */
  public HTableDescriptor setRegionMemStoreReplication(boolean memstoreReplication) {
    getDelegateeForModification().setRegionMemStoreReplication(memstoreReplication);
    return this;
  }

  public HTableDescriptor setPriority(int priority) {
    getDelegateeForModification().setPriority(priority);
    return this;
  }

  @Override
  public int getPriority() {
    return delegatee.getPriority();
  }

  /**
   * Returns all the column family names of the current table. The map of
   * HTableDescriptor contains mapping of family name to HColumnDescriptors.
   * This returns all the keys of the family map which represents the column
   * family names of the table.
   *
   * @return Immutable sorted set of the keys of the families.
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             (<a href="https://issues.apache.org/jira/browse/HBASE-18008">HBASE-18008</a>).
   *             Use {@link #getColumnFamilyNames()}.
   */
  @Deprecated
  public Set<byte[]> getFamiliesKeys() {
    return delegatee.getColumnFamilyNames();
  }

  /**
   * Returns the count of the column families of the table.
   *
   * @return Count of column families of the table
   */
  @Override
  public int getColumnFamilyCount() {
    return delegatee.getColumnFamilyCount();
  }

  /**
   * Returns an array all the {@link HColumnDescriptor} of the column families
   * of the table.
   *
   * @return Array of all the HColumnDescriptors of the current table
   * @deprecated since 2.0.0 and will be removed in 3.0.0.
   * @see #getFamilies()
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-18008">HBASE-18008</a>
   */
  @Deprecated
  @Override
  public HColumnDescriptor[] getColumnFamilies() {
    return Stream.of(delegatee.getColumnFamilies())
            .map(this::toHColumnDescriptor)
            .toArray(size -> new HColumnDescriptor[size]);
  }

  /**
   * Returns the HColumnDescriptor for a specific column family with name as
   * specified by the parameter column.
   * @param column Column family name
   * @return Column descriptor for the passed family name or the family on
   * passed in column.
   * @deprecated since 2.0.0 and will be removed in 3.0.0. Use {@link #getColumnFamily(byte[])}
   *   instead.
   * @see #getColumnFamily(byte[])
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-18008">HBASE-18008</a>
   */
  @Deprecated
  public HColumnDescriptor getFamily(final byte[] column) {
    return toHColumnDescriptor(delegatee.getColumnFamily(column));
  }


  /**
   * Removes the HColumnDescriptor with name specified by the parameter column
   * from the table descriptor
   *
   * @param column Name of the column family to be removed.
   * @return Column descriptor for the passed family name or the family on
   * passed in column.
   */
  public HColumnDescriptor removeFamily(final byte [] column) {
    return toHColumnDescriptor(getDelegateeForModification().removeColumnFamily(column));
  }

  /**
   * Return a HColumnDescriptor for user to keep the compatibility as much as possible.
   * @param desc read-only ColumnFamilyDescriptor
   * @return The older implementation of ColumnFamilyDescriptor
   */
  protected HColumnDescriptor toHColumnDescriptor(ColumnFamilyDescriptor desc) {
    if (desc == null) {
      return null;
    } else if (desc instanceof ModifyableColumnFamilyDescriptor) {
      return new HColumnDescriptor((ModifyableColumnFamilyDescriptor) desc);
    } else if (desc instanceof HColumnDescriptor) {
      return (HColumnDescriptor) desc;
    } else {
      return new HColumnDescriptor(new ModifyableColumnFamilyDescriptor(desc));
    }
  }

  /**
   * Add a table coprocessor to this table. The coprocessor
   * type must be org.apache.hadoop.hbase.coprocessor.RegionCoprocessor.
   * It won't check if the class can be loaded or not.
   * Whether a coprocessor is loadable or not will be determined when
   * a region is opened.
   * @param className Full class name.
   * @throws IOException
   */
  public HTableDescriptor addCoprocessor(String className) throws IOException {
    getDelegateeForModification().setCoprocessor(className);
    return this;
  }

  /**
   * Add a table coprocessor to this table. The coprocessor
   * type must be org.apache.hadoop.hbase.coprocessor.RegionCoprocessor.
   * It won't check if the class can be loaded or not.
   * Whether a coprocessor is loadable or not will be determined when
   * a region is opened.
   * @param jarFilePath Path of the jar file. If it's null, the class will be
   * loaded from default classloader.
   * @param className Full class name.
   * @param priority Priority
   * @param kvs Arbitrary key-value parameter pairs passed into the coprocessor.
   * @throws IOException
   */
  public HTableDescriptor addCoprocessor(String className, Path jarFilePath,
                             int priority, final Map<String, String> kvs)
  throws IOException {
    getDelegateeForModification().setCoprocessor(
      CoprocessorDescriptorBuilder.newBuilder(className)
        .setJarPath(jarFilePath == null ? null : jarFilePath.toString())
        .setPriority(priority)
        .setProperties(kvs == null ? Collections.emptyMap() : kvs)
        .build());
    return this;
  }

  /**
   * Add a table coprocessor to this table. The coprocessor
   * type must be org.apache.hadoop.hbase.coprocessor.RegionCoprocessor.
   * It won't check if the class can be loaded or not.
   * Whether a coprocessor is loadable or not will be determined when
   * a region is opened.
   * @param specStr The Coprocessor specification all in in one String formatted so matches
   * {@link HConstants#CP_HTD_ATTR_VALUE_PATTERN}
   * @throws IOException
   */
  public HTableDescriptor addCoprocessorWithSpec(final String specStr) throws IOException {
    getDelegateeForModification().setCoprocessorWithSpec(specStr);
    return this;
  }

  /**
   * Check if the table has an attached co-processor represented by the name className
   *
   * @param classNameToMatch - Class name of the co-processor
   * @return true of the table has a co-processor className
   */
  @Override
  public boolean hasCoprocessor(String classNameToMatch) {
    return delegatee.hasCoprocessor(classNameToMatch);
  }

  @Override
  public Collection<CoprocessorDescriptor> getCoprocessorDescriptors() {
    return delegatee.getCoprocessorDescriptors();
  }

  /**
   * Remove a coprocessor from those set on the table
   * @param className Class name of the co-processor
   */
  public void removeCoprocessor(String className) {
    getDelegateeForModification().removeCoprocessor(className);
  }

  public final static String NAMESPACE_FAMILY_INFO = TableDescriptorBuilder.NAMESPACE_FAMILY_INFO;
  public final static byte[] NAMESPACE_FAMILY_INFO_BYTES = TableDescriptorBuilder.NAMESPACE_FAMILY_INFO_BYTES;
  public final static byte[] NAMESPACE_COL_DESC_BYTES = TableDescriptorBuilder.NAMESPACE_COL_DESC_BYTES;

  /** Table descriptor for namespace table */
  public static final HTableDescriptor NAMESPACE_TABLEDESC
    = new HTableDescriptor(TableDescriptorBuilder.NAMESPACE_TABLEDESC);

  /**
   * @deprecated since 0.94.1
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-6188">HBASE-6188</a>
   */
  @Deprecated
  public HTableDescriptor setOwner(User owner) {
    getDelegateeForModification().setOwner(owner);
    return this;
  }

  /**
   * @deprecated since 0.94.1
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-6188">HBASE-6188</a>
   */
  // used by admin.rb:alter(table_name,*args) to update owner.
  @Deprecated
  public HTableDescriptor setOwnerString(String ownerString) {
    getDelegateeForModification().setOwnerString(ownerString);
    return this;
  }

  /**
   * @deprecated since 0.94.1
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-6188">HBASE-6188</a>
   */
  @Override
  @Deprecated
  public String getOwnerString() {
    return delegatee.getOwnerString();
  }

  /**
   * @return This instance serialized with pb with pb magic prefix
   * @see #parseFrom(byte[])
   */
  public byte[] toByteArray() {
    return TableDescriptorBuilder.toByteArray(delegatee);
  }

  /**
   * @param bytes A pb serialized {@link HTableDescriptor} instance with pb magic prefix
   * @return An instance of {@link HTableDescriptor} made from <code>bytes</code>
   * @throws DeserializationException
   * @throws IOException
   * @see #toByteArray()
   */
  public static HTableDescriptor parseFrom(final byte [] bytes)
  throws DeserializationException, IOException {
    TableDescriptor desc = TableDescriptorBuilder.parseFrom(bytes);
    if (desc instanceof ModifyableTableDescriptor) {
      return new HTableDescriptor((ModifyableTableDescriptor) desc);
    } else {
      return new HTableDescriptor(desc);
    }
  }

  /**
   * Getter for accessing the configuration value by key
   */
  public String getConfigurationValue(String key) {
    return delegatee.getValue(key);
  }

  /**
   * Getter for fetching an unmodifiable map.
   */
  public Map<String, String> getConfiguration() {
    return delegatee.getValues().entrySet().stream()
            .collect(Collectors.toMap(
                    e -> Bytes.toString(e.getKey().get(), e.getKey().getOffset(), e.getKey().getLength()),
                    e -> Bytes.toString(e.getValue().get(), e.getValue().getOffset(), e.getValue().getLength())
            ));
  }

  /**
   * Setter for storing a configuration setting in map.
   * @param key Config key. Same as XML config key e.g. hbase.something.or.other.
   * @param value String value. If null, removes the setting.
   */
  public HTableDescriptor setConfiguration(String key, String value) {
    getDelegateeForModification().setValue(key, value);
    return this;
  }

  /**
   * Remove a config setting represented by the key from the map
   */
  public void removeConfiguration(final String key) {
    getDelegateeForModification().removeValue(Bytes.toBytes(key));
  }

  @Override
  public Bytes getValue(Bytes key) {
    return delegatee.getValue(key);
  }

  @Override
  public String getValue(String key) {
    return delegatee.getValue(key);
  }

  @Override
  public byte[] getValue(byte[] key) {
    return delegatee.getValue(key);
  }

  @Override
  public Set<byte[]> getColumnFamilyNames() {
    return delegatee.getColumnFamilyNames();
  }

  @Override
  public boolean hasColumnFamily(byte[] name) {
    return delegatee.hasColumnFamily(name);
  }

  @Override
  public ColumnFamilyDescriptor getColumnFamily(byte[] name) {
    return delegatee.getColumnFamily(name);
  }

  protected ModifyableTableDescriptor getDelegateeForModification() {
    return delegatee;
  }

  @Override
  public Optional<String> getRegionServerGroup() {
    return delegatee.getRegionServerGroup();
  }
}
