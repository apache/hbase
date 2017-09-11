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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.stream.Stream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @since 2.0.0
 */
@InterfaceAudience.Public
public class TableDescriptorBuilder {
  public static final Log LOG = LogFactory.getLog(TableDescriptorBuilder.class);
  @InterfaceAudience.Private
  public static final String SPLIT_POLICY = "SPLIT_POLICY";
  private static final Bytes SPLIT_POLICY_KEY = new Bytes(Bytes.toBytes(SPLIT_POLICY));
  /**
   * Used by HBase Shell interface to access this metadata
   * attribute which denotes the maximum size of the store file after which a
   * region split occurs.
   */
  @InterfaceAudience.Private
  public static final String MAX_FILESIZE = "MAX_FILESIZE";
  private static final Bytes MAX_FILESIZE_KEY
          = new Bytes(Bytes.toBytes(MAX_FILESIZE));

  @InterfaceAudience.Private
  public static final String OWNER = "OWNER";
  @InterfaceAudience.Private
  public static final Bytes OWNER_KEY
          = new Bytes(Bytes.toBytes(OWNER));

  /**
   * Used by rest interface to access this metadata attribute
   * which denotes if the table is Read Only.
   */
  @InterfaceAudience.Private
  public static final String READONLY = "READONLY";
  private static final Bytes READONLY_KEY
          = new Bytes(Bytes.toBytes(READONLY));

  /**
   * Used by HBase Shell interface to access this metadata
   * attribute which denotes if the table is compaction enabled.
   */
  @InterfaceAudience.Private
  public static final String COMPACTION_ENABLED = "COMPACTION_ENABLED";
  private static final Bytes COMPACTION_ENABLED_KEY
          = new Bytes(Bytes.toBytes(COMPACTION_ENABLED));

  /**
   * Used by HBase Shell interface to access this metadata
   * attribute which represents the maximum size of the memstore after which its
   * contents are flushed onto the disk.
   */
  @InterfaceAudience.Private
  public static final String MEMSTORE_FLUSHSIZE = "MEMSTORE_FLUSHSIZE";
  private static final Bytes MEMSTORE_FLUSHSIZE_KEY
          = new Bytes(Bytes.toBytes(MEMSTORE_FLUSHSIZE));

  @InterfaceAudience.Private
  public static final String FLUSH_POLICY = "FLUSH_POLICY";
  private static final Bytes FLUSH_POLICY_KEY = new Bytes(Bytes.toBytes(FLUSH_POLICY));
  /**
   * Used by rest interface to access this metadata attribute
   * which denotes if it is a catalog table, either <code> hbase:meta </code>.
   */
  @InterfaceAudience.Private
  public static final String IS_META = "IS_META";
  private static final Bytes IS_META_KEY
          = new Bytes(Bytes.toBytes(IS_META));

  /**
   * {@link Durability} setting for the table.
   */
  @InterfaceAudience.Private
  public static final String DURABILITY = "DURABILITY";
  private static final Bytes DURABILITY_KEY
          = new Bytes(Bytes.toBytes("DURABILITY"));

  /**
   * The number of region replicas for the table.
   */
  @InterfaceAudience.Private
  public static final String REGION_REPLICATION = "REGION_REPLICATION";
  private static final Bytes REGION_REPLICATION_KEY
          = new Bytes(Bytes.toBytes(REGION_REPLICATION));

  /**
   * The flag to indicate whether or not the memstore should be
   * replicated for read-replicas (CONSISTENCY =&gt; TIMELINE).
   */
  @InterfaceAudience.Private
  public static final String REGION_MEMSTORE_REPLICATION = "REGION_MEMSTORE_REPLICATION";
  private static final Bytes REGION_MEMSTORE_REPLICATION_KEY
          = new Bytes(Bytes.toBytes(REGION_MEMSTORE_REPLICATION));

  private static final Bytes REGION_REPLICA_WAIT_FOR_PRIMARY_FLUSH_CONF_KEY
          = new Bytes(Bytes.toBytes(RegionReplicaUtil.REGION_REPLICA_WAIT_FOR_PRIMARY_FLUSH_CONF_KEY));
  /**
   * Used by shell/rest interface to access this metadata
   * attribute which denotes if the table should be treated by region
   * normalizer.
   */
  @InterfaceAudience.Private
  public static final String NORMALIZATION_ENABLED = "NORMALIZATION_ENABLED";
  private static final Bytes NORMALIZATION_ENABLED_KEY
          = new Bytes(Bytes.toBytes(NORMALIZATION_ENABLED));

  /**
   * Default durability for HTD is USE_DEFAULT, which defaults to HBase-global
   * default value
   */
  private static final Durability DEFAULT_DURABLITY = Durability.USE_DEFAULT;

  @InterfaceAudience.Private
  public static final String PRIORITY = "PRIORITY";
  private static final Bytes PRIORITY_KEY
          = new Bytes(Bytes.toBytes(PRIORITY));

  /**
   * Relative priority of the table used for rpc scheduling
   */
  private static final int DEFAULT_PRIORITY = HConstants.NORMAL_QOS;

  /**
   * Constant that denotes whether the table is READONLY by default and is false
   */
  public static final boolean DEFAULT_READONLY = false;

  /**
   * Constant that denotes whether the table is compaction enabled by default
   */
  public static final boolean DEFAULT_COMPACTION_ENABLED = true;

  /**
   * Constant that denotes whether the table is normalized by default.
   */
  public static final boolean DEFAULT_NORMALIZATION_ENABLED = false;

  /**
   * Constant that denotes the maximum default size of the memstore after which
   * the contents are flushed to the store files
   */
  public static final long DEFAULT_MEMSTORE_FLUSH_SIZE = 1024 * 1024 * 128L;

  public static final int DEFAULT_REGION_REPLICATION = 1;

  public static final boolean DEFAULT_REGION_MEMSTORE_REPLICATION = true;

  private final static Map<String, String> DEFAULT_VALUES = new HashMap<>();
  private final static Set<Bytes> RESERVED_KEYWORDS = new HashSet<>();

  static {
    DEFAULT_VALUES.put(MAX_FILESIZE,
            String.valueOf(HConstants.DEFAULT_MAX_FILE_SIZE));
    DEFAULT_VALUES.put(READONLY, String.valueOf(DEFAULT_READONLY));
    DEFAULT_VALUES.put(MEMSTORE_FLUSHSIZE,
            String.valueOf(DEFAULT_MEMSTORE_FLUSH_SIZE));
    DEFAULT_VALUES.put(DURABILITY, DEFAULT_DURABLITY.name()); //use the enum name
    DEFAULT_VALUES.put(REGION_REPLICATION, String.valueOf(DEFAULT_REGION_REPLICATION));
    DEFAULT_VALUES.put(NORMALIZATION_ENABLED, String.valueOf(DEFAULT_NORMALIZATION_ENABLED));
    DEFAULT_VALUES.put(PRIORITY, String.valueOf(DEFAULT_PRIORITY));
    DEFAULT_VALUES.keySet().stream()
            .map(s -> new Bytes(Bytes.toBytes(s))).forEach(RESERVED_KEYWORDS::add);
    RESERVED_KEYWORDS.add(IS_META_KEY);
  }

  @InterfaceAudience.Private
  public final static String NAMESPACE_FAMILY_INFO = "info";
  @InterfaceAudience.Private
  public final static byte[] NAMESPACE_FAMILY_INFO_BYTES = Bytes.toBytes(NAMESPACE_FAMILY_INFO);
  @InterfaceAudience.Private
  public final static byte[] NAMESPACE_COL_DESC_BYTES = Bytes.toBytes("d");

  /**
   * Table descriptor for namespace table
   */
  public static final TableDescriptor NAMESPACE_TABLEDESC
    = TableDescriptorBuilder.newBuilder(TableName.NAMESPACE_TABLE_NAME)
                            .addColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(NAMESPACE_FAMILY_INFO_BYTES)
                              // Ten is arbitrary number.  Keep versions to help debugging.
                              .setMaxVersions(10)
                              .setInMemory(true)
                              .setBlocksize(8 * 1024)
                              .setScope(HConstants.REPLICATION_SCOPE_LOCAL)
                              // Enable cache of data blocks in L1 if more than one caching tier deployed:
                              // e.g. if using CombinedBlockCache (BucketCache).
                              .setCacheDataInL1(true)
                              .build())
                            .build();
  private final ModifyableTableDescriptor desc;

  /**
   * @param desc The table descriptor to serialize
   * @return This instance serialized with pb with pb magic prefix
   */
  public static byte[] toByteArray(TableDescriptor desc) {
    if (desc instanceof ModifyableTableDescriptor) {
      return ((ModifyableTableDescriptor) desc).toByteArray();
    }
    return new ModifyableTableDescriptor(desc).toByteArray();
  }

  /**
   * The input should be created by {@link #toByteArray}.
   * @param pbBytes A pb serialized TableDescriptor instance with pb magic prefix
   * @return This instance serialized with pb with pb magic prefix
   * @throws org.apache.hadoop.hbase.exceptions.DeserializationException
   */
  public static TableDescriptor parseFrom(byte[] pbBytes) throws DeserializationException {
    return ModifyableTableDescriptor.parseFrom(pbBytes);
  }

  public static TableDescriptorBuilder newBuilder(final TableName name) {
    return new TableDescriptorBuilder(name);
  }

  public static TableDescriptor copy(TableDescriptor desc) {
    return new ModifyableTableDescriptor(desc);
  }

  public static TableDescriptor copy(TableName name, TableDescriptor desc) {
    return new ModifyableTableDescriptor(name, desc);
  }

  /**
   * Copy all values, families, and name from the input.
   * @param desc The desciptor to copy
   * @return A clone of input
   */
  public static TableDescriptorBuilder newBuilder(final TableDescriptor desc) {
    return new TableDescriptorBuilder(desc);
  }

  private TableDescriptorBuilder(final TableName name) {
    this.desc = new ModifyableTableDescriptor(name);
  }

  private TableDescriptorBuilder(final TableDescriptor desc) {
    this.desc = new ModifyableTableDescriptor(desc);
  }

  public TableDescriptorBuilder addCoprocessor(String className) throws IOException {
    return addCoprocessor(className, null, Coprocessor.PRIORITY_USER, null);
  }

  public TableDescriptorBuilder addCoprocessor(String className, Path jarFilePath,
          int priority, final Map<String, String> kvs) throws IOException {
    desc.addCoprocessor(className, jarFilePath, priority, kvs);
    return this;
  }

  public TableDescriptorBuilder addCoprocessorWithSpec(final String specStr) throws IOException {
    desc.addCoprocessorWithSpec(specStr);
    return this;
  }

  public TableDescriptorBuilder addColumnFamily(final ColumnFamilyDescriptor family) {
    desc.addColumnFamily(family);
    return this;
  }

  public TableDescriptorBuilder modifyColumnFamily(final ColumnFamilyDescriptor family) {
    desc.modifyColumnFamily(family);
    return this;
  }

  public TableDescriptorBuilder removeValue(Bytes key) {
    desc.removeValue(key);
    return this;
  }

  public TableDescriptorBuilder removeValue(byte[] key) {
    desc.removeValue(key);
    return this;
  }

  public TableDescriptorBuilder removeColumnFamily(final byte[] name) {
    desc.removeColumnFamily(name);
    return this;
  }

  public TableDescriptorBuilder removeCoprocessor(String className) {
    desc.removeCoprocessor(className);
    return this;
  }

  public TableDescriptorBuilder setCompactionEnabled(final boolean isEnable) {
    desc.setCompactionEnabled(isEnable);
    return this;
  }

  public TableDescriptorBuilder setDurability(Durability durability) {
    desc.setDurability(durability);
    return this;
  }

  public TableDescriptorBuilder setFlushPolicyClassName(String clazz) {
    desc.setFlushPolicyClassName(clazz);
    return this;
  }

  public TableDescriptorBuilder setMaxFileSize(long maxFileSize) {
    desc.setMaxFileSize(maxFileSize);
    return this;
  }

  public TableDescriptorBuilder setMemStoreFlushSize(long memstoreFlushSize) {
    desc.setMemStoreFlushSize(memstoreFlushSize);
    return this;
  }

  public TableDescriptorBuilder setNormalizationEnabled(final boolean isEnable) {
    desc.setNormalizationEnabled(isEnable);
    return this;
  }

  @Deprecated
  public TableDescriptorBuilder setOwner(User owner) {
    desc.setOwner(owner);
    return this;
  }

  @Deprecated
  public TableDescriptorBuilder setOwnerString(String ownerString) {
    desc.setOwnerString(ownerString);
    return this;
  }

  public TableDescriptorBuilder setPriority(int priority) {
    desc.setPriority(priority);
    return this;
  }

  public TableDescriptorBuilder setReadOnly(final boolean readOnly) {
    desc.setReadOnly(readOnly);
    return this;
  }

  public TableDescriptorBuilder setRegionMemstoreReplication(boolean memstoreReplication) {
    desc.setRegionMemstoreReplication(memstoreReplication);
    return this;
  }

  public TableDescriptorBuilder setRegionReplication(int regionReplication) {
    desc.setRegionReplication(regionReplication);
    return this;
  }

  public TableDescriptorBuilder setRegionSplitPolicyClassName(String clazz) {
    desc.setRegionSplitPolicyClassName(clazz);
    return this;
  }

  public TableDescriptorBuilder setValue(final String key, final String value) {
    desc.setValue(key, value);
    return this;
  }

  public TableDescriptorBuilder setValue(final Bytes key, final Bytes value) {
    desc.setValue(key, value);
    return this;
  }

  public TableDescriptorBuilder setValue(final byte[] key, final byte[] value) {
    desc.setValue(key, value);
    return this;
  }

  public TableDescriptor build() {
    return new ModifyableTableDescriptor(desc);
  }

  /**
   * TODO: make this private after removing the HTableDescriptor
   */
  @InterfaceAudience.Private
  public static class ModifyableTableDescriptor
          implements TableDescriptor, Comparable<ModifyableTableDescriptor> {

    private final TableName name;

    /**
     * A map which holds the metadata information of the table. This metadata
     * includes values like IS_META, SPLIT_POLICY, MAX_FILE_SIZE,
     * READONLY, MEMSTORE_FLUSHSIZE etc...
     */
    private final Map<Bytes, Bytes> values = new HashMap<>();

    /**
     * Maps column family name to the respective FamilyDescriptors
     */
    private final Map<byte[], ColumnFamilyDescriptor> families
            = new TreeMap<>(Bytes.BYTES_RAWCOMPARATOR);

    /**
     * Construct a table descriptor specifying a TableName object
     *
     * @param name Table name.
     * TODO: make this private after removing the HTableDescriptor
     */
    @InterfaceAudience.Private
    public ModifyableTableDescriptor(final TableName name) {
      this(name, Collections.EMPTY_LIST, Collections.EMPTY_MAP);
    }

    private ModifyableTableDescriptor(final TableDescriptor desc) {
      this(desc.getTableName(), Arrays.asList(desc.getColumnFamilies()), desc.getValues());
    }

    /**
     * Construct a table descriptor by cloning the descriptor passed as a
     * parameter.
     * <p>
     * Makes a deep copy of the supplied descriptor.
     * @param name The new name
     * @param desc The descriptor.
     * TODO: make this private after removing the HTableDescriptor
     */
    @InterfaceAudience.Private
    @Deprecated // only used by HTableDescriptor. remove this method if HTD is removed
    public ModifyableTableDescriptor(final TableName name, final TableDescriptor desc) {
      this(name, Arrays.asList(desc.getColumnFamilies()), desc.getValues());
    }

    private ModifyableTableDescriptor(final TableName name, final Collection<ColumnFamilyDescriptor> families,
            Map<Bytes, Bytes> values) {
      this.name = name;
      families.forEach(c -> this.families.put(c.getName(), ColumnFamilyDescriptorBuilder.copy(c)));
      this.values.putAll(values);
      this.values.put(IS_META_KEY,
        new Bytes(Bytes.toBytes(Boolean.toString(name.equals(TableName.META_TABLE_NAME)))));
    }

    /**
     * Checks if this table is <code> hbase:meta </code> region.
     *
     * @return true if this table is <code> hbase:meta </code> region
     */
    @Override
    public boolean isMetaRegion() {
      return getOrDefault(IS_META_KEY, Boolean::valueOf, false);
    }

    /**
     * Checks if the table is a <code>hbase:meta</code> table
     *
     * @return true if table is <code> hbase:meta </code> region.
     */
    @Override
    public boolean isMetaTable() {
      return isMetaRegion();
    }

    @Override
    public Bytes getValue(Bytes key) {
      Bytes rval = values.get(key);
      return rval == null ? null : new Bytes(rval.copyBytes());
    }

    @Override
    public String getValue(String key) {
      Bytes rval = values.get(new Bytes(Bytes.toBytes(key)));
      return rval == null ? null : Bytes.toString(rval.get(), rval.getOffset(), rval.getLength());
    }

    @Override
    public byte[] getValue(byte[] key) {
      Bytes value = values.get(new Bytes(key));
      return value == null ? null : value.copyBytes();
    }

    private <T> T getOrDefault(Bytes key, Function<String, T> function, T defaultValue) {
      Bytes value = values.get(key);
      if (value == null) {
        return defaultValue;
      } else {
        return function.apply(Bytes.toString(value.get(), value.getOffset(), value.getLength()));
      }
    }

    /**
     * Getter for fetching an unmodifiable {@link #values} map.
     *
     * @return unmodifiable map {@link #values}.
     * @see #values
     */
    @Override
    public Map<Bytes, Bytes> getValues() {
      // shallow pointer copy
      return Collections.unmodifiableMap(values);
    }

    /**
     * Setter for storing metadata as a (key, value) pair in {@link #values} map
     *
     * @param key The key.
     * @param value The value. If null, removes the setting.
     * @return the modifyable TD
     * @see #values
     */
    public ModifyableTableDescriptor setValue(byte[] key, byte[] value) {
      return setValue(toBytesOrNull(key, v -> v),
              toBytesOrNull(value, v -> v));
    }

    public ModifyableTableDescriptor setValue(String key, String value) {
      return setValue(toBytesOrNull(key, Bytes::toBytes),
              toBytesOrNull(value, Bytes::toBytes));
    }

    /*
     * @param key The key.
     * @param value The value. If null, removes the setting.
     */
    private ModifyableTableDescriptor setValue(final Bytes key,
            final String value) {
      return setValue(key, toBytesOrNull(value, Bytes::toBytes));
    }

    /*
     * Setter for storing metadata as a (key, value) pair in {@link #values} map
     *
     * @param key The key.
     * @param value The value. If null, removes the setting.
     */
    public ModifyableTableDescriptor setValue(final Bytes key, final Bytes value) {
      if (value == null) {
        values.remove(key);
      } else {
        values.put(key, value);
      }
      return this;
    }

    private static <T> Bytes toBytesOrNull(T t, Function<T, byte[]> f) {
      if (t == null) {
        return null;
      } else {
        return new Bytes(f.apply(t));
      }
    }

    /**
     * Remove metadata represented by the key from the {@link #values} map
     *
     * @param key Key whose key and value we're to remove from TableDescriptor
     * parameters.
     * @return the modifyable TD
     */
    public ModifyableTableDescriptor removeValue(Bytes key) {
      return setValue(key, (Bytes) null);
    }

    /**
     * Remove metadata represented by the key from the {@link #values} map
     *
     * @param key Key whose key and value we're to remove from TableDescriptor
     * parameters.
     * @return the modifyable TD
     */
    public ModifyableTableDescriptor removeValue(final byte[] key) {
      return removeValue(new Bytes(key));
    }

    /**
     * Check if the readOnly flag of the table is set. If the readOnly flag is
     * set then the contents of the table can only be read from but not
     * modified.
     *
     * @return true if all columns in the table should be read only
     */
    @Override
    public boolean isReadOnly() {
      return getOrDefault(READONLY_KEY, Boolean::valueOf, DEFAULT_READONLY);
    }

    /**
     * Setting the table as read only sets all the columns in the table as read
     * only. By default all tables are modifiable, but if the readOnly flag is
     * set to true then the contents of the table can only be read but not
     * modified.
     *
     * @param readOnly True if all of the columns in the table should be read
     * only.
     * @return the modifyable TD
     */
    public ModifyableTableDescriptor setReadOnly(final boolean readOnly) {
      return setValue(READONLY_KEY, Boolean.toString(readOnly));
    }

    /**
     * Check if the compaction enable flag of the table is true. If flag is
     * false then no minor/major compactions will be done in real.
     *
     * @return true if table compaction enabled
     */
    @Override
    public boolean isCompactionEnabled() {
      return getOrDefault(COMPACTION_ENABLED_KEY, Boolean::valueOf, DEFAULT_COMPACTION_ENABLED);
    }

    /**
     * Setting the table compaction enable flag.
     *
     * @param isEnable True if enable compaction.
     * @return the modifyable TD
     */
    public ModifyableTableDescriptor setCompactionEnabled(final boolean isEnable) {
      return setValue(COMPACTION_ENABLED_KEY, Boolean.toString(isEnable));
    }

    /**
     * Check if normalization enable flag of the table is true. If flag is false
     * then no region normalizer won't attempt to normalize this table.
     *
     * @return true if region normalization is enabled for this table
     */
    @Override
    public boolean isNormalizationEnabled() {
      return getOrDefault(NORMALIZATION_ENABLED_KEY, Boolean::valueOf, DEFAULT_NORMALIZATION_ENABLED);
    }

    /**
     * Setting the table normalization enable flag.
     *
     * @param isEnable True if enable normalization.
     * @return the modifyable TD
     */
    public ModifyableTableDescriptor setNormalizationEnabled(final boolean isEnable) {
      return setValue(NORMALIZATION_ENABLED_KEY, Boolean.toString(isEnable));
    }

    /**
     * Sets the {@link Durability} setting for the table. This defaults to
     * Durability.USE_DEFAULT.
     *
     * @param durability enum value
     * @return the modifyable TD
     */
    public ModifyableTableDescriptor setDurability(Durability durability) {
      return setValue(DURABILITY_KEY, durability.name());
    }

    /**
     * Returns the durability setting for the table.
     *
     * @return durability setting for the table.
     */
    @Override
    public Durability getDurability() {
      return getOrDefault(DURABILITY_KEY, Durability::valueOf, DEFAULT_DURABLITY);
    }

    /**
     * Get the name of the table
     *
     * @return TableName
     */
    @Override
    public TableName getTableName() {
      return name;
    }

    /**
     * This sets the class associated with the region split policy which
     * determines when a region split should occur. The class used by default is
     * defined in org.apache.hadoop.hbase.regionserver.RegionSplitPolicy
     *
     * @param clazz the class name
     * @return the modifyable TD
     */
    public ModifyableTableDescriptor setRegionSplitPolicyClassName(String clazz) {
      return setValue(SPLIT_POLICY_KEY, clazz);
    }

    /**
     * This gets the class associated with the region split policy which
     * determines when a region split should occur. The class used by default is
     * defined in org.apache.hadoop.hbase.regionserver.RegionSplitPolicy
     *
     * @return the class name of the region split policy for this table. If this
     * returns null, the default split policy is used.
     */
    @Override
    public String getRegionSplitPolicyClassName() {
      return getOrDefault(SPLIT_POLICY_KEY, Function.identity(), null);
    }

    /**
     * Returns the maximum size upto which a region can grow to after which a
     * region split is triggered. The region size is represented by the size of
     * the biggest store file in that region.
     *
     * @return max hregion size for table, -1 if not set.
     *
     * @see #setMaxFileSize(long)
     */
    @Override
    public long getMaxFileSize() {
      return getOrDefault(MAX_FILESIZE_KEY, Long::valueOf, (long) -1);
    }

    /**
     * Sets the maximum size upto which a region can grow to after which a
     * region split is triggered. The region size is represented by the size of
     * the biggest store file in that region, i.e. If the biggest store file
     * grows beyond the maxFileSize, then the region split is triggered. This
     * defaults to a value of 256 MB.
     * <p>
     * This is not an absolute value and might vary. Assume that a single row
     * exceeds the maxFileSize then the storeFileSize will be greater than
     * maxFileSize since a single row cannot be split across multiple regions
     * </p>
     *
     * @param maxFileSize The maximum file size that a store file can grow to
     * before a split is triggered.
     * @return the modifyable TD
     */
    public ModifyableTableDescriptor setMaxFileSize(long maxFileSize) {
      return setValue(MAX_FILESIZE_KEY, Long.toString(maxFileSize));
    }

    /**
     * Returns the size of the memstore after which a flush to filesystem is
     * triggered.
     *
     * @return memory cache flush size for each hregion, -1 if not set.
     *
     * @see #setMemStoreFlushSize(long)
     */
    @Override
    public long getMemStoreFlushSize() {
      return getOrDefault(MEMSTORE_FLUSHSIZE_KEY, Long::valueOf, (long) -1);
    }

    /**
     * Represents the maximum size of the memstore after which the contents of
     * the memstore are flushed to the filesystem. This defaults to a size of 64
     * MB.
     *
     * @param memstoreFlushSize memory cache flush size for each hregion
     * @return the modifyable TD
     */
    public ModifyableTableDescriptor setMemStoreFlushSize(long memstoreFlushSize) {
      return setValue(MEMSTORE_FLUSHSIZE_KEY, Long.toString(memstoreFlushSize));
    }

    /**
     * This sets the class associated with the flush policy which determines
     * determines the stores need to be flushed when flushing a region. The
     * class used by default is defined in
     * org.apache.hadoop.hbase.regionserver.FlushPolicy.
     *
     * @param clazz the class name
     * @return the modifyable TD
     */
    public ModifyableTableDescriptor setFlushPolicyClassName(String clazz) {
      return setValue(FLUSH_POLICY_KEY, clazz);
    }

    /**
     * This gets the class associated with the flush policy which determines the
     * stores need to be flushed when flushing a region. The class used by
     * default is defined in org.apache.hadoop.hbase.regionserver.FlushPolicy.
     *
     * @return the class name of the flush policy for this table. If this
     * returns null, the default flush policy is used.
     */
    @Override
    public String getFlushPolicyClassName() {
      return getOrDefault(FLUSH_POLICY_KEY, Function.identity(), null);
    }

    /**
     * Adds a column family. For the updating purpose please use
     * {@link #modifyColumnFamily(ColumnFamilyDescriptor)} instead.
     *
     * @param family to add.
     * @return the modifyable TD
     */
    public ModifyableTableDescriptor addColumnFamily(final ColumnFamilyDescriptor family) {
      if (family.getName() == null || family.getName().length <= 0) {
        throw new IllegalArgumentException("Family name cannot be null or empty");
      }
      if (hasColumnFamily(family.getName())) {
        throw new IllegalArgumentException("Family '"
                + family.getNameAsString() + "' already exists so cannot be added");
      }
      return putColumnFamily(family);
    }

    /**
     * Modifies the existing column family.
     *
     * @param family to update
     * @return this (for chained invocation)
     */
    public ModifyableTableDescriptor modifyColumnFamily(final ColumnFamilyDescriptor family) {
      if (family.getName() == null || family.getName().length <= 0) {
        throw new IllegalArgumentException("Family name cannot be null or empty");
      }
      if (!hasColumnFamily(family.getName())) {
        throw new IllegalArgumentException("Column family '" + family.getNameAsString()
                + "' does not exist");
      }
      return putColumnFamily(family);
    }

    private ModifyableTableDescriptor putColumnFamily(ColumnFamilyDescriptor family) {
      families.put(family.getName(), family);
      return this;
    }

    /**
     * Checks to see if this table contains the given column family
     *
     * @param familyName Family name or column name.
     * @return true if the table contains the specified family name
     */
    @Override
    public boolean hasColumnFamily(final byte[] familyName) {
      return families.containsKey(familyName);
    }

    /**
     * @return Name of this table and then a map of all of the column family descriptors.
     */
    @Override
    public String toString() {
      StringBuilder s = new StringBuilder();
      s.append('\'').append(Bytes.toString(name.getName())).append('\'');
      s.append(getValues(true));
      families.values().forEach(f -> s.append(", ").append(f));
      return s.toString();
    }

    /**
     * @return Name of this table and then a map of all of the column family
     * descriptors (with only the non-default column family attributes)
     */
    public String toStringCustomizedValues() {
      StringBuilder s = new StringBuilder();
      s.append('\'').append(Bytes.toString(name.getName())).append('\'');
      s.append(getValues(false));
      families.values().forEach(hcd -> s.append(", ").append(hcd.toStringCustomizedValues()));
      return s.toString();
    }

    /**
     * @return map of all table attributes formatted into string.
     */
    public String toStringTableAttributes() {
      return getValues(true).toString();
    }

    private StringBuilder getValues(boolean printDefaults) {
      StringBuilder s = new StringBuilder();

      // step 1: set partitioning and pruning
      Set<Bytes> reservedKeys = new TreeSet<>();
      Set<Bytes> userKeys = new TreeSet<>();
      for (Map.Entry<Bytes, Bytes> entry : values.entrySet()) {
        if (entry.getKey() == null || entry.getKey().get() == null) {
          continue;
        }
        String key = Bytes.toString(entry.getKey().get());
        // in this section, print out reserved keywords + coprocessor info
        if (!RESERVED_KEYWORDS.contains(entry.getKey()) && !key.startsWith("coprocessor$")) {
          userKeys.add(entry.getKey());
          continue;
        }
        // only print out IS_META if true
        String value = Bytes.toString(entry.getValue().get());
        if (key.equalsIgnoreCase(IS_META)) {
          if (Boolean.valueOf(value) == false) {
            continue;
          }
        }
        // see if a reserved key is a default value. may not want to print it out
        if (printDefaults
                || !DEFAULT_VALUES.containsKey(key)
                || !DEFAULT_VALUES.get(key).equalsIgnoreCase(value)) {
          reservedKeys.add(entry.getKey());
        }
      }

      // early exit optimization
      boolean hasAttributes = !reservedKeys.isEmpty() || !userKeys.isEmpty();
      if (!hasAttributes) {
        return s;
      }

      s.append(", {");
      // step 2: printing attributes
      if (hasAttributes) {
        s.append("TABLE_ATTRIBUTES => {");

        // print all reserved keys first
        boolean printCommaForAttr = false;
        for (Bytes k : reservedKeys) {
          String key = Bytes.toString(k.get());
          String value = Bytes.toStringBinary(values.get(k).get());
          if (printCommaForAttr) {
            s.append(", ");
          }
          printCommaForAttr = true;
          s.append(key);
          s.append(" => ");
          s.append('\'').append(value).append('\'');
        }

        if (!userKeys.isEmpty()) {
          // print all non-reserved as a separate subset
          if (printCommaForAttr) {
            s.append(", ");
          }
          s.append(HConstants.METADATA).append(" => ");
          s.append("{");
          boolean printCommaForCfg = false;
          for (Bytes k : userKeys) {
            String key = Bytes.toString(k.get());
            String value = Bytes.toStringBinary(values.get(k).get());
            if (printCommaForCfg) {
              s.append(", ");
            }
            printCommaForCfg = true;
            s.append('\'').append(key).append('\'');
            s.append(" => ");
            s.append('\'').append(value).append('\'');
          }
          s.append("}");
        }
      }

      s.append("}"); // end METHOD
      return s;
    }

    /**
     * Compare the contents of the descriptor with another one passed as a
     * parameter. Checks if the obj passed is an instance of ModifyableTableDescriptor,
     * if yes then the contents of the descriptors are compared.
     *
     * @param obj The object to compare
     * @return true if the contents of the the two descriptors exactly match
     *
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj instanceof ModifyableTableDescriptor) {
        return TableDescriptor.COMPARATOR.compare(this, (ModifyableTableDescriptor) obj) == 0;
      }
      return false;
    }

    /**
     * @return hash code
     */
    @Override
    public int hashCode() {
      int result = this.name.hashCode();
      if (this.families.size() > 0) {
        for (ColumnFamilyDescriptor e : this.families.values()) {
          result ^= e.hashCode();
        }
      }
      result ^= values.hashCode();
      return result;
    }

    // Comparable
    /**
     * Compares the descriptor with another descriptor which is passed as a
     * parameter. This compares the content of the two descriptors and not the
     * reference.
     *
     * @param other The MTD to compare
     * @return 0 if the contents of the descriptors are exactly matching, 1 if
     * there is a mismatch in the contents
     */
    @Override
    public int compareTo(final ModifyableTableDescriptor other) {
      return TableDescriptor.COMPARATOR.compare(this, other);
    }

    @Override
    public ColumnFamilyDescriptor[] getColumnFamilies() {
      return families.values().toArray(new ColumnFamilyDescriptor[families.size()]);
    }

    /**
     * Return true if there are at least one cf whose replication scope is
     * serial.
     */
    @Override
    public boolean hasSerialReplicationScope() {
      return Stream.of(getColumnFamilies())
              .anyMatch(column -> column.getScope() == HConstants.REPLICATION_SCOPE_SERIAL);
    }

    /**
     * Returns the configured replicas per region
     */
    @Override
    public int getRegionReplication() {
      return getOrDefault(REGION_REPLICATION_KEY, Integer::valueOf, DEFAULT_REGION_REPLICATION);
    }

    /**
     * Sets the number of replicas per region.
     *
     * @param regionReplication the replication factor per region
     * @return the modifyable TD
     */
    public ModifyableTableDescriptor setRegionReplication(int regionReplication) {
      return setValue(REGION_REPLICATION_KEY, Integer.toString(regionReplication));
    }

    /**
     * @return true if the read-replicas memstore replication is enabled.
     */
    @Override
    public boolean hasRegionMemstoreReplication() {
      return getOrDefault(REGION_MEMSTORE_REPLICATION_KEY, Boolean::valueOf, DEFAULT_REGION_MEMSTORE_REPLICATION);
    }

    /**
     * Enable or Disable the memstore replication from the primary region to the
     * replicas. The replication will be used only for meta operations (e.g.
     * flush, compaction, ...)
     *
     * @param memstoreReplication true if the new data written to the primary
     * region should be replicated. false if the secondaries can tollerate to
     * have new data only when the primary flushes the memstore.
     * @return the modifyable TD
     */
    public ModifyableTableDescriptor setRegionMemstoreReplication(boolean memstoreReplication) {
      setValue(REGION_MEMSTORE_REPLICATION_KEY, Boolean.toString(memstoreReplication));
      // If the memstore replication is setup, we do not have to wait for observing a flush event
      // from primary before starting to serve reads, because gaps from replication is not applicable
      return setValue(REGION_REPLICA_WAIT_FOR_PRIMARY_FLUSH_CONF_KEY,
              Boolean.toString(memstoreReplication));
    }

    public ModifyableTableDescriptor setPriority(int priority) {
      return setValue(PRIORITY_KEY, Integer.toString(priority));
    }

    @Override
    public int getPriority() {
      return getOrDefault(PRIORITY_KEY, Integer::valueOf, DEFAULT_PRIORITY);
    }

    /**
     * Returns all the column family names of the current table. The map of
     * TableDescriptor contains mapping of family name to ColumnFamilyDescriptor.
     * This returns all the keys of the family map which represents the column
     * family names of the table.
     *
     * @return Immutable sorted set of the keys of the families.
     */
    @Override
    public Set<byte[]> getColumnFamilyNames() {
      return Collections.unmodifiableSet(this.families.keySet());
    }

    /**
     * Returns the ColumnFamilyDescriptor for a specific column family with name as
     * specified by the parameter column.
     *
     * @param column Column family name
     * @return Column descriptor for the passed family name or the family on
     * passed in column.
     */
    @Override
    public ColumnFamilyDescriptor getColumnFamily(final byte[] column) {
      return this.families.get(column);
    }

    /**
     * Removes the ColumnFamilyDescriptor with name specified by the parameter column
     * from the table descriptor
     *
     * @param column Name of the column family to be removed.
     * @return Column descriptor for the passed family name or the family on
     * passed in column.
     */
    public ColumnFamilyDescriptor removeColumnFamily(final byte[] column) {
      return this.families.remove(column);
    }

    /**
     * Add a table coprocessor to this table. The coprocessor type must be
     * org.apache.hadoop.hbase.coprocessor.RegionObserver or Endpoint. It won't
     * check if the class can be loaded or not. Whether a coprocessor is
     * loadable or not will be determined when a region is opened.
     *
     * @param className Full class name.
     * @throws IOException
     * @return the modifyable TD
     */
    public ModifyableTableDescriptor addCoprocessor(String className) throws IOException {
      return addCoprocessor(className, null, Coprocessor.PRIORITY_USER, null);
    }

    /**
     * Add a table coprocessor to this table. The coprocessor type must be
     * org.apache.hadoop.hbase.coprocessor.RegionObserver or Endpoint. It won't
     * check if the class can be loaded or not. Whether a coprocessor is
     * loadable or not will be determined when a region is opened.
     *
     * @param jarFilePath Path of the jar file. If it's null, the class will be
     * loaded from default classloader.
     * @param className Full class name.
     * @param priority Priority
     * @param kvs Arbitrary key-value parameter pairs passed into the
     * coprocessor.
     * @throws IOException
     * @return the modifyable TD
     */
    public ModifyableTableDescriptor addCoprocessor(String className, Path jarFilePath,
            int priority, final Map<String, String> kvs)
            throws IOException {
      checkHasCoprocessor(className);

      // Validate parameter kvs and then add key/values to kvString.
      StringBuilder kvString = new StringBuilder();
      if (kvs != null) {
        for (Map.Entry<String, String> e : kvs.entrySet()) {
          if (!e.getKey().matches(HConstants.CP_HTD_ATTR_VALUE_PARAM_KEY_PATTERN)) {
            throw new IOException("Illegal parameter key = " + e.getKey());
          }
          if (!e.getValue().matches(HConstants.CP_HTD_ATTR_VALUE_PARAM_VALUE_PATTERN)) {
            throw new IOException("Illegal parameter (" + e.getKey()
                    + ") value = " + e.getValue());
          }
          if (kvString.length() != 0) {
            kvString.append(',');
          }
          kvString.append(e.getKey());
          kvString.append('=');
          kvString.append(e.getValue());
        }
      }

      String value = ((jarFilePath == null) ? "" : jarFilePath.toString())
              + "|" + className + "|" + Integer.toString(priority) + "|"
              + kvString.toString();
      return addCoprocessorToMap(value);
    }

    /**
     * Add a table coprocessor to this table. The coprocessor type must be
     * org.apache.hadoop.hbase.coprocessor.RegionObserver or Endpoint. It won't
     * check if the class can be loaded or not. Whether a coprocessor is
     * loadable or not will be determined when a region is opened.
     *
     * @param specStr The Coprocessor specification all in in one String
     * formatted so matches {@link HConstants#CP_HTD_ATTR_VALUE_PATTERN}
     * @throws IOException
     * @return the modifyable TD
     */
    public ModifyableTableDescriptor addCoprocessorWithSpec(final String specStr) throws IOException {
      String className = getCoprocessorClassNameFromSpecStr(specStr);
      if (className == null) {
        throw new IllegalArgumentException("Format does not match "
                + HConstants.CP_HTD_ATTR_VALUE_PATTERN + ": " + specStr);
      }
      checkHasCoprocessor(className);
      return addCoprocessorToMap(specStr);
    }

    private void checkHasCoprocessor(final String className) throws IOException {
      if (hasCoprocessor(className)) {
        throw new IOException("Coprocessor " + className + " already exists.");
      }
    }

    /**
     * Add coprocessor to values Map
     *
     * @param specStr The Coprocessor specification all in in one String
     * formatted so matches {@link HConstants#CP_HTD_ATTR_VALUE_PATTERN}
     * @return Returns <code>this</code>
     */
    private ModifyableTableDescriptor addCoprocessorToMap(final String specStr) {
      if (specStr == null) {
        return this;
      }
      // generate a coprocessor key
      int maxCoprocessorNumber = 0;
      Matcher keyMatcher;
      for (Map.Entry<Bytes, Bytes> e : this.values.entrySet()) {
        keyMatcher = HConstants.CP_HTD_ATTR_KEY_PATTERN.matcher(Bytes.toString(e.getKey().get()));
        if (!keyMatcher.matches()) {
          continue;
        }
        maxCoprocessorNumber = Math.max(Integer.parseInt(keyMatcher.group(1)), maxCoprocessorNumber);
      }
      maxCoprocessorNumber++;
      String key = "coprocessor$" + Integer.toString(maxCoprocessorNumber);
      return setValue(new Bytes(Bytes.toBytes(key)), new Bytes(Bytes.toBytes(specStr)));
    }

    /**
     * Check if the table has an attached co-processor represented by the name
     * className
     *
     * @param classNameToMatch - Class name of the co-processor
     * @return true of the table has a co-processor className
     */
    @Override
    public boolean hasCoprocessor(String classNameToMatch) {
      Matcher keyMatcher;
      for (Map.Entry<Bytes, Bytes> e
              : this.values.entrySet()) {
        keyMatcher
                = HConstants.CP_HTD_ATTR_KEY_PATTERN.matcher(
                        Bytes.toString(e.getKey().get()));
        if (!keyMatcher.matches()) {
          continue;
        }
        String className = getCoprocessorClassNameFromSpecStr(Bytes.toString(e.getValue().get()));
        if (className == null) {
          continue;
        }
        if (className.equals(classNameToMatch.trim())) {
          return true;
        }
      }
      return false;
    }

    /**
     * Return the list of attached co-processor represented by their name
     * className
     *
     * @return The list of co-processors classNames
     */
    @Override
    public List<String> getCoprocessors() {
      List<String> result = new ArrayList<>(this.values.entrySet().size());
      Matcher keyMatcher;
      for (Map.Entry<Bytes, Bytes> e : this.values.entrySet()) {
        keyMatcher = HConstants.CP_HTD_ATTR_KEY_PATTERN.matcher(Bytes.toString(e.getKey().get()));
        if (!keyMatcher.matches()) {
          continue;
        }
        String className = getCoprocessorClassNameFromSpecStr(Bytes.toString(e.getValue().get()));
        if (className == null) {
          continue;
        }
        result.add(className); // classname is the 2nd field
      }
      return result;
    }

    /**
     * @param spec String formatted as per
     * {@link HConstants#CP_HTD_ATTR_VALUE_PATTERN}
     * @return Class parsed from passed in <code>spec</code> or null if no match
     * or classpath found
     */
    private static String getCoprocessorClassNameFromSpecStr(final String spec) {
      Matcher matcher = HConstants.CP_HTD_ATTR_VALUE_PATTERN.matcher(spec);
      // Classname is the 2nd field
      return matcher != null && matcher.matches() ? matcher.group(2).trim() : null;
    }

    /**
     * Remove a coprocessor from those set on the table
     *
     * @param className Class name of the co-processor
     */
    public void removeCoprocessor(String className) {
      Bytes match = null;
      Matcher keyMatcher;
      Matcher valueMatcher;
      for (Map.Entry<Bytes, Bytes> e : this.values
              .entrySet()) {
        keyMatcher = HConstants.CP_HTD_ATTR_KEY_PATTERN.matcher(Bytes.toString(e
                .getKey().get()));
        if (!keyMatcher.matches()) {
          continue;
        }
        valueMatcher = HConstants.CP_HTD_ATTR_VALUE_PATTERN.matcher(Bytes
                .toString(e.getValue().get()));
        if (!valueMatcher.matches()) {
          continue;
        }
        // get className and compare
        String clazz = valueMatcher.group(2).trim(); // classname is the 2nd field
        // remove the CP if it is present
        if (clazz.equals(className.trim())) {
          match = e.getKey();
          break;
        }
      }
      // if we found a match, remove it
      if (match != null) {
        ModifyableTableDescriptor.this.removeValue(match);
      }
    }

    @Deprecated
    public ModifyableTableDescriptor setOwner(User owner) {
      return setOwnerString(owner != null ? owner.getShortName() : null);
    }

    // used by admin.rb:alter(table_name,*args) to update owner.
    @Deprecated
    public ModifyableTableDescriptor setOwnerString(String ownerString) {
      return setValue(OWNER_KEY, ownerString);
    }

    @Override
    @Deprecated
    public String getOwnerString() {
      // Note that every table should have an owner (i.e. should have OWNER_KEY set).
      // hbase:meta should return system user as owner, not null (see
      // MasterFileSystem.java:bootstrap()).
      return getOrDefault(OWNER_KEY, Function.identity(), null);
    }

    /**
     * @return the bytes in pb format
     */
    private byte[] toByteArray() {
      return ProtobufUtil.prependPBMagic(ProtobufUtil.toTableSchema(this).toByteArray());
    }

    /**
     * @param bytes A pb serialized {@link ModifyableTableDescriptor} instance
     * with pb magic prefix
     * @return An instance of {@link ModifyableTableDescriptor} made from
     * <code>bytes</code>
     * @throws DeserializationException
     * @see #toByteArray()
     */
    private static TableDescriptor parseFrom(final byte[] bytes)
            throws DeserializationException {
      if (!ProtobufUtil.isPBMagicPrefix(bytes)) {
        throw new DeserializationException("Expected PB encoded ModifyableTableDescriptor");
      }
      int pblen = ProtobufUtil.lengthOfPBMagic();
      HBaseProtos.TableSchema.Builder builder = HBaseProtos.TableSchema.newBuilder();
      try {
        ProtobufUtil.mergeFrom(builder, bytes, pblen, bytes.length - pblen);
        return ProtobufUtil.toTableDescriptor(builder.build());
      } catch (IOException e) {
        throw new DeserializationException(e);
      }
    }

    @Override
    public int getColumnFamilyCount() {
      return families.size();
    }
  }

}
