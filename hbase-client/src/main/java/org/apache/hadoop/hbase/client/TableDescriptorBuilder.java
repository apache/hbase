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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.regex.Matcher;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Bytes;

@InterfaceAudience.Public
public class TableDescriptorBuilder {

  private static final Log LOG = LogFactory.getLog(TableDescriptorBuilder.class);

  public static final String SPLIT_POLICY = "SPLIT_POLICY";

  /**
   * <em>INTERNAL</em> Used by HBase Shell interface to access this metadata
   * attribute which denotes the maximum size of the store file after which a
   * region split occurs.
   */
  public static final String MAX_FILESIZE = "MAX_FILESIZE";
  private static final Bytes MAX_FILESIZE_KEY
          = new Bytes(Bytes.toBytes(MAX_FILESIZE));

  public static final String OWNER = "OWNER";
  public static final Bytes OWNER_KEY
          = new Bytes(Bytes.toBytes(OWNER));

  /**
   * <em>INTERNAL</em> Used by rest interface to access this metadata attribute
   * which denotes if the table is Read Only.
   */
  public static final String READONLY = "READONLY";
  private static final Bytes READONLY_KEY
          = new Bytes(Bytes.toBytes(READONLY));

  /**
   * <em>INTERNAL</em> Used by HBase Shell interface to access this metadata
   * attribute which denotes if the table is compaction enabled.
   */
  public static final String COMPACTION_ENABLED = "COMPACTION_ENABLED";
  private static final Bytes COMPACTION_ENABLED_KEY
          = new Bytes(Bytes.toBytes(COMPACTION_ENABLED));

  /**
   * <em>INTERNAL</em> Used by HBase Shell interface to access this metadata
   * attribute which represents the maximum size of the memstore after which its
   * contents are flushed onto the disk.
   */
  public static final String MEMSTORE_FLUSHSIZE = "MEMSTORE_FLUSHSIZE";
  private static final Bytes MEMSTORE_FLUSHSIZE_KEY
          = new Bytes(Bytes.toBytes(MEMSTORE_FLUSHSIZE));

  public static final String FLUSH_POLICY = "FLUSH_POLICY";

  /**
   * <em>INTERNAL</em> Used by rest interface to access this metadata attribute
   * which denotes if the table is a -ROOT- region or not.
   */
  public static final String IS_ROOT = "IS_ROOT";
  private static final Bytes IS_ROOT_KEY
          = new Bytes(Bytes.toBytes(IS_ROOT));

  /**
   * <em>INTERNAL</em> Used by rest interface to access this metadata attribute
   * which denotes if it is a catalog table, either <code> hbase:meta </code> or <code> -ROOT-
   * </code>.
   */
  public static final String IS_META = "IS_META";
  private static final Bytes IS_META_KEY
          = new Bytes(Bytes.toBytes(IS_META));

  /**
   * <em>INTERNAL</em> {@link Durability} setting for the table.
   */
  public static final String DURABILITY = "DURABILITY";
  private static final Bytes DURABILITY_KEY
          = new Bytes(Bytes.toBytes("DURABILITY"));

  /**
   * <em>INTERNAL</em> number of region replicas for the table.
   */
  public static final String REGION_REPLICATION = "REGION_REPLICATION";
  private static final Bytes REGION_REPLICATION_KEY
          = new Bytes(Bytes.toBytes(REGION_REPLICATION));

  /**
   * <em>INTERNAL</em> flag to indicate whether or not the memstore should be
   * replicated for read-replicas (CONSISTENCY =&gt; TIMELINE).
   */
  public static final String REGION_MEMSTORE_REPLICATION = "REGION_MEMSTORE_REPLICATION";
  private static final Bytes REGION_MEMSTORE_REPLICATION_KEY
          = new Bytes(Bytes.toBytes(REGION_MEMSTORE_REPLICATION));

  /**
   * <em>INTERNAL</em> Used by shell/rest interface to access this metadata
   * attribute which denotes if the table should be treated by region
   * normalizer.
   */
  public static final String NORMALIZATION_ENABLED = "NORMALIZATION_ENABLED";
  private static final Bytes NORMALIZATION_ENABLED_KEY
          = new Bytes(Bytes.toBytes(NORMALIZATION_ENABLED));

  /**
   * Default durability for HTD is USE_DEFAULT, which defaults to HBase-global
   * default value
   */
  private static final Durability DEFAULT_DURABLITY = Durability.USE_DEFAULT;

  public static final String PRIORITY = "PRIORITY";
  private static final Bytes PRIORITY_KEY
          = new Bytes(Bytes.toBytes(PRIORITY));

  /**
   * Relative priority of the table used for rpc scheduling
   */
  private static final int DEFAULT_PRIORITY = HConstants.NORMAL_QOS;

  /*
     *  The below are ugly but better than creating them each time till we
     *  replace booleans being saved as Strings with plain booleans.  Need a
     *  migration script to do this.  TODO.
   */
  private static final Bytes FALSE
          = new Bytes(Bytes.toBytes(Boolean.FALSE.toString()));

  private static final Bytes TRUE
          = new Bytes(Bytes.toBytes(Boolean.TRUE.toString()));

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
    RESERVED_KEYWORDS.add(IS_ROOT_KEY);
    RESERVED_KEYWORDS.add(IS_META_KEY);
  }

  public final static String NAMESPACE_FAMILY_INFO = "info";
  public final static byte[] NAMESPACE_FAMILY_INFO_BYTES = Bytes.toBytes(NAMESPACE_FAMILY_INFO);
  public final static byte[] NAMESPACE_COL_DESC_BYTES = Bytes.toBytes("d");

  /**
   * Table descriptor for namespace table
   */
  public static final TableDescriptor NAMESPACE_TABLEDESC
    = TableDescriptorBuilder.newBuilder(TableName.NAMESPACE_TABLE_NAME)
                            .addFamily(new HColumnDescriptor(NAMESPACE_FAMILY_INFO)
                              // Ten is arbitrary number.  Keep versions to help debugging.
                              .setMaxVersions(10)
                              .setInMemory(true)
                              .setBlocksize(8 * 1024)
                              .setScope(HConstants.REPLICATION_SCOPE_LOCAL)
                              // Enable cache of data blocks in L1 if more than one caching tier deployed:
                              // e.g. if using CombinedBlockCache (BucketCache).
                              .setCacheDataInL1(true))
                            .doBuild();
  private final ModifyableTableDescriptor desc;

  /**
   * @param desc The table descriptor to serialize
   * @return This instance serialized with pb with pb magic prefix
   */
  public static byte[] toByteArray(TableDescriptor desc) {
    if (desc instanceof ModifyableTableDescriptor) {
      return ((ModifyableTableDescriptor) desc).toByteArray();
    }
    // TODO: remove this if the HTableDescriptor is removed
    if (desc instanceof HTableDescriptor) {
      return ((HTableDescriptor) desc).toByteArray();
    }
    return new ModifyableTableDescriptor(desc).toByteArray();
  }

  /**
   * The input should be created by {@link #toByteArray}.
   * @param pbBytes A pb serialized TableDescriptor instance with pb magic prefix
   * @return This instance serialized with pb with pb magic prefix
   * @throws org.apache.hadoop.hbase.exceptions.DeserializationException
   */
  public static TableDescriptorBuilder newBuilder(byte[] pbBytes) throws DeserializationException {
    return new TableDescriptorBuilder(ModifyableTableDescriptor.parseFrom(pbBytes));
  }

  public static TableDescriptorBuilder newBuilder(final TableName name) {
    return new TableDescriptorBuilder(name);
  }

  /**
   * Copy all configuration, values, families, and name from the input.
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

  public TableDescriptorBuilder addFamily(final HColumnDescriptor family) {
    desc.addFamily(family);
    return this;
  }

  public TableDescriptorBuilder modifyFamily(final HColumnDescriptor family) {
    desc.modifyFamily(family);
    return this;
  }

  public TableDescriptorBuilder remove(Bytes key) {
    desc.remove(key);
    return this;
  }

  public TableDescriptorBuilder remove(byte[] key) {
    desc.remove(key);
    return this;
  }

  public TableDescriptorBuilder removeConfiguration(final String key) {
    desc.removeConfiguration(key);
    return this;
  }

  public TableDescriptorBuilder removeFamily(final byte[] column) {
    desc.removeFamily(column);
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

  public TableDescriptorBuilder setConfiguration(String key, String value) {
    desc.setConfiguration(key, value);
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

  public TableDescriptorBuilder setValue(final Bytes key, final Bytes value) {
    desc.setValue(key, value);
    return this;
  }

  public TableDescriptorBuilder setValue(final byte[] key, final byte[] value) {
    desc.setValue(key, value);
    return this;
  }

  // TODO: replaced the HTableDescriptor by TableDescriptor
  public HTableDescriptor build() {
    return new HTableDescriptor(desc);
  }

  // TODO: remove this in HBase 3.0.0.
  private TableDescriptor doBuild() {
    return new ModifyableTableDescriptor(desc);
  }

  /**
   * ModifyableTableDescriptor contains the details about an HBase table such as the
   * descriptors of all the column families, is the table a catalog table, <code> -ROOT-
   * </code> or <code> hbase:meta </code>, if the table is read only, the
   * maximum size of the memstore, when the region split should occur,
   * coprocessors associated with it etc... TODO: make this private after
   * removing the HTableDescriptor
   */
  @InterfaceAudience.Private
  public static class ModifyableTableDescriptor
          implements TableDescriptor, Comparable<ModifyableTableDescriptor> {

    private final TableName name;

    /**
     * A map which holds the metadata information of the table. This metadata
     * includes values like IS_ROOT, IS_META, SPLIT_POLICY, MAX_FILE_SIZE,
     * READONLY, MEMSTORE_FLUSHSIZE etc...
     */
    private final Map<Bytes, Bytes> values = new HashMap<>();

    /**
     * A map which holds the configuration specific to the table. The keys of
     * the map have the same names as config keys and override the defaults with
     * table-specific settings. Example usage may be for compactions, etc.
     */
    private final Map<String, String> configuration = new HashMap<>();

    /**
     * Maps column family name to the respective HColumnDescriptors
     */
    private final Map<byte[], HColumnDescriptor> families
            = new TreeMap<>(Bytes.BYTES_RAWCOMPARATOR);

    /**
     * Construct a table descriptor specifying a TableName object
     *
     * @param name Table name.
     * @see
     * <a href="https://issues.apache.org/jira/browse/HBASE-174">HADOOP-1581
     * HBASE: (HBASE-174) Un-openable tablename bug</a>
     */
    private ModifyableTableDescriptor(final TableName name) {
      this(name, Collections.EMPTY_LIST, Collections.EMPTY_MAP, Collections.EMPTY_MAP);
    }

    /**
     * Construct a table descriptor by cloning the descriptor passed as a
     * parameter.
     * <p>
     * Makes a deep copy of the supplied descriptor.
     * TODO: make this private after removing the HTableDescriptor
     * @param desc The descriptor.
     */
    @InterfaceAudience.Private
    protected ModifyableTableDescriptor(final TableDescriptor desc) {
      this(desc.getTableName(), desc.getFamilies(), desc.getValues(), desc.getConfiguration());
    }

    // TODO: make this private after removing the HTableDescriptor
    @InterfaceAudience.Private
    public ModifyableTableDescriptor(final TableName name, final Collection<HColumnDescriptor> families,
            Map<Bytes, Bytes> values, Map<String, String> configuration) {
      this.name = name;
      families.forEach(c -> this.families.put(c.getName(), new HColumnDescriptor(c)));
      values.forEach(this.values::put);
      configuration.forEach(this.configuration::put);
      setMetaFlags(name);
    }

    /*
     * Set meta flags on this table.
     * IS_ROOT_KEY is set if its a -ROOT- table
     * IS_META_KEY is set either if its a -ROOT- or a hbase:meta table
     * Called by constructors.
     * @param name
     */
    private void setMetaFlags(final TableName name) {
      values.put(IS_META_KEY, isRootRegion()
              || name.equals(TableName.META_TABLE_NAME) ? TRUE : FALSE);
    }

    /**
     * Check if the descriptor represents a <code> -ROOT- </code> region.
     *
     * @return true if this is a <code> -ROOT- </code> region
     */
    @Override
    public boolean isRootRegion() {
      return isSomething(IS_ROOT_KEY, false);
    }

    /**
     * Checks if this table is <code> hbase:meta </code> region.
     *
     * @return true if this table is <code> hbase:meta </code> region
     */
    @Override
    public boolean isMetaRegion() {
      return isSomething(IS_META_KEY, false);
    }

    private boolean isSomething(final Bytes key,
            final boolean valueIfNull) {
      byte[] value = getValue(key);
      if (value != null) {
        return Boolean.valueOf(Bytes.toString(value));
      }
      return valueIfNull;
    }

    /**
     * Checks if the table is a <code>hbase:meta</code> table
     *
     * @return true if table is <code> hbase:meta </code> region.
     */
    @Override
    public boolean isMetaTable() {
      return isMetaRegion() && !isRootRegion();
    }

    /**
     * Getter for accessing the metadata associated with the key
     *
     * @param key The key.
     * @return The value.
     * @see #values
     */
    @Override
    public byte[] getValue(byte[] key) {
      return getValue(new Bytes(key));
    }

    private byte[] getValue(final Bytes key) {
      Bytes ibw = values.get(key);
      if (ibw == null) {
        return null;
      }
      return ibw.get();
    }

    /**
     * Getter for accessing the metadata associated with the key
     *
     * @param key The key.
     * @return The value.
     * @see #values
     */
    public String getValue(String key) {
      byte[] value = getValue(Bytes.toBytes(key));
      if (value == null) {
        return null;
      }
      return Bytes.toString(value);
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
        remove(key);
      } else {
        values.put(key, value);
      }
      return this;
    }

    /**
     * Setter for storing metadata as a (key, value) pair in {@link #values} map
     *
     * @param key The key.
     * @param value The value. If null, removes the setting.
     * @return the modifyable TD
     * @see #values
     */
    public ModifyableTableDescriptor setValue(String key, String value) {
      return setValue(toBytesOrNull(key, Bytes::toBytes),
              toBytesOrNull(value, Bytes::toBytes));
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
     */
    public void remove(final String key) {
      remove(new Bytes(Bytes.toBytes(key)));
    }

    /**
     * Remove metadata represented by the key from the {@link #values} map
     *
     * @param key Key whose key and value we're to remove from TableDescriptor
     * parameters.
     */
    public void remove(Bytes key) {
      values.remove(key);
    }

    /**
     * Remove metadata represented by the key from the {@link #values} map
     *
     * @param key Key whose key and value we're to remove from TableDescriptor
     * parameters.
     */
    public void remove(final byte[] key) {
      remove(new Bytes(key));
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
      return isSomething(READONLY_KEY, DEFAULT_READONLY);
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
      return setValue(READONLY_KEY, readOnly ? TRUE : FALSE);
    }

    /**
     * Check if the compaction enable flag of the table is true. If flag is
     * false then no minor/major compactions will be done in real.
     *
     * @return true if table compaction enabled
     */
    @Override
    public boolean isCompactionEnabled() {
      return isSomething(COMPACTION_ENABLED_KEY, DEFAULT_COMPACTION_ENABLED);
    }

    /**
     * Setting the table compaction enable flag.
     *
     * @param isEnable True if enable compaction.
     * @return the modifyable TD
     */
    public ModifyableTableDescriptor setCompactionEnabled(final boolean isEnable) {
      return setValue(COMPACTION_ENABLED_KEY, isEnable ? TRUE : FALSE);
    }

    /**
     * Check if normalization enable flag of the table is true. If flag is false
     * then no region normalizer won't attempt to normalize this table.
     *
     * @return true if region normalization is enabled for this table
     */
    @Override
    public boolean isNormalizationEnabled() {
      return isSomething(NORMALIZATION_ENABLED_KEY, DEFAULT_NORMALIZATION_ENABLED);
    }

    /**
     * Setting the table normalization enable flag.
     *
     * @param isEnable True if enable normalization.
     * @return the modifyable TD
     */
    public ModifyableTableDescriptor setNormalizationEnabled(final boolean isEnable) {
      return setValue(NORMALIZATION_ENABLED_KEY, isEnable ? TRUE : FALSE);
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
      byte[] durabilityValue = getValue(DURABILITY_KEY);
      if (durabilityValue == null) {
        return DEFAULT_DURABLITY;
      } else {
        try {
          return Durability.valueOf(Bytes.toString(durabilityValue));
        } catch (IllegalArgumentException ex) {
          LOG.warn("Received " + ex + " because Durability value for TableDescriptor"
                  + " is not known. Durability:" + Bytes.toString(durabilityValue));
          return DEFAULT_DURABLITY;
        }
      }
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
      return setValue(SPLIT_POLICY, clazz);
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
      return getValue(SPLIT_POLICY);
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
      byte[] value = getValue(MAX_FILESIZE_KEY);
      if (value != null) {
        return Long.parseLong(Bytes.toString(value));
      }
      return -1;
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
      byte[] value = getValue(MEMSTORE_FLUSHSIZE_KEY);
      if (value != null) {
        return Long.parseLong(Bytes.toString(value));
      }
      return -1;
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
      return setValue(FLUSH_POLICY, clazz);
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
      return getValue(FLUSH_POLICY);
    }

    /**
     * Adds a column family. For the updating purpose please use
     * {@link #modifyFamily(HColumnDescriptor)} instead.
     *
     * @param family HColumnDescriptor of family to add.
     * @return the modifyable TD
     */
    public ModifyableTableDescriptor addFamily(final HColumnDescriptor family) {
      if (family.getName() == null || family.getName().length <= 0) {
        throw new IllegalArgumentException("Family name cannot be null or empty");
      }
      if (hasFamily(family.getName())) {
        throw new IllegalArgumentException("Family '"
                + family.getNameAsString() + "' already exists so cannot be added");
      }
      return setFamily(family);
    }

    /**
     * Modifies the existing column family.
     *
     * @param family HColumnDescriptor of family to update
     * @return this (for chained invocation)
     */
    public ModifyableTableDescriptor modifyFamily(final HColumnDescriptor family) {
      if (family.getName() == null || family.getName().length <= 0) {
        throw new IllegalArgumentException("Family name cannot be null or empty");
      }
      if (!hasFamily(family.getName())) {
        throw new IllegalArgumentException("Column family '" + family.getNameAsString()
                + "' does not exist");
      }
      return setFamily(family);
    }

    // TODO: make this private after removing the UnmodifyableTableDescriptor
    protected ModifyableTableDescriptor setFamily(HColumnDescriptor family) {
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
    public boolean hasFamily(final byte[] familyName) {
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
        // only print out IS_ROOT/IS_META if true
        String value = Bytes.toString(entry.getValue().get());
        if (key.equalsIgnoreCase(IS_ROOT) || key.equalsIgnoreCase(IS_META)) {
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
      if (!hasAttributes && configuration.isEmpty()) {
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
          // print all non-reserved, advanced config keys as a separate subset
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

      // step 3: printing all configuration:
      if (!configuration.isEmpty()) {
        if (hasAttributes) {
          s.append(", ");
        }
        s.append(HConstants.CONFIGURATION).append(" => ");
        s.append('{');
        boolean printCommaForConfig = false;
        for (Map.Entry<String, String> e : configuration.entrySet()) {
          if (printCommaForConfig) {
            s.append(", ");
          }
          printCommaForConfig = true;
          s.append('\'').append(e.getKey()).append('\'');
          s.append(" => ");
          s.append('\'').append(e.getValue()).append('\'');
        }
        s.append("}");
      }
      s.append("}"); // end METHOD
      return s;
    }

    /**
     * Compare the contents of the descriptor with another one passed as a
     * parameter. Checks if the obj passed is an instance of ModifyableTableDescriptor,
     * if yes then the contents of the descriptors are compared.
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
      if (obj == null) {
        return false;
      }
      if (!(obj instanceof ModifyableTableDescriptor)) {
        return false;
      }
      return compareTo((ModifyableTableDescriptor) obj) == 0;
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
      int result = this.name.hashCode();
      if (this.families.size() > 0) {
        for (HColumnDescriptor e : this.families.values()) {
          result ^= e.hashCode();
        }
      }
      result ^= values.hashCode();
      result ^= configuration.hashCode();
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
      int result = this.name.compareTo(other.name);
      if (result == 0) {
        result = families.size() - other.families.size();
      }
      if (result == 0 && families.size() != other.families.size()) {
        result = Integer.valueOf(families.size()).compareTo(other.families.size());
      }
      if (result == 0) {
        for (Iterator<HColumnDescriptor> it = families.values().iterator(),
                it2 = other.families.values().iterator(); it.hasNext();) {
          result = it.next().compareTo(it2.next());
          if (result != 0) {
            break;
          }
        }
      }
      if (result == 0) {
        // punt on comparison for ordering, just calculate difference
        result = this.values.hashCode() - other.values.hashCode();
        if (result < 0) {
          result = -1;
        } else if (result > 0) {
          result = 1;
        }
      }
      if (result == 0) {
        result = this.configuration.hashCode() - other.configuration.hashCode();
        if (result < 0) {
          result = -1;
        } else if (result > 0) {
          result = 1;
        }
      }
      return result;
    }

    /**
     * Returns an unmodifiable collection of all the {@link HColumnDescriptor}
     * of all the column families of the table.
     *
     * @return Immutable collection of {@link HColumnDescriptor} of all the
     * column families.
     */
    @Override
    public Collection<HColumnDescriptor> getFamilies() {
      return Collections.unmodifiableCollection(this.families.values());
    }

    /**
     * Return true if there are at least one cf whose replication scope is
     * serial.
     */
    @Override
    public boolean hasSerialReplicationScope() {
      return getFamilies()
              .stream()
              .anyMatch(column -> column.getScope() == HConstants.REPLICATION_SCOPE_SERIAL);
    }

    /**
     * Returns the configured replicas per region
     */
    @Override
    public int getRegionReplication() {
      return getIntValue(REGION_REPLICATION_KEY, DEFAULT_REGION_REPLICATION);
    }

    private int getIntValue(Bytes key, int defaultVal) {
      byte[] val = getValue(key);
      if (val == null || val.length == 0) {
        return defaultVal;
      }
      return Integer.parseInt(Bytes.toString(val));
    }

    /**
     * Sets the number of replicas per region.
     *
     * @param regionReplication the replication factor per region
     * @return the modifyable TD
     */
    public ModifyableTableDescriptor setRegionReplication(int regionReplication) {
      return setValue(REGION_REPLICATION_KEY,
              new Bytes(Bytes.toBytes(Integer.toString(regionReplication))));
    }

    /**
     * @return true if the read-replicas memstore replication is enabled.
     */
    @Override
    public boolean hasRegionMemstoreReplication() {
      return isSomething(REGION_MEMSTORE_REPLICATION_KEY, DEFAULT_REGION_MEMSTORE_REPLICATION);
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
      setValue(REGION_MEMSTORE_REPLICATION_KEY, memstoreReplication ? TRUE : FALSE);
      // If the memstore replication is setup, we do not have to wait for observing a flush event
      // from primary before starting to serve reads, because gaps from replication is not applicable
      return setConfiguration(RegionReplicaUtil.REGION_REPLICA_WAIT_FOR_PRIMARY_FLUSH_CONF_KEY,
              Boolean.toString(memstoreReplication));
    }

    public ModifyableTableDescriptor setPriority(int priority) {
      return setValue(PRIORITY_KEY, Integer.toString(priority));
    }

    @Override
    public int getPriority() {
      return getIntValue(PRIORITY_KEY, DEFAULT_PRIORITY);
    }

    /**
     * Returns all the column family names of the current table. The map of
     * TableDescriptor contains mapping of family name to HColumnDescriptors.
     * This returns all the keys of the family map which represents the column
     * family names of the table.
     *
     * @return Immutable sorted set of the keys of the families.
     */
    @Override
    public Set<byte[]> getFamiliesKeys() {
      return Collections.unmodifiableSet(this.families.keySet());
    }

    /**
     * Returns the count of the column families of the table.
     *
     * @return Count of column families of the table
     */
    @Override
    public int getColumnFamilyCount() {
      return families.size();
    }

    /**
     * Returns an array all the {@link HColumnDescriptor} of the column families
     * of the table.
     *
     * @return Array of all the HColumnDescriptors of the current table
     *
     * @see #getFamilies()
     */
    @Override
    public HColumnDescriptor[] getColumnFamilies() {
      Collection<HColumnDescriptor> hColumnDescriptors = getFamilies();
      return hColumnDescriptors.toArray(new HColumnDescriptor[hColumnDescriptors.size()]);
    }

    /**
     * Returns the HColumnDescriptor for a specific column family with name as
     * specified by the parameter column.
     *
     * @param column Column family name
     * @return Column descriptor for the passed family name or the family on
     * passed in column.
     */
    @Override
    public HColumnDescriptor getFamily(final byte[] column) {
      return this.families.get(column);
    }

    /**
     * Removes the HColumnDescriptor with name specified by the parameter column
     * from the table descriptor
     *
     * @param column Name of the column family to be removed.
     * @return Column descriptor for the passed family name or the family on
     * passed in column.
     */
    public HColumnDescriptor removeFamily(final byte[] column) {
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
        remove(match);
      }
    }

    @Deprecated
    public ModifyableTableDescriptor setOwner(User owner) {
      return setOwnerString(owner != null ? owner.getShortName() : null);
    }

    // used by admin.rb:alter(table_name,*args) to update owner.
    @Deprecated
    public ModifyableTableDescriptor setOwnerString(String ownerString) {
      if (ownerString != null) {
        setValue(OWNER_KEY, ownerString);
      } else {
        remove(OWNER_KEY);
      }
      return this;
    }

    @Override
    @Deprecated
    public String getOwnerString() {
      if (getValue(OWNER_KEY) != null) {
        return Bytes.toString(getValue(OWNER_KEY));
      }
      // Note that every table should have an owner (i.e. should have OWNER_KEY set).
      // hbase:meta and -ROOT- should return system user as owner, not null (see
      // MasterFileSystem.java:bootstrap()).
      return null;
    }

    public byte[] toByteArray() {
      return ProtobufUtil.prependPBMagic(ProtobufUtil.convertToTableSchema(this).toByteArray());
    }

    /**
     * @param bytes A pb serialized {@link ModifyableTableDescriptor} instance
     * with pb magic prefix
     * @return An instance of {@link ModifyableTableDescriptor} made from
     * <code>bytes</code>
     * @throws DeserializationException
     * @see #toByteArray()
     */
    public static TableDescriptor parseFrom(final byte[] bytes)
            throws DeserializationException {
      if (!ProtobufUtil.isPBMagicPrefix(bytes)) {
        throw new DeserializationException("Expected PB encoded ModifyableTableDescriptor");
      }
      int pblen = ProtobufUtil.lengthOfPBMagic();
      HBaseProtos.TableSchema.Builder builder = HBaseProtos.TableSchema.newBuilder();
      try {
        ProtobufUtil.mergeFrom(builder, bytes, pblen, bytes.length - pblen);
        return ProtobufUtil.convertToTableDesc(builder.build());
      } catch (IOException e) {
        throw new DeserializationException(e);
      }
    }

    /**
     * Getter for accessing the configuration value by key
     */
    @Override
    public String getConfigurationValue(String key) {
      return configuration.get(key);
    }

    /**
     * Getter for fetching an unmodifiable {@link #configuration} map.
     */
    @Override
    public Map<String, String> getConfiguration() {
      // shallow pointer copy
      return Collections.unmodifiableMap(configuration);
    }

    /**
     * Setter for storing a configuration setting in {@link #configuration} map.
     *
     * @param key Config key. Same as XML config key e.g.
     * hbase.something.or.other.
     * @param value String value. If null, removes the setting.
     * @return the modifyable TD
     */
    public ModifyableTableDescriptor setConfiguration(String key, String value) {
      if (value == null) {
        removeConfiguration(key);
      } else {
        configuration.put(key, value);
      }
      return this;
    }

    /**
     * Remove a config setting represented by the key from the
     * {@link #configuration} map
     * @param key Config key.
     */
    public void removeConfiguration(final String key) {
      configuration.remove(key);
    }
  }

}
