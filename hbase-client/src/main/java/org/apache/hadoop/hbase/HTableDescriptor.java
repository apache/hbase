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
import java.util.regex.Matcher;

import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.BytesBytesPair;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.ColumnFamilySchema;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.NameStringPair;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.TableSchema;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.WritableComparable;

/**
 * HTableDescriptor contains the details about an HBase table  such as the descriptors of
 * all the column families, is the table a catalog table, <code> -ROOT- </code> or
 * <code> hbase:meta </code>, if the table is read only, the maximum size of the memstore,
 * when the region split should occur, coprocessors associated with it etc...
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class HTableDescriptor implements WritableComparable<HTableDescriptor> {

  private static final Log LOG = LogFactory.getLog(HTableDescriptor.class);

  /**
   *  Changes prior to version 3 were not recorded here.
   *  Version 3 adds metadata as a map where keys and values are byte[].
   *  Version 4 adds indexes
   *  Version 5 removed transactional pollution -- e.g. indexes
   *  Version 6 changed metadata to BytesBytesPair in PB
   *  Version 7 adds table-level configuration
   */
  private static final byte TABLE_DESCRIPTOR_VERSION = 7;

  private TableName name = null;

  /**
   * A map which holds the metadata information of the table. This metadata
   * includes values like IS_ROOT, IS_META, DEFERRED_LOG_FLUSH, SPLIT_POLICY,
   * MAX_FILE_SIZE, READONLY, MEMSTORE_FLUSHSIZE etc...
   */
  private final Map<ImmutableBytesWritable, ImmutableBytesWritable> values =
    new HashMap<ImmutableBytesWritable, ImmutableBytesWritable>();

  /**
   * A map which holds the configuration specific to the table.
   * The keys of the map have the same names as config keys and override the defaults with
   * table-specific settings. Example usage may be for compactions, etc.
   */
  private final Map<String, String> configuration = new HashMap<String, String>();

  public static final String SPLIT_POLICY = "SPLIT_POLICY";

  /**
   * <em>INTERNAL</em> Used by HBase Shell interface to access this metadata
   * attribute which denotes the maximum size of the store file after which
   * a region split occurs
   *
   * @see #getMaxFileSize()
   */
  public static final String MAX_FILESIZE = "MAX_FILESIZE";
  private static final ImmutableBytesWritable MAX_FILESIZE_KEY =
    new ImmutableBytesWritable(Bytes.toBytes(MAX_FILESIZE));

  public static final String OWNER = "OWNER";
  public static final ImmutableBytesWritable OWNER_KEY =
    new ImmutableBytesWritable(Bytes.toBytes(OWNER));

  /**
   * <em>INTERNAL</em> Used by rest interface to access this metadata
   * attribute which denotes if the table is Read Only
   *
   * @see #isReadOnly()
   */
  public static final String READONLY = "READONLY";
  private static final ImmutableBytesWritable READONLY_KEY =
    new ImmutableBytesWritable(Bytes.toBytes(READONLY));

  /**
   * <em>INTERNAL</em> Used by HBase Shell interface to access this metadata
   * attribute which denotes if the table is compaction enabled
   *
   * @see #isCompactionEnabled()
   */
  public static final String COMPACTION_ENABLED = "COMPACTION_ENABLED";
  private static final ImmutableBytesWritable COMPACTION_ENABLED_KEY =
    new ImmutableBytesWritable(Bytes.toBytes(COMPACTION_ENABLED));

  /**
   * <em>INTERNAL</em> Used by HBase Shell interface to access this metadata
   * attribute which represents the maximum size of the memstore after which
   * its contents are flushed onto the disk
   *
   * @see #getMemStoreFlushSize()
   */
  public static final String MEMSTORE_FLUSHSIZE = "MEMSTORE_FLUSHSIZE";
  private static final ImmutableBytesWritable MEMSTORE_FLUSHSIZE_KEY =
    new ImmutableBytesWritable(Bytes.toBytes(MEMSTORE_FLUSHSIZE));

  public static final String FLUSH_POLICY = "FLUSH_POLICY";

  /**
   * <em>INTERNAL</em> Used by rest interface to access this metadata
   * attribute which denotes if the table is a -ROOT- region or not
   *
   * @see #isRootRegion()
   */
  public static final String IS_ROOT = "IS_ROOT";
  private static final ImmutableBytesWritable IS_ROOT_KEY =
    new ImmutableBytesWritable(Bytes.toBytes(IS_ROOT));

  /**
   * <em>INTERNAL</em> Used by rest interface to access this metadata
   * attribute which denotes if it is a catalog table, either
   * <code> hbase:meta </code> or <code> -ROOT- </code>
   *
   * @see #isMetaRegion()
   */
  public static final String IS_META = "IS_META";
  private static final ImmutableBytesWritable IS_META_KEY =
    new ImmutableBytesWritable(Bytes.toBytes(IS_META));

  /**
   * <em>INTERNAL</em> Used by HBase Shell interface to access this metadata
   * attribute which denotes if the deferred log flush option is enabled.
   * @deprecated Use {@link #DURABILITY} instead.
   */
  @Deprecated
  public static final String DEFERRED_LOG_FLUSH = "DEFERRED_LOG_FLUSH";
  @Deprecated
  private static final ImmutableBytesWritable DEFERRED_LOG_FLUSH_KEY =
    new ImmutableBytesWritable(Bytes.toBytes(DEFERRED_LOG_FLUSH));

  /**
   * <em>INTERNAL</em> {@link Durability} setting for the table.
   */
  public static final String DURABILITY = "DURABILITY";
  private static final ImmutableBytesWritable DURABILITY_KEY =
      new ImmutableBytesWritable(Bytes.toBytes("DURABILITY"));

  /**
   * <em>INTERNAL</em> number of region replicas for the table.
   */
  public static final String REGION_REPLICATION = "REGION_REPLICATION";
  private static final ImmutableBytesWritable REGION_REPLICATION_KEY =
      new ImmutableBytesWritable(Bytes.toBytes(REGION_REPLICATION));

  /**
   * <em>INTERNAL</em> flag to indicate whether or not the memstore should be replicated
   * for read-replicas (CONSISTENCY => TIMELINE).
   */
  public static final String REGION_MEMSTORE_REPLICATION = "REGION_MEMSTORE_REPLICATION";
  private static final ImmutableBytesWritable REGION_MEMSTORE_REPLICATION_KEY =
      new ImmutableBytesWritable(Bytes.toBytes(REGION_MEMSTORE_REPLICATION));

  /** Default durability for HTD is USE_DEFAULT, which defaults to HBase-global default value */
  private static final Durability DEFAULT_DURABLITY = Durability.USE_DEFAULT;

  /*
   *  The below are ugly but better than creating them each time till we
   *  replace booleans being saved as Strings with plain booleans.  Need a
   *  migration script to do this.  TODO.
   */
  private static final ImmutableBytesWritable FALSE =
    new ImmutableBytesWritable(Bytes.toBytes(Boolean.FALSE.toString()));

  private static final ImmutableBytesWritable TRUE =
    new ImmutableBytesWritable(Bytes.toBytes(Boolean.TRUE.toString()));

  private static final boolean DEFAULT_DEFERRED_LOG_FLUSH = false;

  /**
   * Constant that denotes whether the table is READONLY by default and is false
   */
  public static final boolean DEFAULT_READONLY = false;

  /**
   * Constant that denotes whether the table is compaction enabled by default
   */
  public static final boolean DEFAULT_COMPACTION_ENABLED = true;

  /**
   * Constant that denotes the maximum default size of the memstore after which
   * the contents are flushed to the store files
   */
  public static final long DEFAULT_MEMSTORE_FLUSH_SIZE = 1024*1024*128L;

  public static final int DEFAULT_REGION_REPLICATION = 1;

  public static final boolean DEFAULT_REGION_MEMSTORE_REPLICATION = true;

  private final static Map<String, String> DEFAULT_VALUES
    = new HashMap<String, String>();
  private final static Set<ImmutableBytesWritable> RESERVED_KEYWORDS
    = new HashSet<ImmutableBytesWritable>();
  static {
    DEFAULT_VALUES.put(MAX_FILESIZE,
        String.valueOf(HConstants.DEFAULT_MAX_FILE_SIZE));
    DEFAULT_VALUES.put(READONLY, String.valueOf(DEFAULT_READONLY));
    DEFAULT_VALUES.put(MEMSTORE_FLUSHSIZE,
        String.valueOf(DEFAULT_MEMSTORE_FLUSH_SIZE));
    DEFAULT_VALUES.put(DEFERRED_LOG_FLUSH,
        String.valueOf(DEFAULT_DEFERRED_LOG_FLUSH));
    DEFAULT_VALUES.put(DURABILITY, DEFAULT_DURABLITY.name()); //use the enum name
    DEFAULT_VALUES.put(REGION_REPLICATION, String.valueOf(DEFAULT_REGION_REPLICATION));
    for (String s : DEFAULT_VALUES.keySet()) {
      RESERVED_KEYWORDS.add(new ImmutableBytesWritable(Bytes.toBytes(s)));
    }
    RESERVED_KEYWORDS.add(IS_ROOT_KEY);
    RESERVED_KEYWORDS.add(IS_META_KEY);
  }

  /**
   * Cache of whether this is a meta table or not.
   */
  private volatile Boolean meta = null;
  /**
   * Cache of whether this is root table or not.
   */
  private volatile Boolean root = null;

  /**
   * Durability setting for the table
   */
  private Durability durability = null;

  /**
   * Maps column family name to the respective HColumnDescriptors
   */
  private final Map<byte [], HColumnDescriptor> families =
    new TreeMap<byte [], HColumnDescriptor>(Bytes.BYTES_RAWCOMPARATOR);

  /**
   * <em> INTERNAL </em> Private constructor used internally creating table descriptors for
   * catalog tables, <code>hbase:meta</code> and <code>-ROOT-</code>.
   */
  @InterfaceAudience.Private
  protected HTableDescriptor(final TableName name, HColumnDescriptor[] families) {
    setName(name);
    for(HColumnDescriptor descriptor : families) {
      this.families.put(descriptor.getName(), descriptor);
    }
  }

  /**
   * <em> INTERNAL </em>Private constructor used internally creating table descriptors for
   * catalog tables, <code>hbase:meta</code> and <code>-ROOT-</code>.
   */
  protected HTableDescriptor(final TableName name, HColumnDescriptor[] families,
      Map<ImmutableBytesWritable,ImmutableBytesWritable> values) {
    setName(name);
    for(HColumnDescriptor descriptor : families) {
      this.families.put(descriptor.getName(), descriptor);
    }
    for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> entry:
        values.entrySet()) {
      setValue(entry.getKey(), entry.getValue());
    }
  }

  /**
   * Default constructor which constructs an empty object.
   * For deserializing an HTableDescriptor instance only.
   * @deprecated As of release 0.96
   *             (<a href="https://issues.apache.org/jira/browse/HBASE-5453">HBASE-5453</a>).
   *             This will be removed in HBase 2.0.0.
   *             Used by Writables and Writables are going away.
   */
  @Deprecated
  public HTableDescriptor() {
    super();
  }

  /**
   * Construct a table descriptor specifying a TableName object
   * @param name Table name.
   * @see <a href="HADOOP-1581">HADOOP-1581 HBASE: Un-openable tablename bug</a>
   */
  public HTableDescriptor(final TableName name) {
    super();
    setName(name);
  }

  /**
   * Construct a table descriptor specifying a byte array table name
   * @param name Table name.
   * @see <a href="HADOOP-1581">HADOOP-1581 HBASE: Un-openable tablename bug</a>
   */
  @Deprecated
  public HTableDescriptor(final byte[] name) {
    this(TableName.valueOf(name));
  }

  /**
   * Construct a table descriptor specifying a String table name
   * @param name Table name.
   * @see <a href="HADOOP-1581">HADOOP-1581 HBASE: Un-openable tablename bug</a>
   */
  @Deprecated
  public HTableDescriptor(final String name) {
    this(TableName.valueOf(name));
  }

  /**
   * Construct a table descriptor by cloning the descriptor passed as a parameter.
   * <p>
   * Makes a deep copy of the supplied descriptor.
   * Can make a modifiable descriptor from an UnmodifyableHTableDescriptor.
   * @param desc The descriptor.
   */
  public HTableDescriptor(final HTableDescriptor desc) {
    super();
    setName(desc.name);
    setMetaFlags(this.name);
    for (HColumnDescriptor c: desc.families.values()) {
      this.families.put(c.getName(), new HColumnDescriptor(c));
    }
    for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> e:
        desc.values.entrySet()) {
      setValue(e.getKey(), e.getValue());
    }
    for (Map.Entry<String, String> e : desc.configuration.entrySet()) {
      this.configuration.put(e.getKey(), e.getValue());
    }
  }

  /*
   * Set meta flags on this table.
   * IS_ROOT_KEY is set if its a -ROOT- table
   * IS_META_KEY is set either if its a -ROOT- or a hbase:meta table
   * Called by constructors.
   * @param name
   */
  private void setMetaFlags(final TableName name) {
    setMetaRegion(isRootRegion() ||
        name.equals(TableName.META_TABLE_NAME));
  }

  /**
   * Check if the descriptor represents a <code> -ROOT- </code> region.
   *
   * @return true if this is a <code> -ROOT- </code> region
   */
  public boolean isRootRegion() {
    if (this.root == null) {
      this.root = isSomething(IS_ROOT_KEY, false)? Boolean.TRUE: Boolean.FALSE;
    }
    return this.root.booleanValue();
  }

  /**
   * <em> INTERNAL </em> Used to denote if the current table represents
   * <code> -ROOT- </code> region. This is used internally by the
   * HTableDescriptor constructors
   *
   * @param isRoot true if this is the <code> -ROOT- </code> region
   */
  protected void setRootRegion(boolean isRoot) {
    // TODO: Make the value a boolean rather than String of boolean.
    setValue(IS_ROOT_KEY, isRoot? TRUE: FALSE);
  }

  /**
   * Checks if this table is <code> hbase:meta </code>
   * region.
   *
   * @return true if this table is <code> hbase:meta </code>
   * region
   */
  public boolean isMetaRegion() {
    if (this.meta == null) {
      this.meta = calculateIsMetaRegion();
    }
    return this.meta.booleanValue();
  }

  private synchronized Boolean calculateIsMetaRegion() {
    byte [] value = getValue(IS_META_KEY);
    return (value != null)? Boolean.valueOf(Bytes.toString(value)): Boolean.FALSE;
  }

  private boolean isSomething(final ImmutableBytesWritable key,
      final boolean valueIfNull) {
    byte [] value = getValue(key);
    if (value != null) {
      return Boolean.valueOf(Bytes.toString(value));
    }
    return valueIfNull;
  }

  /**
   * <em> INTERNAL </em> Used to denote if the current table represents
   * <code> -ROOT- </code> or <code> hbase:meta </code> region. This is used
   * internally by the HTableDescriptor constructors
   *
   * @param isMeta true if its either <code> -ROOT- </code> or
   * <code> hbase:meta </code> region
   */
  protected void setMetaRegion(boolean isMeta) {
    setValue(IS_META_KEY, isMeta? TRUE: FALSE);
  }

  /**
   * Checks if the table is a <code>hbase:meta</code> table
   *
   * @return true if table is <code> hbase:meta </code> region.
   */
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
  public byte[] getValue(byte[] key) {
    return getValue(new ImmutableBytesWritable(key));
  }

  private byte[] getValue(final ImmutableBytesWritable key) {
    ImmutableBytesWritable ibw = values.get(key);
    if (ibw == null)
      return null;
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
    if (value == null)
      return null;
    return Bytes.toString(value);
  }

  /**
   * Getter for fetching an unmodifiable {@link #values} map.
   *
   * @return unmodifiable map {@link #values}.
   * @see #values
   */
  public Map<ImmutableBytesWritable,ImmutableBytesWritable> getValues() {
    // shallow pointer copy
    return Collections.unmodifiableMap(values);
  }

  /**
   * Setter for storing metadata as a (key, value) pair in {@link #values} map
   *
   * @param key The key.
   * @param value The value.
   * @see #values
   */
  public HTableDescriptor setValue(byte[] key, byte[] value) {
    setValue(new ImmutableBytesWritable(key), new ImmutableBytesWritable(value));
    return this;
  }

  /*
   * @param key The key.
   * @param value The value.
   */
  private HTableDescriptor setValue(final ImmutableBytesWritable key,
      final String value) {
    setValue(key, new ImmutableBytesWritable(Bytes.toBytes(value)));
    return this;
  }

  /*
   * Setter for storing metadata as a (key, value) pair in {@link #values} map
   *
   * @param key The key.
   * @param value The value.
   */
  public HTableDescriptor setValue(final ImmutableBytesWritable key,
      final ImmutableBytesWritable value) {
    if (key.compareTo(DEFERRED_LOG_FLUSH_KEY) == 0) {
      boolean isDeferredFlush = Boolean.valueOf(Bytes.toString(value.get()));
      LOG.warn("HTableDescriptor property:" + DEFERRED_LOG_FLUSH + " is deprecated, " +
          "use " + DURABILITY + " instead");
      setDurability(isDeferredFlush ? Durability.ASYNC_WAL : DEFAULT_DURABLITY);
      return this;
    }
    values.put(key, value);
    return this;
  }

  /**
   * Setter for storing metadata as a (key, value) pair in {@link #values} map
   *
   * @param key The key.
   * @param value The value.
   * @see #values
   */
  public HTableDescriptor setValue(String key, String value) {
    if (value == null) {
      remove(key);
    } else {
      setValue(Bytes.toBytes(key), Bytes.toBytes(value));
    }
    return this;
  }

  /**
   * Remove metadata represented by the key from the {@link #values} map
   *
   * @param key Key whose key and value we're to remove from HTableDescriptor
   * parameters.
   */
  public void remove(final String key) {
    remove(new ImmutableBytesWritable(Bytes.toBytes(key)));
  }

  /**
   * Remove metadata represented by the key from the {@link #values} map
   *
   * @param key Key whose key and value we're to remove from HTableDescriptor
   * parameters.
   */
  public void remove(ImmutableBytesWritable key) {
    values.remove(key);
  }

  /**
   * Remove metadata represented by the key from the {@link #values} map
   *
   * @param key Key whose key and value we're to remove from HTableDescriptor
   * parameters.
   */
  public void remove(final byte [] key) {
    remove(new ImmutableBytesWritable(key));
  }

  /**
   * Check if the readOnly flag of the table is set. If the readOnly flag is
   * set then the contents of the table can only be read from but not modified.
   *
   * @return true if all columns in the table should be read only
   */
  public boolean isReadOnly() {
    return isSomething(READONLY_KEY, DEFAULT_READONLY);
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
    return setValue(READONLY_KEY, readOnly? TRUE: FALSE);
  }

  /**
   * Check if the compaction enable flag of the table is true. If flag is
   * false then no minor/major compactions will be done in real.
   *
   * @return true if table compaction enabled
   */
  public boolean isCompactionEnabled() {
    return isSomething(COMPACTION_ENABLED_KEY, DEFAULT_COMPACTION_ENABLED);
  }

  /**
   * Setting the table compaction enable flag.
   *
   * @param isEnable True if enable compaction.
   */
  public HTableDescriptor setCompactionEnabled(final boolean isEnable) {
    setValue(COMPACTION_ENABLED_KEY, isEnable ? TRUE : FALSE);
    return this;
  }

  /**
   * Sets the {@link Durability} setting for the table. This defaults to Durability.USE_DEFAULT.
   * @param durability enum value
   */
  public HTableDescriptor setDurability(Durability durability) {
    this.durability = durability;
    setValue(DURABILITY_KEY, durability.name());
    return this;
  }

  /**
   * Returns the durability setting for the table.
   * @return durability setting for the table.
   */
  public Durability getDurability() {
    if (this.durability == null) {
      byte[] durabilityValue = getValue(DURABILITY_KEY);
      if (durabilityValue == null) {
        this.durability = DEFAULT_DURABLITY;
      } else {
        try {
          this.durability = Durability.valueOf(Bytes.toString(durabilityValue));
        } catch (IllegalArgumentException ex) {
          LOG.warn("Received " + ex + " because Durability value for HTableDescriptor"
            + " is not known. Durability:" + Bytes.toString(durabilityValue));
          this.durability = DEFAULT_DURABLITY;
        }
      }
    }
    return this.durability;
  }

  /**
   * Get the name of the table
   *
   * @return TableName
   */
  public TableName getTableName() {
    return name;
  }

  /**
   * Get the name of the table as a byte array.
   *
   * @return name of table
   * @deprecated Use {@link #getTableName()} instead
   */
  @Deprecated
  public byte[] getName() {
    return name.getName();
  }

  /**
   * Get the name of the table as a String
   *
   * @return name of table as a String
   */
  public String getNameAsString() {
    return name.getNameAsString();
  }

  /**
   * This sets the class associated with the region split policy which
   * determines when a region split should occur.  The class used by
   * default is defined in {@link org.apache.hadoop.hbase.regionserver.RegionSplitPolicy}
   * @param clazz the class name
   */
  public HTableDescriptor setRegionSplitPolicyClassName(String clazz) {
    setValue(SPLIT_POLICY, clazz);
    return this;
  }

  /**
   * This gets the class associated with the region split policy which
   * determines when a region split should occur.  The class used by
   * default is defined in {@link org.apache.hadoop.hbase.regionserver.RegionSplitPolicy}
   *
   * @return the class name of the region split policy for this table.
   * If this returns null, the default split policy is used.
   */
   public String getRegionSplitPolicyClassName() {
    return getValue(SPLIT_POLICY);
  }

  /**
   * Set the name of the table.
   *
   * @param name name of table
   */
  @Deprecated
  public HTableDescriptor setName(byte[] name) {
    setName(TableName.valueOf(name));
    return this;
  }

  @Deprecated
  public HTableDescriptor setName(TableName name) {
    this.name = name;
    setMetaFlags(this.name);
    return this;
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
  public long getMaxFileSize() {
    byte [] value = getValue(MAX_FILESIZE_KEY);
    if (value != null) {
      return Long.parseLong(Bytes.toString(value));
    }
    return -1;
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
    setValue(MAX_FILESIZE_KEY, Long.toString(maxFileSize));
    return this;
  }

  /**
   * Returns the size of the memstore after which a flush to filesystem is triggered.
   *
   * @return memory cache flush size for each hregion, -1 if not set.
   *
   * @see #setMemStoreFlushSize(long)
   */
  public long getMemStoreFlushSize() {
    byte [] value = getValue(MEMSTORE_FLUSHSIZE_KEY);
    if (value != null) {
      return Long.parseLong(Bytes.toString(value));
    }
    return -1;
  }

  /**
   * Represents the maximum size of the memstore after which the contents of the
   * memstore are flushed to the filesystem. This defaults to a size of 64 MB.
   *
   * @param memstoreFlushSize memory cache flush size for each hregion
   */
  public HTableDescriptor setMemStoreFlushSize(long memstoreFlushSize) {
    setValue(MEMSTORE_FLUSHSIZE_KEY, Long.toString(memstoreFlushSize));
    return this;
  }

  /**
   * This sets the class associated with the flush policy which determines determines the stores
   * need to be flushed when flushing a region. The class used by default is defined in
   * {@link org.apache.hadoop.hbase.regionserver.FlushPolicy}
   * @param clazz the class name
   */
  public HTableDescriptor setFlushPolicyClassName(String clazz) {
    setValue(FLUSH_POLICY, clazz);
    return this;
  }

  /**
   * This gets the class associated with the flush policy which determines the stores need to be
   * flushed when flushing a region. The class used by default is defined in
   * {@link org.apache.hadoop.hbase.regionserver.FlushPolicy}
   * @return the class name of the flush policy for this table. If this returns null, the default
   *         flush policy is used.
   */
  public String getFlushPolicyClassName() {
    return getValue(FLUSH_POLICY);
  }

  /**
   * Adds a column family.
   * For the updating purpose please use {@link #modifyFamily(HColumnDescriptor)} instead.
   * @param family HColumnDescriptor of family to add.
   */
  public HTableDescriptor addFamily(final HColumnDescriptor family) {
    if (family.getName() == null || family.getName().length <= 0) {
      throw new IllegalArgumentException("Family name cannot be null or empty");
    }
    if (hasFamily(family.getName())) {
      throw new IllegalArgumentException("Family '" +
        family.getNameAsString() + "' already exists so cannot be added");
    }
    this.families.put(family.getName(), family);
    return this;
  }

  /**
   * Modifies the existing column family.
   * @param family HColumnDescriptor of family to update
   * @return this (for chained invocation)
   */
  public HTableDescriptor modifyFamily(final HColumnDescriptor family) {
    if (family.getName() == null || family.getName().length <= 0) {
      throw new IllegalArgumentException("Family name cannot be null or empty");
    }
    if (!hasFamily(family.getName())) {
      throw new IllegalArgumentException("Column family '" + family.getNameAsString()
        + "' does not exist");
    }
    this.families.put(family.getName(), family);
    return this;
  }

  /**
   * Checks to see if this table contains the given column family
   * @param familyName Family name or column name.
   * @return true if the table contains the specified family name
   */
  public boolean hasFamily(final byte [] familyName) {
    return families.containsKey(familyName);
  }

  /**
   * @return Name of this table and then a map of all of the column family
   * descriptors.
   * @see #getNameAsString()
   */
  @Override
  public String toString() {
    StringBuilder s = new StringBuilder();
    s.append('\'').append(Bytes.toString(name.getName())).append('\'');
    s.append(getValues(true));
    for (HColumnDescriptor f : families.values()) {
      s.append(", ").append(f);
    }
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
    for(HColumnDescriptor hcd : families.values()) {
      s.append(", ").append(hcd.toStringCustomizedValues());
    }
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
    Set<ImmutableBytesWritable> reservedKeys = new TreeSet<ImmutableBytesWritable>();
    Set<ImmutableBytesWritable> userKeys = new TreeSet<ImmutableBytesWritable>();
    for (ImmutableBytesWritable k : values.keySet()) {
      if (k == null || k.get() == null) continue;
      String key = Bytes.toString(k.get());
      // in this section, print out reserved keywords + coprocessor info
      if (!RESERVED_KEYWORDS.contains(k) && !key.startsWith("coprocessor$")) {
        userKeys.add(k);
        continue;
      }
      // only print out IS_ROOT/IS_META if true
      String value = Bytes.toString(values.get(k).get());
      if (key.equalsIgnoreCase(IS_ROOT) || key.equalsIgnoreCase(IS_META)) {
        if (Boolean.valueOf(value) == false) continue;
      }
      // see if a reserved key is a default value. may not want to print it out
      if (printDefaults
          || !DEFAULT_VALUES.containsKey(key)
          || !DEFAULT_VALUES.get(key).equalsIgnoreCase(value)) {
        reservedKeys.add(k);
      }
    }

    // early exit optimization
    boolean hasAttributes = !reservedKeys.isEmpty() || !userKeys.isEmpty();
    if (!hasAttributes && configuration.isEmpty()) return s;

    s.append(", {");
    // step 2: printing attributes
    if (hasAttributes) {
      s.append("TABLE_ATTRIBUTES => {");

      // print all reserved keys first
      boolean printCommaForAttr = false;
      for (ImmutableBytesWritable k : reservedKeys) {
        String key = Bytes.toString(k.get());
        String value = Bytes.toStringBinary(values.get(k).get());
        if (printCommaForAttr) s.append(", ");
        printCommaForAttr = true;
        s.append(key);
        s.append(" => ");
        s.append('\'').append(value).append('\'');
      }

      if (!userKeys.isEmpty()) {
        // print all non-reserved, advanced config keys as a separate subset
        if (printCommaForAttr) s.append(", ");
        printCommaForAttr = true;
        s.append(HConstants.METADATA).append(" => ");
        s.append("{");
        boolean printCommaForCfg = false;
        for (ImmutableBytesWritable k : userKeys) {
          String key = Bytes.toString(k.get());
          String value = Bytes.toStringBinary(values.get(k).get());
          if (printCommaForCfg) s.append(", ");
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
        if (printCommaForConfig) s.append(", ");
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
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof HTableDescriptor)) {
      return false;
    }
    return compareTo((HTableDescriptor)obj) == 0;
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    int result = this.name.hashCode();
    result ^= Byte.valueOf(TABLE_DESCRIPTOR_VERSION).hashCode();
    if (this.families != null && this.families.size() > 0) {
      for (HColumnDescriptor e: this.families.values()) {
        result ^= e.hashCode();
      }
    }
    result ^= values.hashCode();
    result ^= configuration.hashCode();
    return result;
  }

  /**
   * <em> INTERNAL </em> This method is a part of {@link WritableComparable} interface
   * and is used for de-serialization of the HTableDescriptor over RPC
   * @deprecated Writables are going away.  Use pb {@link #parseFrom(byte[])} instead.
   */
  @Deprecated
  @Override
  public void readFields(DataInput in) throws IOException {
    int version = in.readInt();
    if (version < 3)
      throw new IOException("versions < 3 are not supported (and never existed!?)");
    // version 3+
    name = TableName.valueOf(Bytes.readByteArray(in));
    setRootRegion(in.readBoolean());
    setMetaRegion(in.readBoolean());
    values.clear();
    configuration.clear();
    int numVals = in.readInt();
    for (int i = 0; i < numVals; i++) {
      ImmutableBytesWritable key = new ImmutableBytesWritable();
      ImmutableBytesWritable value = new ImmutableBytesWritable();
      key.readFields(in);
      value.readFields(in);
      setValue(key, value);
    }
    families.clear();
    int numFamilies = in.readInt();
    for (int i = 0; i < numFamilies; i++) {
      HColumnDescriptor c = new HColumnDescriptor();
      c.readFields(in);
      families.put(c.getName(), c);
    }
    if (version >= 7) {
      int numConfigs = in.readInt();
      for (int i = 0; i < numConfigs; i++) {
        ImmutableBytesWritable key = new ImmutableBytesWritable();
        ImmutableBytesWritable value = new ImmutableBytesWritable();
        key.readFields(in);
        value.readFields(in);
        configuration.put(
          Bytes.toString(key.get(), key.getOffset(), key.getLength()),
          Bytes.toString(value.get(), value.getOffset(), value.getLength()));
      }
    }
  }

  /**
   * <em> INTERNAL </em> This method is a part of {@link WritableComparable} interface
   * and is used for serialization of the HTableDescriptor over RPC
   * @deprecated Writables are going away.
   * Use {@link com.google.protobuf.MessageLite#toByteArray} instead.
   */
  @Deprecated
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(TABLE_DESCRIPTOR_VERSION);
    Bytes.writeByteArray(out, name.toBytes());
    out.writeBoolean(isRootRegion());
    out.writeBoolean(isMetaRegion());
    out.writeInt(values.size());
    for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> e:
        values.entrySet()) {
      e.getKey().write(out);
      e.getValue().write(out);
    }
    out.writeInt(families.size());
    for(Iterator<HColumnDescriptor> it = families.values().iterator();
        it.hasNext(); ) {
      HColumnDescriptor family = it.next();
      family.write(out);
    }
    out.writeInt(configuration.size());
    for (Map.Entry<String, String> e : configuration.entrySet()) {
      new ImmutableBytesWritable(Bytes.toBytes(e.getKey())).write(out);
      new ImmutableBytesWritable(Bytes.toBytes(e.getValue())).write(out);
    }
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
    int result = this.name.compareTo(other.name);
    if (result == 0) {
      result = families.size() - other.families.size();
    }
    if (result == 0 && families.size() != other.families.size()) {
      result = Integer.valueOf(families.size()).compareTo(
          Integer.valueOf(other.families.size()));
    }
    if (result == 0) {
      for (Iterator<HColumnDescriptor> it = families.values().iterator(),
          it2 = other.families.values().iterator(); it.hasNext(); ) {
        result = it.next().compareTo(it2.next());
        if (result != 0) {
          break;
        }
      }
    }
    if (result == 0) {
      // punt on comparison for ordering, just calculate difference
      result = this.values.hashCode() - other.values.hashCode();
      if (result < 0)
        result = -1;
      else if (result > 0)
        result = 1;
    }
    if (result == 0) {
      result = this.configuration.hashCode() - other.configuration.hashCode();
      if (result < 0)
        result = -1;
      else if (result > 0)
        result = 1;
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
  public Collection<HColumnDescriptor> getFamilies() {
    return Collections.unmodifiableCollection(this.families.values());
  }

  /**
   * Returns the configured replicas per region
   */
  public int getRegionReplication() {
    byte[] val = getValue(REGION_REPLICATION_KEY);
    if (val == null || val.length == 0) {
      return DEFAULT_REGION_REPLICATION;
    }
    return Integer.parseInt(Bytes.toString(val));
  }

  /**
   * Sets the number of replicas per region.
   * @param regionReplication the replication factor per region
   */
  public HTableDescriptor setRegionReplication(int regionReplication) {
    setValue(REGION_REPLICATION_KEY,
        new ImmutableBytesWritable(Bytes.toBytes(Integer.toString(regionReplication))));
    return this;
  }

  /**
   * @return true if the read-replicas memstore replication is enabled.
   */
  public boolean hasRegionMemstoreReplication() {
    return isSomething(REGION_MEMSTORE_REPLICATION_KEY, DEFAULT_REGION_MEMSTORE_REPLICATION);
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
  public HTableDescriptor setRegionMemstoreReplication(boolean memstoreReplication) {
    setValue(REGION_MEMSTORE_REPLICATION_KEY, memstoreReplication ? TRUE : FALSE);
    // If the memstore replication is setup, we do not have to wait for observing a flush event
    // from primary before starting to serve reads, because gaps from replication is not applicable
    setConfiguration(RegionReplicaUtil.REGION_REPLICA_WAIT_FOR_PRIMARY_FLUSH_CONF_KEY,
      Boolean.toString(memstoreReplication));
    return this;
  }

  /**
   * Returns all the column family names of the current table. The map of
   * HTableDescriptor contains mapping of family name to HColumnDescriptors.
   * This returns all the keys of the family map which represents the column
   * family names of the table.
   *
   * @return Immutable sorted set of the keys of the families.
   */
  public Set<byte[]> getFamiliesKeys() {
    return Collections.unmodifiableSet(this.families.keySet());
  }

  /**
   * Returns an array all the {@link HColumnDescriptor} of the column families
   * of the table.
   *
   * @return Array of all the HColumnDescriptors of the current table
   *
   * @see #getFamilies()
   */
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
  public HColumnDescriptor getFamily(final byte [] column) {
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
  public HColumnDescriptor removeFamily(final byte [] column) {
    return this.families.remove(column);
  }

  /**
   * Add a table coprocessor to this table. The coprocessor
   * type must be {@link org.apache.hadoop.hbase.coprocessor.RegionObserver}
   * or Endpoint.
   * It won't check if the class can be loaded or not.
   * Whether a coprocessor is loadable or not will be determined when
   * a region is opened.
   * @param className Full class name.
   * @throws IOException
   */
  public HTableDescriptor addCoprocessor(String className) throws IOException {
    addCoprocessor(className, null, Coprocessor.PRIORITY_USER, null);
    return this;
  }

  /**
   * Add a table coprocessor to this table. The coprocessor
   * type must be {@link org.apache.hadoop.hbase.coprocessor.RegionObserver}
   * or Endpoint.
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
    checkHasCoprocessor(className);

    // Validate parameter kvs and then add key/values to kvString.
    StringBuilder kvString = new StringBuilder();
    if (kvs != null) {
      for (Map.Entry<String, String> e: kvs.entrySet()) {
        if (!e.getKey().matches(HConstants.CP_HTD_ATTR_VALUE_PARAM_KEY_PATTERN)) {
          throw new IOException("Illegal parameter key = " + e.getKey());
        }
        if (!e.getValue().matches(HConstants.CP_HTD_ATTR_VALUE_PARAM_VALUE_PATTERN)) {
          throw new IOException("Illegal parameter (" + e.getKey() +
              ") value = " + e.getValue());
        }
        if (kvString.length() != 0) {
          kvString.append(',');
        }
        kvString.append(e.getKey());
        kvString.append('=');
        kvString.append(e.getValue());
      }
    }

    String value = ((jarFilePath == null)? "" : jarFilePath.toString()) +
        "|" + className + "|" + Integer.toString(priority) + "|" +
        kvString.toString();
    return addCoprocessorToMap(value);
  }

  /**
   * Add a table coprocessor to this table. The coprocessor
   * type must be {@link org.apache.hadoop.hbase.coprocessor.RegionObserver}
   * or Endpoint.
   * It won't check if the class can be loaded or not.
   * Whether a coprocessor is loadable or not will be determined when
   * a region is opened.
   * @param specStr The Coprocessor specification all in in one String formatted so matches
   * {@link HConstants#CP_HTD_ATTR_VALUE_PATTERN}
   * @throws IOException
   */
  public HTableDescriptor addCoprocessorWithSpec(final String specStr) throws IOException {
    String className = getCoprocessorClassNameFromSpecStr(specStr);
    if (className == null) {
      throw new IllegalArgumentException("Format does not match " +
        HConstants.CP_HTD_ATTR_VALUE_PATTERN + ": " + specStr);
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
   * @param specStr The Coprocessor specification all in in one String formatted so matches
   * {@link HConstants#CP_HTD_ATTR_VALUE_PATTERN}
   * @return Returns <code>this</code>
   */
  private HTableDescriptor addCoprocessorToMap(final String specStr) {
    if (specStr == null) return this;
    // generate a coprocessor key
    int maxCoprocessorNumber = 0;
    Matcher keyMatcher;
    for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> e:
        this.values.entrySet()) {
      keyMatcher =
          HConstants.CP_HTD_ATTR_KEY_PATTERN.matcher(
              Bytes.toString(e.getKey().get()));
      if (!keyMatcher.matches()) {
        continue;
      }
      maxCoprocessorNumber = Math.max(Integer.parseInt(keyMatcher.group(1)), maxCoprocessorNumber);
    }
    maxCoprocessorNumber++;
    String key = "coprocessor$" + Integer.toString(maxCoprocessorNumber);
    this.values.put(new ImmutableBytesWritable(Bytes.toBytes(key)),
      new ImmutableBytesWritable(Bytes.toBytes(specStr)));
    return this;
  }

  /**
   * Check if the table has an attached co-processor represented by the name className
   *
   * @param classNameToMatch - Class name of the co-processor
   * @return true of the table has a co-processor className
   */
  public boolean hasCoprocessor(String classNameToMatch) {
    Matcher keyMatcher;
    Matcher valueMatcher;
    for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> e:
        this.values.entrySet()) {
      keyMatcher =
          HConstants.CP_HTD_ATTR_KEY_PATTERN.matcher(
              Bytes.toString(e.getKey().get()));
      if (!keyMatcher.matches()) {
        continue;
      }
      String className = getCoprocessorClassNameFromSpecStr(Bytes.toString(e.getValue().get()));
      if (className == null) continue;
      if (className.equals(classNameToMatch.trim())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Return the list of attached co-processor represented by their name className
   *
   * @return The list of co-processors classNames
   */
  public List<String> getCoprocessors() {
    List<String> result = new ArrayList<String>();
    Matcher keyMatcher;
    Matcher valueMatcher;
    for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> e : this.values.entrySet()) {
      keyMatcher = HConstants.CP_HTD_ATTR_KEY_PATTERN.matcher(Bytes.toString(e.getKey().get()));
      if (!keyMatcher.matches()) {
        continue;
      }
      String className = getCoprocessorClassNameFromSpecStr(Bytes.toString(e.getValue().get()));
      if (className == null) continue;
      result.add(className); // classname is the 2nd field
    }
    return result;
  }

  /**
   * @param spec String formatted as per {@link HConstants#CP_HTD_ATTR_VALUE_PATTERN}
   * @return Class parsed from passed in <code>spec</code> or null if no match or classpath found
   */
  private static String getCoprocessorClassNameFromSpecStr(final String spec) {
    Matcher matcher = HConstants.CP_HTD_ATTR_VALUE_PATTERN.matcher(spec);
    // Classname is the 2nd field
    return matcher != null && matcher.matches()? matcher.group(2).trim(): null;
  }

  /**
   * Remove a coprocessor from those set on the table
   * @param className Class name of the co-processor
   */
  public void removeCoprocessor(String className) {
    ImmutableBytesWritable match = null;
    Matcher keyMatcher;
    Matcher valueMatcher;
    for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> e : this.values
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
    if (match != null)
      remove(match);
  }

  /**
   * Returns the {@link Path} object representing the table directory under
   * path rootdir
   *
   * Deprecated use FSUtils.getTableDir() instead.
   *
   * @param rootdir qualified path of HBase root directory
   * @param tableName name of table
   * @return {@link Path} for table
   */
  @Deprecated
  public static Path getTableDir(Path rootdir, final byte [] tableName) {
    //This is bad I had to mirror code from FSUTils.getTableDir since
    //there is no module dependency between hbase-client and hbase-server
    TableName name = TableName.valueOf(tableName);
    return new Path(rootdir, new Path(HConstants.BASE_NAMESPACE_DIR,
              new Path(name.getNamespaceAsString(), new Path(name.getQualifierAsString()))));
  }

  /**
   * Table descriptor for <code>hbase:meta</code> catalog table
   * @deprecated Use TableDescriptors#get(TableName.META_TABLE_NAME) or
   * HBaseAdmin#getTableDescriptor(TableName.META_TABLE_NAME) instead.
   */
  @Deprecated
  public static final HTableDescriptor META_TABLEDESC = new HTableDescriptor(
      TableName.META_TABLE_NAME,
      new HColumnDescriptor[] {
          new HColumnDescriptor(HConstants.CATALOG_FAMILY)
              // Ten is arbitrary number.  Keep versions to help debugging.
              .setMaxVersions(HConstants.DEFAULT_HBASE_META_VERSIONS)
              .setInMemory(true)
              .setBlocksize(HConstants.DEFAULT_HBASE_META_BLOCK_SIZE)
              .setScope(HConstants.REPLICATION_SCOPE_LOCAL)
              // Disable blooms for meta.  Needs work.  Seems to mess w/ getClosestOrBefore.
              .setBloomFilterType(BloomType.NONE)
              // Enable cache of data blocks in L1 if more than one caching tier deployed:
              // e.g. if using CombinedBlockCache (BucketCache).
              .setCacheDataInL1(true)
      });

  static {
    try {
      META_TABLEDESC.addCoprocessor(
          "org.apache.hadoop.hbase.coprocessor.MultiRowMutationEndpoint",
          null, Coprocessor.PRIORITY_SYSTEM, null);
    } catch (IOException ex) {
      //LOG.warn("exception in loading coprocessor for the hbase:meta table");
      throw new RuntimeException(ex);
    }
  }

  public final static String NAMESPACE_FAMILY_INFO = "info";
  public final static byte[] NAMESPACE_FAMILY_INFO_BYTES = Bytes.toBytes(NAMESPACE_FAMILY_INFO);
  public final static byte[] NAMESPACE_COL_DESC_BYTES = Bytes.toBytes("d");

  /** Table descriptor for namespace table */
  public static final HTableDescriptor NAMESPACE_TABLEDESC = new HTableDescriptor(
      TableName.NAMESPACE_TABLE_NAME,
      new HColumnDescriptor[] {
          new HColumnDescriptor(NAMESPACE_FAMILY_INFO)
              // Ten is arbitrary number.  Keep versions to help debugging.
              .setMaxVersions(10)
              .setInMemory(true)
              .setBlocksize(8 * 1024)
              .setScope(HConstants.REPLICATION_SCOPE_LOCAL)
              // Enable cache of data blocks in L1 if more than one caching tier deployed:
              // e.g. if using CombinedBlockCache (BucketCache).
              .setCacheDataInL1(true)
      });

  @Deprecated
  public HTableDescriptor setOwner(User owner) {
    return setOwnerString(owner != null ? owner.getShortName() : null);
  }

  // used by admin.rb:alter(table_name,*args) to update owner.
  @Deprecated
  public HTableDescriptor setOwnerString(String ownerString) {
    if (ownerString != null) {
      setValue(OWNER_KEY, ownerString);
    } else {
      remove(OWNER_KEY);
    }
    return this;
  }

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

  /**
   * @return This instance serialized with pb with pb magic prefix
   * @see #parseFrom(byte[])
   */
  public byte [] toByteArray() {
    return ProtobufUtil.prependPBMagic(convert().toByteArray());
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
    if (!ProtobufUtil.isPBMagicPrefix(bytes)) {
      return (HTableDescriptor)Writables.getWritable(bytes, new HTableDescriptor());
    }
    int pblen = ProtobufUtil.lengthOfPBMagic();
    TableSchema.Builder builder = TableSchema.newBuilder();
    TableSchema ts;
    try {
      ProtobufUtil.mergeFrom(builder, bytes, pblen, bytes.length - pblen);
      ts = builder.build();
    } catch (IOException e) {
      throw new DeserializationException(e);
    }
    return convert(ts);
  }

  /**
   * @return Convert the current {@link HTableDescriptor} into a pb TableSchema instance.
   */
  public TableSchema convert() {
    TableSchema.Builder builder = TableSchema.newBuilder();
    builder.setTableName(ProtobufUtil.toProtoTableName(getTableName()));
    for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> e: this.values.entrySet()) {
      BytesBytesPair.Builder aBuilder = BytesBytesPair.newBuilder();
      aBuilder.setFirst(ByteStringer.wrap(e.getKey().get()));
      aBuilder.setSecond(ByteStringer.wrap(e.getValue().get()));
      builder.addAttributes(aBuilder.build());
    }
    for (HColumnDescriptor hcd: getColumnFamilies()) {
      builder.addColumnFamilies(hcd.convert());
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
   * @param ts A pb TableSchema instance.
   * @return An {@link HTableDescriptor} made from the passed in pb <code>ts</code>.
   */
  public static HTableDescriptor convert(final TableSchema ts) {
    List<ColumnFamilySchema> list = ts.getColumnFamiliesList();
    HColumnDescriptor [] hcds = new HColumnDescriptor[list.size()];
    int index = 0;
    for (ColumnFamilySchema cfs: list) {
      hcds[index++] = HColumnDescriptor.convert(cfs);
    }
    HTableDescriptor htd = new HTableDescriptor(
        ProtobufUtil.toTableName(ts.getTableName()),
        hcds);
    for (BytesBytesPair a: ts.getAttributesList()) {
      htd.setValue(a.getFirst().toByteArray(), a.getSecond().toByteArray());
    }
    for (NameStringPair a: ts.getConfigurationList()) {
      htd.setConfiguration(a.getName(), a.getValue());
    }
    return htd;
  }

  /**
   * Getter for accessing the configuration value by key
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
   * @param value String value. If null, removes the setting.
   */
  public HTableDescriptor setConfiguration(String key, String value) {
    if (value == null) {
      removeConfiguration(key);
    } else {
      configuration.put(key, value);
    }
    return this;
  }

  /**
   * Remove a config setting represented by the key from the {@link #configuration} map
   */
  public void removeConfiguration(final String key) {
    configuration.remove(key);
  }

  public static HTableDescriptor metaTableDescriptor(final Configuration conf)
      throws IOException {
    HTableDescriptor metaDescriptor = new HTableDescriptor(
      TableName.META_TABLE_NAME,
      new HColumnDescriptor[] {
        new HColumnDescriptor(HConstants.CATALOG_FAMILY)
          .setMaxVersions(conf.getInt(HConstants.HBASE_META_VERSIONS,
            HConstants.DEFAULT_HBASE_META_VERSIONS))
          .setInMemory(true)
          .setBlocksize(conf.getInt(HConstants.HBASE_META_BLOCK_SIZE,
            HConstants.DEFAULT_HBASE_META_BLOCK_SIZE))
          .setScope(HConstants.REPLICATION_SCOPE_LOCAL)
          // Disable blooms for meta.  Needs work.  Seems to mess w/ getClosestOrBefore.
          .setBloomFilterType(BloomType.NONE)
          .setCacheDataInL1(true)
         });
    metaDescriptor.addCoprocessor(
      "org.apache.hadoop.hbase.coprocessor.MultiRowMutationEndpoint",
      null, Coprocessor.PRIORITY_SYSTEM, null);
    return metaDescriptor;
  }

}
