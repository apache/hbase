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

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * TableDescriptor contains the details about an HBase table such as the descriptors of
 * all the column families, is the table a catalog table, <code> -ROOT- </code> or
 * <code> hbase:meta </code>, if the table is read only, the maximum size of the memstore,
 * when the region split should occur, coprocessors associated with it etc...
 */
@InterfaceAudience.Public
public interface TableDescriptor {

  /**
   * Returns an array all the {@link HColumnDescriptor} of the column families
   * of the table.
   *
   * @return Array of all the HColumnDescriptors of the current table
   *
   * @see #getFamilies()
   */
  HColumnDescriptor[] getColumnFamilies();

  /**
   * Returns the count of the column families of the table.
   *
   * @return Count of column families of the table
   */
  int getColumnFamilyCount();

  /**
   * Getter for fetching an unmodifiable map.
   *
   * @return an unmodifiable map
   */
  Map<String, String> getConfiguration();

  /**
   * Getter for accessing the configuration value by key
   *
   * @param key the key whose associated value is to be returned
   * @return the value to which the specified key is mapped, or {@code null} if
   * this map contains no mapping for the key
   */
  String getConfigurationValue(String key);

  /**
   * Return the list of attached co-processor represented by their name
   * className
   *
   * @return The list of co-processors classNames
   */
  Collection<String> getCoprocessors();

  /**
   * Returns the durability setting for the table.
   *
   * @return durability setting for the table.
   */
  Durability getDurability();

  /**
   * Returns an unmodifiable collection of all the {@link HColumnDescriptor} of
   * all the column families of the table.
   *
   * @return Immutable collection of {@link HColumnDescriptor} of all the column
   * families.
   */
  Collection<HColumnDescriptor> getFamilies();

  /**
   * Returns all the column family names of the current table. The map of
   * TableDescriptor contains mapping of family name to HColumnDescriptors.
   * This returns all the keys of the family map which represents the column
   * family names of the table.
   *
   * @return Immutable sorted set of the keys of the families.
   */
  Set<byte[]> getFamiliesKeys();

  /**
   * Returns the HColumnDescriptor for a specific column family with name as
   * specified by the parameter column.
   *
   * @param column Column family name
   * @return Column descriptor for the passed family name or the family on
   * passed in column.
   */
  HColumnDescriptor getFamily(final byte[] column);

  /**
   * This gets the class associated with the flush policy which determines the
   * stores need to be flushed when flushing a region. The class used by default
   * is defined in org.apache.hadoop.hbase.regionserver.FlushPolicy.
   *
   * @return the class name of the flush policy for this table. If this returns
   * null, the default flush policy is used.
   */
  String getFlushPolicyClassName();

  /**
   * Returns the maximum size upto which a region can grow to after which a
   * region split is triggered. The region size is represented by the size of
   * the biggest store file in that region.
   *
   * @return max hregion size for table, -1 if not set.
   */
  long getMaxFileSize();

  /**
   * Returns the size of the memstore after which a flush to filesystem is
   * triggered.
   *
   * @return memory cache flush size for each hregion, -1 if not set.
   */
  long getMemStoreFlushSize();

  int getPriority();

  /**
   * @return Returns the configured replicas per region
   */
  int getRegionReplication();

  /**
   * This gets the class associated with the region split policy which
   * determines when a region split should occur. The class used by default is
   * defined in org.apache.hadoop.hbase.regionserver.RegionSplitPolicy
   *
   * @return the class name of the region split policy for this table. If this
   * returns null, the default split policy is used.
   */
  String getRegionSplitPolicyClassName();

  /**
   * Get the name of the table
   *
   * @return TableName
   */
  TableName getTableName();

  @Deprecated
  String getOwnerString();

  /**
   * Getter for accessing the metadata associated with the key
   *
   * @param key The key.
   * @return The value.
   */
  byte[] getValue(byte[] key);

  /**
   * @return Getter for fetching an unmodifiable map.
   */
  Map<Bytes, Bytes> getValues();

  /**
   * Check if the table has an attached co-processor represented by the name
   * className
   *
   * @param classNameToMatch - Class name of the co-processor
   * @return true of the table has a co-processor className
   */
  boolean hasCoprocessor(String classNameToMatch);

  /**
   * Checks to see if this table contains the given column family
   *
   * @param familyName Family name or column name.
   * @return true if the table contains the specified family name
   */
  boolean hasFamily(final byte[] familyName);

  /**
   * @return true if the read-replicas memstore replication is enabled.
   */
  boolean hasRegionMemstoreReplication();

  /**
   * @return true if there are at least one cf whose replication scope is
   * serial.
   */
  boolean hasSerialReplicationScope();

  /**
   * Check if the compaction enable flag of the table is true. If flag is false
   * then no minor/major compactions will be done in real.
   *
   * @return true if table compaction enabled
   */
  boolean isCompactionEnabled();

  /**
   * Checks if this table is <code> hbase:meta </code> region.
   *
   * @return true if this table is <code> hbase:meta </code> region
   */
  boolean isMetaRegion();

  /**
   * Checks if the table is a <code>hbase:meta</code> table
   *
   * @return true if table is <code> hbase:meta </code> region.
   */
  boolean isMetaTable();

  /**
   * Check if normalization enable flag of the table is true. If flag is false
   * then no region normalizer won't attempt to normalize this table.
   *
   * @return true if region normalization is enabled for this table
   */
  boolean isNormalizationEnabled();

  /**
   * Check if the readOnly flag of the table is set. If the readOnly flag is set
   * then the contents of the table can only be read from but not modified.
   *
   * @return true if all columns in the table should be read only
   */
  boolean isReadOnly();

  /**
   * Check if the descriptor represents a <code> -ROOT- </code> region.
   *
   * @return true if this is a <code> -ROOT- </code> region
   */
  boolean isRootRegion();

}
