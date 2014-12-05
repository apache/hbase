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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScannable;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.security.access.AccessControlConstants;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.visibility.CellVisibility;
import org.apache.hadoop.hbase.security.visibility.VisibilityConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class Mutation extends OperationWithAttributes implements Row, CellScannable,
    HeapSize {
  public static final long MUTATION_OVERHEAD = ClassSize.align(
      // This
      ClassSize.OBJECT +
      // row + OperationWithAttributes.attributes
      2 * ClassSize.REFERENCE +
      // Timestamp
      1 * Bytes.SIZEOF_LONG +
      // durability
      ClassSize.REFERENCE +
      // familyMap
      ClassSize.REFERENCE +
      // familyMap
      ClassSize.TREEMAP);

  /**
   * The attribute for storing the list of clusters that have consumed the change.
   */
  private static final String CONSUMED_CLUSTER_IDS = "_cs.id";

  /**
   * The attribute for storing TTL for the result of the mutation.
   */
  private static final String OP_ATTRIBUTE_TTL = "_ttl";

  protected byte [] row = null;
  protected long ts = HConstants.LATEST_TIMESTAMP;
  protected Durability durability = Durability.USE_DEFAULT;

  // A Map sorted by column family.
  protected NavigableMap<byte [], List<Cell>> familyMap =
    new TreeMap<byte [], List<Cell>>(Bytes.BYTES_COMPARATOR);

  @Override
  public CellScanner cellScanner() {
    return CellUtil.createCellScanner(getFamilyCellMap());
  }

  /**
   * Creates an empty list if one doesn't exist for the given column family
   * or else it returns the associated list of Cell objects.
   *
   * @param family column family
   * @return a list of Cell objects, returns an empty list if one doesn't exist.
   */
  List<Cell> getCellList(byte[] family) {
    List<Cell> list = this.familyMap.get(family);
    if (list == null) {
      list = new ArrayList<Cell>();
    }
    return list;
  }

  /*
   * Create a KeyValue with this objects row key and the Put identifier.
   *
   * @return a KeyValue with this objects row key and the Put identifier.
   */
  KeyValue createPutKeyValue(byte[] family, byte[] qualifier, long ts, byte[] value) {
    return new KeyValue(this.row, family, qualifier, ts, KeyValue.Type.Put, value);
  }

  /**
   * Create a KeyValue with this objects row key and the Put identifier.
   * @param family
   * @param qualifier
   * @param ts
   * @param value
   * @param tags - Specify the Tags as an Array {@link KeyValue.Tag}
   * @return a KeyValue with this objects row key and the Put identifier.
   */
  KeyValue createPutKeyValue(byte[] family, byte[] qualifier, long ts, byte[] value, Tag[] tags) {
    KeyValue kvWithTag = new KeyValue(this.row, family, qualifier, ts, value, tags);
    return kvWithTag;
  }

  /*
   * Create a KeyValue with this objects row key and the Put identifier.
   *
   * @return a KeyValue with this objects row key and the Put identifier.
   */
  KeyValue createPutKeyValue(byte[] family, ByteBuffer qualifier, long ts, ByteBuffer value,
                             Tag[] tags) {
    return new KeyValue(this.row, 0, this.row == null ? 0 : this.row.length,
        family, 0, family == null ? 0 : family.length,
        qualifier, ts, KeyValue.Type.Put, value, tags != null ? Arrays.asList(tags) : null);
  }

  /**
   * Compile the column family (i.e. schema) information
   * into a Map. Useful for parsing and aggregation by debugging,
   * logging, and administration tools.
   * @return Map
   */
  @Override
  public Map<String, Object> getFingerprint() {
    Map<String, Object> map = new HashMap<String, Object>();
    List<String> families = new ArrayList<String>();
    // ideally, we would also include table information, but that information
    // is not stored in each Operation instance.
    map.put("families", families);
    for (Map.Entry<byte [], List<Cell>> entry : this.familyMap.entrySet()) {
      families.add(Bytes.toStringBinary(entry.getKey()));
    }
    return map;
  }

  /**
   * Compile the details beyond the scope of getFingerprint (row, columns,
   * timestamps, etc.) into a Map along with the fingerprinted information.
   * Useful for debugging, logging, and administration tools.
   * @param maxCols a limit on the number of columns output prior to truncation
   * @return Map
   */
  @Override
  public Map<String, Object> toMap(int maxCols) {
    // we start with the fingerprint map and build on top of it.
    Map<String, Object> map = getFingerprint();
    // replace the fingerprint's simple list of families with a
    // map from column families to lists of qualifiers and kv details
    Map<String, List<Map<String, Object>>> columns =
      new HashMap<String, List<Map<String, Object>>>();
    map.put("families", columns);
    map.put("row", Bytes.toStringBinary(this.row));
    int colCount = 0;
    // iterate through all column families affected
    for (Map.Entry<byte [], List<Cell>> entry : this.familyMap.entrySet()) {
      // map from this family to details for each cell affected within the family
      List<Map<String, Object>> qualifierDetails = new ArrayList<Map<String, Object>>();
      columns.put(Bytes.toStringBinary(entry.getKey()), qualifierDetails);
      colCount += entry.getValue().size();
      if (maxCols <= 0) {
        continue;
      }
      // add details for each cell
      for (Cell cell: entry.getValue()) {
        if (--maxCols <= 0 ) {
          continue;
        }
        Map<String, Object> cellMap = cellToStringMap(cell);
        qualifierDetails.add(cellMap);
      }
    }
    map.put("totalColumns", colCount);
    // add the id if set
    if (getId() != null) {
      map.put("id", getId());
    }
    // Add the TTL if set
    // Long.MAX_VALUE is the default, and is interpreted to mean this attribute
    // has not been set.
    if (getTTL() != Long.MAX_VALUE) {
      map.put("ttl", getTTL());
    }
    return map;
  }

  private static Map<String, Object> cellToStringMap(Cell c) {
    Map<String, Object> stringMap = new HashMap<String, Object>();
    stringMap.put("qualifier", Bytes.toStringBinary(c.getQualifierArray(), c.getQualifierOffset(),
                c.getQualifierLength()));
    stringMap.put("timestamp", c.getTimestamp());
    stringMap.put("vlen", c.getValueLength());
    List<Tag> tags = Tag.asList(c.getTagsArray(), c.getTagsOffset(), c.getTagsLength());
    if (tags != null) {
      List<String> tagsString = new ArrayList<String>();
      for (Tag t : tags) {
        tagsString.add((t.getType()) + ":" + Bytes.toStringBinary(t.getValue()));
      }
      stringMap.put("tag", tagsString);
    }
    return stringMap;
  }

  /**
   * Set the durability for this mutation
   * @param d
   */
  public Mutation setDurability(Durability d) {
    this.durability = d;
    return this;
  }

  /** Get the current durability */
  public Durability getDurability() {
    return this.durability;
  }

  /**
   * Method for retrieving the put's familyMap
   * @return familyMap
   */
  public NavigableMap<byte [], List<Cell>> getFamilyCellMap() {
    return this.familyMap;
  }

  /**
   * Method for setting the put's familyMap
   */
  public Mutation setFamilyCellMap(NavigableMap<byte [], List<Cell>> map) {
    // TODO: Shut this down or move it up to be a Constructor.  Get new object rather than change
    // this internal data member.
    this.familyMap = map;
    return this;
  }

  /**
   * Method to check if the familyMap is empty
   * @return true if empty, false otherwise
   */
  public boolean isEmpty() {
    return familyMap.isEmpty();
  }

  /**
   * Method for retrieving the delete's row
   * @return row
   */
  @Override
  public byte [] getRow() {
    return this.row;
  }

  @Override
  public int compareTo(final Row d) {
    return Bytes.compareTo(this.getRow(), d.getRow());
  }

  /**
   * Method for retrieving the timestamp
   * @return timestamp
   */
  public long getTimeStamp() {
    return this.ts;
  }

  /**
   * Marks that the clusters with the given clusterIds have consumed the mutation
   * @param clusterIds of the clusters that have consumed the mutation
   */
  public Mutation setClusterIds(List<UUID> clusterIds) {
    ByteArrayDataOutput out = ByteStreams.newDataOutput();
    out.writeInt(clusterIds.size());
    for (UUID clusterId : clusterIds) {
      out.writeLong(clusterId.getMostSignificantBits());
      out.writeLong(clusterId.getLeastSignificantBits());
    }
    setAttribute(CONSUMED_CLUSTER_IDS, out.toByteArray());
    return this;
  }

  /**
   * @return the set of clusterIds that have consumed the mutation
   */
  public List<UUID> getClusterIds() {
    List<UUID> clusterIds = new ArrayList<UUID>();
    byte[] bytes = getAttribute(CONSUMED_CLUSTER_IDS);
    if(bytes != null) {
      ByteArrayDataInput in = ByteStreams.newDataInput(bytes);
      int numClusters = in.readInt();
      for(int i=0; i<numClusters; i++){
        clusterIds.add(new UUID(in.readLong(), in.readLong()));
      }
    }
    return clusterIds;
  }

  /**
   * Sets the visibility expression associated with cells in this Mutation.
   * It is illegal to set <code>CellVisibility</code> on <code>Delete</code> mutation.
   * @param expression
   */
  public Mutation setCellVisibility(CellVisibility expression) {
    this.setAttribute(VisibilityConstants.VISIBILITY_LABELS_ATTR_KEY, ProtobufUtil
        .toCellVisibility(expression).toByteArray());
    return this;
  }

  /**
   * @return CellVisibility associated with cells in this Mutation.
   * @throws DeserializationException
   */
  public CellVisibility getCellVisibility() throws DeserializationException {
    byte[] cellVisibilityBytes = this.getAttribute(VisibilityConstants.VISIBILITY_LABELS_ATTR_KEY);
    if (cellVisibilityBytes == null) return null;
    return ProtobufUtil.toCellVisibility(cellVisibilityBytes);
  }

  /**
   * Number of KeyValues carried by this Mutation.
   * @return the total number of KeyValues
   */
  public int size() {
    int size = 0;
    for (List<Cell> cells : this.familyMap.values()) {
      size += cells.size();
    }
    return size;
  }

  /**
   * @return the number of different families
   */
  public int numFamilies() {
    return familyMap.size();
  }

  /**
   * @return Calculate what Mutation adds to class heap size.
   */
  @Override
  public long heapSize() {
    long heapsize = MUTATION_OVERHEAD;
    // Adding row
    heapsize += ClassSize.align(ClassSize.ARRAY + this.row.length);

    // Adding map overhead
    heapsize +=
      ClassSize.align(this.familyMap.size() * ClassSize.MAP_ENTRY);
    for(Map.Entry<byte [], List<Cell>> entry : this.familyMap.entrySet()) {
      //Adding key overhead
      heapsize +=
        ClassSize.align(ClassSize.ARRAY + entry.getKey().length);

      //This part is kinds tricky since the JVM can reuse references if you
      //store the same value, but have a good match with SizeOf at the moment
      //Adding value overhead
      heapsize += ClassSize.align(ClassSize.ARRAYLIST);
      int size = entry.getValue().size();
      heapsize += ClassSize.align(ClassSize.ARRAY +
          size * ClassSize.REFERENCE);

      for(Cell cell : entry.getValue()) {
        heapsize += CellUtil.estimatedHeapSizeOf(cell);
      }
    }
    heapsize += getAttributeSize();
    heapsize += extraHeapSize();
    return ClassSize.align(heapsize);
  }

  /**
   * @return The serialized ACL for this operation, or null if none
   */
  public byte[] getACL() {
    return getAttribute(AccessControlConstants.OP_ATTRIBUTE_ACL);
  }

  /**
   * @param user User short name
   * @param perms Permissions for the user
   */
  public Mutation setACL(String user, Permission perms) {
    setAttribute(AccessControlConstants.OP_ATTRIBUTE_ACL,
      ProtobufUtil.toUsersAndPermissions(user, perms).toByteArray());
    return this;
  }

  /**
   * @param perms A map of permissions for a user or users
   */
  public Mutation setACL(Map<String, Permission> perms) {
    ListMultimap<String, Permission> permMap = ArrayListMultimap.create();
    for (Map.Entry<String, Permission> entry : perms.entrySet()) {
      permMap.put(entry.getKey(), entry.getValue());
    }
    setAttribute(AccessControlConstants.OP_ATTRIBUTE_ACL,
      ProtobufUtil.toUsersAndPermissions(permMap).toByteArray());
    return this;
  }

  /**
   * Return the TTL requested for the result of the mutation, in milliseconds.
   * @return the TTL requested for the result of the mutation, in milliseconds,
   * or Long.MAX_VALUE if unset
   */
  public long getTTL() {
    byte[] ttlBytes = getAttribute(OP_ATTRIBUTE_TTL);
    if (ttlBytes != null) {
      return Bytes.toLong(ttlBytes);
    }
    return Long.MAX_VALUE;
  }

  /**
   * Set the TTL desired for the result of the mutation, in milliseconds.
   * @param ttl the TTL desired for the result of the mutation, in milliseconds
   * @return this
   */
  public Mutation setTTL(long ttl) {
    setAttribute(OP_ATTRIBUTE_TTL, Bytes.toBytes(ttl));
    return this;
  }

  /**
   * Subclasses should override this method to add the heap size of their own fields.
   * @return the heap size to add (will be aligned).
   */
  protected long extraHeapSize(){
    return 0L;
  }


  /**
   * @param row Row to check
   * @throws IllegalArgumentException Thrown if <code>row</code> is empty or null or
   * &gt; {@link HConstants#MAX_ROW_LENGTH}
   * @return <code>row</code>
   */
  static byte [] checkRow(final byte [] row) {
    return checkRow(row, 0, row == null? 0: row.length);
  }

  /**
   * @param row Row to check
   * @param offset
   * @param length
   * @throws IllegalArgumentException Thrown if <code>row</code> is empty or null or
   * &gt; {@link HConstants#MAX_ROW_LENGTH}
   * @return <code>row</code>
   */
  static byte [] checkRow(final byte [] row, final int offset, final int length) {
    if (row == null) {
      throw new IllegalArgumentException("Row buffer is null");
    }
    if (length == 0) {
      throw new IllegalArgumentException("Row length is 0");
    }
    if (length > HConstants.MAX_ROW_LENGTH) {
      throw new IllegalArgumentException("Row length " + length + " is > " +
        HConstants.MAX_ROW_LENGTH);
    }
    return row;
  }

  static void checkRow(ByteBuffer row) {
    if (row == null) {
      throw new IllegalArgumentException("Row buffer is null");
    }
    if (row.remaining() == 0) {
      throw new IllegalArgumentException("Row length is 0");
    }
    if (row.remaining() > HConstants.MAX_ROW_LENGTH) {
      throw new IllegalArgumentException("Row length " + row.remaining() + " is > " +
          HConstants.MAX_ROW_LENGTH);
    }
  }
}
