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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilder;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.ExtendedCellScannable;
import org.apache.hadoop.hbase.ExtendedCellScanner;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.IndividualBytesFieldCell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.security.access.AccessControlConstants;
import org.apache.hadoop.hbase.security.access.AccessControlUtil;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.visibility.CellVisibility;
import org.apache.hadoop.hbase.security.visibility.VisibilityConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.collect.ArrayListMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.ListMultimap;
import org.apache.hbase.thirdparty.com.google.common.io.ByteArrayDataInput;
import org.apache.hbase.thirdparty.com.google.common.io.ByteArrayDataOutput;
import org.apache.hbase.thirdparty.com.google.common.io.ByteStreams;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;

@InterfaceAudience.Public
public abstract class Mutation extends OperationWithAttributes
  implements Row, ExtendedCellScannable, HeapSize {
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
      ClassSize.TREEMAP +
      // priority
      ClassSize.INTEGER);

  /**
   * The attribute for storing the list of clusters that have consumed the change.
   */
  private static final String CONSUMED_CLUSTER_IDS = "_cs.id";

  /**
   * The attribute for storing TTL for the result of the mutation.
   */
  private static final String OP_ATTRIBUTE_TTL = "_ttl";

  private static final String RETURN_RESULTS = "_rr_";

  // TODO: row should be final
  protected byte[] row;
  protected long ts = HConstants.LATEST_TIMESTAMP;
  protected Durability durability = Durability.USE_DEFAULT;

  // A Map sorted by column family.
  protected final NavigableMap<byte[], List<ExtendedCell>> familyMap;

  /**
   * empty construction. We need this empty construction to keep binary compatibility.
   */
  protected Mutation() {
    this.familyMap = new TreeMap<>(Bytes.BYTES_COMPARATOR);
  }

  protected Mutation(Mutation clone) {
    super(clone);
    this.row = clone.getRow();
    this.ts = clone.getTimestamp();
    this.familyMap = clone.familyMap.entrySet().stream()
      .collect(Collectors.toMap(e -> e.getKey(), e -> new ArrayList<>(e.getValue()), (k, v) -> {
        throw new RuntimeException("collisions!!!");
      }, () -> new TreeMap<>(Bytes.BYTES_COMPARATOR)));
  }

  /**
   * Construct the mutation with user defined data.
   * @param row       row. CAN'T be null
   * @param ts        timestamp
   * @param familyMap the map to collect all cells internally. CAN'T be null
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  protected Mutation(byte[] row, long ts, NavigableMap<byte[], List<Cell>> familyMap) {
    this.row = Preconditions.checkNotNull(row);
    Preconditions.checkArgument(row.length > 0, "Row can't be empty");
    this.ts = ts;
    // We do not allow other Cell types in HBase so here we just do a simple cast
    this.familyMap = (NavigableMap) Preconditions.checkNotNull(familyMap);
  }

  /**
   * For client users: You should only use the return value as a
   * {@link org.apache.hadoop.hbase.CellScanner}, {@link ExtendedCellScanner} is marked as
   * IA.Private which means there is no guarantee about its API stability.
   */
  @Override
  public ExtendedCellScanner cellScanner() {
    return PrivateCellUtil.createExtendedCellScanner(familyMap);
  }

  /**
   * Creates an empty list if one doesn't exist for the given column family or else it returns the
   * associated list of Cell objects.
   * @param family column family
   * @return a list of Cell objects, returns an empty list if one doesn't exist.
   */
  List<ExtendedCell> getCellList(byte[] family) {
    return familyMap.computeIfAbsent(family, k -> new ArrayList<>());
  }

  /**
   * Create a KeyValue with this objects row key and the Put identifier.
   * @return a KeyValue with this objects row key and the Put identifier.
   */
  KeyValue createPutKeyValue(byte[] family, byte[] qualifier, long ts, byte[] value) {
    return new KeyValue(this.row, family, qualifier, ts, KeyValue.Type.Put, value);
  }

  /**
   * Create a KeyValue with this objects row key and the Put identifier.
   * @return a KeyValue with this objects row key and the Put identifier.
   */
  KeyValue createPutKeyValue(byte[] family, byte[] qualifier, long ts, byte[] value, Tag[] tags) {
    KeyValue kvWithTag = new KeyValue(this.row, family, qualifier, ts, value, tags);
    return kvWithTag;
  }

  /**
   * Create a KeyValue with this objects row key and the Put identifier.
   * @return a KeyValue with this objects row key and the Put identifier.
   */
  KeyValue createPutKeyValue(byte[] family, ByteBuffer qualifier, long ts, ByteBuffer value,
    Tag[] tags) {
    return new KeyValue(this.row, 0, this.row == null ? 0 : this.row.length, family, 0,
      family == null ? 0 : family.length, qualifier, ts, KeyValue.Type.Put, value,
      tags != null ? Arrays.asList(tags) : null);
  }

  /**
   * Compile the column family (i.e. schema) information into a Map. Useful for parsing and
   * aggregation by debugging, logging, and administration tools.
   */
  @Override
  public Map<String, Object> getFingerprint() {
    Map<String, Object> map = new HashMap<>();
    List<String> families = new ArrayList<>(getFamilyCellMap().entrySet().size());
    // ideally, we would also include table information, but that information
    // is not stored in each Operation instance.
    map.put("families", families);
    for (Map.Entry<byte[], List<Cell>> entry : getFamilyCellMap().entrySet()) {
      families.add(Bytes.toStringBinary(entry.getKey()));
    }
    return map;
  }

  /**
   * Compile the details beyond the scope of getFingerprint (row, columns, timestamps, etc.) into a
   * Map along with the fingerprinted information. Useful for debugging, logging, and administration
   * tools.
   * @param maxCols a limit on the number of columns output prior to truncation
   */
  @Override
  public Map<String, Object> toMap(int maxCols) {
    // we start with the fingerprint map and build on top of it.
    Map<String, Object> map = getFingerprint();
    // replace the fingerprint's simple list of families with a
    // map from column families to lists of qualifiers and kv details
    Map<String, List<Map<String, Object>>> columns = new HashMap<>();
    map.put("families", columns);
    map.put("row", Bytes.toStringBinary(this.row));
    int colCount = 0;
    // iterate through all column families affected
    for (Map.Entry<byte[], List<ExtendedCell>> entry : familyMap.entrySet()) {
      // map from this family to details for each cell affected within the family
      List<Map<String, Object>> qualifierDetails = new ArrayList<>();
      columns.put(Bytes.toStringBinary(entry.getKey()), qualifierDetails);
      colCount += entry.getValue().size();
      if (maxCols <= 0) {
        continue;
      }
      // add details for each cell
      for (ExtendedCell cell : entry.getValue()) {
        if (--maxCols <= 0) {
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
    map.put("ts", this.ts);
    return map;
  }

  private static Map<String, Object> cellToStringMap(ExtendedCell c) {
    Map<String, Object> stringMap = new HashMap<>();
    stringMap.put("qualifier",
      Bytes.toStringBinary(c.getQualifierArray(), c.getQualifierOffset(), c.getQualifierLength()));
    stringMap.put("timestamp", c.getTimestamp());
    stringMap.put("vlen", c.getValueLength());
    List<Tag> tags = PrivateCellUtil.getTags(c);
    if (tags != null) {
      List<String> tagsString = new ArrayList<>(tags.size());
      for (Tag t : tags) {
        tagsString.add(t.getType() + ":" + Bytes.toStringBinary(Tag.cloneValue(t)));
      }
      stringMap.put("tag", tagsString);
    }
    return stringMap;
  }

  /**
   * Set the durability for this mutation
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
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public NavigableMap<byte[], List<Cell>> getFamilyCellMap() {
    return (NavigableMap) this.familyMap;
  }

  /**
   * Method to check if the familyMap is empty
   * @return true if empty, false otherwise
   */
  public boolean isEmpty() {
    return getFamilyCellMap().isEmpty();
  }

  /**
   * Method for retrieving the delete's row
   */
  @Override
  public byte[] getRow() {
    return this.row;
  }

  /**
   * Method for retrieving the timestamp.
   */
  public long getTimestamp() {
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

  /** Returns the set of clusterIds that have consumed the mutation */
  public List<UUID> getClusterIds() {
    List<UUID> clusterIds = new ArrayList<>();
    byte[] bytes = getAttribute(CONSUMED_CLUSTER_IDS);
    if (bytes != null) {
      ByteArrayDataInput in = ByteStreams.newDataInput(bytes);
      int numClusters = in.readInt();
      for (int i = 0; i < numClusters; i++) {
        clusterIds.add(new UUID(in.readLong(), in.readLong()));
      }
    }
    return clusterIds;
  }

  /**
   * Sets the visibility expression associated with cells in this Mutation.
   */
  public Mutation setCellVisibility(CellVisibility expression) {
    this.setAttribute(VisibilityConstants.VISIBILITY_LABELS_ATTR_KEY,
      toCellVisibility(expression).toByteArray());
    return this;
  }

  /** Returns CellVisibility associated with cells in this Mutation. n */
  public CellVisibility getCellVisibility() throws DeserializationException {
    byte[] cellVisibilityBytes = this.getAttribute(VisibilityConstants.VISIBILITY_LABELS_ATTR_KEY);
    if (cellVisibilityBytes == null) return null;
    return toCellVisibility(cellVisibilityBytes);
  }

  /**
   * Create a protocol buffer CellVisibility based on a client CellVisibility.
   * @return a protocol buffer CellVisibility
   */
  static ClientProtos.CellVisibility toCellVisibility(CellVisibility cellVisibility) {
    ClientProtos.CellVisibility.Builder builder = ClientProtos.CellVisibility.newBuilder();
    builder.setExpression(cellVisibility.getExpression());
    return builder.build();
  }

  /**
   * Convert a protocol buffer CellVisibility to a client CellVisibility
   * @return the converted client CellVisibility
   */
  private static CellVisibility toCellVisibility(ClientProtos.CellVisibility proto) {
    if (proto == null) return null;
    return new CellVisibility(proto.getExpression());
  }

  /**
   * Convert a protocol buffer CellVisibility bytes to a client CellVisibility
   * @return the converted client CellVisibility
   */
  private static CellVisibility toCellVisibility(byte[] protoBytes)
    throws DeserializationException {
    if (protoBytes == null) return null;
    ClientProtos.CellVisibility.Builder builder = ClientProtos.CellVisibility.newBuilder();
    ClientProtos.CellVisibility proto = null;
    try {
      ProtobufUtil.mergeFrom(builder, protoBytes);
      proto = builder.build();
    } catch (IOException e) {
      throw new DeserializationException(e);
    }
    return toCellVisibility(proto);
  }

  /**
   * Number of KeyValues carried by this Mutation.
   * @return the total number of KeyValues
   */
  public int size() {
    int size = 0;
    for (List<Cell> cells : getFamilyCellMap().values()) {
      size += cells.size();
    }
    return size;
  }

  /** Returns the number of different families */
  public int numFamilies() {
    return getFamilyCellMap().size();
  }

  /** Returns Calculate what Mutation adds to class heap size. */
  @Override
  public long heapSize() {
    long heapsize = MUTATION_OVERHEAD;
    // Adding row
    heapsize += ClassSize.align(ClassSize.ARRAY + this.row.length);

    // Adding map overhead
    heapsize += ClassSize.align(getFamilyCellMap().size() * ClassSize.MAP_ENTRY);
    for (Map.Entry<byte[], List<Cell>> entry : getFamilyCellMap().entrySet()) {
      // Adding key overhead
      heapsize += ClassSize.align(ClassSize.ARRAY + entry.getKey().length);

      // This part is kinds tricky since the JVM can reuse references if you
      // store the same value, but have a good match with SizeOf at the moment
      // Adding value overhead
      heapsize += ClassSize.align(ClassSize.ARRAYLIST);
      int size = entry.getValue().size();
      heapsize += ClassSize.align(ClassSize.ARRAY + size * ClassSize.REFERENCE);

      for (Cell cell : entry.getValue()) {
        heapsize += cell.heapSize();
      }
    }
    heapsize += getAttributeSize();
    heapsize += extraHeapSize();
    return ClassSize.align(heapsize);
  }

  /** Returns The serialized ACL for this operation, or null if none */
  public byte[] getACL() {
    return getAttribute(AccessControlConstants.OP_ATTRIBUTE_ACL);
  }

  /**
   * Set the ACL for this operation.
   * @param user  User short name
   * @param perms Permissions for the user
   */
  public Mutation setACL(String user, Permission perms) {
    setAttribute(AccessControlConstants.OP_ATTRIBUTE_ACL,
      AccessControlUtil.toUsersAndPermissions(user, perms).toByteArray());
    return this;
  }

  /**
   * Set the ACL for this operation.
   * @param perms A map of permissions for a user or users
   */
  public Mutation setACL(Map<String, Permission> perms) {
    ListMultimap<String, Permission> permMap = ArrayListMultimap.create();
    for (Map.Entry<String, Permission> entry : perms.entrySet()) {
      permMap.put(entry.getKey(), entry.getValue());
    }
    setAttribute(AccessControlConstants.OP_ATTRIBUTE_ACL,
      AccessControlUtil.toUsersAndPermissions(permMap).toByteArray());
    return this;
  }

  /**
   * Return the TTL requested for the result of the mutation, in milliseconds.
   * @return the TTL requested for the result of the mutation, in milliseconds, or Long.MAX_VALUE if
   *         unset
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
   */
  public Mutation setTTL(long ttl) {
    setAttribute(OP_ATTRIBUTE_TTL, Bytes.toBytes(ttl));
    return this;
  }

  /** Returns current value for returnResults */
  // Used by Increment and Append only.
  @InterfaceAudience.Private
  protected boolean isReturnResults() {
    byte[] v = getAttribute(RETURN_RESULTS);
    return v == null ? true : Bytes.toBoolean(v);
  }

  @InterfaceAudience.Private
  // Used by Increment and Append only.
  protected Mutation setReturnResults(boolean returnResults) {
    setAttribute(RETURN_RESULTS, Bytes.toBytes(returnResults));
    return this;
  }

  /**
   * Subclasses should override this method to add the heap size of their own fields.
   * @return the heap size to add (will be aligned).
   */
  protected long extraHeapSize() {
    return 0L;
  }

  /**
   * Set the timestamp of the delete.
   */
  public Mutation setTimestamp(long timestamp) {
    checkTimestamp(timestamp);
    this.ts = timestamp;
    return this;
  }

  /**
   * A convenience method to determine if this object's familyMap contains a value assigned to the
   * given family &amp; qualifier. Both given arguments must match the KeyValue object to return
   * true.
   * @param family    column family
   * @param qualifier column qualifier
   * @return returns true if the given family and qualifier already has an existing KeyValue object
   *         in the family map.
   */
  public boolean has(byte[] family, byte[] qualifier) {
    return has(family, qualifier, this.ts, HConstants.EMPTY_BYTE_ARRAY, true, true);
  }

  /**
   * A convenience method to determine if this object's familyMap contains a value assigned to the
   * given family, qualifier and timestamp. All 3 given arguments must match the KeyValue object to
   * return true.
   * @param family    column family
   * @param qualifier column qualifier
   * @param ts        timestamp
   * @return returns true if the given family, qualifier and timestamp already has an existing
   *         KeyValue object in the family map.
   */
  public boolean has(byte[] family, byte[] qualifier, long ts) {
    return has(family, qualifier, ts, HConstants.EMPTY_BYTE_ARRAY, false, true);
  }

  /**
   * A convenience method to determine if this object's familyMap contains a value assigned to the
   * given family, qualifier and timestamp. All 3 given arguments must match the KeyValue object to
   * return true.
   * @param family    column family
   * @param qualifier column qualifier
   * @param value     value to check
   * @return returns true if the given family, qualifier and value already has an existing KeyValue
   *         object in the family map.
   */
  public boolean has(byte[] family, byte[] qualifier, byte[] value) {
    return has(family, qualifier, this.ts, value, true, false);
  }

  /**
   * A convenience method to determine if this object's familyMap contains the given value assigned
   * to the given family, qualifier and timestamp. All 4 given arguments must match the KeyValue
   * object to return true.
   * @param family    column family
   * @param qualifier column qualifier
   * @param ts        timestamp
   * @param value     value to check
   * @return returns true if the given family, qualifier timestamp and value already has an existing
   *         KeyValue object in the family map.
   */
  public boolean has(byte[] family, byte[] qualifier, long ts, byte[] value) {
    return has(family, qualifier, ts, value, false, false);
  }

  /**
   * Returns a list of all KeyValue objects with matching column family and qualifier.
   * @param family    column family
   * @param qualifier column qualifier
   * @return a list of KeyValue objects with the matching family and qualifier, returns an empty
   *         list if one doesn't exist for the given family.
   */
  public List<Cell> get(byte[] family, byte[] qualifier) {
    List<Cell> filteredList = new ArrayList<>();
    for (Cell cell : getCellList(family)) {
      if (CellUtil.matchingQualifier(cell, qualifier)) {
        filteredList.add(cell);
      }
    }
    return filteredList;
  }

  /**
   * Private method to determine if this object's familyMap contains the given value assigned to the
   * given family, qualifier and timestamp, respecting the 2 boolean arguments.
   */
  protected boolean has(byte[] family, byte[] qualifier, long ts, byte[] value, boolean ignoreTS,
    boolean ignoreValue) {
    List<ExtendedCell> list = getCellList(family);
    if (list.isEmpty()) {
      return false;
    }
    // Boolean analysis of ignoreTS/ignoreValue.
    // T T => 2
    // T F => 3 (first is always true)
    // F T => 2
    // F F => 1
    if (!ignoreTS && !ignoreValue) {
      for (ExtendedCell cell : list) {
        if (
          CellUtil.matchingFamily(cell, family) && CellUtil.matchingQualifier(cell, qualifier)
            && CellUtil.matchingValue(cell, value) && cell.getTimestamp() == ts
        ) {
          return true;
        }
      }
    } else if (ignoreValue && !ignoreTS) {
      for (Cell cell : list) {
        if (
          CellUtil.matchingFamily(cell, family) && CellUtil.matchingQualifier(cell, qualifier)
            && cell.getTimestamp() == ts
        ) {
          return true;
        }
      }
    } else if (!ignoreValue && ignoreTS) {
      for (Cell cell : list) {
        if (
          CellUtil.matchingFamily(cell, family) && CellUtil.matchingQualifier(cell, qualifier)
            && CellUtil.matchingValue(cell, value)
        ) {
          return true;
        }
      }
    } else {
      for (Cell cell : list) {
        if (CellUtil.matchingFamily(cell, family) && CellUtil.matchingQualifier(cell, qualifier)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * @param row Row to check
   * @throws IllegalArgumentException Thrown if <code>row</code> is empty or null or &gt;
   *                                  {@link HConstants#MAX_ROW_LENGTH}
   * @return <code>row</code>
   */
  static byte[] checkRow(final byte[] row) {
    return checkRow(row, 0, row == null ? 0 : row.length);
  }

  /**
   * @param row Row to check
   * @throws IllegalArgumentException Thrown if <code>row</code> is empty or null or &gt;
   *                                  {@link HConstants#MAX_ROW_LENGTH}
   * @return <code>row</code>
   */
  static byte[] checkRow(final byte[] row, final int offset, final int length) {
    if (row == null) {
      throw new IllegalArgumentException("Row buffer is null");
    }
    if (length == 0) {
      throw new IllegalArgumentException("Row length is 0");
    }
    if (length > HConstants.MAX_ROW_LENGTH) {
      throw new IllegalArgumentException(
        "Row length " + length + " is > " + HConstants.MAX_ROW_LENGTH);
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
      throw new IllegalArgumentException(
        "Row length " + row.remaining() + " is > " + HConstants.MAX_ROW_LENGTH);
    }
  }

  protected final void checkTimestamp(long ts) {
    Preconditions.checkArgument(ts >= 0, "Timestamp cannot be negative. ts=%s", ts);
  }

  Mutation add(Cell cell) throws IOException {
    // Checking that the row of the kv is the same as the mutation
    // TODO: It is fraught with risk if user pass the wrong row.
    // Throwing the IllegalArgumentException is more suitable I'd say.
    if (!CellUtil.matchingRows(cell, this.row)) {
      throw new WrongRowIOException("The row in " + cell.toString()
        + " doesn't match the original one " + Bytes.toStringBinary(this.row));
    }

    byte[] family;

    if (cell instanceof IndividualBytesFieldCell) {
      family = cell.getFamilyArray();
    } else {
      family = CellUtil.cloneFamily(cell);
    }

    if (family == null || family.length == 0) {
      throw new IllegalArgumentException("Family cannot be null");
    }

    if (cell instanceof ExtendedCell) {
      getCellList(family).add((ExtendedCell) cell);
    } else {
      throw new IllegalArgumentException("Unsupported cell type: " + cell.getClass().getName());
    }
    return this;
  }

  /**
   * get a CellBuilder instance that already has relevant Type and Row set.
   * @param cellBuilderType e.g CellBuilderType.SHALLOW_COPY
   * @return CellBuilder which already has relevant Type and Row set.
   */
  public abstract CellBuilder getCellBuilder(CellBuilderType cellBuilderType);

  /**
   * get a CellBuilder instance that already has relevant Type and Row set. the default
   * CellBuilderType is CellBuilderType.SHALLOW_COPY
   * @return CellBuilder which already has relevant Type and Row set.
   */
  public CellBuilder getCellBuilder() {
    return getCellBuilder(CellBuilderType.SHALLOW_COPY);
  }

  /**
   * get a CellBuilder instance that already has relevant Type and Row set.
   * @param cellBuilderType e.g CellBuilderType.SHALLOW_COPY
   * @param cellType        e.g Cell.Type.Put
   * @return CellBuilder which already has relevant Type and Row set.
   */
  protected final CellBuilder getCellBuilder(CellBuilderType cellBuilderType, Cell.Type cellType) {
    CellBuilder builder = CellBuilderFactory.create(cellBuilderType).setRow(row).setType(cellType);
    return new CellBuilder() {
      @Override
      public CellBuilder setRow(byte[] row) {
        return this;
      }

      @Override
      public CellBuilder setType(Cell.Type type) {
        return this;
      }

      @Override
      public CellBuilder setRow(byte[] row, int rOffset, int rLength) {
        return this;
      }

      @Override
      public CellBuilder setFamily(byte[] family) {
        builder.setFamily(family);
        return this;
      }

      @Override
      public CellBuilder setFamily(byte[] family, int fOffset, int fLength) {
        builder.setFamily(family, fOffset, fLength);
        return this;
      }

      @Override
      public CellBuilder setQualifier(byte[] qualifier) {
        builder.setQualifier(qualifier);
        return this;
      }

      @Override
      public CellBuilder setQualifier(byte[] qualifier, int qOffset, int qLength) {
        builder.setQualifier(qualifier, qOffset, qLength);
        return this;
      }

      @Override
      public CellBuilder setTimestamp(long timestamp) {
        builder.setTimestamp(timestamp);
        return this;
      }

      @Override
      public CellBuilder setValue(byte[] value) {
        builder.setValue(value);
        return this;
      }

      @Override
      public CellBuilder setValue(byte[] value, int vOffset, int vLength) {
        builder.setValue(value, vOffset, vLength);
        return this;
      }

      @Override
      public Cell build() {
        return builder.build();
      }

      @Override
      public CellBuilder clear() {
        builder.clear();
        // reset the row and type
        builder.setRow(row);
        builder.setType(cellType);
        return this;
      }
    };
  }
}
