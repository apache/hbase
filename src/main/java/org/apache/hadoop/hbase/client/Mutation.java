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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

public abstract class Mutation extends OperationWithAttributes implements Row {
  private static final Log LOG = LogFactory.getLog(Mutation.class);
  // Attribute used in Mutations to indicate the originating cluster.
  private static final String CLUSTER_ID_ATTR = "_c.id_";
  private static final String DURABILITY_ID_ATTR = "_dur_";

  /**
   * The attribute for storing the list of clusters that have consumed the change.
   */
  private static final String CONSUMED_CLUSTER_IDS = "_cs.id";
  protected byte [] row = null;
  protected long ts = HConstants.LATEST_TIMESTAMP;
  protected long lockId = -1L;
  protected boolean writeToWAL = true;
  protected Map<byte [], List<KeyValue>> familyMap =
      new TreeMap<byte [], List<KeyValue>>(Bytes.BYTES_COMPARATOR);

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
    for (Map.Entry<byte [], List<KeyValue>> entry : this.familyMap.entrySet()) {
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
    for (Map.Entry<byte [], List<KeyValue>> entry : this.familyMap.entrySet()) {
      // map from this family to details for each kv affected within the family
      List<Map<String, Object>> qualifierDetails =
        new ArrayList<Map<String, Object>>();
      columns.put(Bytes.toStringBinary(entry.getKey()), qualifierDetails);
      colCount += entry.getValue().size();
      if (maxCols <= 0) {
        continue;
      }
      // add details for each kv
      for (KeyValue kv : entry.getValue()) {
        if (--maxCols <= 0 ) {
          continue;
        }
        Map<String, Object> kvMap = kv.toStringMap();
        // row and family information are already available in the bigger map
        kvMap.remove("row");
        kvMap.remove("family");
        qualifierDetails.add(kvMap);
      }
    }
    map.put("totalColumns", colCount);
    // add the id if set
    if (getId() != null) {
      map.put("id", getId());
    }
    return map;
  }

  /**
   * @deprecated Use {@link #getDurability()} instead.
   * @return true if edits should be applied to WAL, false if not
   */
  public boolean getWriteToWAL() {
    return this.writeToWAL;
  }

  /**
   * Set whether this Delete should be written to the WAL or not.
   * Not writing the WAL means you may lose edits on server crash.
   * This method will reset any changes made via {@link #setDurability(Durability)} 
   * @param write true if edits should be written to WAL, false if not
   * @deprecated Use {@link #setDurability(Durability)} instead.
   */
  public void setWriteToWAL(boolean write) {
    setDurability(write ? Durability.USE_DEFAULT : Durability.SKIP_WAL);
  }

  /**
   * Set the durability for this mutation.
   * Note that RegionServers prior to 0.94.7 will only honor {@link Durability#SKIP_WAL}.
   * This method will reset any changes made via {@link #setWriteToWAL(boolean)} 
   * @param d
   */
  public void setDurability(Durability d) {
    setAttribute(DURABILITY_ID_ATTR, Bytes.toBytes(d.ordinal()));
    this.writeToWAL = d != Durability.SKIP_WAL;
  }

  /** Get the current durability */
  public Durability getDurability() {
    byte[] attr = getAttribute(DURABILITY_ID_ATTR);
    if (attr != null) {
      try {
        return Durability.valueOf(Bytes.toInt(attr));
      } catch (IllegalArgumentException iax) {
        LOG.warn("Invalid or unknown durability settting", iax);
      }
    }
    return writeToWAL ? Durability.USE_DEFAULT : Durability.SKIP_WAL;
  }

  /**
   * Method for retrieving the put's familyMap
   * @return familyMap
   */
  public Map<byte [], List<KeyValue>> getFamilyMap() {
    return this.familyMap;
  }

  /**
   * Method for setting the put's familyMap
   */
  public void setFamilyMap(Map<byte [], List<KeyValue>> map) {
    this.familyMap = map;
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

  public int compareTo(final Row d) {
    return Bytes.compareTo(this.getRow(), d.getRow());
  }

  /**
   * Method for retrieving the delete's RowLock
   * @return RowLock
   * @deprecated {@link RowLock} and associated operations are deprecated
   */
  public RowLock getRowLock() {
    return new RowLock(this.row, this.lockId);
  }

  /**
   * Method for retrieving the delete's lock ID.
   *
   * @return The lock ID.
   * @deprecated {@link RowLock} and associated operations are deprecated
   */
  public long getLockId() {
  return this.lockId;
  }

  /**
   * Method for retrieving the timestamp
   * @return timestamp
   */
  public long getTimeStamp() {
    return this.ts;
  }

  /**
   * Set the replication custer id.
   * @param clusterId
   */
  public void setClusterId(UUID clusterId) {
    if (clusterId == null) return;
    byte[] val = new byte[2*Bytes.SIZEOF_LONG];
    Bytes.putLong(val, 0, clusterId.getMostSignificantBits());
    Bytes.putLong(val, Bytes.SIZEOF_LONG, clusterId.getLeastSignificantBits());
    setAttribute(CLUSTER_ID_ATTR, val);
  }

  /**
   * @return The replication cluster id.
   */
  public UUID getClusterId() {
    byte[] attr = getAttribute(CLUSTER_ID_ATTR);
    if (attr == null) {
      return HConstants.DEFAULT_CLUSTER_ID;
    }
    return new UUID(Bytes.toLong(attr,0), Bytes.toLong(attr, Bytes.SIZEOF_LONG));
  }

  /**
   * Marks that the clusters with the given clusterIds have consumed the mutation
   * @param clusterIds of the clusters that have consumed the mutation
   */
  public void setClusterIds(List<UUID> clusterIds) {
    ByteArrayDataOutput out = ByteStreams.newDataOutput();
    out.writeInt(clusterIds.size());
    for (UUID clusterId : clusterIds) {
      out.writeLong(clusterId.getMostSignificantBits());
      out.writeLong(clusterId.getLeastSignificantBits());
    }
    setAttribute(CONSUMED_CLUSTER_IDS, out.toByteArray());
  }

  /**
   * @return the set of cluster Ids that have consumed the mutation
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
   * @return the total number of KeyValues
   */
  public int size() {
    int size = 0;
    for(List<KeyValue> kvList : this.familyMap.values()) {
      size += kvList.size();
    }
    return size;
  }

  /**
   * @return the number of different families
   */
  public int numFamilies() {
    return familyMap.size();
  }
}
