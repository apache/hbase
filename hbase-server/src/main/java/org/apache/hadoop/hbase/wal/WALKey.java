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
package org.apache.hadoop.hbase.wal;

import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.SequenceId;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;


/**
 * Key for WAL Entry.
 */
@InterfaceAudience.LimitedPrivate({HBaseInterfaceAudience.REPLICATION,
    HBaseInterfaceAudience.COPROC})
public interface WALKey extends SequenceId, Comparable<WALKey> {
  /**
   * Unmodifiable empty list of UUIDs.
   */
  List<UUID> EMPTY_UUIDS = Collections.unmodifiableList(new ArrayList<UUID>());

  default long estimatedSerializedSizeOf() {
    return 0;
  }

  /**
   * @return encoded region name
   */
  byte[] getEncodedRegionName();

  /**
   * @return table name
   */
  TableName getTableName();

  /**
   * @return the write time
   */
  long getWriteTime();

  /**
   * @return The nonce group
   */
  default long getNonceGroup() {
    return HConstants.NO_NONCE;
  }

  /**
   * @return The nonce
   */
  default long getNonce() {
    return HConstants.NO_NONCE;
  }

  UUID getOriginatingClusterId();

  /**
   * Return a positive long if current WALKeyImpl is created from a replay edit; a replay edit is an
   * edit that came in when replaying WALs of a crashed server.
   * @return original sequence number of the WALEdit
   */
  long getOrigLogSeqNum();

  /**
   * Add a named String value to this WALKey to be persisted into the WAL
   * @param attributeKey Name of the attribute
   * @param attributeValue Value of the attribute
   */
  void addExtendedAttribute(String attributeKey, byte[] attributeValue);

    /**
     * Return a named String value injected into the WALKey during processing, such as by a
     * coprocessor
     * @param attributeKey The key of a key / value pair
     */
  default byte[] getExtendedAttribute(String attributeKey){
    return null;
  }

    /**
     * Returns a map of all extended attributes injected into this WAL key.
     */
  default Map<String, byte[]> getExtendedAttributes() {
    return new HashMap<>();
  }
  /**
   * Produces a string map for this key. Useful for programmatic use and
   * manipulation of the data stored in an WALKeyImpl, for example, printing
   * as JSON.
   *
   * @return a Map containing data from this key
   */
  default Map<String, Object> toStringMap() {
    Map<String, Object> stringMap = new HashMap<>();
    stringMap.put("table", getTableName());
    stringMap.put("region", Bytes.toStringBinary(getEncodedRegionName()));
    stringMap.put("sequence", getSequenceId());
    Map<String, byte[]> extendedAttributes = getExtendedAttributes();
    if (extendedAttributes != null){
      for (Map.Entry<String, byte[]> entry : extendedAttributes.entrySet()){
        stringMap.put(entry.getKey(), Bytes.toStringBinary(entry.getValue()));
      }
    }
    return stringMap;
  }
}
