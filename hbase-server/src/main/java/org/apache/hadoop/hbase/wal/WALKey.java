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
 * Read-only. No Setters. For limited audience such as Coprocessors.
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
    return stringMap;
  }
}
