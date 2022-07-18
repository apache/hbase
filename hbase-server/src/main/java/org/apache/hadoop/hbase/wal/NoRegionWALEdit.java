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
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * This creates WALEdit which are not tied to any HRegion. We skip running co-processor methods
 * {@link org.apache.hadoop.hbase.regionserver.wal.WALCoprocessorHost#preWALWrite( RegionInfo, WALKey, WALEdit)}
 * and @{@link org.apache.hadoop.hbase.regionserver.wal.WALCoprocessorHost#postWALWrite( RegionInfo, WALKey, WALEdit)}
 * for this edit.
 */
@InterfaceAudience.LimitedPrivate({ HBaseInterfaceAudience.REPLICATION,
  HBaseInterfaceAudience.COPROC })
public class NoRegionWALEdit extends WALEdit {

  public NoRegionWALEdit() {
    super();
  }

  /**
   * Creates a replication tracker edit with {@link #METAFAMILY} family and
   * {@link #REPLICATION_MARKER} qualifier and has null value.
   * @param rowKey    rowkey
   * @param timestamp timestamp
   */
  public static WALEdit createReplicationMarkerEdit(byte[] rowKey, long timestamp) {
    KeyValue kv =
      new KeyValue(rowKey, METAFAMILY, REPLICATION_MARKER, timestamp, KeyValue.Type.Put);
    return new NoRegionWALEdit().add(kv);
  }
}
