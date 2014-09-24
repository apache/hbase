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

package org.apache.hadoop.hbase.executor;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * List of all HBase event handler types.  Event types are named by a
 * convention: event type names specify the component from which the event
 * originated and then where its destined -- e.g. RS2ZK_ prefix means the
 * event came from a regionserver destined for zookeeper -- and then what
 * the even is; e.g. REGION_OPENING.
 *
 * <p>We give the enums indices so we can add types later and keep them
 * grouped together rather than have to add them always to the end as we
 * would have to if we used raw enum ordinals.
 */
@InterfaceAudience.Private
public enum EventType {
  // Messages originating from RS (NOTE: there is NO direct communication from
  // RS to Master). These are a result of RS updates into ZK.
  // RS_ZK_REGION_CLOSING    (1),   // It is replaced by M_ZK_REGION_CLOSING(HBASE-4739)
  
  /**
   * RS_ZK_REGION_CLOSED<br>
   * 
   * RS has finished closing a region.
   */
  RS_ZK_REGION_CLOSED       (2, ExecutorType.MASTER_CLOSE_REGION),
  /**
   * RS_ZK_REGION_OPENING<br>
   * 
   * RS is in process of opening a region.
   */
  RS_ZK_REGION_OPENING      (3, null),
  /**
   * RS_ZK_REGION_OPENED<br>
   * 
   * RS has finished opening a region.
   */
  RS_ZK_REGION_OPENED       (4, ExecutorType.MASTER_OPEN_REGION),
  /**
   * RS_ZK_REGION_SPLITTING<br>
   *
   * RS has started a region split after master says it's ok to move on.
   */
  RS_ZK_REGION_SPLITTING    (5, null),
  /**
   * RS_ZK_REGION_SPLIT<br>
   *
   * RS split has completed and is notifying the master.
   */
  RS_ZK_REGION_SPLIT        (6, ExecutorType.MASTER_SERVER_OPERATIONS),
  /**
   * RS_ZK_REGION_FAILED_OPEN<br>
   * 
   * RS failed to open a region.
   */
  RS_ZK_REGION_FAILED_OPEN  (7, ExecutorType.MASTER_CLOSE_REGION),
  /**
   * RS_ZK_REGION_MERGING<br>
   *
   * RS has started merging regions after master says it's ok to move on.
   */
  RS_ZK_REGION_MERGING      (8, null),
  /**
   * RS_ZK_REGION_MERGE<br>
   *
   * RS region merge has completed and is notifying the master.
   */
  RS_ZK_REGION_MERGED       (9, ExecutorType.MASTER_SERVER_OPERATIONS),
  /**
   * RS_ZK_REQUEST_REGION_SPLIT<br>
   *
   * RS has requested to split a region. This is to notify master
   * and check with master if the region is in a state good to split.
   */
  RS_ZK_REQUEST_REGION_SPLIT    (10, null),
  /**
   * RS_ZK_REQUEST_REGION_MERGE<br>
   *
   * RS has requested to merge two regions. This is to notify master
   * and check with master if two regions is in states good to merge.
   */
  RS_ZK_REQUEST_REGION_MERGE    (11, null),

  /**
   * Messages originating from Master to RS.<br>
   * M_RS_OPEN_REGION<br>
   * Master asking RS to open a region.
   */
  M_RS_OPEN_REGION          (20, ExecutorType.RS_OPEN_REGION),
  /**
   * Messages originating from Master to RS.<br>
   * M_RS_OPEN_ROOT<br>
   * Master asking RS to open root.
   */
  M_RS_OPEN_ROOT            (21, ExecutorType.RS_OPEN_ROOT),
  /**
   * Messages originating from Master to RS.<br>
   * M_RS_OPEN_META<br>
   * Master asking RS to open meta.
   */
  M_RS_OPEN_META            (22, ExecutorType.RS_OPEN_META),
  /**
   * Messages originating from Master to RS.<br>
   * M_RS_CLOSE_REGION<br>
   * Master asking RS to close a region.
   */
  M_RS_CLOSE_REGION         (23, ExecutorType.RS_CLOSE_REGION),
  /**
   * Messages originating from Master to RS.<br>
   * M_RS_CLOSE_ROOT<br>
   * Master asking RS to close root.
   */
  M_RS_CLOSE_ROOT           (24, ExecutorType.RS_CLOSE_ROOT),
  /**
   * Messages originating from Master to RS.<br>
   * M_RS_CLOSE_META<br>
   * Master asking RS to close meta.
   */
  M_RS_CLOSE_META           (25, ExecutorType.RS_CLOSE_META),

  /**
   * Messages originating from Client to Master.<br>
   * C_M_MERGE_REGION<br>
   * Client asking Master to merge regions.
   */
  C_M_MERGE_REGION          (30, ExecutorType.MASTER_TABLE_OPERATIONS),
  /**
   * Messages originating from Client to Master.<br>
   * C_M_DELETE_TABLE<br>
   * Client asking Master to delete a table.
   */
  C_M_DELETE_TABLE          (40, ExecutorType.MASTER_TABLE_OPERATIONS),
  /**
   * Messages originating from Client to Master.<br>
   * C_M_DISABLE_TABLE<br>
   * Client asking Master to disable a table.
   */
  C_M_DISABLE_TABLE         (41, ExecutorType.MASTER_TABLE_OPERATIONS),
  /**
   * Messages originating from Client to Master.<br>
   * C_M_ENABLE_TABLE<br>
   * Client asking Master to enable a table.
   */
  C_M_ENABLE_TABLE          (42, ExecutorType.MASTER_TABLE_OPERATIONS),
  /**
   * Messages originating from Client to Master.<br>
   * C_M_MODIFY_TABLE<br>
   * Client asking Master to modify a table.
   */
  C_M_MODIFY_TABLE          (43, ExecutorType.MASTER_TABLE_OPERATIONS),
  /**
   * Messages originating from Client to Master.<br>
   * C_M_ADD_FAMILY<br>
   * Client asking Master to add family to table.
   */
  C_M_ADD_FAMILY            (44, null),
  /**
   * Messages originating from Client to Master.<br>
   * C_M_DELETE_FAMILY<br>
   * Client asking Master to delete family of table.
   */
  C_M_DELETE_FAMILY         (45, null),
  /**
   * Messages originating from Client to Master.<br>
   * C_M_MODIFY_FAMILY<br>
   * Client asking Master to modify family of table.
   */
  C_M_MODIFY_FAMILY         (46, null),
  /**
   * Messages originating from Client to Master.<br>
   * C_M_CREATE_TABLE<br>
   * Client asking Master to create a table.
   */
  C_M_CREATE_TABLE          (47, ExecutorType.MASTER_TABLE_OPERATIONS),
  /**
   * Messages originating from Client to Master.<br>
   * C_M_SNAPSHOT_TABLE<br>
   * Client asking Master to snapshot an offline table.
   */
  C_M_SNAPSHOT_TABLE        (48, ExecutorType.MASTER_TABLE_OPERATIONS),
  /**
   * Messages originating from Client to Master.<br>
   * C_M_RESTORE_SNAPSHOT<br>
   * Client asking Master to restore a snapshot.
   */
  C_M_RESTORE_SNAPSHOT      (49, ExecutorType.MASTER_TABLE_OPERATIONS),

  // Updates from master to ZK. This is done by the master and there is
  // nothing to process by either Master or RS
  /**
   * M_ZK_REGION_OFFLINE
   * Master adds this region as offline in ZK
   */
  M_ZK_REGION_OFFLINE       (50, null),
  /**
   * M_ZK_REGION_CLOSING
   * Master adds this region as closing in ZK
   */
  M_ZK_REGION_CLOSING       (51, null),
  
  /**
   * Master controlled events to be executed on the master
   * M_SERVER_SHUTDOWN
   * Master is processing shutdown of a RS
   */
  M_SERVER_SHUTDOWN         (70, ExecutorType.MASTER_SERVER_OPERATIONS),
  /**
   * Master controlled events to be executed on the master.<br>
   * M_META_SERVER_SHUTDOWN <br>
   * Master is processing shutdown of RS hosting a meta region (-ROOT- or hbase:meta).
   */
  M_META_SERVER_SHUTDOWN    (72, ExecutorType.MASTER_META_SERVER_OPERATIONS),
  /**
   * Master controlled events to be executed on the master.<br>
   * 
   * M_MASTER_RECOVERY<br>
   * Master is processing recovery of regions found in ZK RIT
   */
  M_MASTER_RECOVERY         (73, ExecutorType.MASTER_SERVER_OPERATIONS),
  /**
   * Master controlled events to be executed on the master.<br>
   * 
   * M_LOG_REPLAY<br>
   * Master is processing log replay of failed region server
   */
  M_LOG_REPLAY              (74, ExecutorType.M_LOG_REPLAY_OPS),

  /**
   * RS controlled events to be executed on the RS.<br>
   * 
   * RS_PARALLEL_SEEK
   */
  RS_PARALLEL_SEEK          (80, ExecutorType.RS_PARALLEL_SEEK),
  
  /**
   * RS wal recovery work items(either creating recover.edits or directly replay wals)
   * to be executed on the RS.<br>
   * 
   * RS_LOG_REPLAY
   */
  RS_LOG_REPLAY             (81, ExecutorType.RS_LOG_REPLAY_OPS);

  private final int code;
  private final ExecutorType executor;

  /**
   * Constructor
   */
  EventType(final int code, final ExecutorType executor) {
    this.code = code;
    this.executor = executor;
  }

  public int getCode() {
    return this.code;
  }

  public static EventType get(final int code) {
    // Is this going to be slow?  Its used rare but still...
    for (EventType et: EventType.values()) {
      if (et.getCode() == code) return et;
    }
    throw new IllegalArgumentException("Unknown code " + code);
  }

  public boolean isOnlineSchemaChangeSupported() {
    return (
      this.equals(EventType.C_M_ADD_FAMILY) ||
      this.equals(EventType.C_M_DELETE_FAMILY) ||
      this.equals(EventType.C_M_MODIFY_FAMILY) ||
      this.equals(EventType.C_M_MODIFY_TABLE)
    );
  }

  ExecutorType getExecutorServiceType() {
    return this.executor;
  }
}
