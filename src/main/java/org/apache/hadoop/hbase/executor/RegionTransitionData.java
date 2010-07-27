/**
 * Copyright 2010 The Apache Software Foundation
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.HMsg;
import org.apache.hadoop.hbase.executor.HBaseEventHandler.HBaseEventType;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Writable;

/**
 * Data serialized into ZooKeeper for region transitions.
 */
public class RegionTransitionData implements Writable {
  /**
   * Type of transition event (offline, opening, opened, closing, closed).
   * Required.
   */
  private HBaseEventType eventType;

  /** Region being transitioned.  Required. */
  private String regionName;

  /** Server event originated from.  Optional. */
  private String serverName;

  /** Time the event was created.  Required but automatically set. */
  private long timeStamp;

  /** Temporary.  Holds payload used doing transitions via heartbeats. */
  private HMsg hmsg; // to be removed shortly once we stop using heartbeats

  /**
   * Writable constructor.  Do not use directly.
   */
  public RegionTransitionData() {}

  /**
   * Construct data for a new region transition event with the specified event
   * type and region name.
   *
   * <p>Used when the server name is not known (the master is setting it).  This
   * happens during cluster startup or during failure scenarios.  When
   * processing a failed regionserver, the master assigns the regions from that
   * server to other servers though the region was never 'closed'.  During
   * master failover, the new master may have regions stuck in transition
   * without a destination so may have to set regions offline and generate a new
   * assignment.
   *
   * <p>Since only the master uses this constructor, the type should always be
   * {@link HBaseEventType#M2ZK_REGION_OFFLINE}.
   *
   * @param eventType type of event
   * @param regionName name of region
   */
  public RegionTransitionData(HBaseEventType eventType, String regionName) {
    this(eventType, regionName, null);
  }

  /**
   * Construct data for a new region transition event with the specified event
   * type, region name, and server name.
   *
   * <p>Used when the server name is known (a regionserver is setting it).
   *
   * <p>Valid types for this constructor are {@link HBaseEventType#RS2ZK_REGION_CLOSING},
   * {@link HBaseEventType#RS2ZK_REGION_CLOSED}, {@link HBaseEventType#RS2ZK_REGION_OPENING},
   * and {@link HBaseEventType#RS2ZK_REGION_OPENED}.
   *
   * @param eventType type of event
   * @param regionName name of region
   * @param serverName name of server setting data
   */
  public RegionTransitionData(HBaseEventType eventType, String regionName,
      String serverName) {
    this(eventType, regionName, serverName, null);
  }

  /**
   * Construct data for a fully-specified, old-format region transition event
   * which uses HMsg/heartbeats.
   *
   * TODO: Remove this constructor once we stop using heartbeats.
   *
   * @param eventType
   * @param regionName
   * @param serverName
   * @param hmsg
   */
  public RegionTransitionData(HBaseEventType eventType, String regionName,
      String serverName, HMsg hmsg) {
    this.eventType = eventType;
    this.timeStamp = System.currentTimeMillis();
    this.regionName = regionName;
    this.serverName = serverName;
    this.hmsg = hmsg;
  }

  /**
   * Gets the type of region transition event.
   *
   * <p>One of:
   * <ul>
   * <li>{@link HBaseEventType#M2ZK_REGION_OFFLINE}
   * <li>{@link HBaseEventType#RS2ZK_REGION_CLOSING}
   * <li>{@link HBaseEventType#RS2ZK_REGION_CLOSED}
   * <li>{@link HBaseEventType#RS2ZK_REGION_OPENING}
   * <li>{@link HBaseEventType#RS2ZK_REGION_OPENED}
   * </ul>
   * @return type of region transition event
   */
  public HBaseEventType getEventType() {
    return eventType;
  }

  /**
   * Gets the encoded name of the region being transitioned.
   *
   * <p>Region name is required so this never returns null.
   * @return region name
   */
  public String getRegionName() {
    return regionName;
  }

  /**
   * Gets the server the event originated from.  If null, this event originated
   * from the master.
   *
   * @return server name of originating regionserver, or null if from master
   */
  public String getServerName() {
    return serverName;
  }

  /**
   * Gets the timestamp when this event was created.
   *
   * @return time event was created
   */
  public long getTimeStamp() {
    return timeStamp;
  }

  /**
   * Gets the {@link HMsg} payload of this region transition event.
   * @return heartbeat payload
   */
  public HMsg getHmsg() {
    return hmsg;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // the event type byte
    eventType = HBaseEventType.fromByte(in.readByte());
    // the timestamp
    timeStamp = in.readLong();
    // the encoded name of the region being transitioned
    regionName = in.readUTF();
    // remaining fields are optional so prefixed with boolean
    // the name of the regionserver sending the data
    if(in.readBoolean()) {
      serverName = in.readUTF();
    }
    // hmsg
    if(in.readBoolean()) {
      hmsg = new HMsg();
      hmsg.readFields(in);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeByte(eventType.getByteValue());
    out.writeLong(System.currentTimeMillis());
    out.writeUTF(regionName);
    // remaining fields are optional so prefixed with boolean
    out.writeBoolean(serverName != null);
    if(serverName != null) {
      out.writeUTF(serverName);
    }
    out.writeBoolean(hmsg != null);
    if(hmsg != null) {
      hmsg.write(out);
    }
  }

  /**
   * Get the bytes for this instance.  Throws a {@link RuntimeException} if
   * there is an error deserializing this instance because it represents a code
   * bug.
   * @return binary representation of this instance
   */
  public byte [] getBytes() {
    try {
      return Writables.getBytes(this);
    } catch(IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get an instance from bytes.  Throws a {@link RuntimeException} if
   * there is an error serializing this instance from bytes because it
   * represents a code bug.
   * @param bytes binary representation of this instance
   * @return instance of this class
   */
  public static RegionTransitionData fromBytes(byte [] bytes) {
    try {
      RegionTransitionData data = new RegionTransitionData();
      Writables.getWritable(bytes, data);
      return data;
    } catch(IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String toString() {
    return "region=" + regionName + ",server=" + serverName + ",state=" +
        eventType;
  }
}
