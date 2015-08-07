/**
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

import java.io.IOException;

import org.apache.hadoop.hbase.util.ByteStringer;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Current state of a region in transition.  Holds state of a region as it moves through the
 * steps that take it from offline to open, etc.  Used by regionserver, master, and zk packages.
 * Encapsulates protobuf serialization/deserialization so we don't leak generated pb outside this
 * class.  Create an instance using createRegionTransition(EventType, byte[], ServerName).
 * <p>Immutable
 */
@InterfaceAudience.Private
public class RegionTransition {
  private final ZooKeeperProtos.RegionTransition rt;

  /**
   * Shutdown constructor
   */
  private RegionTransition() {
    this(null);
  }

  private RegionTransition(final ZooKeeperProtos.RegionTransition rt) {
    this.rt = rt;
  }

  public EventType getEventType() {
    return EventType.get(this.rt.getEventTypeCode());
  }

  public ServerName getServerName() {
    return ProtobufUtil.toServerName(this.rt.getServerName());
  }

  public long getCreateTime() {
    return this.rt.getCreateTime();
  }

  /**
   * @return Full region name
   */
  public byte [] getRegionName() {
    return this.rt.getRegionName().toByteArray();
  }

  public byte [] getPayload() {
    return this.rt.getPayload().toByteArray();
  }

  @Override
  public String toString() {
    byte [] payload = getPayload();
    return "region=" + Bytes.toStringBinary(getRegionName()) + ", state=" + getEventType() +
      ", servername=" + getServerName() + ", createTime=" + this.getCreateTime() +
      ", payload.length=" + (payload == null? 0: payload.length);
  }

  /**
   * @param type
   * @param regionName
   * @param sn
   * @return a serialized pb {@link RegionTransition}
   */
  public static RegionTransition createRegionTransition(final EventType type,
      final byte [] regionName, final ServerName sn) {
    return createRegionTransition(type, regionName, sn, null);
  }

  /**
   * @param type
   * @param regionName
   * @param sn
   * @param payload May be null
   * @return a serialized pb {@link RegionTransition}
   */
  public static RegionTransition createRegionTransition(final EventType type,
      final byte [] regionName, final ServerName sn, final byte [] payload) {
    org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.ServerName pbsn =
      org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.ServerName.newBuilder().
        setHostName(sn.getHostname()).setPort(sn.getPort()).setStartCode(sn.getStartcode()).build();
    ZooKeeperProtos.RegionTransition.Builder builder = ZooKeeperProtos.RegionTransition.newBuilder().
      setEventTypeCode(type.getCode()).setRegionName(ByteStringer.wrap(regionName)).
        setServerName(pbsn);
    builder.setCreateTime(System.currentTimeMillis());
    if (payload != null) builder.setPayload(ByteStringer.wrap(payload));
    return new RegionTransition(builder.build());
  }

  /**
   * @param data Serialized date to parse.
   * @return A RegionTransition instance made of the passed <code>data</code>
   * @throws DeserializationException 
   * @see #toByteArray()
   */
  public static RegionTransition parseFrom(final byte [] data) throws DeserializationException {
    ProtobufUtil.expectPBMagicPrefix(data);
    try {
      int prefixLen = ProtobufUtil.lengthOfPBMagic();
      ZooKeeperProtos.RegionTransition.Builder builder =
          ZooKeeperProtos.RegionTransition.newBuilder();
      ProtobufUtil.mergeFrom(builder, data, prefixLen, data.length - prefixLen);
      return new RegionTransition(builder.build());
    } catch (IOException e) {
      throw new DeserializationException(e);
    }
  }

  /**
   * @return This instance serialized into a byte array
   * @see #parseFrom(byte[])
   */
  public byte [] toByteArray() {
    return ProtobufUtil.prependPBMagic(this.rt.toByteArray());
  }
}
