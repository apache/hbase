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
package org.apache.hadoop.hbase.io.asyncfs.monitor;

import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for monitor the wal file flush performance.
 * Each active wal file has a StreamSlowMonitor.
 */
@InterfaceAudience.Private
public class StreamSlowMonitor implements ConfigurationObserver {
  private static final Logger LOG = LoggerFactory.getLogger(StreamSlowMonitor.class);

  /**
   * Configure for the min count for a datanode detected slow.
   * If a datanode is detected slow times up to this count, then it will be added to the exclude
   * datanode cache by {@link ExcludeDatanodeManager#tryAddExcludeDN(DatanodeInfo, String)}
   * of this regionsever.
   */
  private static final String WAL_SLOW_DETECT_MIN_COUNT_KEY =
    "hbase.regionserver.async.wal.min.slow.detect.count";
  private static final int DEFAULT_WAL_SLOW_DETECT_MIN_COUNT = 3;

  /**
   * Configure for the TTL of the data that a datanode detected slow.
   */
  private static final String WAL_SLOW_DETECT_DATA_TTL_KEY =
    "hbase.regionserver.async.wal.slow.detect.data.ttl.ms";
  private static final long DEFAULT_WAL_SLOW_DETECT_DATA_TTL = 10 * 60 * 1000; // 10min in ms

  /**
   * Configure for the slow packet process time, a duration from send to ACK.
   */
  private static final String DATANODE_SLOW_PACKET_PROCESS_TIME_KEY =
    "hbase.regionserver.async.wal.datanode.slow.packet.process.time.millis";
  private static final long DEFAULT_DATANODE_SLOW_PACKET_PROCESS_TIME = 6000; //6s in ms

  /**
   * Configure for the packet flush speed.
   */
  private static final String DATANODE_SLOW_PACKET_FLUSH_SPEED_KEY =
    "hbase.regionserver.async.wal.datanode.slow.packet.flush.speed.kbs";
  private static final double DEFAULT_DATANODE_SLOW_PACKET_FLUSH_SPEED = 0.1;

  private final String name;
  // this is a map of datanodeInfo->queued slow PacketAckData
  private final Map<DatanodeInfo, Deque<PacketAckData>> datanodeSlowDataQueue =
    new ConcurrentHashMap<>();
  private final ExcludeDatanodeManager excludeDatanodeManager;

  private int minSlowDetectCount;
  private long slowDataTtl;
  private long slowPacketAckMillis;
  private double slowPacketAckSpeed;

  public StreamSlowMonitor(Configuration conf, String name,
      ExcludeDatanodeManager excludeDatanodeManager) {
    setConf(conf);
    this.name = name;
    this.excludeDatanodeManager = excludeDatanodeManager;
    LOG.info("New stream slow monitor {}", this.name);
  }

  public static StreamSlowMonitor create(Configuration conf, String name) {
    return new StreamSlowMonitor(conf, name, new ExcludeDatanodeManager(conf));
  }

  /**
   * Check if the packet process time shows that the relevant datanode is a slow node.
   * @param datanodeInfo the datanode that processed the packet
   * @param packetDataLen the data length of the packet
   * @param processTime the process time of the packet on the datanode
   * @param lastAckTimestamp the last acked timestamp of the packet on another datanode
   * @param unfinished if the packet is unfinished flushed to the datanode replicas
   */
  public void checkProcessTimeAndSpeed(DatanodeInfo datanodeInfo, long packetDataLen,
      long processTime, long lastAckTimestamp, int unfinished) {
    long current = EnvironmentEdgeManager.currentTime();
    boolean slow = processTime > slowPacketAckMillis ||
        (packetDataLen > 100 && (double) packetDataLen / processTime < slowPacketAckSpeed);
    if (slow) {
      // check if large diff ack timestamp between replicas
      if ((lastAckTimestamp > 0 && current - lastAckTimestamp > slowPacketAckMillis / 2) || (
          lastAckTimestamp <= 0 && unfinished == 0)) {
        LOG.info("Slow datanode: {}, data length={}, duration={}ms, unfinishedReplicas={}, "
            + "lastAckTimestamp={}, monitor name: {}", datanodeInfo, packetDataLen, processTime,
          unfinished, lastAckTimestamp, this.name);
        if (addSlowAckData(datanodeInfo, packetDataLen, processTime)) {
          excludeDatanodeManager.tryAddExcludeDN(datanodeInfo, "slow packet ack");
        }
      }
    }
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
    setConf(conf);
  }

  private boolean addSlowAckData(DatanodeInfo datanodeInfo, long dataLength, long processTime) {
    Deque<PacketAckData> slowDNQueue = datanodeSlowDataQueue.computeIfAbsent(datanodeInfo,
      d -> new ConcurrentLinkedDeque<>());
    long current = System.currentTimeMillis();
    while (!slowDNQueue.isEmpty() && (current - slowDNQueue.getFirst().getTimestamp() > slowDataTtl
      || slowDNQueue.size() >= minSlowDetectCount)) {
      slowDNQueue.removeFirst();
    }
    slowDNQueue.addLast(new PacketAckData(dataLength, processTime));
    return slowDNQueue.size() >= minSlowDetectCount;
  }

  private void setConf(Configuration conf) {
    this.minSlowDetectCount = conf.getInt(WAL_SLOW_DETECT_MIN_COUNT_KEY,
      DEFAULT_WAL_SLOW_DETECT_MIN_COUNT);
    this.slowDataTtl = conf.getLong(WAL_SLOW_DETECT_DATA_TTL_KEY, DEFAULT_WAL_SLOW_DETECT_DATA_TTL);
    this.slowPacketAckMillis = conf.getLong(DATANODE_SLOW_PACKET_PROCESS_TIME_KEY,
      DEFAULT_DATANODE_SLOW_PACKET_PROCESS_TIME);
    this.slowPacketAckSpeed = conf.getDouble(DATANODE_SLOW_PACKET_FLUSH_SPEED_KEY,
      DEFAULT_DATANODE_SLOW_PACKET_FLUSH_SPEED);
  }

  public ExcludeDatanodeManager getExcludeDatanodeManager() {
    return excludeDatanodeManager;
  }

  private static class PacketAckData {
    private final long dataLength;
    private final long processTime;
    private final long timestamp;

    public PacketAckData(long dataLength, long processTime) {
      this.dataLength = dataLength;
      this.processTime = processTime;
      this.timestamp = EnvironmentEdgeManager.currentTime();
    }

    public long getDataLength() {
      return dataLength;
    }

    public long getProcessTime() {
      return processTime;
    }

    public long getTimestamp() {
      return timestamp;
    }
  }
}
