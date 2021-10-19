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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamSlowMonitor implements ConfigurationObserver {
  private static final Logger LOG = LoggerFactory.getLogger(StreamSlowMonitor.class);

  private static final String WAL_SLOW_DETECT_MIN_COUNT_KEY =
    "hbase.regionserver.async.wal.min.slow.detect.count";
  private static final int DEFAULT_WAL_SLOW_DETECT_MIN_COUNT = 3;

  private static final String WAL_SLOW_DETECT_DATA_TTL_KEY =
    "hbase.regionserver.async.wal.slow.detect.data.ttl.ms";
  private static final long DEFAULT_WAL_SLOW_DETECT_DATA_TTL = 10 * 60 * 1000; // 10min in ms

  private static final String SLOW_PACKET_ACK_TIME_KEY =
    "hbase.regionserver.async.wal.slow.packet.ack.time.millis";
  private static final long DEFAULT_SLOW_PACKET_ACK_TIME = 2000; //2s in ms

  private static final String SLOW_PACKET_ACK_SPEED_KEY =
    "hbase.regionserver.async.wal.slow.packet.ack.speed.kbs";
  private static final double DEFAULT_SLOW_PACKET_ACK_SPEED = 0.1;

  private final String name;
  private final Map<DatanodeInfo, Deque<PacketAckData>> datanodeSlowDataQueue =
    new ConcurrentHashMap<>();
  private final ExcludeDatanodeManager excludeDatanodeManager;

  private int minSlowDetectCount;
  private long slowDataTtl;
  private long slowPacketAckMillis;
  private double slowPacketAckSpeed;

  public StreamSlowMonitor(Configuration conf, String name, ExcludeDatanodeManager excludeDatanodeManager) {
    setConf(conf);
    this.name = name;
    this.excludeDatanodeManager = excludeDatanodeManager;
    LOG.info("New stream slow monitor {}", this.name);
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

  public void checkProcessTimeAndSpeed(DatanodeInfo datanodeInfo, long dataLength, long processTime,
      long lastAckTimestamp, int unfinished) {
    long current = EnvironmentEdgeManager.currentTime();
    boolean slow = processTime > slowPacketAckMillis ||
        (dataLength > 100 && (double) dataLength / processTime < slowPacketAckSpeed);
    if (slow) {
      // check if large diff ack timestamp between replicas
      if ((lastAckTimestamp > 0 && current - lastAckTimestamp > slowPacketAckMillis / 2) || (
          lastAckTimestamp <= 0 && unfinished == 0)) {
        LOG.info("Slow datanode: {}, data length={}, duration={}ms, unfinishedReplicas={}, "
            + "lastAckTimestamp={}, monitor name: {}", datanodeInfo, dataLength, processTime,
          unfinished, lastAckTimestamp, this.name);
        if (addSlowAckData(datanodeInfo, dataLength, processTime)) {
          excludeDatanodeManager.addExcludeDN(datanodeInfo, "slow packet ack");
        }
      }
    }
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
    setConf(conf);
  }

  private void setConf(Configuration conf) {
    this.minSlowDetectCount = conf.getInt(WAL_SLOW_DETECT_MIN_COUNT_KEY,
      DEFAULT_WAL_SLOW_DETECT_MIN_COUNT);
    this.slowDataTtl = conf.getLong(WAL_SLOW_DETECT_DATA_TTL_KEY, DEFAULT_WAL_SLOW_DETECT_DATA_TTL);
    this.slowPacketAckMillis = conf.getLong(SLOW_PACKET_ACK_TIME_KEY, DEFAULT_SLOW_PACKET_ACK_TIME);
    this.slowPacketAckSpeed = conf.getDouble(SLOW_PACKET_ACK_SPEED_KEY,
      DEFAULT_SLOW_PACKET_ACK_SPEED);
  }

  public ExcludeDatanodeManager getExcludeDatanodeManager() {
    return excludeDatanodeManager;
  }
}
