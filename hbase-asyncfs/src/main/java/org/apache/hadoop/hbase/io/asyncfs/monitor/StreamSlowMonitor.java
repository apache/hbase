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

import static org.apache.hadoop.hbase.io.asyncfs.monitor.ExcludeDatanodeManager.DEFAULT_WAL_EXCLUDE_DATANODE_TTL;
import static org.apache.hadoop.hbase.io.asyncfs.monitor.ExcludeDatanodeManager.DEFAULT_WAL_MAX_EXCLUDE_SLOW_DATANODE_COUNT;
import static org.apache.hadoop.hbase.io.asyncfs.monitor.ExcludeDatanodeManager.WAL_EXCLUDE_DATANODE_TTL_KEY;
import static org.apache.hadoop.hbase.io.asyncfs.monitor.ExcludeDatanodeManager.WAL_MAX_EXCLUDE_SLOW_DATANODE_COUNT_KEY;

import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hbase.thirdparty.com.google.common.cache.CacheLoader;
import org.apache.hbase.thirdparty.com.google.common.cache.LoadingCache;

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
   * Configure for the speed check of packet min length.
   * For packets whose data length smaller than this value, check slow by processing time.
   * While for packets whose data length larger than this value, check slow by flushing speed.
   */
  private static final String DATANODE_PACKET_FLUSH_CHECK_SPEED_MIN_DATA_LENGTH_KEY =
    "hbase.regionserver.async.wal.datanode.slow.check.speed.packet.data.length.min";
  // 64KB
  private static final long DEFAULT_DATANODE_PACKET_FLUSH_CHECK_SPEED_MIN_DATA_LENGTH = 64 * 1024;

  /**
   * Configure for the slow packet process time, a duration from send to ACK.
   * The processing time check is for packets that data length smaller than
   * {@link StreamSlowMonitor#DATANODE_PACKET_FLUSH_CHECK_SPEED_MIN_DATA_LENGTH_KEY}
   */
  public static final String DATANODE_SLOW_PACKET_PROCESS_TIME_KEY =
    "hbase.regionserver.async.wal.datanode.slow.packet.process.time.millis";
  // 6s in ms
  private static final long DEFAULT_DATANODE_SLOW_PACKET_PROCESS_TIME = 6000;

  /**
   * Configure for the check of large packet(which is configured by
   * {@link StreamSlowMonitor#DATANODE_PACKET_FLUSH_CHECK_SPEED_MIN_DATA_LENGTH_KEY}) flush speed.
   * e.g. If the configured slow packet process time is smaller than 10s, then here 20KB/s means
   * 64KB should be processed in less than 3.2s.
   */
  private static final String DATANODE_SLOW_PACKET_FLUSH_MIN_SPEED_KEY =
    "hbase.regionserver.async.wal.datanode.slow.packet.speed.min.kbs";
  // 20KB/s
  private static final double DEFAULT_DATANODE_SLOW_PACKET_FLUSH_MIN_SPEED = 20;

  private final String name;
  // this is a map of datanodeInfo->queued slow PacketAckData
  private final LoadingCache<DatanodeInfo, Deque<PacketAckData>> datanodeSlowDataQueue;
  private final ExcludeDatanodeManager excludeDatanodeManager;

  private int minSlowDetectCount;
  private long slowDataTtl;
  private long slowPacketAckMs;
  private double minPacketFlushSpeedKBs;
  private long minLengthForSpeedCheck;

  public StreamSlowMonitor(Configuration conf, String name,
      ExcludeDatanodeManager excludeDatanodeManager) {
    setConf(conf);
    this.name = name;
    this.excludeDatanodeManager = excludeDatanodeManager;
    this.datanodeSlowDataQueue = CacheBuilder.newBuilder()
      .maximumSize(conf.getInt(WAL_MAX_EXCLUDE_SLOW_DATANODE_COUNT_KEY,
        DEFAULT_WAL_MAX_EXCLUDE_SLOW_DATANODE_COUNT))
      .expireAfterWrite(conf.getLong(WAL_EXCLUDE_DATANODE_TTL_KEY,
        DEFAULT_WAL_EXCLUDE_DATANODE_TTL), TimeUnit.HOURS)
      .build(new CacheLoader<DatanodeInfo, Deque<PacketAckData>>() {
        @Override
        public Deque<PacketAckData> load(DatanodeInfo key) throws Exception {
          return new ConcurrentLinkedDeque<>();
        }
      });
    LOG.info("New stream slow monitor {}", this.name);
  }

  public static StreamSlowMonitor create(Configuration conf, String name) {
    return new StreamSlowMonitor(conf, name, new ExcludeDatanodeManager(conf));
  }

  /**
   * Check if the packet process time shows that the relevant datanode is a slow node.
   * @param datanodeInfo the datanode that processed the packet
   * @param packetDataLen the data length of the packet (in bytes)
   * @param processTimeMs the process time (in ms) of the packet on the datanode,
   * @param lastAckTimestamp the last acked timestamp of the packet on another datanode
   * @param unfinished if the packet is unfinished flushed to the datanode replicas
   */
  public void checkProcessTimeAndSpeed(DatanodeInfo datanodeInfo, long packetDataLen,
      long processTimeMs, long lastAckTimestamp, int unfinished) {
    long current = EnvironmentEdgeManager.currentTime();
    // Here are two conditions used to determine whether a datanode is slow,
    // 1. For small packet, we just have a simple time limit, without considering
    // the size of the packet.
    // 2. For large packet, we will calculate the speed, and check if the speed is too slow.
    boolean slow = (packetDataLen <= minLengthForSpeedCheck && processTimeMs > slowPacketAckMs) || (
      packetDataLen > minLengthForSpeedCheck
        && (double) packetDataLen / processTimeMs < minPacketFlushSpeedKBs);
    if (slow) {
      // Check if large diff ack timestamp between replicas,
      // should try to avoid misjudgments that caused by GC STW.
      if ((lastAckTimestamp > 0 && current - lastAckTimestamp > slowPacketAckMs / 2) || (
          lastAckTimestamp <= 0 && unfinished == 0)) {
        LOG.info("Slow datanode: {}, data length={}, duration={}ms, unfinishedReplicas={}, "
            + "lastAckTimestamp={}, monitor name: {}", datanodeInfo, packetDataLen, processTimeMs,
          unfinished, lastAckTimestamp, this.name);
        if (addSlowAckData(datanodeInfo, packetDataLen, processTimeMs)) {
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
    Deque<PacketAckData> slowDNQueue = datanodeSlowDataQueue.getUnchecked(datanodeInfo);
    long current = EnvironmentEdgeManager.currentTime();
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
    this.slowPacketAckMs = conf.getLong(DATANODE_SLOW_PACKET_PROCESS_TIME_KEY,
        DEFAULT_DATANODE_SLOW_PACKET_PROCESS_TIME);
    this.minLengthForSpeedCheck = conf.getLong(
        DATANODE_PACKET_FLUSH_CHECK_SPEED_MIN_DATA_LENGTH_KEY,
        DEFAULT_DATANODE_PACKET_FLUSH_CHECK_SPEED_MIN_DATA_LENGTH);
    this.minPacketFlushSpeedKBs = conf.getDouble(DATANODE_SLOW_PACKET_FLUSH_MIN_SPEED_KEY,
      DEFAULT_DATANODE_SLOW_PACKET_FLUSH_MIN_SPEED);
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
