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
package org.apache.hadoop.hbase.replication.regionserver;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ZooKeeper-backed replicated bulk load event tracker.
 */
@InterfaceAudience.Private
public class ZKReplicationBulkLoadEventTracker implements ReplicationBulkLoadEventTracker {

  private static final Logger LOG =
    LoggerFactory.getLogger(ZKReplicationBulkLoadEventTracker.class);

  public static final String ZNODE_KEY = "hbase.replication.bulkload.event.tracker.znode";
  public static final String ZNODE_DEFAULT = "bulkload-events";
  public static final String BUCKET_WIDTH_MS_KEY =
    "hbase.replication.bulkload.event.bucket.width.ms";
  public static final long BUCKET_WIDTH_MS_DEFAULT = TimeUnit.HOURS.toMillis(1);
  public static final String WAIT_TIMEOUT_MS_KEY =
    "hbase.replication.bulkload.event.wait.timeout.ms";
  public static final long WAIT_TIMEOUT_MS_DEFAULT = TimeUnit.MINUTES.toMillis(1);
  public static final String WAIT_INTERVAL_MS_KEY =
    "hbase.replication.bulkload.event.wait.interval.ms";
  public static final long WAIT_INTERVAL_MS_DEFAULT = TimeUnit.SECONDS.toMillis(1);

  private static final String IN_PROGRESS = "in-progress";
  private static final String DONE = "done";

  private final ZKWatcher zkw;
  private final long bucketWidthMs;
  private final long waitTimeoutMs;
  private final long waitIntervalMs;
  private final String inProgressZNode;
  private final String doneZNode;

  public ZKReplicationBulkLoadEventTracker(Configuration conf, ZKWatcher zkw) {
    this.zkw = Objects.requireNonNull(zkw, "zkw");
    this.bucketWidthMs = Math.max(1, conf.getLong(BUCKET_WIDTH_MS_KEY, BUCKET_WIDTH_MS_DEFAULT));
    this.waitTimeoutMs = Math.max(0, conf.getLong(WAIT_TIMEOUT_MS_KEY, WAIT_TIMEOUT_MS_DEFAULT));
    this.waitIntervalMs = Math.max(1, conf.getLong(WAIT_INTERVAL_MS_KEY, WAIT_INTERVAL_MS_DEFAULT));

    String baseZNode = ZNodePaths.joinZNode(this.zkw.getZNodePaths().replicationZNode,
      conf.get(ZNODE_KEY, ZNODE_DEFAULT));
    this.inProgressZNode = ZNodePaths.joinZNode(baseZNode, IN_PROGRESS);
    this.doneZNode = ZNodePaths.joinZNode(baseZNode, DONE);
  }

  @Override
  public ReplicationBulkLoadEventTracker.Event newEvent(String replicationClusterId,
    TableName table, byte[] encodedRegionName, long bulkLoadSeqNum, long writeTime) {
    String bucket = Long.toString(Math.max(0, writeTime) / bucketWidthMs);
    String eventKey = replicationClusterId + '\n' + table.getNameWithNamespaceInclAsString() + '\n'
      + Bytes.toStringBinary(encodedRegionName) + '\n' + bulkLoadSeqNum;
    return new ReplicationBulkLoadEventTracker.Event(bucket,
      MD5Hash.getMD5AsHex(Bytes.toBytes(eventKey)), eventKey);
  }

  @Override
  public ReplicationBulkLoadEventTracker.ClaimResult
    claim(ReplicationBulkLoadEventTracker.Event event) throws IOException {
    long deadline = EnvironmentEdgeManager.currentTime() + waitTimeoutMs;
    String inProgressPath = getInProgressPath(event);
    try {
      while (true) {
        if (isDoneInZK(event)) {
          return ReplicationBulkLoadEventTracker.ClaimResult.COMPLETED;
        }
        ZKUtil.createWithParents(zkw, ZKUtil.getParent(inProgressPath));
        if (ZKUtil.createEphemeralNodeAndWatch(zkw, inProgressPath, event.getData())) {
          try {
            if (isDoneInZK(event)) {
              deleteCreatedInProgressMarker(inProgressPath, event);
              return ReplicationBulkLoadEventTracker.ClaimResult.COMPLETED;
            }
            return ReplicationBulkLoadEventTracker.ClaimResult.CLAIMED;
          } catch (KeeperException e) {
            deleteCreatedInProgressMarker(inProgressPath, event);
            throw new IOException("Failed to verify replicated bulkload event completion " + event,
              e);
          }
        }
        if (isDoneInZK(event)) {
          return ReplicationBulkLoadEventTracker.ClaimResult.COMPLETED;
        }
        long now = EnvironmentEdgeManager.currentTime();
        if (now >= deadline) {
          throw new IOException("Timed out waiting for replicated bulkload event " + event);
        }
        sleep(Math.min(waitIntervalMs, deadline - now));
      }
    } catch (KeeperException e) {
      throw new IOException("Failed to claim replicated bulkload event " + event, e);
    }
  }

  private void deleteCreatedInProgressMarker(String inProgressPath,
    ReplicationBulkLoadEventTracker.Event event) {
    try {
      ZKUtil.deleteNodeFailSilent(zkw, inProgressPath);
    } catch (KeeperException e) {
      LOG.warn("Failed to delete replicated bulkload in-progress marker {}", event, e);
    }
  }

  @Override
  public void markDone(ReplicationBulkLoadEventTracker.Event event) throws IOException {
    try {
      ZKUtil.createSetData(zkw, getDonePath(event), event.getData());
    } catch (KeeperException e) {
      throw new IOException("Failed to mark replicated bulkload event done " + event, e);
    }
  }

  @Override
  public void release(ReplicationBulkLoadEventTracker.Event event) throws IOException {
    try {
      ZKUtil.deleteNodeFailSilent(zkw, getInProgressPath(event));
    } catch (KeeperException e) {
      throw new IOException("Failed to release replicated bulkload event " + event, e);
    }
  }

  @Override
  public boolean isInProgress(ReplicationBulkLoadEventTracker.Event event) throws IOException {
    try {
      return isInProgressInZK(event);
    } catch (KeeperException e) {
      throw new IOException("Failed to check replicated bulkload event progress " + event, e);
    }
  }

  @Override
  public boolean isDone(ReplicationBulkLoadEventTracker.Event event) throws IOException {
    try {
      return isDoneInZK(event);
    } catch (KeeperException e) {
      throw new IOException("Failed to check replicated bulkload event completion " + event, e);
    }
  }

  @Override
  public int cleanDoneMarkers(long ttlMs) throws IOException {
    try {
      long minAgeMs = Math.max(0, ttlMs);
      long now = EnvironmentEdgeManager.currentTime();
      List<String> buckets = ZKUtil.listChildrenNoWatch(zkw, doneZNode);
      if (buckets == null) {
        return 0;
      }
      int deleted = 0;
      for (String bucket : buckets) {
        String doneBucketPath = ZNodePaths.joinZNode(doneZNode, bucket);
        List<String> eventIds = ZKUtil.listChildrenNoWatch(zkw, doneBucketPath);
        if (eventIds == null) {
          continue;
        }
        for (String eventId : eventIds) {
          String donePath = ZNodePaths.joinZNode(doneBucketPath, eventId);
          Stat stat = new Stat();
          if (ZKUtil.getDataNoWatch(zkw, donePath, stat) == null) {
            continue;
          }
          if (now - stat.getMtime() < minAgeMs) {
            continue;
          }
          String inProgressPath =
            ZNodePaths.joinZNode(ZNodePaths.joinZNode(inProgressZNode, bucket), eventId);
          if (ZKUtil.checkExists(zkw, inProgressPath) != -1) {
            continue;
          }
          ZKUtil.deleteNodeFailSilent(zkw, donePath);
          deleted++;
        }
        deleteIfEmpty(doneBucketPath);
        deleteIfEmpty(ZNodePaths.joinZNode(inProgressZNode, bucket));
      }
      return deleted;
    } catch (KeeperException e) {
      throw new IOException("Failed to clean replicated bulkload event markers", e);
    }
  }

  private void deleteIfEmpty(String znode) throws KeeperException {
    List<String> children = ZKUtil.listChildrenNoWatch(zkw, znode);
    if (children == null || !children.isEmpty()) {
      return;
    }
    try {
      ZKUtil.deleteNodeFailSilent(zkw, znode);
    } catch (KeeperException.NotEmptyException e) {
      // Another region server added a child after our list; keep the bucket.
    }
  }

  private boolean isInProgressInZK(ReplicationBulkLoadEventTracker.Event event)
    throws KeeperException {
    return ZKUtil.checkExists(zkw, getInProgressPath(event)) != -1;
  }

  private boolean isDoneInZK(ReplicationBulkLoadEventTracker.Event event) throws KeeperException {
    return ZKUtil.checkExists(zkw, getDonePath(event)) != -1;
  }

  private String getInProgressPath(ReplicationBulkLoadEventTracker.Event event) {
    return ZNodePaths.joinZNode(ZNodePaths.joinZNode(inProgressZNode, event.getBucket()),
      event.getEventId());
  }

  private String getDonePath(ReplicationBulkLoadEventTracker.Event event) {
    return ZNodePaths.joinZNode(ZNodePaths.joinZNode(doneZNode, event.getBucket()),
      event.getEventId());
  }

  private void sleep(long sleepMs) throws InterruptedIOException {
    try {
      Thread.sleep(Math.max(1, sleepMs));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      InterruptedIOException ioe =
        new InterruptedIOException("Interrupted while waiting for replicated bulkload event");
      ioe.initCause(e);
      throw ioe;
    }
  }
}
