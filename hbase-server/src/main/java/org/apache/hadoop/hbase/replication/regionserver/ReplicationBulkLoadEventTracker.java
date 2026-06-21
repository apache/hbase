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
import java.nio.charset.StandardCharsets;
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

/**
 * Coordinates replicated bulk load events across all region servers in the sink cluster.
 */
@InterfaceAudience.Private
public class ReplicationBulkLoadEventTracker {

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

  private static final byte[] EMPTY_BYTES = new byte[0];
  private static final String IN_PROGRESS = "in-progress";
  private static final String DONE = "done";

  private final ZKWatcher zkw;
  private final long bucketWidthMs;
  private final long waitTimeoutMs;
  private final long waitIntervalMs;
  private final String inProgressZNode;
  private final String doneZNode;

  public ReplicationBulkLoadEventTracker(Configuration conf, ZKWatcher zkw) {
    this.zkw = Objects.requireNonNull(zkw, "zkw");
    this.bucketWidthMs = Math.max(1, conf.getLong(BUCKET_WIDTH_MS_KEY, BUCKET_WIDTH_MS_DEFAULT));
    this.waitTimeoutMs = Math.max(0, conf.getLong(WAIT_TIMEOUT_MS_KEY, WAIT_TIMEOUT_MS_DEFAULT));
    this.waitIntervalMs = Math.max(1, conf.getLong(WAIT_INTERVAL_MS_KEY, WAIT_INTERVAL_MS_DEFAULT));

    String baseZNode = ZNodePaths.joinZNode(this.zkw.getZNodePaths().replicationZNode,
      conf.get(ZNODE_KEY, ZNODE_DEFAULT));
    this.inProgressZNode = ZNodePaths.joinZNode(baseZNode, IN_PROGRESS);
    this.doneZNode = ZNodePaths.joinZNode(baseZNode, DONE);
  }

  public Event newEvent(String replicationClusterId, TableName table, byte[] encodedRegionName,
    long bulkLoadSeqNum, long writeTime) {
    String bucket = Long.toString(Math.max(0, writeTime) / bucketWidthMs);
    String eventKey = replicationClusterId + '\n' + table.getNameWithNamespaceInclAsString() + '\n'
      + Bytes.toStringBinary(encodedRegionName) + '\n' + bulkLoadSeqNum;
    return new Event(bucket, MD5Hash.getMD5AsHex(Bytes.toBytes(eventKey)), eventKey);
  }

  public ClaimResult claim(Event event) throws IOException {
    long deadline = EnvironmentEdgeManager.currentTime() + waitTimeoutMs;
    String inProgressPath = getInProgressPath(event);
    try {
      while (true) {
        if (isDone(event)) {
          return ClaimResult.COMPLETED;
        }
        ZKUtil.createWithParents(zkw, ZKUtil.getParent(inProgressPath));
        if (ZKUtil.createEphemeralNodeAndWatch(zkw, inProgressPath, event.getData())) {
          return ClaimResult.CLAIMED;
        }
        if (isDone(event)) {
          return ClaimResult.COMPLETED;
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

  public void markDone(Event event) throws IOException {
    try {
      ZKUtil.createSetData(zkw, getDonePath(event), event.getData());
    } catch (KeeperException e) {
      throw new IOException("Failed to mark replicated bulkload event done " + event, e);
    }
  }

  public void release(Event event) throws IOException {
    try {
      ZKUtil.deleteNodeFailSilent(zkw, getInProgressPath(event));
    } catch (KeeperException e) {
      throw new IOException("Failed to release replicated bulkload event " + event, e);
    }
  }

  public boolean isInProgress(Event event) throws KeeperException {
    return ZKUtil.checkExists(zkw, getInProgressPath(event)) != -1;
  }

  public boolean isDone(Event event) throws KeeperException {
    return ZKUtil.checkExists(zkw, getDonePath(event)) != -1;
  }

  public int cleanDoneMarkers(long ttlMs) throws KeeperException {
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

  private String getInProgressPath(Event event) {
    return ZNodePaths.joinZNode(ZNodePaths.joinZNode(inProgressZNode, event.getBucket()),
      event.getEventId());
  }

  private String getDonePath(Event event) {
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

  public enum ClaimResult {
    CLAIMED(true),
    COMPLETED(false);

    private final boolean claimed;

    ClaimResult(boolean claimed) {
      this.claimed = claimed;
    }

    public boolean isClaimed() {
      return claimed;
    }
  }

  public static final class Event {
    private final String bucket;
    private final String eventId;
    private final String eventKey;

    private Event(String bucket, String eventId, String eventKey) {
      this.bucket = bucket;
      this.eventId = eventId;
      this.eventKey = eventKey;
    }

    public String getBucket() {
      return bucket;
    }

    public String getEventId() {
      return eventId;
    }

    private byte[] getData() {
      return eventKey == null ? EMPTY_BYTES : eventKey.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Event)) {
        return false;
      }
      Event event = (Event) o;
      return Objects.equals(bucket, event.bucket) && Objects.equals(eventId, event.eventId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(bucket, eventId);
    }

    @Override
    public String toString() {
      return "Event{bucket='" + bucket + "', eventId='" + eventId + "'}";
    }
  }
}
