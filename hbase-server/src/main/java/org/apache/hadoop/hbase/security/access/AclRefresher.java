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
package org.apache.hadoop.hbase.security.access;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ListMultimap;

/**
 * Applies ACL changes to the local {@link AuthManager} cache of a master or a region server.
 * <p>
 * All cache mutations go through one serial queue and follow two rules:
 * <ul>
 * <li><b>Timestamp watermark</b>: every update batch carries the master assigned timestamp of the
 * writes it was built from. Batches whose timestamp is not newer than the last applied one are
 * dropped, so stale deliveries (e.g. a remote procedure replayed after a master failover) cannot
 * regress the cache or resurrect deleted entries.</li>
 * <li><b>Full reload</b>: {@link #reloadFromTable()} scans {@code hbase:acl} and swaps in the
 * result as a single queue task. Updates enqueued before the task are already reflected in the scan
 * (their writes completed before the scan started), updates arriving during the scan are applied
 * after the swap, so a reload never loses a concurrent update.</li>
 * </ul>
 * </p>
 */
@InterfaceAudience.Private
public final class AclRefresher implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(AclRefresher.class);

  private final Configuration conf;
  private final AclSnapshot snapshot;
  private final ExecutorService executor;
  private final AtomicLong appliedTs = new AtomicLong(0L);

  public AclRefresher(Configuration conf, AclSnapshot snapshot) {
    this.conf = conf;
    this.snapshot = snapshot;
    this.executor = Executors.newSingleThreadExecutor(r -> {
      Thread t = new Thread(r, "AclRefresher");
      t.setDaemon(true);
      return t;
    });
  }

  /** Highest update timestamp applied so far. */
  public long getAppliedTimestamp() {
    return appliedTs.get();
  }

  /**
   * Enqueues a batch of incremental entry updates, e.g. from a RefreshAcl remote procedure or from
   * a local {@code hbase:acl} write. Dropped when not newer than the last applied update.
   */
  public void submitUpdates(long ts, List<AclSnapshot.EntryUpdate> updates) {
    executor.execute(() -> {
      if (!isCurrent(ts, "update batch")) {
        return;
      }
      snapshot.applyUpdates(updates);
      appliedTs.set(ts);
    });
  }

  /**
   * Enqueues a full snapshot pushed by the master. Dropped when not newer than the last applied
   * update, see {@link #submitUpdates}.
   */
  public void submitFullSnapshot(long ts, Map<byte[], ListMultimap<String, UserPermission>> perms) {
    executor.execute(() -> {
      if (!isCurrent(ts, "full snapshot")) {
        return;
      }
      snapshot.applyFullSnapshot(perms);
      appliedTs.set(ts);
    });
  }

  /**
   * Reads the whole {@code hbase:acl} table and swaps it into the cache as one queue task, never
   * losing a concurrent update and never moving the watermark backwards. Blocks until done. A
   * missing acl table (fresh cluster) is loaded as empty content.
   */
  public void reloadFromTable() throws IOException {
    Future<Void> future = executor.submit(() -> {
      PermissionStorage.AclTableContent content;
      try {
        content = PermissionStorage.loadAllSnapshot(conf);
      } catch (TableNotFoundException e) {
        content = new PermissionStorage.AclTableContent(Collections.emptyMap(), 0L);
      }
      snapshot.applyFullSnapshot(content.getPermissions());
      appliedTs.accumulateAndGet(content.getMaxTimestamp(), Math::max);
      return null;
    });
    try {
      future.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(
        "Interrupted while reloading ACLs from " + PermissionStorage.ACL_TABLE_NAME, e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      throw new IOException("Failed to reload ACLs from " + PermissionStorage.ACL_TABLE_NAME,
        cause);
    }
  }

  private boolean isCurrent(long ts, String what) {
    if (ts <= appliedTs.get()) {
      LOG.debug("Dropping stale ACL {}: ts={}, applied={}", what, ts, appliedTs.get());
      return false;
    }
    return true;
  }

  /** Waits until all queued updates have been applied. Visible for testing. */
  void flush() throws IOException {
    try {
      executor.submit(() -> null).get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while waiting for pending ACL updates", e);
    } catch (ExecutionException e) {
      throw new IOException("Failed waiting for pending ACL updates", e.getCause());
    }
  }

  @Override
  public void close() {
    executor.shutdownNow();
  }
}
