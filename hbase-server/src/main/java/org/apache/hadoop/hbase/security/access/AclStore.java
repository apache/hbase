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
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ListMultimap;

/**
 * Region server side ACL holder: maintains the region server's current {@link AclSnapshot} of
 * {@code hbase:acl}. The snapshot is fed from three sources, all funneled through this class:
 * <ul>
 * <li><b>startup load</b>: {@link #reloadFromTable()} after registering with the master, non
 * superuser requests are held until it completes;</li>
 * <li><b>master pushes</b>: {@link #applyUpdates} / {@link #applyFullSnapshot} from
 * {@code RefreshAclRemoteCallable}; applies are synchronous, so a completed refresh procedure means
 * the change is already visible;</li>
 * <li><b>periodic reload</b>: {@link #reloadFromTable()} on an internal chore, the fallback for
 * lost pushes and for direct writes that bypass the master.</li>
 * </ul>
 * Every batch carries the master assigned timestamp of the writes it was built from; batches whose
 * timestamp is not newer than the last applied one are dropped, so stale deliveries (e.g. a remote
 * procedure replayed after a master failover) cannot regress the snapshot or resurrect deleted
 * entries. {@link #reloadFromTable()} never moves the timestamp watermark backwards either.
 * <p>
 * Applies and reloads serialize on this object; a reload holds it for the duration of its
 * {@code hbase:acl} scan, so an update arriving during the scan is applied after the swap and can
 * never be lost to it.
 * </p>
 */
@InterfaceAudience.Private
public final class AclStore implements Closeable {

  /** Period of the periodic full reload. */
  public static final String ACL_RELOAD_PERIOD_MSEC_KEY = "hbase.security.acl.reload.period.msec";

  private static final int DEFAULT_ACL_RELOAD_PERIOD_MSEC = 5 * 60 * 1000;

  private static final Logger LOG = LoggerFactory.getLogger(AclStore.class);

  private final Configuration conf;
  private final AclSnapshot snapshot;
  private final ScheduledChore reloadChore;
  private final AtomicLong appliedTs = new AtomicLong(0L);

  public AclStore(Configuration conf, AclSnapshot snapshot, Stoppable stopper) {
    this.conf = conf;
    this.snapshot = snapshot;
    this.reloadChore = new ScheduledChore("AclReloadChore", stopper,
      conf.getInt(ACL_RELOAD_PERIOD_MSEC_KEY, DEFAULT_ACL_RELOAD_PERIOD_MSEC)) {
      @Override
      protected void chore() {
        try {
          reloadFromTable();
        } catch (IOException e) {
          LOG.warn("Failed to reload ACLs from {}, keeping the current snapshot",
            PermissionStorage.ACL_TABLE_NAME, e);
        }
      }
    };
  }

  /**
   * Schedules the periodic full reload. Skipped when authorization is disabled; the startup load
   * and the master pushes do not happen either in that case.
   */
  public void start(ChoreService choreService) {
    if (AccessChecker.isAuthorizationSupported(conf)) {
      choreService.scheduleChore(reloadChore);
    }
  }

  /** Highest update timestamp applied so far. */
  public long getAppliedTimestamp() {
    return appliedTs.get();
  }

  /**
   * Applies a batch of incremental entry updates pushed by the master. Dropped when not newer than
   * the last applied update, see the class javadoc.
   */
  public synchronized void applyUpdates(long ts, List<AclSnapshot.EntryUpdate> updates) {
    if (!isCurrent(ts, "update batch")) {
      return;
    }
    snapshot.applyUpdates(updates);
    appliedTs.set(ts);
  }

  /**
   * Applies a full snapshot pushed by the master. Dropped when not newer than the last applied
   * update, see the class javadoc.
   */
  public synchronized void applyFullSnapshot(long ts,
    Map<byte[], ListMultimap<String, UserPermission>> perms) {
    if (!isCurrent(ts, "full snapshot")) {
      return;
    }
    snapshot.applyFullSnapshot(perms);
    appliedTs.set(ts);
  }

  /**
   * Reads the whole {@code hbase:acl} table and swaps it into the snapshot, never losing a
   * concurrent update and never moving the watermark backwards. Blocks until done. A missing acl
   * table (fresh cluster) is loaded as empty content.
   */
  public synchronized void reloadFromTable() throws IOException {
    PermissionStorage.AclTableContent content;
    try {
      content = PermissionStorage.loadAllSnapshot(conf);
    } catch (TableNotFoundException e) {
      content = new PermissionStorage.AclTableContent(Collections.emptyMap(), 0L);
    }
    snapshot.applyFullSnapshot(content.getPermissions());
    appliedTs.accumulateAndGet(content.getMaxTimestamp(), Math::max);
  }

  private boolean isCurrent(long ts, String what) {
    if (ts <= appliedTs.get()) {
      LOG.debug("Dropping stale ACL {}: ts={}, applied={}", what, ts, appliedTs.get());
      return false;
    }
    return true;
  }

  @Override
  public void close() {
    reloadChore.cancel(true);
  }
}
