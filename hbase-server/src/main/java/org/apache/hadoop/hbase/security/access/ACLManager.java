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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.procedure.RefreshAclProcedure;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ListMultimap;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.AclEntryUpdate;

/**
 * Master side ACL update pipeline. The {@code hbase:acl} table is the authoritative store; this
 * class assigns the monotonic timestamps of its writes, tracks the pending entry updates and pushes
 * change sets to all registered region servers through a single in-flight
 * {@link RefreshAclProcedure}: updates arriving while a push is running are merged into the next
 * one, so at most one broadcast is in flight and pushes are naturally coalesced.
 * <p>
 * The local {@link AuthManager} cache is updated as soon as the write lands in the table, without
 * waiting for the broadcast. Servers that miss a push (crash, abandonment) converge through their
 * startup load and the periodic reload.
 * </p>
 */
@InterfaceAudience.Private
public class ACLManager {

  private static final Logger LOG = LoggerFactory.getLogger(ACLManager.class);

  private final Configuration conf;
  private final MasterServices masterServices;
  private final AclRefresher refresher;

  /**
   * Timestamps assigned to acl table writes. Seeded from the acl table content and the wall clock
   * at activation, so they never move backwards across restarts or master failovers and the RS side
   * watermark of {@link AclRefresher} stays meaningful.
   */
  private final AtomicLong lastTs = new AtomicLong(0L);

  /** Latest update per entry; guarded by {@code this}. */
  private final Map<byte[], PendingUpdate> pending = new TreeMap<>(Bytes.BYTES_RAWCOMPARATOR);

  private boolean pushInFlight;

  private static final class PendingUpdate {
    private final long ts;
    private final AuthManager.EntryUpdate update;

    PendingUpdate(long ts, AuthManager.EntryUpdate update) {
      this.ts = ts;
      this.update = update;
    }
  }

  /** A set of entry updates to broadcast, e.g. the drain of the pending queue. */
  public static final class ChangeSet {
    private final List<AclEntryUpdate> updates;
    private final boolean full;
    private final long ts;

    ChangeSet(List<AclEntryUpdate> updates, boolean full, long ts) {
      this.updates = updates;
      this.full = full;
      this.ts = ts;
    }

    public List<AclEntryUpdate> getUpdates() {
      return updates;
    }

    public boolean isFull() {
      return full;
    }

    public long getTs() {
      return ts;
    }
  }

  public ACLManager(Configuration conf, MasterServices masterServices, AclRefresher refresher) {
    this.conf = conf;
    this.masterServices = masterServices;
    this.refresher = refresher;
  }

  /**
   * Seeds the timestamp clock, loads the master's own cache, pushes one full round to all servers
   * and starts listening to {@code hbase:acl} writes. Called when this master becomes active.
   */
  public void initialize() throws IOException {
    PermissionStorage.AclSnapshot snapshot;
    try {
      snapshot = PermissionStorage.loadAllSnapshot(conf);
    } catch (TableNotFoundException e) {
      // a fresh cluster: the acl table is created later by the AccessController postStartMaster
      // hook
      snapshot = new PermissionStorage.AclSnapshot(Collections.emptyMap(), 0L);
    }
    long seed = Math.max(EnvironmentEdgeManager.currentTime(), snapshot.getMaxTimestamp() + 1);
    lastTs.set(seed);
    refresher.submitFullSnapshot(seed, snapshot.getPermissions());
    // the push timestamp is above every write the snapshot contains, so the push is never
    // dropped as stale by an RS; later writes get higher timestamps and apply on top of it
    submitFullPush(toProto(snapshot.getPermissions()), nextTimestamp());
    LOG.info("ACLManager initialized, timestamps start at {}", seed);
  }

  /**
   * Next timestamp for an {@code hbase:acl} write, to be stamped into the cells of the write so the
   * cell timestamps carry the master assigned write order.
   */
  public long nextTimestamp() {
    long now = EnvironmentEdgeManager.currentTime();
    return lastTs.updateAndGet(prev -> Math.max(now, prev + 1));
  }

  /**
   * Queues an updated entry for broadcast to all servers. Callers apply the update to their local
   * cache themselves, see {@code AclTableManager}.
   */
  public void onEntryUpdate(long ts, AuthManager.EntryUpdate update) {
    synchronized (this) {
      pending.put(update.getEntry(), new PendingUpdate(ts, update));
      maybeSubmitPush();
    }
  }

  /**
   * Takes all pending entry updates as one change set and returns {@code null} when there are none.
   * Called by {@link RefreshAclProcedure} as its first step; the change set is persisted in the
   * procedure store, so it survives a master failover and is replayed at-least-once.
   */
  public synchronized ChangeSet drainPending() {
    if (pending.isEmpty()) {
      return null;
    }
    long ts = 0L;
    List<AclEntryUpdate> updates = new ArrayList<>(pending.size());
    for (PendingUpdate p : pending.values()) {
      updates.add(toProto(p.update));
      ts = Math.max(ts, p.ts);
    }
    pending.clear();
    return new ChangeSet(updates, false, ts);
  }

  /**
   * Called when a push procedure terminates (success or not); submits the next one when updates
   * piled up meanwhile.
   */
  public synchronized void onPushComplete() {
    pushInFlight = false;
    maybeSubmitPush();
  }

  public synchronized boolean isPushInFlight() {
    return pushInFlight;
  }

  public long getLastTimestamp() {
    return lastTs.get();
  }

  private void maybeSubmitPush() {
    if (pushInFlight || pending.isEmpty()) {
      return;
    }
    pushInFlight = true;
    submitPushProcedure(new RefreshAclProcedure());
  }

  private synchronized void submitFullPush(List<AclEntryUpdate> updates, long ts) {
    pushInFlight = true;
    submitPushProcedure(new RefreshAclProcedure(updates, true, ts));
  }

  /** Visible for testing: hands the push procedure over to the procedure executor. */
  void submitPushProcedure(RefreshAclProcedure procedure) {
    masterServices.getMasterProcedureExecutor().submitProcedure(procedure);
  }

  private static List<AclEntryUpdate>
    toProto(Map<byte[], ListMultimap<String, UserPermission>> snapshot) {
    List<AclEntryUpdate> updates = new ArrayList<>(snapshot.size());
    for (Map.Entry<byte[], ListMultimap<String, UserPermission>> e : snapshot.entrySet()) {
      updates.add(AclEntryUpdate.newBuilder().setEntry(ByteString.copyFrom(e.getKey()))
        .setPerms(AccessControlUtil.toUserTablePermissions(e.getValue())).build());
    }
    return updates;
  }

  private static AclEntryUpdate toProto(AuthManager.EntryUpdate update) {
    AclEntryUpdate.Builder builder = AclEntryUpdate.newBuilder()
      .setEntry(ByteString.copyFrom(update.getEntry())).setDeleted(update.isDeleted());
    if (!update.isDeleted()) {
      builder.setPerms(AccessControlUtil.toUserTablePermissions(update.getPermissions()));
    }
    return builder.build();
  }
}
