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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.procedure.RefreshAclProcedure;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ListMultimap;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.AclEntryUpdate;

/**
 * Master side ACL holder: the {@code hbase:acl} table is the authoritative store, this class owns
 * the master's {@link AclSnapshot} of it and drives everything around it:
 * <ul>
 * <li>performs permission updates and queries on {@code hbase:acl} (all writes go through this
 * class);</li>
 * <li>assigns the monotonic timestamps of its writes;</li>
 * <li>keeps its snapshot in step with every write (the entry state is applied synchronously, so the
 * master never lags behind its own writes and needs no periodic reload);</li>
 * <li>pushes the changes to all region servers through a single in-flight
 * {@link RefreshAclProcedure}: updates arriving while a push is running are merged into the next
 * one, so at most one broadcast is in flight and pushes are naturally coalesced.</li>
 * </ul>
 * The refresh procedure only updates the region servers; the master applies each change to its own
 * snapshot at write time and is reloaded from the table when it becomes active.
 * <p>
 * The whole write path (timestamp assignment, table write, entry read back, local apply, queueing
 * for broadcast) is serialized, so writes apply locally in their timestamp order and a concurrent
 * write can never overwrite a newer state with an older one. The lock is held across the table
 * write; ACL changes are rare admin operations, this does not hurt.
 * </p>
 * Servers that miss a push (crash, abandonment) converge through their startup load and the
 * periodic reload. Direct writes to {@code hbase:acl} that bypass this class are only picked up by
 * that periodic reload.
 */
@InterfaceAudience.Private
public class ACLManager {

  private static final Logger LOG = LoggerFactory.getLogger(ACLManager.class);

  private final Configuration conf;
  private final MasterServices masterServices;
  private final Connection connection;
  private final AclSnapshot snapshot;
  private final AccessChecker accessChecker;

  /** Guards the write path; see the class javadoc. */
  private final Object writeLock = new Object();

  /**
   * Timestamps assigned to acl table writes. Seeded from the acl table content and the wall clock
   * at activation, so they never move backwards across restarts or master failovers and the RS side
   * watermark of {@code AclStore} stays meaningful.
   */
  private final AtomicLong lastTs = new AtomicLong(0L);

  /** Latest update per entry; guarded by {@code this}. */
  private final Map<byte[], PendingUpdate> pending = new TreeMap<>(Bytes.BYTES_RAWCOMPARATOR);

  private boolean pushInFlight;

  private static final class PendingUpdate {
    private final long ts;
    private final AclSnapshot.EntryUpdate update;

    PendingUpdate(long ts, AclSnapshot.EntryUpdate update) {
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

  public ACLManager(Configuration conf, MasterServices masterServices, Connection connection,
    AclSnapshot snapshot, AccessChecker accessChecker) {
    this.conf = conf;
    this.masterServices = masterServices;
    this.connection = connection;
    this.snapshot = snapshot;
    this.accessChecker = accessChecker;
  }

  /**
   * Seeds the timestamp clock, loads the master's snapshot from {@code hbase:acl} and pushes one
   * full round to all region servers. Called when this master becomes active.
   */
  public void initialize() throws IOException {
    PermissionStorage.AclTableContent content;
    long seed;
    synchronized (writeLock) {
      try {
        content = PermissionStorage.loadAllSnapshot(conf);
      } catch (TableNotFoundException e) {
        // a fresh cluster: the acl table is created later by the AccessController postStartMaster
        // hook
        content = new PermissionStorage.AclTableContent(Collections.emptyMap(), 0L);
      }
      seed = Math.max(EnvironmentEdgeManager.currentTime(), content.getMaxTimestamp() + 1);
      lastTs.set(seed);
      // serialized with the write path, so a write either lands before the scan (and is part of
      // the snapshot) or applies its entry update after the swap
      snapshot.applyFullSnapshot(content.getPermissions());
    }
    // the push timestamp is above every write the snapshot contains, so the push is never
    // dropped as stale by an RS; later writes get higher timestamps and apply on top of it
    submitFullPush(toProto(content.getPermissions()), nextTimestamp());
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

  /** Grants the given permission, optionally merging it into the existing actions. */
  public void grant(UserPermission userPermission, boolean mergeExistingPermissions)
    throws IOException {
    synchronized (writeLock) {
      byte[] entry = PermissionStorage.userPermissionRowKey(userPermission.getPermission());
      long ts = nextTimestamp();
      try (Table t = connection.getTable(PermissionStorage.ACL_TABLE_NAME)) {
        PermissionStorage.addUserPermission(conf, userPermission, t, mergeExistingPermissions, ts);
        announce(t, entry, ts);
      }
    }
  }

  /** Revokes the given permission. */
  public void revoke(UserPermission userPermission) throws IOException {
    synchronized (writeLock) {
      byte[] entry = PermissionStorage.userPermissionRowKey(userPermission.getPermission());
      long ts = nextTimestamp();
      try (Table t = connection.getTable(PermissionStorage.ACL_TABLE_NAME)) {
        PermissionStorage.removeUserPermission(conf, userPermission, t, ts);
        announce(t, entry, ts);
      }
    }
  }

  /** Drops all permissions of a removed table. */
  public void removeTablePermissions(TableName tableName) throws IOException {
    synchronized (writeLock) {
      long ts = nextTimestamp();
      try (Table t = connection.getTable(PermissionStorage.ACL_TABLE_NAME)) {
        PermissionStorage.removeTablePermissions(conf, tableName, t, ts);
        announce(t, tableName.getName(), ts);
      }
    }
  }

  /** Drops all permissions of a removed namespace. */
  public void removeNamespacePermissions(String namespace) throws IOException {
    synchronized (writeLock) {
      byte[] entry = Bytes.toBytes(PermissionStorage.toNamespaceEntry(namespace));
      long ts = nextTimestamp();
      try (Table t = connection.getTable(PermissionStorage.ACL_TABLE_NAME)) {
        PermissionStorage.removeNamespacePermissions(conf, namespace, t, ts);
        announce(t, entry, ts);
      }
    }
  }

  public List<UserPermission> getUserTablePermissions(TableName table, byte[] cf, byte[] cq,
    String userName, boolean filter) throws IOException {
    return PermissionStorage.getUserTablePermissions(conf, table, cf, cq, userName, filter);
  }

  public List<UserPermission> getUserNamespacePermissions(String namespace, String userName,
    boolean filter) throws IOException {
    return PermissionStorage.getUserNamespacePermissions(conf, namespace, userName, filter);
  }

  public List<UserPermission> getUserPermissions(String userName, boolean filter)
    throws IOException {
    return PermissionStorage.getUserPermissions(conf, null, null, null, userName, filter);
  }

  /**
   * Checks the given permissions of the given user against the authorization cache.
   * @param caller   the request user
   * @param userName the user to check, when different from the caller the caller needs the
   *                 permission to check another user
   */
  public List<Boolean> hasUserPermissions(User caller, String userName,
    List<Permission> permissions) throws IOException {
    if (!caller.getShortName().equals(userName)) {
      List<String> groups = AccessChecker.getUserGroups(userName);
      caller = new AccessChecker.InputUser(userName, groups.toArray(new String[groups.size()]));
    }
    List<Boolean> results = new ArrayList<>(permissions.size());
    if (accessChecker != null) {
      for (Permission permission : permissions) {
        results.add(accessChecker.hasUserPermission(caller, "hasUserPermissions", permission));
      }
    } else {
      for (int i = 0; i < permissions.size(); i++) {
        results.add(true);
      }
    }
    return results;
  }

  /**
   * Reads the entry state back after the write landed, applies it to the local snapshot and queues
   * it for broadcast to the region servers.
   */
  private void announce(Table t, byte[] entry, long ts) throws IOException {
    ListMultimap<String, UserPermission> perms =
      PermissionStorage.getPermissions(conf, entry, t, null, null, null, false);
    AclSnapshot.EntryUpdate update = perms.isEmpty()
      ? AclSnapshot.EntryUpdate.deleted(entry)
      : AclSnapshot.EntryUpdate.replace(entry, perms);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Announcing acl entry {} update at ts {}, deleted={}", Bytes.toStringBinary(entry),
        ts, update.isDeleted());
    }
    // the write is done and the write path is serialized, the state read back is the current one:
    // apply it right away so the master's snapshot never lags behind its own writes
    snapshot.applyUpdates(Collections.singletonList(update));
    onEntryUpdate(ts, update);
  }

  /**
   * Queues an updated entry for broadcast to all region servers. Called by the write path after it
   * applied the update locally.
   */
  public void onEntryUpdate(long ts, AclSnapshot.EntryUpdate update) {
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

  private static AclEntryUpdate toProto(AclSnapshot.EntryUpdate update) {
    AclEntryUpdate.Builder builder = AclEntryUpdate.newBuilder()
      .setEntry(ByteString.copyFrom(update.getEntry())).setDeleted(update.isDeleted());
    if (!update.isDeleted()) {
      builder.setPerms(AccessControlUtil.toUserTablePermissions(update.getPermissions()));
    }
    return builder.build();
  }
}
