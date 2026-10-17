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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.hbase.thirdparty.com.google.common.collect.ArrayListMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.ListMultimap;

@InterfaceAudience.Private
@Tag(SecurityTests.TAG)
@Tag(SmallTests.TAG)
public class TestAclStore {

  private static final TableName TEST_TABLE = TableName.valueOf("aclStoreTable");

  private AuthManager authManager;
  private AclSnapshot aclSnapshot;
  private AclStore store;
  private User george;

  @BeforeEach
  public void setUp() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.set(Superusers.SUPERUSER_CONF_KEY, "admin");
    Superusers.initialize(conf);
    aclSnapshot = new AclSnapshot();
    authManager = new AuthManager(aclSnapshot);
    store = new AclStore(conf, aclSnapshot, new Stoppable() {
      @Override
      public void stop(String why) {
      }

      @Override
      public boolean isStopped() {
        return false;
      }
    });
    george = User.createUserForTesting(conf, "george", new String[0]);
  }

  @AfterEach
  public void tearDown() {
    store.close();
  }

  @Test
  public void testUpdatesApplyInTimestampOrder() throws IOException {
    store.applyUpdates(100, Collections.singletonList(replace(Permission.Action.READ)));
    assertTrue(canRead());
    assertEquals(100, store.getAppliedTimestamp());

    // not newer than the applied one: dropped, the cache does not regress
    store.applyUpdates(50, Collections.singletonList(replace(Permission.Action.WRITE)));
    assertTrue(canRead());
    assertFalse(canWrite());
    assertEquals(100, store.getAppliedTimestamp());

    store.applyUpdates(150, Collections.singletonList(replace(Permission.Action.WRITE)));
    assertTrue(canWrite());
    assertEquals(150, store.getAppliedTimestamp());
  }

  @Test
  public void testDeletedEntriesNotResurrectedByStaleUpdates() throws IOException {
    store.applyUpdates(100, Collections.singletonList(replace(Permission.Action.READ)));
    store.applyUpdates(200,
      Collections.singletonList(AclSnapshot.EntryUpdate.deleted(TEST_TABLE.getName())));
    assertFalse(canRead());

    // a stale update from before the delete must not bring the entry back
    store.applyUpdates(150, Collections.singletonList(replace(Permission.Action.READ)));
    assertFalse(canRead());
    assertEquals(200, store.getAppliedTimestamp());
  }

  @Test
  public void testFullSnapshotAdvancesWatermark() throws IOException {
    store.applyFullSnapshot(200, snapshot(replace(Permission.Action.READ)));
    assertTrue(canRead());
    assertEquals(200, store.getAppliedTimestamp());

    // older than the snapshot content: dropped
    store.applyUpdates(150, Collections.singletonList(replace(Permission.Action.WRITE)));
    assertTrue(canRead());
    assertFalse(canWrite());

    // newer than the snapshot: applied on top of it
    store.applyUpdates(250, Collections.singletonList(replace(Permission.Action.WRITE)));
    assertTrue(canWrite());
    assertEquals(250, store.getAppliedTimestamp());
  }

  @Test
  public void testStaleFullSnapshotIsDropped() throws IOException {
    store.applyUpdates(300, Collections.singletonList(replace(Permission.Action.WRITE)));
    assertTrue(canWrite());

    // an older full snapshot (e.g. a replayed procedure after a master failover) is dropped
    store.applyFullSnapshot(200, snapshot(replace(Permission.Action.READ)));
    assertTrue(canWrite());
    assertFalse(canRead());
  }

  private AclSnapshot.EntryUpdate replace(Permission.Action action) {
    ListMultimap<String, UserPermission> perms = ArrayListMultimap.create();
    perms.put("george",
      new UserPermission("george", Permission.newBuilder(TEST_TABLE).withActions(action).build()));
    return AclSnapshot.EntryUpdate.replace(TEST_TABLE.getName(), perms);
  }

  private Map<byte[], ListMultimap<String, UserPermission>> snapshot(AclSnapshot.EntryUpdate e) {
    Map<byte[], ListMultimap<String, UserPermission>> map =
      new TreeMap<>(Bytes.BYTES_RAWCOMPARATOR);
    map.put(e.getEntry(), e.getPermissions());
    return map;
  }

  private boolean canRead() {
    return authManager.authorizeUserTable(george, TEST_TABLE, Permission.Action.READ);
  }

  private boolean canWrite() {
    return authManager.authorizeUserTable(george, TEST_TABLE, Permission.Action.WRITE);
  }
}
