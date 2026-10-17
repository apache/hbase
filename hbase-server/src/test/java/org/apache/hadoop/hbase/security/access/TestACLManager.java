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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.MockNoopMasterServices;
import org.apache.hadoop.hbase.master.procedure.RefreshAclProcedure;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.yetus.audience.InterfaceAudience;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.hbase.thirdparty.com.google.common.collect.ArrayListMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.ListMultimap;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.AclEntryUpdate;

@InterfaceAudience.Private
@Tag(SecurityTests.TAG)
@Tag(SmallTests.TAG)
public class TestACLManager {

  private static final TableName TEST_TABLE = TableName.valueOf("aclManagerTable");
  private static final TableName TEST_TABLE2 = TableName.valueOf("aclManagerTable2");

  private final List<RefreshAclProcedure> submitted = new ArrayList<>();

  private ACLManager aclManager;

  @BeforeEach
  public void setUp() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.set(Superusers.SUPERUSER_CONF_KEY, "admin");
    Superusers.initialize(conf);
    AclSnapshot aclSnapshot = new AclSnapshot();
    // this test only exercises the push pipeline: no acl table access, no permission checks
    aclManager = new ACLManager(conf, new MockNoopMasterServices(conf), null, aclSnapshot, null) {
      @Override
      void submitPushProcedure(RefreshAclProcedure procedure) {
        submitted.add(procedure);
      }
    };
  }

  @Test
  public void testTimestampsAreMonotonic() {
    long first = aclManager.nextTimestamp();
    assertTrue(aclManager.nextTimestamp() > first);
    assertTrue(aclManager.getLastTimestamp() >= first);
  }

  @Test
  public void testPendingUpdatesMergePerEntry() {
    aclManager.onEntryUpdate(100, replace(TEST_TABLE, "george", Permission.Action.READ));
    aclManager.onEntryUpdate(110, replace(TEST_TABLE, "george", Permission.Action.WRITE));
    aclManager.onEntryUpdate(120, replace(TEST_TABLE2, "hubert", Permission.Action.READ));
    // one push is in flight for all of them
    assertEquals(1, submitted.size());

    ACLManager.ChangeSet changeSet = aclManager.drainPending();
    assertNotNull(changeSet);
    // the latest write of an entry wins and each entry appears at most once
    assertEquals(2, changeSet.getUpdates().size());
    assertEquals(120, changeSet.getTs());
    for (AclEntryUpdate update : changeSet.getUpdates()) {
      if (update.getEntry().toStringUtf8().equals(TEST_TABLE.getNameAsString())) {
        // the later write of the entry won
        assertFalse(update.getDeleted());
        assertEquals(1, update.getPerms().getUserPermissionsCount());
        assertEquals("george", update.getPerms().getUserPermissions(0).getUser().toStringUtf8());
      }
    }
    assertNull(aclManager.drainPending());
  }

  @Test
  public void testSingleFlight() {
    aclManager.onEntryUpdate(100, replace(TEST_TABLE, "george", Permission.Action.READ));
    assertEquals(1, submitted.size());

    // updates arriving while a push runs only queue up
    aclManager.onEntryUpdate(110, replace(TEST_TABLE, "george", Permission.Action.WRITE));
    aclManager.onEntryUpdate(120, replace(TEST_TABLE2, "hubert", Permission.Action.READ));
    assertEquals(1, submitted.size());
    assertNotNull(aclManager.drainPending());

    // an update arriving after the drain, while the push is still running
    aclManager.onEntryUpdate(130, replace(TEST_TABLE, "george", Permission.Action.ADMIN));
    assertEquals(1, submitted.size());

    // it goes into the next push once the running one completes
    aclManager.onPushComplete();
    assertEquals(2, submitted.size());
    assertNotNull(aclManager.drainPending());

    // nothing pending at completion: no further push
    aclManager.onPushComplete();
    assertEquals(2, submitted.size());
  }

  @Test
  public void testDeletedUpdateQueued() {
    aclManager.onEntryUpdate(100, replace(TEST_TABLE, "george", Permission.Action.READ));
    aclManager.onEntryUpdate(150, AclSnapshot.EntryUpdate.deleted(TEST_TABLE.getName()));
    ACLManager.ChangeSet changeSet = aclManager.drainPending();
    assertNotNull(changeSet);
    assertEquals(1, changeSet.getUpdates().size());
    assertTrue(changeSet.getUpdates().get(0).getDeleted());
    assertEquals(150, changeSet.getTs());
  }

  private AclSnapshot.EntryUpdate replace(TableName table, String user, Permission.Action action) {
    ListMultimap<String, UserPermission> perms = ArrayListMultimap.create();
    perms.put(user,
      new UserPermission(user, Permission.newBuilder(table).withActions(action).build()));
    return AclSnapshot.EntryUpdate.replace(table.getName(), perms);
  }
}
