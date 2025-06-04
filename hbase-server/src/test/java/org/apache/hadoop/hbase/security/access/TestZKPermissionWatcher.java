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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter.Predicate;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ArrayListMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.ListMultimap;

/**
 * Test the reading and writing of access permissions to and from zookeeper.
 */
@Category({ SecurityTests.class, MediumTests.class })
public class TestZKPermissionWatcher {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestZKPermissionWatcher.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestZKPermissionWatcher.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static AuthManager AUTH_A;
  private static AuthManager AUTH_B;
  private static ZKPermissionWatcher WATCHER_A;
  private static ZKPermissionWatcher WATCHER_B;
  private final static Abortable ABORTABLE = new Abortable() {
    private final AtomicBoolean abort = new AtomicBoolean(false);

    @Override
    public void abort(String why, Throwable e) {
      LOG.info(why, e);
      abort.set(true);
    }

    @Override
    public boolean isAborted() {
      return abort.get();
    }
  };

  private static TableName TEST_TABLE = TableName.valueOf("perms_test");

  @BeforeClass
  public static void beforeClass() throws Exception {
    // setup configuration
    Configuration conf = UTIL.getConfiguration();
    SecureTestUtil.enableSecurity(conf);

    // start minicluster
    UTIL.startMiniCluster();
    AUTH_A = new AuthManager(conf);
    AUTH_B = new AuthManager(conf);
    WATCHER_A = new ZKPermissionWatcher(
      new ZKWatcher(conf, "TestZKPermissionsWatcher_1", ABORTABLE), AUTH_A, conf);
    WATCHER_B = new ZKPermissionWatcher(
      new ZKWatcher(conf, "TestZKPermissionsWatcher_2", ABORTABLE), AUTH_B, conf);
    WATCHER_A.start();
    WATCHER_B.start();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    WATCHER_A.close();
    WATCHER_B.close();
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testPermissionsWatcher() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    User george = User.createUserForTesting(conf, "george", new String[] {});
    User hubert = User.createUserForTesting(conf, "hubert", new String[] {});
    ListMultimap<String, UserPermission> permissions = ArrayListMultimap.create();

    assertFalse(AUTH_A.authorizeUserTable(george, TEST_TABLE, Permission.Action.READ));
    assertFalse(AUTH_A.authorizeUserTable(george, TEST_TABLE, Permission.Action.WRITE));
    assertFalse(AUTH_A.authorizeUserTable(hubert, TEST_TABLE, Permission.Action.READ));
    assertFalse(AUTH_A.authorizeUserTable(hubert, TEST_TABLE, Permission.Action.WRITE));

    assertFalse(AUTH_B.authorizeUserTable(george, TEST_TABLE, Permission.Action.READ));
    assertFalse(AUTH_B.authorizeUserTable(george, TEST_TABLE, Permission.Action.WRITE));
    assertFalse(AUTH_B.authorizeUserTable(hubert, TEST_TABLE, Permission.Action.READ));
    assertFalse(AUTH_B.authorizeUserTable(hubert, TEST_TABLE, Permission.Action.WRITE));

    // update ACL: george RW
    writeToZookeeper(WATCHER_A,
      updatePermissions(permissions, george, Permission.Action.READ, Permission.Action.WRITE));
    waitForModification(AUTH_B, 1000);

    // check it
    assertTrue(AUTH_A.authorizeUserTable(george, TEST_TABLE, Permission.Action.READ));
    assertTrue(AUTH_A.authorizeUserTable(george, TEST_TABLE, Permission.Action.WRITE));
    assertTrue(AUTH_B.authorizeUserTable(george, TEST_TABLE, Permission.Action.READ));
    assertTrue(AUTH_B.authorizeUserTable(george, TEST_TABLE, Permission.Action.WRITE));
    assertFalse(AUTH_A.authorizeUserTable(hubert, TEST_TABLE, Permission.Action.READ));
    assertFalse(AUTH_A.authorizeUserTable(hubert, TEST_TABLE, Permission.Action.WRITE));
    assertFalse(AUTH_B.authorizeUserTable(hubert, TEST_TABLE, Permission.Action.READ));
    assertFalse(AUTH_B.authorizeUserTable(hubert, TEST_TABLE, Permission.Action.WRITE));

    // update ACL: hubert R
    writeToZookeeper(WATCHER_B, updatePermissions(permissions, hubert, Permission.Action.READ));
    waitForModification(AUTH_A, 1000);

    // check it
    assertTrue(AUTH_A.authorizeUserTable(george, TEST_TABLE, Permission.Action.READ));
    assertTrue(AUTH_A.authorizeUserTable(george, TEST_TABLE, Permission.Action.WRITE));
    assertTrue(AUTH_B.authorizeUserTable(george, TEST_TABLE, Permission.Action.READ));
    assertTrue(AUTH_B.authorizeUserTable(george, TEST_TABLE, Permission.Action.WRITE));
    assertTrue(AUTH_A.authorizeUserTable(hubert, TEST_TABLE, Permission.Action.READ));
    assertFalse(AUTH_A.authorizeUserTable(hubert, TEST_TABLE, Permission.Action.WRITE));
    assertTrue(AUTH_B.authorizeUserTable(hubert, TEST_TABLE, Permission.Action.READ));
    assertFalse(AUTH_B.authorizeUserTable(hubert, TEST_TABLE, Permission.Action.WRITE));
  }

  @Test
  public void testRaceConditionOnPermissionUpdate() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    User tom = User.createUserForTesting(conf, "tom", new String[] {});
    User jerry = User.createUserForTesting(conf, "jerry", new String[] {});
    ListMultimap<String, UserPermission> permissions = ArrayListMultimap.create();

    // update ACL: george RW
    writeToZookeeper(WATCHER_A,
      updatePermissions(permissions, tom, Permission.Action.READ, Permission.Action.WRITE));
    waitForModification(AUTH_A, 1000);

    // check it
    assertTrue(AUTH_A.authorizeUserTable(tom, TEST_TABLE, Permission.Action.READ));
    assertTrue(AUTH_A.authorizeUserTable(tom, TEST_TABLE, Permission.Action.WRITE));

    // update ACL: hubert A
    writeToZookeeper(WATCHER_A, updatePermissions(permissions, jerry, Permission.Action.ADMIN));
    // intended not to waitForModification(AUTH_A, 1000);
    // check george permission should not be updated/removed while updating permission for hubert
    for (int i = 0; i < 5000; i++) {
      assertTrue(AUTH_A.authorizeUserTable(tom, TEST_TABLE, Permission.Action.READ));
      assertTrue(AUTH_A.authorizeUserTable(tom, TEST_TABLE, Permission.Action.WRITE));
    }
  }

  private ListMultimap<String, UserPermission> updatePermissions(
    ListMultimap<String, UserPermission> permissions, User user, Permission.Action... actions) {
    List<UserPermission> acl = new ArrayList<>(1);
    acl.add(new UserPermission(user.getShortName(),
      Permission.newBuilder(TEST_TABLE).withActions(actions).build()));
    permissions.putAll(user.getShortName(), acl);
    return permissions;
  }

  private void writeToZookeeper(ZKPermissionWatcher watcher,
    ListMultimap<String, UserPermission> permissions) {
    byte[] serialized =
      PermissionStorage.writePermissionsAsBytes(permissions, UTIL.getConfiguration());
    watcher.writeToZookeeper(TEST_TABLE.getName(), serialized);
  }

  private void waitForModification(AuthManager authManager, long sleep) throws Exception {
    final long mtime = authManager.getMTime();
    // Wait for the update to propagate
    UTIL.waitFor(10000, 100, new Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return authManager.getMTime() > mtime;
      }
    });
    Thread.sleep(sleep);
  }
}
