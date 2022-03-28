/**
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
package org.apache.hadoop.hbase.zookeeper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.testclassification.ZKTests;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category({ ZKTests.class, SmallTests.class })
public class TestZKUtilNoServer {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestZKUtilNoServer.class);

  @Test
  public void testUnsecure() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.set(Superusers.SUPERUSER_CONF_KEY, "user1");
    String node = "/hbase/testUnsecure";
    ZKWatcher watcher = new ZKWatcher(conf, node, null, false);
    List<ACL> aclList = watcher.createACL(node, false);
    assertEquals(1, aclList.size());
    assertTrue(aclList.contains(Ids.OPEN_ACL_UNSAFE.iterator().next()));
  }

  @Test
  public void testSecuritySingleSuperuser() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.set(Superusers.SUPERUSER_CONF_KEY, "user1");
    String node = "/hbase/testSecuritySingleSuperuser";
    ZKWatcher watcher = new ZKWatcher(conf, node, null, false);
    List<ACL> aclList = watcher.createACL(node, true);
    assertEquals(2, aclList.size()); // 1+1, since ACL will be set for the creator by default
    assertTrue(aclList.contains(new ACL(Perms.ALL, new Id("sasl", "user1"))));
    assertTrue(aclList.contains(Ids.CREATOR_ALL_ACL.iterator().next()));
  }

  @Test
  public void testCreateACL() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.set(Superusers.SUPERUSER_CONF_KEY, "user1,@group1,user2,@group2,user3");
    String node = "/hbase/testCreateACL";
    ZKWatcher watcher = new ZKWatcher(conf, node, null, false);
    List<ACL> aclList = watcher.createACL(node, true);
    assertEquals(4, aclList.size()); // 3+1, since ACL will be set for the creator by default
    assertFalse(aclList.contains(new ACL(Perms.ALL, new Id("sasl", "@group1"))));
    assertFalse(aclList.contains(new ACL(Perms.ALL, new Id("sasl", "@group2"))));
    assertTrue(aclList.contains(new ACL(Perms.ALL, new Id("sasl", "user1"))));
    assertTrue(aclList.contains(new ACL(Perms.ALL, new Id("sasl", "user2"))));
    assertTrue(aclList.contains(new ACL(Perms.ALL, new Id("sasl", "user3"))));
  }

  @Test
  public void testCreateACLWithSameUser() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.set(Superusers.SUPERUSER_CONF_KEY, "user4,@group1,user5,user6");
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("user4"));
    String node = "/hbase/testCreateACL";
    ZKWatcher watcher = new ZKWatcher(conf, node, null, false);
    List<ACL> aclList = watcher.createACL(node, true);
    assertEquals(3, aclList.size()); // 3, since service user the same as one of superuser
    assertFalse(aclList.contains(new ACL(Perms.ALL, new Id("sasl", "@group1"))));
    assertTrue(aclList.contains(new ACL(Perms.ALL, new Id("auth", ""))));
    assertTrue(aclList.contains(new ACL(Perms.ALL, new Id("sasl", "user5"))));
    assertTrue(aclList.contains(new ACL(Perms.ALL, new Id("sasl", "user6"))));
  }

  @Test(expected = KeeperException.SystemErrorException.class)
  public void testInterruptedDuringAction()
      throws IOException, KeeperException, InterruptedException {
    final RecoverableZooKeeper recoverableZk = Mockito.mock(RecoverableZooKeeper.class);
    ZKWatcher zkw = new ZKWatcher(HBaseConfiguration.create(), "unittest", null) {
      @Override
      public RecoverableZooKeeper getRecoverableZooKeeper() {
        return recoverableZk;
      }
    };
    Mockito.doThrow(new InterruptedException()).when(recoverableZk)
        .getChildren(zkw.getZNodePaths().baseZNode, null);
    ZKUtil.listChildrenNoWatch(zkw, zkw.getZNodePaths().baseZNode);
  }
}
