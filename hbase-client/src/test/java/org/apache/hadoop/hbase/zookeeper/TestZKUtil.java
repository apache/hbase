/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.zookeeper;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

/**
 *
 */
@Category({SmallTests.class})
public class TestZKUtil {

  @Test
  public void testUnsecure() throws ZooKeeperConnectionException, IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.set(Superusers.SUPERUSER_CONF_KEY, "user1");
    String node = "/hbase/testUnsecure";
    ZooKeeperWatcher watcher = new ZooKeeperWatcher(conf, node, null, false);
    List<ACL> aclList = ZKUtil.createACL(watcher, node, false);
    Assert.assertEquals(1, aclList.size());
    Assert.assertTrue(aclList.contains(Ids.OPEN_ACL_UNSAFE.iterator().next()));
  }

  @Test
  public void testSecuritySingleSuperuser() throws ZooKeeperConnectionException, IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.set(Superusers.SUPERUSER_CONF_KEY, "user1");
    String node = "/hbase/testSecuritySingleSuperuser";
    ZooKeeperWatcher watcher = new ZooKeeperWatcher(conf, node, null, false);
    List<ACL> aclList = ZKUtil.createACL(watcher, node, true);
    Assert.assertEquals(2, aclList.size()); // 1+1, since ACL will be set for the creator by default
    Assert.assertTrue(aclList.contains(new ACL(Perms.ALL, new Id("sasl", "user1"))));
    Assert.assertTrue(aclList.contains(Ids.CREATOR_ALL_ACL.iterator().next()));
  }

  @Test
  public void testCreateACL() throws ZooKeeperConnectionException, IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.set(Superusers.SUPERUSER_CONF_KEY, "user1,@group1,user2,@group2,user3");
    String node = "/hbase/testCreateACL";
    ZooKeeperWatcher watcher = new ZooKeeperWatcher(conf, node, null, false);
    List<ACL> aclList = ZKUtil.createACL(watcher, node, true);
    Assert.assertEquals(4, aclList.size()); // 3+1, since ACL will be set for the creator by default
    Assert.assertFalse(aclList.contains(new ACL(Perms.ALL, new Id("sasl", "@group1"))));
    Assert.assertFalse(aclList.contains(new ACL(Perms.ALL, new Id("sasl", "@group2"))));
    Assert.assertTrue(aclList.contains(new ACL(Perms.ALL, new Id("sasl", "user1"))));
    Assert.assertTrue(aclList.contains(new ACL(Perms.ALL, new Id("sasl", "user2"))));
    Assert.assertTrue(aclList.contains(new ACL(Perms.ALL, new Id("sasl", "user3"))));
  }

  @Test
  public void testCreateACLWithSameUser() throws ZooKeeperConnectionException, IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.set(Superusers.SUPERUSER_CONF_KEY, "user4,@group1,user5,user6");
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("user4"));
    String node = "/hbase/testCreateACL";
    ZooKeeperWatcher watcher = new ZooKeeperWatcher(conf, node, null, false);
    List<ACL> aclList = ZKUtil.createACL(watcher, node, true);
    Assert.assertEquals(3, aclList.size()); // 3, since service user the same as one of superuser
    Assert.assertFalse(aclList.contains(new ACL(Perms.ALL, new Id("sasl", "@group1"))));
    Assert.assertTrue(aclList.contains(new ACL(Perms.ALL, new Id("auth", ""))));
    Assert.assertTrue(aclList.contains(new ACL(Perms.ALL, new Id("sasl", "user5"))));
    Assert.assertTrue(aclList.contains(new ACL(Perms.ALL, new Id("sasl", "user6"))));
  }

  @Test
  public void testInterruptedDuringAction()
      throws ZooKeeperConnectionException, IOException, KeeperException, InterruptedException {
    final RecoverableZooKeeper recoverableZk = Mockito.mock(RecoverableZooKeeper.class);
    ZooKeeperWatcher zkw = new ZooKeeperWatcher(HBaseConfiguration.create(), "unittest", null) {
      @Override
      public RecoverableZooKeeper getRecoverableZooKeeper() {
        return recoverableZk;
      }
    };
    Mockito.doThrow(new InterruptedException()).when(recoverableZk)
        .getChildren(zkw.assignmentZNode, null);
    try {
      ZKUtil.listChildrenNoWatch(zkw, zkw.assignmentZNode);
    } catch (KeeperException.SystemErrorException e) {
      // expected
      return;
    }
    Assert.fail("Should have thrown KeeperException but not");
  }
}
