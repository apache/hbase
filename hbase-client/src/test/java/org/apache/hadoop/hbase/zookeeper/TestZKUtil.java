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
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 *
 */
@Category({SmallTests.class})
public class TestZKUtil {

  @Test
  public void testCreateACL() throws ZooKeeperConnectionException, IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.set(Superusers.SUPERUSER_CONF_KEY, "user1,@group1,user2,@group2,user3");
    String node = "/hbase/testCreateACL";
    ZooKeeperWatcher watcher = new ZooKeeperWatcher(conf, node, null, false);
    List<ACL> aclList = ZKUtil.createACL(watcher, node, true);
    Assert.assertEquals(aclList.size(), 4); // 3+1, since ACL will be set for the creator by default
    Assert.assertTrue(!aclList.contains(new ACL(Perms.ALL, new Id("auth", "@group1")))
        && !aclList.contains(new ACL(Perms.ALL, new Id("auth", "@group2"))));
    Assert.assertTrue(aclList.contains(new ACL(Perms.ALL, new Id("auth", "user1")))
        && aclList.contains(new ACL(Perms.ALL, new Id("auth", "user2")))
        && aclList.contains(new ACL(Perms.ALL, new Id("auth", "user3"))));
  }
}
