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

package org.apache.hadoop.hbase.security.token;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test the refreshKeys in ZKSecretWatcher
 */
@Category({ SecurityTests.class, SmallTests.class })
public class TestZKSecretWatcherRefreshKeys {
  private static final Log LOG = LogFactory.getLog(TestZKSecretWatcherRefreshKeys.class);
  private static HBaseTestingUtility TEST_UTIL;

  private static class MockAbortable implements Abortable {
    private boolean abort;
    public void abort(String reason, Throwable e) {
      LOG.info("Aborting: "+reason, e);
      abort = true;
    }

    public boolean isAborted() {
      return abort;
    }
  }
  
  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    TEST_UTIL.startMiniZKCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniZKCluster();
  }

  private static ZooKeeperWatcher newZK(Configuration conf, String name,
      Abortable abort) throws Exception {
    Configuration copy = HBaseConfiguration.create(conf);
    ZooKeeperWatcher zk = new ZooKeeperWatcher(copy, name, abort);
    return zk;
  }

  @Test
  public void testRefreshKeys() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    ZooKeeperWatcher zk = newZK(conf, "127.0.0.1", new MockAbortable());
    AuthenticationTokenSecretManager keyManager = 
        new AuthenticationTokenSecretManager(conf, zk, "127.0.0.1", 
            60 * 60 * 1000, 60 * 1000);
    ZKSecretWatcher watcher = new ZKSecretWatcher(conf, zk, keyManager);
    ZKUtil.deleteChildrenRecursively(zk, watcher.getKeysParentZNode());
    Integer[] keys = { 1, 2, 3, 4, 5, 6 };
    for (Integer key : keys) {
      AuthenticationKey ak = new AuthenticationKey(key,
          System.currentTimeMillis() + 600 * 1000, null);
      ZKUtil.createWithParents(zk,
          ZKUtil.joinZNode(watcher.getKeysParentZNode(), key.toString()),
          Writables.getBytes(ak));
    }
    Assert.assertNull(keyManager.getCurrentKey());
    watcher.refreshKeys();
    for (Integer key : keys) {
      Assert.assertNotNull(keyManager.getKey(key.intValue()));
    }
  }
}
