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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CountDownLatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the synchronization of token authentication master keys through
 * ZKSecretWatcher
 */
@Category({SecurityTests.class, MediumTests.class})
public class TestZKSecretWatcher {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestZKSecretWatcher.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestZKSecretWatcher.class);
  private static HBaseTestingUtility TEST_UTIL;
  private static AuthenticationTokenSecretManager KEY_MASTER;
  private static AuthenticationTokenSecretManagerForTest KEY_SLAVE;
  private static AuthenticationTokenSecretManager KEY_SLAVE2;
  private static AuthenticationTokenSecretManager KEY_SLAVE3;

  private static class MockAbortable implements Abortable {
    private boolean abort;
    @Override
    public void abort(String reason, Throwable e) {
      LOG.info("Aborting: "+reason, e);
      abort = true;
    }

    @Override
    public boolean isAborted() {
      return abort;
    }
  }

  // We subclass AuthenticationTokenSecretManager so that testKeyUpdate can receive
  // notification on the removal of keyId
  private static class AuthenticationTokenSecretManagerForTest
  extends AuthenticationTokenSecretManager {
    private CountDownLatch latch = new CountDownLatch(1);

    public AuthenticationTokenSecretManagerForTest(Configuration conf,
                                                   ZKWatcher zk, String serverName,
                                                   long keyUpdateInterval, long tokenMaxLifetime) {
      super(conf, zk, serverName, keyUpdateInterval, tokenMaxLifetime);
    }

    @Override
    synchronized boolean removeKey(Integer keyId) {
      boolean b = super.removeKey(keyId);
      if (b) {
        latch.countDown();
      }
      return b;
    }

    CountDownLatch getLatch() {
      return latch;
    }
  }

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    TEST_UTIL.startMiniZKCluster();
    Configuration conf = TEST_UTIL.getConfiguration();

    ZKWatcher zk = newZK(conf, "server1", new MockAbortable());
    AuthenticationTokenSecretManagerForTest[] tmp = new AuthenticationTokenSecretManagerForTest[2];
    tmp[0] = new AuthenticationTokenSecretManagerForTest(
        conf, zk, "server1", 60*60*1000, 60*1000);
    tmp[0].start();

    zk = newZK(conf, "server2", new MockAbortable());
    tmp[1] = new AuthenticationTokenSecretManagerForTest(
        conf, zk, "server2", 60*60*1000, 60*1000);
    tmp[1].start();

    while (KEY_MASTER == null) {
      for (int i=0; i<2; i++) {
        if (tmp[i].isMaster()) {
          KEY_MASTER = tmp[i];
          KEY_SLAVE = tmp[ (i+1) % 2 ];
          break;
        }
      }
      Thread.sleep(500);
    }
    LOG.info("Master is "+KEY_MASTER.getName()+
        ", slave is "+KEY_SLAVE.getName());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniZKCluster();
  }

  @Test
  public void testKeyUpdate() throws Exception {
    // sanity check
    assertTrue(KEY_MASTER.isMaster());
    assertFalse(KEY_SLAVE.isMaster());
    int maxKeyId = 0;

    KEY_MASTER.rollCurrentKey();
    AuthenticationKey key1 = KEY_MASTER.getCurrentKey();
    assertNotNull(key1);
    LOG.debug("Master current key (key1) {}", key1);

    // wait for slave to update
    Thread.sleep(1000);
    AuthenticationKey slaveCurrent = KEY_SLAVE.getCurrentKey();
    assertNotNull(slaveCurrent);
    assertEquals(key1, slaveCurrent);
    LOG.debug("Slave current key (key1) {}", slaveCurrent);

    // generate two more keys then expire the original
    KEY_MASTER.rollCurrentKey();
    AuthenticationKey key2 = KEY_MASTER.getCurrentKey();
    LOG.debug("Master new current key (key2) {}", key2);
    KEY_MASTER.rollCurrentKey();
    AuthenticationKey key3 = KEY_MASTER.getCurrentKey();
    LOG.debug("Master new current key (key3) {}", key3);

    // force expire the original key
    key1.setExpiration(EnvironmentEdgeManager.currentTime() - 100000);
    KEY_MASTER.removeExpiredKeys();
    // verify removed from master
    assertNull(KEY_MASTER.getKey(key1.getKeyId()));

    // Wait for slave to catch up. When remove hits KEY_SLAVE, we'll clear
    // the latch and will progress beyond the await.
    KEY_SLAVE.getLatch().await();
    // make sure the slave has both new keys
    AuthenticationKey slave2 = KEY_SLAVE.getKey(key2.getKeyId());
    assertNotNull(slave2);
    assertEquals(key2, slave2);
    AuthenticationKey slave3 = KEY_SLAVE.getKey(key3.getKeyId());
    assertNotNull(slave3);
    assertEquals(key3, slave3);
    slaveCurrent = KEY_SLAVE.getCurrentKey();
    assertEquals(key3, slaveCurrent);
    LOG.debug("Slave current key (key3) {}", slaveCurrent);

    // verify that the expired key has been removed
    Waiter.waitFor(TEST_UTIL.getConfiguration(), 30000,
      () -> KEY_SLAVE.getKey(key1.getKeyId()) == null);
    assertNull("key1=" + KEY_SLAVE.getKey(key1.getKeyId()),
      KEY_SLAVE.getKey(key1.getKeyId()));

    // bring up a new slave
    Configuration conf = TEST_UTIL.getConfiguration();
    ZKWatcher zk = newZK(conf, "server3", new MockAbortable());
    KEY_SLAVE2 = new AuthenticationTokenSecretManager(
        conf, zk, "server3", 60*60*1000, 60*1000);
    KEY_SLAVE2.start();

    Thread.sleep(1000);
    // verify the new slave has current keys (and not expired)
    slave2 = KEY_SLAVE2.getKey(key2.getKeyId());
    assertNotNull(slave2);
    assertEquals(key2, slave2);
    slave3 = KEY_SLAVE2.getKey(key3.getKeyId());
    assertNotNull(slave3);
    assertEquals(key3, slave3);
    slaveCurrent = KEY_SLAVE2.getCurrentKey();
    assertEquals(key3, slaveCurrent);
    assertNull(KEY_SLAVE2.getKey(key1.getKeyId()));

    // test leader failover
    KEY_MASTER.stop();

    // wait for master to stop
    Thread.sleep(1000);
    assertFalse(KEY_MASTER.isMaster());

    // check for a new master
    AuthenticationTokenSecretManager[] mgrs =
        new AuthenticationTokenSecretManager[]{ KEY_SLAVE, KEY_SLAVE2 };
    AuthenticationTokenSecretManager newMaster = null;
    int tries = 0;
    while (newMaster == null && tries++ < 5) {
      for (AuthenticationTokenSecretManager mgr : mgrs) {
        if (mgr.isMaster()) {
          newMaster = mgr;
          break;
        }
      }
      if (newMaster == null) {
        Thread.sleep(500);
      }
    }
    assertNotNull(newMaster);

    AuthenticationKey current = newMaster.getCurrentKey();
    // new master will immediately roll the current key, so it's current may be greater
    assertTrue(current.getKeyId() >= slaveCurrent.getKeyId());
    LOG.debug("New master, current key: "+current.getKeyId());

    // roll the current key again on new master and verify the key ID increments
    newMaster.rollCurrentKey();
    AuthenticationKey newCurrent = newMaster.getCurrentKey();
    LOG.debug("New master, rolled new current key: "+newCurrent.getKeyId());
    assertTrue(newCurrent.getKeyId() > current.getKeyId());

    // add another slave
    ZKWatcher zk3 = newZK(conf, "server4", new MockAbortable());
    KEY_SLAVE3 = new AuthenticationTokenSecretManager(
        conf, zk3, "server4", 60*60*1000, 60*1000);
    KEY_SLAVE3.start();
    Thread.sleep(5000);

    // check master failover again
    newMaster.stop();

    // wait for master to stop
    Thread.sleep(5000);
    assertFalse(newMaster.isMaster());

    // check for a new master
    mgrs = new AuthenticationTokenSecretManager[]{ KEY_SLAVE, KEY_SLAVE2, KEY_SLAVE3 };
    newMaster = null;
    tries = 0;
    while (newMaster == null && tries++ < 5) {
      for (AuthenticationTokenSecretManager mgr : mgrs) {
        if (mgr.isMaster()) {
          newMaster = mgr;
          break;
        }
      }
      if (newMaster == null) {
        Thread.sleep(500);
      }
    }
    assertNotNull(newMaster);

    AuthenticationKey current2 = newMaster.getCurrentKey();
    // new master will immediately roll the current key, so it's current may be greater
    assertTrue(current2.getKeyId() >= newCurrent.getKeyId());
    LOG.debug("New master 2, current key: "+current2.getKeyId());

    // roll the current key again on new master and verify the key ID increments
    newMaster.rollCurrentKey();
    AuthenticationKey newCurrent2 = newMaster.getCurrentKey();
    LOG.debug("New master 2, rolled new current key: "+newCurrent2.getKeyId());
    assertTrue(newCurrent2.getKeyId() > current2.getKeyId());
  }

  private static ZKWatcher newZK(Configuration conf, String name,
                                 Abortable abort) throws Exception {
    Configuration copy = HBaseConfiguration.create(conf);
    ZKWatcher zk = new ZKWatcher(copy, name, abort);
    return zk;
  }
}
