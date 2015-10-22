/*
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.coprocessor.example;

import static org.junit.Assert.assertEquals;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.experimental.categories.Category;

@Category({CoprocessorTests.class, MediumTests.class})
public class TestZooKeeperScanPolicyObserver {
  private static final Log LOG = LogFactory.getLog(TestZooKeeperScanPolicyObserver.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[] F = Bytes.toBytes("fam");
  private static final byte[] Q = Bytes.toBytes("qual");
  private static final byte[] R = Bytes.toBytes("row");

  // @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    System.out.println("HERE!!!!!!!!");
    // Test we can first start the ZK cluster by itself
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        ZooKeeperScanPolicyObserver.class.getName());
    TEST_UTIL.startMiniZKCluster();
    TEST_UTIL.startMiniCluster();
  }

  // @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  // @Ignore @Test
  public void testScanPolicyObserver() throws Exception {
    TableName tableName =
        TableName.valueOf("testScanPolicyObserver");
    HTableDescriptor desc = new HTableDescriptor(tableName);
    HColumnDescriptor hcd = new HColumnDescriptor(F)
    .setMaxVersions(10)
    .setTimeToLive(1);
    desc.addFamily(hcd);
    TEST_UTIL.getHBaseAdmin().createTable(desc);
    Table t = TEST_UTIL.getConnection().getTable(tableName);
    long now = EnvironmentEdgeManager.currentTime();

    ZooKeeperWatcher zkw = new ZooKeeperWatcher(TEST_UTIL.getConfiguration(), "test", null);
    ZooKeeper zk = zkw.getRecoverableZooKeeper().getZooKeeper();
    ZKUtil.createWithParents(zkw, ZooKeeperScanPolicyObserver.node);
    // let's say test last backup was 1h ago
    // using plain ZK here, because RecoverableZooKeeper add extra encoding to the data
    zk.setData(ZooKeeperScanPolicyObserver.node, Bytes.toBytes(now - 3600*1000), -1);

    LOG.debug("Set time: "+Bytes.toLong(Bytes.toBytes(now - 3600*1000)));

    // sleep for 1s to give the ZK change a chance to reach the watcher in the observer.
    // TODO: Better to wait for the data to be propagated
    Thread.sleep(1000);

    long ts = now - 2000;
    Put p = new Put(R);
    p.add(F, Q, ts, Q);
    t.put(p);
    p = new Put(R);
    p.add(F, Q, ts+1, Q);
    t.put(p);

    // these two should be expired but for the override
    // (their ts was 2s in the past)
    Get g = new Get(R);
    g.setMaxVersions(10);
    Result r = t.get(g);
    // still there?
    assertEquals(2, r.size());

    TEST_UTIL.flush(tableName);
    TEST_UTIL.compact(tableName, true);

    g = new Get(R);
    g.setMaxVersions(10);
    r = t.get(g);
    // still there?
    assertEquals(2, r.size());
    zk.setData(ZooKeeperScanPolicyObserver.node, Bytes.toBytes(now), -1);
    LOG.debug("Set time: "+now);

    TEST_UTIL.compact(tableName, true);

    g = new Get(R);
    g.setMaxVersions(10);
    r = t.get(g);
    // should be gone now
    assertEquals(0, r.size());
    t.close();
  }
}
