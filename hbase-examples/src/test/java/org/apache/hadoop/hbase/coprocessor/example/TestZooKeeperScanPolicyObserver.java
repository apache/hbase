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
package org.apache.hadoop.hbase.coprocessor.example;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ CoprocessorTests.class, MediumTests.class })
public class TestZooKeeperScanPolicyObserver {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestZooKeeperScanPolicyObserver.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static TableName NAME = TableName.valueOf("TestCP");

  private static byte[] FAMILY = Bytes.toBytes("cf");

  private static byte[] QUALIFIER = Bytes.toBytes("cq");

  private static Table TABLE;

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.startMiniCluster(3);
    UTIL.getAdmin()
        .createTable(TableDescriptorBuilder.newBuilder(NAME)
            .setCoprocessor(ZooKeeperScanPolicyObserver.class.getName())
            .setValue(ZooKeeperScanPolicyObserver.ZK_ENSEMBLE_KEY,
              UTIL.getZkCluster().getAddress().toString())
            .setValue(ZooKeeperScanPolicyObserver.ZK_SESSION_TIMEOUT_KEY, "2000")
            .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(FAMILY).build()).build());
    TABLE = UTIL.getConnection().getTable(NAME);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (TABLE != null) {
      TABLE.close();
    }
    UTIL.shutdownMiniCluster();
  }

  private void setExpireBefore(long time)
      throws KeeperException, InterruptedException, IOException {
    ZooKeeper zk = UTIL.getZooKeeperWatcher().getRecoverableZooKeeper().getZooKeeper();
    if (zk.exists(ZooKeeperScanPolicyObserver.NODE, false) == null) {
      zk.create(ZooKeeperScanPolicyObserver.NODE, Bytes.toBytes(time), ZooDefs.Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);
    } else {
      zk.setData(ZooKeeperScanPolicyObserver.NODE, Bytes.toBytes(time), -1);
    }
  }

  private void assertValueEquals(int start, int end) throws IOException {
    for (int i = start; i < end; i++) {
      assertEquals(i,
        Bytes.toInt(TABLE.get(new Get(Bytes.toBytes(i))).getValue(FAMILY, QUALIFIER)));
    }
  }

  private void assertNotExists(int start, int end) throws IOException {
    for (int i = start; i < end; i++) {
      assertFalse(TABLE.exists(new Get(Bytes.toBytes(i))));
    }
  }

  private void put(int start, int end, long ts) throws IOException {
    for (int i = start; i < end; i++) {
      TABLE.put(new Put(Bytes.toBytes(i)).addColumn(FAMILY, QUALIFIER, ts, Bytes.toBytes(i)));
    }
  }

  @Test
  public void test() throws IOException, KeeperException, InterruptedException {
    long now = System.currentTimeMillis();
    put(0, 100, now - 10000);
    assertValueEquals(0, 100);

    setExpireBefore(now - 5000);
    Thread.sleep(5000);
    UTIL.getAdmin().flush(NAME);
    assertNotExists(0, 100);

    put(0, 50, now - 1000);
    UTIL.getAdmin().flush(NAME);
    put(50, 100, now - 100);
    UTIL.getAdmin().flush(NAME);
    assertValueEquals(0, 100);

    setExpireBefore(now - 500);
    Thread.sleep(5000);
    UTIL.getAdmin().majorCompact(NAME);
    UTIL.waitFor(30000, () -> UTIL.getHBaseCluster().getRegions(NAME).iterator().next()
        .getStore(FAMILY).getStorefilesCount() == 1);
    assertNotExists(0, 50);
    assertValueEquals(50, 100);
  }
}
