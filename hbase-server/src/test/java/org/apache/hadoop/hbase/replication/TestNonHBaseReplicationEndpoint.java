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
package org.apache.hadoop.hbase.replication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MediumTests.class, ReplicationTests.class })
public class TestNonHBaseReplicationEndpoint {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestNonHBaseReplicationEndpoint.class);

  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static Admin ADMIN;

  private static final TableName tableName = TableName.valueOf("test");
  private static final byte[] famName = Bytes.toBytes("f");

  private static final AtomicBoolean REPLICATED = new AtomicBoolean();

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    UTIL.startMiniCluster();
    ADMIN = UTIL.getAdmin();
  }

  @AfterClass
  public static void teardownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Before
  public void setup() {
    REPLICATED.set(false);
  }

  @Test
  public void test() throws IOException {
    TableDescriptor td = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(famName)
        .setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build())
      .build();
    Table table = UTIL.createTable(td, HBaseTestingUtility.KEYS_FOR_HBA_CREATE_TABLE);

    ReplicationPeerConfig peerConfig = ReplicationPeerConfig.newBuilder()
      .setReplicationEndpointImpl(NonHBaseReplicationEndpoint.class.getName())
      .setReplicateAllUserTables(false)
      .setTableCFsMap(new HashMap<TableName, List<String>>() {{
          put(tableName, new ArrayList<>());
        }
      }).build();

    ADMIN.addReplicationPeer("1", peerConfig);
    loadData(table);

    UTIL.waitFor(10000L, () -> REPLICATED.get());
  }

  protected static void loadData(Table table) throws IOException {
    for (int i = 0; i < 100; i++) {
      Put put = new Put(Bytes.toBytes(Integer.toString(i)));
      put.addColumn(famName, famName, Bytes.toBytes(i));
      table.put(put);
    }
  }

  public static class NonHBaseReplicationEndpoint implements ReplicationEndpoint {

    private boolean running = false;

    @Override
    public void init(Context context) throws IOException {
    }

    @Override
    public boolean canReplicateToSameCluster() {
      return false;
    }

    @Override
    public UUID getPeerUUID() {
      return UUID.randomUUID();
    }

    @Override
    public WALEntryFilter getWALEntryfilter() {
      return null;
    }

    @Override
    public boolean replicate(ReplicateContext replicateContext) {
      REPLICATED.set(true);
      return true;
    }

    @Override
    public boolean isRunning() {
      return running;
    }

    @Override
    public boolean isStarting() {
      return false;
    }

    @Override
    public void start() {
      running = true;
    }

    @Override
    public void awaitRunning() {
      long interval = 100L;
      while (!running) {
        Threads.sleep(interval);
      }
    }

    @Override
    public void awaitRunning(long timeout, TimeUnit unit) {
      long start = System.currentTimeMillis();
      long end = start + unit.toMillis(timeout);
      long interval = 100L;
      while (!running && System.currentTimeMillis() < end) {
        Threads.sleep(interval);
      }
    }

    @Override
    public void stop() {
      running = false;
    }

    @Override
    public void awaitTerminated() {
      long interval = 100L;
      while (running) {
        Threads.sleep(interval);
      }
    }

    @Override
    public void awaitTerminated(long timeout, TimeUnit unit) {
      long start = System.currentTimeMillis();
      long end = start + unit.toMillis(timeout);
      long interval = 100L;
      while (running && System.currentTimeMillis() < end) {
        Threads.sleep(interval);
      }
    }

    @Override
    public Throwable failureCause() {
      return null;
    }

    @Override
    public void peerConfigUpdated(ReplicationPeerConfig rpc) {
    }
  }
}