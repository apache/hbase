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
package org.apache.hadoop.hbase.coprocessor;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKNodeTracker;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests unhandled exceptions thrown by coprocessors running on master. Expected result is that the
 * master will remove the buggy coprocessor from its set of coprocessors and throw a
 * org.apache.hadoop.hbase.exceptions.DoNotRetryIOException back to the client. (HBASE-4014).
 */
@Category({ CoprocessorTests.class, MediumTests.class })
public class TestMasterCoprocessorExceptionWithRemove {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMasterCoprocessorExceptionWithRemove.class);

  public static class MasterTracker extends ZKNodeTracker {
    public boolean masterZKNodeWasDeleted = false;

    public MasterTracker(ZKWatcher zkw, String masterNode, Abortable abortable) {
      super(zkw, masterNode, abortable);
    }

    @Override
    public synchronized void nodeDeleted(String path) {
      if (path.equals("/hbase/master")) {
        masterZKNodeWasDeleted = true;
      }
    }
  }

  public static class BuggyMasterObserver implements MasterCoprocessor, MasterObserver {
    private boolean preCreateTableCalled;
    private boolean postCreateTableCalled;
    private boolean startCalled;
    private boolean postStartMasterCalled;

    @Override
    public Optional<MasterObserver> getMasterObserver() {
      return Optional.of(this);
    }

    @SuppressWarnings("null")
    @Override
    public void postCreateTable(ObserverContext<MasterCoprocessorEnvironment> env,
      TableDescriptor desc, RegionInfo[] regions) throws IOException {
      // Cause a NullPointerException and don't catch it: this should cause the
      // master to throw an o.apache.hadoop.hbase.DoNotRetryIOException to the
      // client.
      Integer i;
      i = null;
      i = i++;
    }

    public boolean wasCreateTableCalled() {
      return preCreateTableCalled && postCreateTableCalled;
    }

    @Override
    public void postStartMaster(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {
      postStartMasterCalled = true;
    }

    public boolean wasStartMasterCalled() {
      return postStartMasterCalled;
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
      startCalled = true;
    }

    public boolean wasStarted() {
      return startCalled;
    }
  }

  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static byte[] TEST_TABLE1 = Bytes.toBytes("observed_table1");
  private static byte[] TEST_FAMILY1 = Bytes.toBytes("fam1");

  private static byte[] TEST_TABLE2 = Bytes.toBytes("table2");
  private static byte[] TEST_FAMILY2 = Bytes.toBytes("fam2");

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, BuggyMasterObserver.class.getName());
    UTIL.getConfiguration().setBoolean(CoprocessorHost.ABORT_ON_ERROR_KEY, false);
    UTIL.startMiniCluster();
  }

  @AfterClass
  public static void teardownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testExceptionFromCoprocessorWhenCreatingTable() throws IOException {
    MiniHBaseCluster cluster = UTIL.getHBaseCluster();

    HMaster master = cluster.getMaster();
    MasterCoprocessorHost host = master.getMasterCoprocessorHost();
    BuggyMasterObserver cp = host.findCoprocessor(BuggyMasterObserver.class);
    assertFalse("No table created yet", cp.wasCreateTableCalled());

    // Set a watch on the zookeeper /hbase/master node. If the master dies,
    // the node will be deleted.
    // Master should *NOT* die:
    // we are testing that the default setting of hbase.coprocessor.abortonerror
    // =false
    // is respected.
    ZKWatcher zkw = new ZKWatcher(UTIL.getConfiguration(), "unittest", new Abortable() {
      @Override
      public void abort(String why, Throwable e) {
        throw new RuntimeException("Fatal ZK error: " + why, e);
      }

      @Override
      public boolean isAborted() {
        return false;
      }
    });

    MasterTracker masterTracker = new MasterTracker(zkw, "/hbase/master", new Abortable() {
      @Override
      public void abort(String why, Throwable e) {
        throw new RuntimeException("Fatal ZooKeeper tracker error, why=", e);
      }

      @Override
      public boolean isAborted() {
        return false;
      }
    });

    masterTracker.start();
    zkw.registerListener(masterTracker);

    // Test (part of the) output that should have be printed by master when it aborts:
    // (namely the part that shows the set of loaded coprocessors).
    // In this test, there is only a single coprocessor (BuggyMasterObserver).
    String coprocessorName = BuggyMasterObserver.class.getName();
    assertTrue(HMaster.getLoadedCoprocessors().contains(coprocessorName));

    HTableDescriptor htd1 = new HTableDescriptor(TableName.valueOf(TEST_TABLE1));
    htd1.addFamily(new HColumnDescriptor(TEST_FAMILY1));

    boolean threwDNRE = false;
    try {
      Admin admin = UTIL.getAdmin();
      admin.createTable(htd1);
    } catch (IOException e) {
      if (e.getClass().getName().equals("org.apache.hadoop.hbase.DoNotRetryIOException")) {
        threwDNRE = true;
      }
    } finally {
      assertTrue(threwDNRE);
    }

    // wait for a few seconds to make sure that the Master hasn't aborted.
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      fail("InterruptedException while sleeping.");
    }

    assertFalse("Master survived coprocessor NPE, as expected.",
      masterTracker.masterZKNodeWasDeleted);

    String loadedCoprocessors = HMaster.getLoadedCoprocessors();
    assertTrue(loadedCoprocessors.contains(coprocessorName));

    // Verify that BuggyMasterObserver has been removed due to its misbehavior
    // by creating another table: should not have a problem this time.
    HTableDescriptor htd2 = new HTableDescriptor(TableName.valueOf(TEST_TABLE2));
    htd2.addFamily(new HColumnDescriptor(TEST_FAMILY2));
    Admin admin = UTIL.getAdmin();
    try {
      admin.createTable(htd2);
    } catch (IOException e) {
      fail("Failed to create table after buggy coprocessor removal: " + e);
    }
  }

}
