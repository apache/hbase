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

import static org.junit.Assert.assertEquals;
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
import org.apache.hadoop.hbase.HConstants;
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
 * master will abort with an informative error message describing the set of its loaded coprocessors
 * for crash diagnosis. (HBASE-4014).
 */
@Category({ CoprocessorTests.class, MediumTests.class })
public class TestMasterCoprocessorExceptionWithAbort {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMasterCoprocessorExceptionWithAbort.class);

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

  public static class CreateTableThread extends Thread {
    HBaseTestingUtility UTIL;

    public CreateTableThread(HBaseTestingUtility UTIL) {
      this.UTIL = UTIL;
    }

    @Override
    public void run() {
      // create a table : master coprocessor will throw an exception and not
      // catch it.
      HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(TEST_TABLE));
      htd.addFamily(new HColumnDescriptor(TEST_FAMILY));
      try {
        Admin admin = UTIL.getAdmin();
        admin.createTable(htd);
        fail("BuggyMasterObserver failed to throw an exception.");
      } catch (IOException e) {
        assertEquals("HBaseAdmin threw an interrupted IOException as expected.",
          "java.io.InterruptedIOException", e.getClass().getName());
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

    @Override
    public void postCreateTable(ObserverContext<MasterCoprocessorEnvironment> env,
      TableDescriptor desc, RegionInfo[] regions) throws IOException {
      // cause a NullPointerException and don't catch it: this will cause the
      // master to abort().
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
  private static byte[] TEST_TABLE = Bytes.toBytes("observed_table");
  private static byte[] TEST_FAMILY = Bytes.toBytes("fam1");

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, BuggyMasterObserver.class.getName());
    conf.setBoolean(CoprocessorHost.ABORT_ON_ERROR_KEY, true);
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2); // Fail fast
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

    // set a watch on the zookeeper /hbase/master node. If the master dies,
    // the node will be deleted.
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
        throw new RuntimeException("Fatal ZK master tracker error, why=", e);
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
    assertTrue(HMaster.getLoadedCoprocessors()
      .contains(TestMasterCoprocessorExceptionWithAbort.BuggyMasterObserver.class.getName()));

    CreateTableThread createTableThread = new CreateTableThread(UTIL);

    // Attempting to create a table (using createTableThread above) triggers an NPE in
    // BuggyMasterObserver.
    // Master will then abort and the /hbase/master zk node will be deleted.
    createTableThread.start();

    // Wait up to 30 seconds for master's /hbase/master zk node to go away after master aborts.
    for (int i = 0; i < 30; i++) {
      if (masterTracker.masterZKNodeWasDeleted == true) {
        break;
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        fail("InterruptedException while waiting for master zk node to " + "be deleted.");
      }
    }

    assertTrue("Master aborted on coprocessor exception, as expected.",
      masterTracker.masterZKNodeWasDeleted);

    createTableThread.interrupt();
    try {
      createTableThread.join(1000);
    } catch (InterruptedException e) {
      assertTrue("Ignoring InterruptedException while waiting for " + " createTableThread.join().",
        true);
    }
  }

}
