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
package org.apache.hadoop.hbase.master.procedure;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests class that validates that "post" observer hook methods are only invoked when the operation was successful.
 */
@Category({MasterTests.class, MediumTests.class})
public class TestMasterObserverPostCalls {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMasterObserverPostCalls.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestMasterObserverPostCalls.class);
  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setupCluster() throws Exception {
    setupConf(UTIL.getConfiguration());
    UTIL.startMiniCluster(1);
  }

  private static void setupConf(Configuration conf) {
    conf.setInt(MasterProcedureConstants.MASTER_PROCEDURE_THREADS, 1);
    conf.set(MasterCoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
        MasterObserverForTest.class.getName());
  }

  @AfterClass
  public static void cleanupTest() throws Exception {
    try {
      UTIL.shutdownMiniCluster();
    } catch (Exception e) {
      LOG.warn("failure shutting down cluster", e);
    }
  }

  public static class MasterObserverForTest implements MasterCoprocessor, MasterObserver {
    private AtomicInteger postHookCalls = null;

    @Override
    public Optional<MasterObserver> getMasterObserver() {
      return Optional.of(this);
    }

    @Override
    public void start(@SuppressWarnings("rawtypes") CoprocessorEnvironment ctx) throws IOException {
      this.postHookCalls = new AtomicInteger(0);
    }

    @Override
    public void postDeleteNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
        String namespace) {
      postHookCalls.incrementAndGet();
    }

    @Override
    public void postModifyNamespace(
        ObserverContext<MasterCoprocessorEnvironment> ctx, NamespaceDescriptor desc) {
      postHookCalls.incrementAndGet();
    }

    @Override
    public void postCreateNamespace(
        ObserverContext<MasterCoprocessorEnvironment> ctx, NamespaceDescriptor desc) {
      postHookCalls.incrementAndGet();
    }

    @Override
    public void postCreateTable(
        ObserverContext<MasterCoprocessorEnvironment> ctx, TableDescriptor td,
        RegionInfo[] regions) {
      postHookCalls.incrementAndGet();
    }

    @Override
    public void postModifyTable(
        ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tn, TableDescriptor td) {
      postHookCalls.incrementAndGet();
    }

    @Override
    public void postDisableTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tn) {
      postHookCalls.incrementAndGet();
    }

    @Override
    public void postDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName tn) {
      postHookCalls.incrementAndGet();
    }
  }

  @Test
  public void testPostDeleteNamespace() throws IOException {
    final Admin admin = UTIL.getAdmin();
    final String ns = "postdeletens";
    final TableName tn1 = TableName.valueOf(ns, "table1");

    admin.createNamespace(NamespaceDescriptor.create(ns).build());
    admin.createTable(TableDescriptorBuilder.newBuilder(tn1)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("f1")).build())
        .build());

    HMaster master = UTIL.getMiniHBaseCluster().getMaster();
    MasterObserverForTest observer = master.getMasterCoprocessorHost().findCoprocessor(
        MasterObserverForTest.class);
    int preCount = observer.postHookCalls.get();
    try {
      admin.deleteNamespace(ns);
      fail("Deleting a non-empty namespace should be disallowed");
    } catch (IOException e) {
      // Pass
    }
    int postCount = observer.postHookCalls.get();
    assertEquals("Expected no invocations of postDeleteNamespace when the operation fails",
        preCount, postCount);

    // Disable and delete the table so that we can delete the NS.
    admin.disableTable(tn1);
    admin.deleteTable(tn1);

    // Validate that the postDeletNS hook is invoked
    preCount = observer.postHookCalls.get();
    admin.deleteNamespace(ns);
    postCount = observer.postHookCalls.get();
    assertEquals("Expected 1 invocation of postDeleteNamespace", preCount + 1, postCount);
  }

  @Test
  public void testPostModifyNamespace() throws IOException {
    final Admin admin = UTIL.getAdmin();
    final String ns = "postmodifyns";

    NamespaceDescriptor nsDesc = NamespaceDescriptor.create(ns).build();
    admin.createNamespace(nsDesc);

    HMaster master = UTIL.getMiniHBaseCluster().getMaster();
    MasterObserverForTest observer = master.getMasterCoprocessorHost().findCoprocessor(
        MasterObserverForTest.class);
    int preCount = observer.postHookCalls.get();
    try {
      admin.modifyNamespace(NamespaceDescriptor.create("nonexistent").build());
      fail("Modifying a missing namespace should fail");
    } catch (IOException e) {
      // Pass
    }
    int postCount = observer.postHookCalls.get();
    assertEquals("Expected no invocations of postModifyNamespace when the operation fails",
        preCount, postCount);

    // Validate that the postDeletNS hook is invoked
    preCount = observer.postHookCalls.get();
    admin.modifyNamespace(
        NamespaceDescriptor.create(nsDesc).addConfiguration("foo", "bar").build());
    postCount = observer.postHookCalls.get();
    assertEquals("Expected 1 invocation of postModifyNamespace", preCount + 1, postCount);
  }

  @Test
  public void testPostCreateNamespace() throws IOException {
    final Admin admin = UTIL.getAdmin();
    final String ns = "postcreatens";

    HMaster master = UTIL.getMiniHBaseCluster().getMaster();
    MasterObserverForTest observer = master.getMasterCoprocessorHost().findCoprocessor(
        MasterObserverForTest.class);

    // Validate that the post hook is called
    int preCount = observer.postHookCalls.get();
    NamespaceDescriptor nsDesc = NamespaceDescriptor.create(ns).build();
    admin.createNamespace(nsDesc);
    int postCount = observer.postHookCalls.get();
    assertEquals("Expected 1 invocation of postModifyNamespace", preCount + 1, postCount);

    // Then, validate that it's not called when the call fails
    preCount = observer.postHookCalls.get();
    try {
      admin.createNamespace(nsDesc);
      fail("Creating an already present namespace should fail");
    } catch (IOException e) {
      // Pass
    }
    postCount = observer.postHookCalls.get();
    assertEquals("Expected no invocations of postModifyNamespace when the operation fails",
        preCount, postCount);
  }

  @Test
  public void testPostCreateTable() throws IOException {
    final Admin admin = UTIL.getAdmin();
    final TableName tn = TableName.valueOf("postcreatetable");
    final TableDescriptor td = TableDescriptorBuilder.newBuilder(tn).setColumnFamily(
        ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("f1")).build()).build();

    HMaster master = UTIL.getMiniHBaseCluster().getMaster();
    MasterObserverForTest observer = master.getMasterCoprocessorHost().findCoprocessor(
        MasterObserverForTest.class);

    // Validate that the post hook is called
    int preCount = observer.postHookCalls.get();
    admin.createTable(td);
    int postCount = observer.postHookCalls.get();
    assertEquals("Expected 1 invocation of postCreateTable", preCount + 1, postCount);

    // Then, validate that it's not called when the call fails
    preCount = observer.postHookCalls.get();
    try {
      admin.createTable(td);
      fail("Creating an already present table should fail");
    } catch (IOException e) {
      // Pass
    }
    postCount = observer.postHookCalls.get();
    assertEquals("Expected no invocations of postCreateTable when the operation fails",
        preCount, postCount);
  }

  @Test
  public void testPostModifyTable() throws IOException {
    final Admin admin = UTIL.getAdmin();
    final TableName tn = TableName.valueOf("postmodifytable");
    final TableDescriptor td = TableDescriptorBuilder.newBuilder(tn).setColumnFamily(
        ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("f1")).build()).build();

    HMaster master = UTIL.getMiniHBaseCluster().getMaster();
    MasterObserverForTest observer = master.getMasterCoprocessorHost().findCoprocessor(
        MasterObserverForTest.class);

    // Create the table
    admin.createTable(td);

    // Validate that the post hook is called
    int preCount = observer.postHookCalls.get();
    admin.modifyTable(td);
    int postCount = observer.postHookCalls.get();
    assertEquals("Expected 1 invocation of postModifyTable", preCount + 1, postCount);

    // Then, validate that it's not called when the call fails
    preCount = observer.postHookCalls.get();
    try {
      admin.modifyTable(TableDescriptorBuilder.newBuilder(TableName.valueOf("missing"))
          .setColumnFamily(td.getColumnFamily(Bytes.toBytes("f1"))).build());
      fail("Modifying a missing table should fail");
    } catch (IOException e) {
      // Pass
    }
    postCount = observer.postHookCalls.get();
    assertEquals("Expected no invocations of postModifyTable when the operation fails",
        preCount, postCount);
  }

  @Test
  public void testPostDisableTable() throws IOException {
    final Admin admin = UTIL.getAdmin();
    final TableName tn = TableName.valueOf("postdisabletable");
    final TableDescriptor td = TableDescriptorBuilder.newBuilder(tn).setColumnFamily(
        ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("f1")).build()).build();

    HMaster master = UTIL.getMiniHBaseCluster().getMaster();
    MasterObserverForTest observer = master.getMasterCoprocessorHost().findCoprocessor(
        MasterObserverForTest.class);

    // Create the table and disable it
    admin.createTable(td);

    // Validate that the post hook is called
    int preCount = observer.postHookCalls.get();
    admin.disableTable(td.getTableName());
    int postCount = observer.postHookCalls.get();
    assertEquals("Expected 1 invocation of postDisableTable", preCount + 1, postCount);

    // Then, validate that it's not called when the call fails
    preCount = observer.postHookCalls.get();
    try {
      admin.disableTable(TableName.valueOf("Missing"));
      fail("Disabling a missing table should fail");
    } catch (IOException e) {
      // Pass
    }
    postCount = observer.postHookCalls.get();
    assertEquals("Expected no invocations of postDisableTable when the operation fails",
        preCount, postCount);
  }

  @Test
  public void testPostDeleteTable() throws IOException {
    final Admin admin = UTIL.getAdmin();
    final TableName tn = TableName.valueOf("postdeletetable");
    final TableDescriptor td = TableDescriptorBuilder.newBuilder(tn).setColumnFamily(
        ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("f1")).build()).build();

    HMaster master = UTIL.getMiniHBaseCluster().getMaster();
    MasterObserverForTest observer = master.getMasterCoprocessorHost().findCoprocessor(
        MasterObserverForTest.class);

    // Create the table and disable it
    admin.createTable(td);
    admin.disableTable(td.getTableName());

    // Validate that the post hook is called
    int preCount = observer.postHookCalls.get();
    admin.deleteTable(td.getTableName());
    int postCount = observer.postHookCalls.get();
    assertEquals("Expected 1 invocation of postDeleteTable", preCount + 1, postCount);

    // Then, validate that it's not called when the call fails
    preCount = observer.postHookCalls.get();
    try {
      admin.deleteTable(TableName.valueOf("missing"));
      fail("Deleting a missing table should fail");
    } catch (IOException e) {
      // Pass
    }
    postCount = observer.postHookCalls.get();
    assertEquals("Expected no invocations of postDeleteTable when the operation fails",
        preCount, postCount);
  }
}
