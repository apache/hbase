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
package org.apache.hadoop.hbase.client;

import static org.apache.hadoop.hbase.client.AsyncProcess.START_LOG_ERRORS_AFTER_COUNT_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.Callable;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceExistException;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.ZKNamespaceManager;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Class to test asynchronous namespace admin operations.
 */
@RunWith(Parameterized.class)
@Category({ LargeTests.class, ClientTests.class })
public class TestAsyncNamespaceAdminApi extends TestAsyncAdminBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAsyncNamespaceAdminApi.class);

  private String prefix = "TestNamespace";
  private static HMaster master;
  private static ZKNamespaceManager zkNamespaceManager;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 60000);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 120000);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);
    TEST_UTIL.getConfiguration().setInt(START_LOG_ERRORS_AFTER_COUNT_KEY, 0);
    TEST_UTIL.startMiniCluster(1);
    ASYNC_CONN = ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration()).get();
    master = TEST_UTIL.getHBaseCluster().getMaster();
    zkNamespaceManager = new ZKNamespaceManager(master.getZooKeeper());
    zkNamespaceManager.start();
    LOG.info("Done initializing cluster");
  }

  @Test
  public void testCreateAndDelete() throws Exception {
    String testName = "testCreateAndDelete";
    String nsName = prefix + "_" + testName;

    // create namespace and verify
    admin.createNamespace(NamespaceDescriptor.create(nsName).build()).join();
    assertEquals(3, admin.listNamespaces().get().size());
    assertEquals(3, admin.listNamespaceDescriptors().get().size());
    TEST_UTIL.waitFor(60000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return zkNamespaceManager.list().size() == 3;
      }
    });
    assertNotNull(zkNamespaceManager.get(nsName));
    // delete namespace and verify
    admin.deleteNamespace(nsName).join();
    assertEquals(2, admin.listNamespaces().get().size());
    assertEquals(2, admin.listNamespaceDescriptors().get().size());
    assertEquals(2, zkNamespaceManager.list().size());
    assertNull(zkNamespaceManager.get(nsName));
  }

  @Test
  public void testDeleteReservedNS() throws Exception {
    boolean exceptionCaught = false;
    try {
      admin.deleteNamespace(NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR).join();
    } catch (Exception exp) {
      LOG.warn(exp.toString(), exp);
      exceptionCaught = true;
    } finally {
      assertTrue(exceptionCaught);
    }

    try {
      admin.deleteNamespace(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR).join();
    } catch (Exception exp) {
      LOG.warn(exp.toString(), exp);
      exceptionCaught = true;
    } finally {
      assertTrue(exceptionCaught);
    }
  }

  @Test
  public void testNamespaceOperations() throws Exception {
    admin.createNamespace(NamespaceDescriptor.create(prefix + "ns1").build()).join();
    admin.createNamespace(NamespaceDescriptor.create(prefix + "ns2").build()).join();

    // create namespace that already exists
    runWithExpectedException(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        admin.createNamespace(NamespaceDescriptor.create(prefix + "ns1").build()).join();
        return null;
      }
    }, NamespaceExistException.class);

    // create a table in non-existing namespace
    runWithExpectedException(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("non_existing_namespace",
          "table1"));
        htd.addFamily(new HColumnDescriptor("family1"));
        admin.createTable(htd).join();
        return null;
      }
    }, NamespaceNotFoundException.class);

    // get descriptor for existing namespace
    NamespaceDescriptor ns1 = admin.getNamespaceDescriptor(prefix + "ns1").get();
    assertEquals(prefix + "ns1", ns1.getName());

    // get descriptor for non-existing namespace
    runWithExpectedException(new Callable<NamespaceDescriptor>() {
      @Override
      public NamespaceDescriptor call() throws Exception {
        return admin.getNamespaceDescriptor("non_existing_namespace").get();
      }
    }, NamespaceNotFoundException.class);

    // delete descriptor for existing namespace
    admin.deleteNamespace(prefix + "ns2").join();

    // delete descriptor for non-existing namespace
    runWithExpectedException(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        admin.deleteNamespace("non_existing_namespace").join();
        return null;
      }
    }, NamespaceNotFoundException.class);

    // modify namespace descriptor for existing namespace
    ns1 = admin.getNamespaceDescriptor(prefix + "ns1").get();
    ns1.setConfiguration("foo", "bar");
    admin.modifyNamespace(ns1).join();
    ns1 = admin.getNamespaceDescriptor(prefix + "ns1").get();
    assertEquals("bar", ns1.getConfigurationValue("foo"));

    // modify namespace descriptor for non-existing namespace
    runWithExpectedException(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        admin.modifyNamespace(NamespaceDescriptor.create("non_existing_namespace").build()).join();
        return null;
      }
    }, NamespaceNotFoundException.class);

    admin.deleteNamespace(prefix + "ns1").join();
  }

  private static <V, E> void runWithExpectedException(Callable<V> callable, Class<E> exceptionClass) {
    try {
      callable.call();
    } catch (Exception ex) {
      LOG.info("Get exception is " + ex);
      assertEquals(exceptionClass, ex.getCause().getClass());
      return;
    }
    fail("Should have thrown exception " + exceptionClass);
  }
}
