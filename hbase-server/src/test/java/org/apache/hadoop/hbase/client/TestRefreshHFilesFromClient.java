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
package org.apache.hadoop.hbase.client;

import static org.apache.hadoop.hbase.client.AsyncConnectionConfiguration.START_LOG_ERRORS_AFTER_COUNT_KEY;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

@Category({ MediumTests.class, ClientTests.class })
public class TestRefreshHFilesFromClient {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRefreshHFilesFromClient.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRefreshHFilesFromClient.class);
  private final static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static AsyncConnection asyncConn;
  private static Admin admin;
  private static Configuration conf;
  private static final TableName TEST_TABLE = TableName.valueOf("testRefreshHFilesTable");
  private static final TableName TEST_INVALID_TABLE =
    TableName.valueOf("testRefreshHFilesInvalidTable");
  private static final String TEST_NAMESPACE = "testRefreshHFilesNamespace";
  private static final String TEST_INVALID_NAMESPACE = "testRefreshHFilesInvalidNamespace";
  private static final String TEST_TABLE_IN_NAMESPACE = TEST_NAMESPACE + ":testTableInNamespace";
  private static final byte[] TEST_FAMILY = Bytes.toBytes("testRefreshHFilesCF1");

  @Before
  public void setUp() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    conf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 60000);
    conf.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 120000);
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    conf.setInt(START_LOG_ERRORS_AFTER_COUNT_KEY, 0);
    conf.setInt(HConstants.ZK_SESSION_TIMEOUT, 1000);

    // Start the test cluster
    TEST_UTIL.startMiniCluster(1);
    asyncConn = ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration()).get();
    admin = TEST_UTIL.getAdmin();
  }

  @After
  public void tearDown() throws Exception {
    Closeables.close(asyncConn, true);
    TEST_UTIL.shutdownMiniCluster();
  }

  private void createNamespace(String namespace) throws RuntimeException {
    try {
      final NamespaceDescriptor nsd = NamespaceDescriptor.create(namespace).build();
      // Create the namespace if it doesn’t exist
      if (
        Arrays.stream(admin.listNamespaceDescriptors())
          .noneMatch(ns -> ns.getName().equals(namespace))
      ) {
        admin.createNamespace(nsd);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void deleteNamespace(String namespace) {
    try {
      // List table in namespace
      TableName[] tables = admin.listTableNamesByNamespace(namespace);
      for (TableName t : tables) {
        TEST_UTIL.deleteTableIfAny(t);
      }
      // Now delete the namespace
      admin.deleteNamespace(namespace);
    } catch (Exception e) {
      LOG.debug(
        "Unable to delete namespace " + namespace + " post test execution. This isn't a failure");
    }
  }

  @Test
  public void testRefreshHFilesForTable() throws Exception {
    try {
      // Create table in default namespace
      TEST_UTIL.createTable(TEST_TABLE, TEST_FAMILY);
      TEST_UTIL.waitTableAvailable(TEST_TABLE);

      // RefreshHFiles for table
      Long procId = admin.refreshHFiles(TEST_TABLE);
      assertTrue(procId >= 0);
    } catch (Exception e) {
      Assert.fail("RefreshHFilesForTable Should Not Throw Exception: " + e);
      throw new RuntimeException(e);
    } finally {
      // Delete table name post test execution
      TEST_UTIL.deleteTableIfAny(TEST_TABLE);
    }
  }

  // Not creating table hence refresh should throw exception
  @Test(expected = TableNotFoundException.class)
  public void testRefreshHFilesForInvalidTable() throws Exception {
    // RefreshHFiles for table
    admin.refreshHFiles(TEST_INVALID_TABLE);
  }

  @Test
  public void testRefreshHFilesForNamespace() throws Exception {
    try {
      createNamespace(TEST_NAMESPACE);

      // Create table under test namespace
      TEST_UTIL.createTable(TableName.valueOf(TEST_TABLE_IN_NAMESPACE), TEST_FAMILY);
      TEST_UTIL.waitTableAvailable(TableName.valueOf(TEST_TABLE_IN_NAMESPACE));

      // RefreshHFiles for namespace
      Long procId = admin.refreshHFiles(TEST_NAMESPACE);
      assertTrue(procId >= 0);

    } catch (Exception e) {
      Assert.fail("RefreshHFilesForAllNamespace Should Not Throw Exception: " + e);
      throw new RuntimeException(e);
    } finally {
      // Delete namespace post test execution
      deleteNamespace(TEST_NAMESPACE);
    }
  }

  @Test(expected = NamespaceNotFoundException.class)
  public void testRefreshHFilesForInvalidNamespace() throws Exception {
    // RefreshHFiles for namespace
    admin.refreshHFiles(TEST_INVALID_NAMESPACE);
  }

  @Test
  public void testRefreshHFilesForAllTables() throws Exception {
    try {
      // Create table in default namespace
      TEST_UTIL.createTable(TEST_TABLE, TEST_FAMILY);
      TEST_UTIL.waitTableAvailable(TEST_TABLE);

      // Create test namespace
      createNamespace(TEST_NAMESPACE);

      // Create table under test namespace
      TEST_UTIL.createTable(TableName.valueOf(TEST_TABLE_IN_NAMESPACE), TEST_FAMILY);
      TEST_UTIL.waitTableAvailable(TableName.valueOf(TEST_TABLE_IN_NAMESPACE));

      // RefreshHFiles for all the tables
      Long procId = admin.refreshHFiles();
      assertTrue(procId >= 0);

    } catch (Exception e) {
      Assert.fail("RefreshHFilesForAllTables Should Not Throw Exception: " + e);
      throw new RuntimeException(e);
    } finally {
      // Delete table name post test execution
      TEST_UTIL.deleteTableIfAny(TEST_TABLE);

      // Delete namespace post test execution
      deleteNamespace(TEST_NAMESPACE);
    }
  }
}
