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
package org.apache.hadoop.hbase.master.procedure;

import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_RETRIES_NUMBER;

import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MasterTests.class, MediumTests.class })
public class TestRefreshHFilesProcedure {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRefreshHFilesProcedure.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRefreshHFilesProcedure.class);

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private Admin admin;
  private ProcedureExecutor<MasterProcedureEnv> procExecutor;
  private static Configuration conf;
  private static final TableName TEST_TABLE = TableName.valueOf("testRefreshHFilesTable");
  private static final String TEST_NAMESPACE = "testRefreshHFilesNamespace";
  private static final String TEST_TABLE_IN_NAMESPACE = TEST_NAMESPACE + ":testTableInNamespace";
  private static final byte[] TEST_FAMILY = Bytes.toBytes("testRefreshHFilesCF1");

  @Before
  public void setup() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    // Shorten the run time of failed unit tests by limiting retries and the session timeout
    // threshold
    conf.setInt(HBASE_CLIENT_RETRIES_NUMBER, 1);
    conf.setInt(HConstants.ZK_SESSION_TIMEOUT, 1000);

    try {
      // Start the test cluster
      TEST_UTIL.startMiniCluster(1);
      admin = TEST_UTIL.getAdmin();
      procExecutor = TEST_UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor();
    } catch (Exception e) {
      TEST_UTIL.shutdownMiniCluster();
      throw new RuntimeException(e);
    }
  }

  @After
  public void tearDown() throws Exception {
    if (admin != null) {
      admin.close();
    }
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

  private void submitProcedureAndAssertNotFailed(RefreshHFilesTableProcedure procedure) {
    long procId = procExecutor.submitProcedure(procedure);
    ProcedureTestingUtility.waitProcedure(procExecutor, procId);
    ProcedureTestingUtility.assertProcNotFailed(procExecutor.getResult(procId));
  }

  @Test
  public void testRefreshHFilesProcedureForTable() throws IOException {
    try {
      // Create table in default namespace
      TEST_UTIL.createTable(TEST_TABLE, TEST_FAMILY);
      TEST_UTIL.waitTableAvailable(TEST_TABLE);

      RefreshHFilesTableProcedure procedure =
        new RefreshHFilesTableProcedure(procExecutor.getEnvironment(), TEST_TABLE);
      submitProcedureAndAssertNotFailed(procedure);

    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      // Delete table name post test execution
      TEST_UTIL.deleteTableIfAny(TEST_TABLE);
    }
  }

  @Test
  public void testRefreshHFilesProcedureForNamespace() {
    try {
      createNamespace(TEST_NAMESPACE);

      // Create table under test namespace
      TEST_UTIL.createTable(TableName.valueOf(TEST_TABLE_IN_NAMESPACE), TEST_FAMILY);
      TEST_UTIL.waitTableAvailable(TableName.valueOf(TEST_TABLE_IN_NAMESPACE));

      RefreshHFilesTableProcedure procedure =
        new RefreshHFilesTableProcedure(procExecutor.getEnvironment(), TEST_NAMESPACE);
      submitProcedureAndAssertNotFailed(procedure);

    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      // Delete namespace post test execution
      deleteNamespace(TEST_NAMESPACE);
    }
  }

  @Test
  public void testRefreshHFilesProcedureForAllTables() throws IOException {
    try {
      // Create table in default namespace
      TEST_UTIL.createTable(TEST_TABLE, TEST_FAMILY);
      TEST_UTIL.waitTableAvailable(TEST_TABLE);

      // Create test namespace
      createNamespace(TEST_NAMESPACE);

      // Create table under test namespace
      TEST_UTIL.createTable(TableName.valueOf(TEST_TABLE_IN_NAMESPACE), TEST_FAMILY);
      TEST_UTIL.waitTableAvailable(TableName.valueOf(TEST_TABLE_IN_NAMESPACE));

      RefreshHFilesTableProcedure procedure =
        new RefreshHFilesTableProcedure(procExecutor.getEnvironment());
      submitProcedureAndAssertNotFailed(procedure);

    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      // Delete table name post test execution
      TEST_UTIL.deleteTableIfAny(TEST_TABLE);

      // Delete namespace post test execution
      deleteNamespace(TEST_NAMESPACE);
    }
  }
}
