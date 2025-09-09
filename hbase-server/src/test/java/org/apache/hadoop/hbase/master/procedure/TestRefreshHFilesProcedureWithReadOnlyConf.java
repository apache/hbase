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

import java.io.IOException;
import org.apache.hadoop.hbase.TestRefreshHFilesBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestRefreshHFilesProcedureWithReadOnlyConf extends TestRefreshHFilesBase {

  @Before
  public void setup() throws Exception {
    // When true is passed only setup for readonly property is done.
    // The initial ReadOnly property will be false for table creation
    baseSetup(true);
  }

  @After
  public void tearDown() throws Exception {
    baseTearDown();
  }

  @Test
  public void testRefreshHFilesProcedureForTable() throws IOException {
    try {
      // Create table in default namespace
      createTableAndWait(TEST_TABLE, TEST_FAMILY);

      setReadOnlyMode(true);
      RefreshHFilesTableProcedure procedure =
        new RefreshHFilesTableProcedure(procExecutor.getEnvironment(), TEST_TABLE);
      submitProcedureAndAssertNotFailed(procedure);

    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      setReadOnlyMode(false);
      // Delete table name post test execution
      deleteTable(TEST_TABLE);
    }
  }

  @Test
  public void testRefreshHFilesProcedureForNamespace() {
    try {
      createNamespace(TEST_NAMESPACE);

      // Create table under test namespace
      createTableInNamespaceAndWait(TEST_NAMESPACE, TEST_TABLE, TEST_FAMILY);

      setReadOnlyMode(true);
      RefreshHFilesTableProcedure procedure =
        new RefreshHFilesTableProcedure(procExecutor.getEnvironment(), TEST_NAMESPACE);
      submitProcedureAndAssertNotFailed(procedure);

    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      setReadOnlyMode(false);
      // Delete namespace post test execution
      // This will delete all tables under namespace hence no explicit table
      // deletion for table under namespace is needed.
      deleteNamespace(TEST_NAMESPACE);
    }
  }

  @Test
  public void testRefreshHFilesProcedureForAllTables() throws IOException {
    try {
      // Create table in default namespace
      createTableAndWait(TEST_TABLE, TEST_FAMILY);

      // Create test namespace
      createNamespace(TEST_NAMESPACE);

      // Create table under test namespace
      createTableInNamespaceAndWait(TEST_NAMESPACE, TEST_TABLE, TEST_FAMILY);

      setReadOnlyMode(true);
      RefreshHFilesTableProcedure procedure =
        new RefreshHFilesTableProcedure(procExecutor.getEnvironment());
      submitProcedureAndAssertNotFailed(procedure);

    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      setReadOnlyMode(false);
      // Delete table name post test execution
      deleteTable(TEST_TABLE);

      // Delete namespace post test execution
      // This will delete all tables under namespace hence no explicit table
      // deletion for table under namespace is needed.
      deleteNamespace(TEST_NAMESPACE);
    }
  }
}
