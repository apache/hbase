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

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.TestRefreshHFilesBase;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MediumTests.class, ClientTests.class })
public class TestRefreshHFilesFromClient extends TestRefreshHFilesBase {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRefreshHFilesFromClient.class);

  private static final TableName TEST_NONEXISTENT_TABLE =
    TableName.valueOf("testRefreshHFilesNonExistentTable");
  private static final String TEST_NONEXISTENT_NAMESPACE = "testRefreshHFilesNonExistentNamespace";

  @Before
  public void setup() throws Exception {
    baseSetup(false);
  }

  @After
  public void tearDown() throws Exception {
    baseTearDown();
  }

  @Test
  public void testRefreshHFilesForTable() throws Exception {
    try {
      // Create table in default namespace
      createTableAndWait(TEST_TABLE, TEST_FAMILY);

      // RefreshHFiles for table
      Long procId = admin.refreshHFiles(TEST_TABLE);
      assertTrue(procId >= 0);
    } catch (Exception e) {
      Assert.fail("RefreshHFilesForTable Should Not Throw Exception: " + e);
      throw new RuntimeException(e);
    } finally {
      // Delete table name post test execution
      deleteTable(TEST_TABLE);
    }
  }

  // Not creating table hence refresh should throw exception
  @Test(expected = TableNotFoundException.class)
  public void testRefreshHFilesForNonExistentTable() throws Exception {
    // RefreshHFiles for table
    admin.refreshHFiles(TEST_NONEXISTENT_TABLE);
  }

  @Test
  public void testRefreshHFilesForNamespace() throws Exception {
    try {
      createNamespace(TEST_NAMESPACE);

      // Create table under test namespace
      createTableInNamespaceAndWait(TEST_NAMESPACE, TEST_TABLE, TEST_FAMILY);

      // RefreshHFiles for namespace
      Long procId = admin.refreshHFiles(TEST_NAMESPACE);
      assertTrue(procId >= 0);

    } catch (Exception e) {
      Assert.fail("RefreshHFilesForAllNamespace Should Not Throw Exception: " + e);
      throw new RuntimeException(e);
    } finally {
      // Delete namespace post test execution
      // This will delete all tables under namespace hence no explicit table
      // deletion for table under namespace is needed.
      deleteNamespace(TEST_NAMESPACE);
    }
  }

  @Test(expected = NamespaceNotFoundException.class)
  public void testRefreshHFilesForNonExistentNamespace() throws Exception {
    // RefreshHFiles for namespace
    admin.refreshHFiles(TEST_NONEXISTENT_NAMESPACE);
  }

  @Test
  public void testRefreshHFilesForAllTables() throws Exception {
    try {
      // Create table in default namespace
      createTableAndWait(TEST_TABLE, TEST_FAMILY);

      // Create test namespace
      createNamespace(TEST_NAMESPACE);

      // Create table under test namespace
      createTableInNamespaceAndWait(TEST_NAMESPACE, TEST_TABLE, TEST_FAMILY);

      // RefreshHFiles for all the tables
      Long procId = admin.refreshHFiles();
      assertTrue(procId >= 0);

    } catch (Exception e) {
      Assert.fail("RefreshHFilesForAllTables Should Not Throw Exception: " + e);
      throw new RuntimeException(e);
    } finally {
      // Delete table name post test execution
      deleteTable(TEST_TABLE);

      // Delete namespace post test execution
      // This will delete all tables under namespace hence no explicit table
      // deletion for table under namespace is needed.
      deleteNamespace(TEST_NAMESPACE);
    }
  }
}
