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
package org.apache.hbase.archetypes.exemplars.shaded_client;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Unit testing for HelloHBase.
 */
@Tag(MediumTests.TAG)
public class TestHelloHBase {

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @BeforeAll
  public static void beforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterAll
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testNamespaceExists() throws Exception {
    final String NONEXISTENT_NAMESPACE = "xyzpdq_nonexistent";
    final String EXISTING_NAMESPACE = "pdqxyz_myExistingNamespace";
    boolean exists;
    Admin admin = TEST_UTIL.getAdmin();

    exists = HelloHBase.namespaceExists(admin, NONEXISTENT_NAMESPACE);
    assertFalse(exists, "#namespaceExists failed: found nonexistent namespace.");

    admin.createNamespace(NamespaceDescriptor.create(EXISTING_NAMESPACE).build());
    exists = HelloHBase.namespaceExists(admin, EXISTING_NAMESPACE);
    assertTrue(exists, "#namespaceExists failed: did NOT find existing namespace.");
    admin.deleteNamespace(EXISTING_NAMESPACE);
  }

  @Test
  public void testCreateNamespaceAndTable() throws Exception {
    Admin admin = TEST_UTIL.getAdmin();
    HelloHBase.createNamespaceAndTable(admin);

    boolean namespaceExists = HelloHBase.namespaceExists(admin, HelloHBase.MY_NAMESPACE_NAME);
    assertTrue(namespaceExists, "#createNamespaceAndTable failed to create namespace.");

    boolean tableExists = admin.tableExists(HelloHBase.MY_TABLE_NAME);
    assertTrue(tableExists, "#createNamespaceAndTable failed to create table.");

    admin.disableTable(HelloHBase.MY_TABLE_NAME);
    admin.deleteTable(HelloHBase.MY_TABLE_NAME);
    admin.deleteNamespace(HelloHBase.MY_NAMESPACE_NAME);
  }

  @Test
  public void testPutRowToTable() throws IOException {
    Admin admin = TEST_UTIL.getAdmin();
    admin.createNamespace(NamespaceDescriptor.create(HelloHBase.MY_NAMESPACE_NAME).build());
    Table table = TEST_UTIL.createTable(HelloHBase.MY_TABLE_NAME, HelloHBase.MY_COLUMN_FAMILY_NAME);

    HelloHBase.putRowToTable(table);
    Result row = table.get(new Get(HelloHBase.MY_ROW_ID));
    assertFalse(row.isEmpty(), "#putRowToTable failed to store row.");

    TEST_UTIL.deleteTable(HelloHBase.MY_TABLE_NAME);
    admin.deleteNamespace(HelloHBase.MY_NAMESPACE_NAME);
  }

  @Test
  public void testDeleteRow() throws IOException {
    Admin admin = TEST_UTIL.getAdmin();
    admin.createNamespace(NamespaceDescriptor.create(HelloHBase.MY_NAMESPACE_NAME).build());
    Table table = TEST_UTIL.createTable(HelloHBase.MY_TABLE_NAME, HelloHBase.MY_COLUMN_FAMILY_NAME);

    table.put(new Put(HelloHBase.MY_ROW_ID).addColumn(HelloHBase.MY_COLUMN_FAMILY_NAME,
      HelloHBase.MY_FIRST_COLUMN_QUALIFIER, Bytes.toBytes("xyz")));
    HelloHBase.deleteRow(table);
    Result row = table.get(new Get(HelloHBase.MY_ROW_ID));
    assertTrue(row.isEmpty(), "#deleteRow failed to delete row.");

    TEST_UTIL.deleteTable(HelloHBase.MY_TABLE_NAME);
    admin.deleteNamespace(HelloHBase.MY_NAMESPACE_NAME);
  }
}
