/**
 *
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
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Unit testing for HelloHBase.
 */
@Category(MediumTests.class)
public class TestHelloHBase {

  private static final HBaseTestingUtility TEST_UTIL
          = new HBaseTestingUtility();

  @BeforeClass
  public static void beforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testNamespaceExists() throws Exception {
    final String NONEXISTENT_NAMESPACE = "xyzpdq_nonexistent";
    final String EXISTING_NAMESPACE = "pdqxyz_myExistingNamespace";
    boolean exists;
    Admin admin = TEST_UTIL.getHBaseAdmin();

    exists = HelloHBase.namespaceExists(admin, NONEXISTENT_NAMESPACE);
    assertEquals("#namespaceExists failed: found nonexistent namespace.",
            false, exists);

    admin.createNamespace
        (NamespaceDescriptor.create(EXISTING_NAMESPACE).build());
    exists = HelloHBase.namespaceExists(admin, EXISTING_NAMESPACE);
    assertEquals("#namespaceExists failed: did NOT find existing namespace.",
            true, exists);
    admin.deleteNamespace(EXISTING_NAMESPACE);
  }

  @Test
  public void testCreateNamespaceAndTable() throws Exception {
    Admin admin = TEST_UTIL.getHBaseAdmin();
    HelloHBase.createNamespaceAndTable(admin);

    boolean namespaceExists
            = HelloHBase.namespaceExists(admin, HelloHBase.MY_NAMESPACE_NAME);
    assertEquals("#createNamespaceAndTable failed to create namespace.",
            true, namespaceExists);

    boolean tableExists = admin.tableExists(HelloHBase.MY_TABLE_NAME);
    assertEquals("#createNamespaceAndTable failed to create table.",
            true, tableExists);

    admin.disableTable(HelloHBase.MY_TABLE_NAME);
    admin.deleteTable(HelloHBase.MY_TABLE_NAME);
    admin.deleteNamespace(HelloHBase.MY_NAMESPACE_NAME);
  }

  @Test
  public void testPutRowToTable() throws IOException {
    Admin admin = TEST_UTIL.getHBaseAdmin();
    admin.createNamespace
        (NamespaceDescriptor.create(HelloHBase.MY_NAMESPACE_NAME).build());
    Table table
            = TEST_UTIL.createTable
                (HelloHBase.MY_TABLE_NAME, HelloHBase.MY_COLUMN_FAMILY_NAME);

    HelloHBase.putRowToTable(table);
    Result row = table.get(new Get(HelloHBase.MY_ROW_ID));
    assertEquals("#putRowToTable failed to store row.", false, row.isEmpty());

    TEST_UTIL.deleteTable(HelloHBase.MY_TABLE_NAME);
    admin.deleteNamespace(HelloHBase.MY_NAMESPACE_NAME);
  }

  @Test
  public void testDeleteRow() throws IOException {
    Admin admin = TEST_UTIL.getHBaseAdmin();
    admin.createNamespace
        (NamespaceDescriptor.create(HelloHBase.MY_NAMESPACE_NAME).build());
    Table table
            = TEST_UTIL.createTable
                (HelloHBase.MY_TABLE_NAME, HelloHBase.MY_COLUMN_FAMILY_NAME);

    table.put(new Put(HelloHBase.MY_ROW_ID).
            addColumn(HelloHBase.MY_COLUMN_FAMILY_NAME,
                    HelloHBase.MY_FIRST_COLUMN_QUALIFIER,
                    Bytes.toBytes("xyz")));
    HelloHBase.deleteRow(table);
    Result row = table.get(new Get(HelloHBase.MY_ROW_ID));
    assertEquals("#deleteRow failed to delete row.", true, row.isEmpty());

    TEST_UTIL.deleteTable(HelloHBase.MY_TABLE_NAME);
    admin.deleteNamespace(HelloHBase.MY_NAMESPACE_NAME);
  }
}
