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
package org.apache.hadoop.hbase.security.access;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ SecurityTests.class, SmallTests.class })
public class TestPermissionBuilder {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestPermissionBuilder.class);

  @Test
  public void testBuildGlobalPermission() {
    // check global permission with empty action
    Permission permission = Permission.newBuilder().build();
    assertTrue(permission instanceof GlobalPermission);
    assertEquals(0, permission.getActions().length);

    // check global permission with ADMIN action
    permission = Permission.newBuilder().withActionCodes(Bytes.toBytes("A")).build();
    assertTrue(permission instanceof GlobalPermission);
    assertEquals(1, permission.getActions().length);
    assertTrue(permission.getActions()[0] == Action.ADMIN);

    byte[] qualifier = Bytes.toBytes("q");
    try {
      permission = Permission.newBuilder().withQualifier(qualifier)
          .withActions(Action.CREATE, Action.READ).build();
      fail("Should throw NPE");
    } catch (NullPointerException e) {
      // catch NPE because set qualifier but table name is null
    }

    permission = Permission.newBuilder().withActionCodes(Bytes.toBytes("ACP"))
        .withActions(Action.READ, Action.ADMIN).build();
    assertEquals(3, permission.getActions().length);
    assertEquals(Action.READ, permission.getActions()[0]);
    assertEquals(Action.CREATE, permission.getActions()[1]);
    assertEquals(Action.ADMIN, permission.getActions()[2]);
  }

  @Test
  public void testBuildNamespacePermission() {
    String namespace = "ns";
    // check namespace permission with CREATE and READ actions
    Permission permission =
        Permission.newBuilder(namespace).withActions(Action.CREATE, Action.READ).build();
    assertTrue(permission instanceof NamespacePermission);
    NamespacePermission namespacePermission = (NamespacePermission) permission;
    assertEquals(namespace, namespacePermission.getNamespace());
    assertEquals(2, permission.getActions().length);
    assertEquals(Action.READ, permission.getActions()[0]);
    assertEquals(Action.CREATE, permission.getActions()[1]);

    byte[] family = Bytes.toBytes("f");
    try {
      permission = Permission.newBuilder(namespace).withFamily(family)
          .withActions(Action.CREATE, Action.READ).build();
      fail("Should throw NPE");
    } catch (NullPointerException e) {
      // catch NPE because set family but table name is null
    }
  }

  @Test
  public void testBuildTablePermission() {
    TableName tableName = TableName.valueOf("ns", "table");
    byte[] family = Bytes.toBytes("f");
    byte[] qualifier = Bytes.toBytes("q");
    // check table permission without family or qualifier
    Permission permission =
        Permission.newBuilder(tableName).withActions(Action.WRITE, Action.READ).build();
    assertTrue(permission instanceof TablePermission);
    assertEquals(2, permission.getActions().length);
    assertEquals(Action.READ, permission.getActions()[0]);
    assertEquals(Action.WRITE, permission.getActions()[1]);
    TablePermission tPerm = (TablePermission) permission;
    assertEquals(tableName, tPerm.getTableName());
    assertEquals(null, tPerm.getFamily());
    assertEquals(null, tPerm.getQualifier());

    // check table permission with family
    permission =
        Permission.newBuilder(tableName).withFamily(family).withActions(Action.EXEC).build();
    assertTrue(permission instanceof TablePermission);
    assertEquals(1, permission.getActions().length);
    assertEquals(Action.EXEC, permission.getActions()[0]);
    tPerm = (TablePermission) permission;
    assertEquals(tableName, tPerm.getTableName());
    assertTrue(Bytes.equals(family, tPerm.getFamily()));
    assertTrue(Bytes.equals(null, tPerm.getQualifier()));

    // check table permission with family and qualifier
    permission =
        Permission.newBuilder(tableName).withFamily(family).withQualifier(qualifier).build();
    assertTrue(permission instanceof TablePermission);
    assertEquals(0, permission.getActions().length);
    tPerm = (TablePermission) permission;
    assertEquals(tableName, tPerm.getTableName());
    assertTrue(Bytes.equals(family, tPerm.getFamily()));
    assertTrue(Bytes.equals(qualifier, tPerm.getQualifier()));
  }
}
