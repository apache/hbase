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
package org.apache.hadoop.hbase.keymeta;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.crypto.KeymetaTestUtils;
import org.apache.hadoop.hbase.regionserver.StoreContext;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MiscTests.class, SmallTests.class })
public class TestKeyNamespaceUtil {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestKeyNamespaceUtil.class);

  @Test
  public void testConstructKeyNamespace_FromTableDescriptorAndFamilyDescriptor() {
    TableDescriptor tableDescriptor = mock(TableDescriptor.class);
    ColumnFamilyDescriptor familyDescriptor = mock(ColumnFamilyDescriptor.class);
    when(tableDescriptor.getTableName()).thenReturn(TableName.valueOf("test"));
    when(familyDescriptor.getNameAsString()).thenReturn("family");
    String keyNamespace = KeyNamespaceUtil.constructKeyNamespace(tableDescriptor, familyDescriptor);
    assertEquals("test/family", keyNamespace);
  }

  @Test
  public void testConstructKeyNamespace_FromStoreContext() {
    // Test store context path construction
    StoreContext storeContext = mock(StoreContext.class);
    ColumnFamilyDescriptor familyDescriptor = mock(ColumnFamilyDescriptor.class);
    when(storeContext.getTableName()).thenReturn(TableName.valueOf("test"));
    when(storeContext.getFamily()).thenReturn(familyDescriptor);
    when(familyDescriptor.getNameAsString()).thenReturn("family");
    String keyNamespace = KeyNamespaceUtil.constructKeyNamespace(storeContext);
    assertEquals("test/family", keyNamespace);
  }

  @Test
  public void testConstructKeyNamespace_FromStoreFileInfo_RegularFile() {
    // Test both regular files and linked files
    StoreFileInfo storeFileInfo = mock(StoreFileInfo.class);
    when(storeFileInfo.isLink()).thenReturn(false);
    Path path = KeymetaTestUtils.createMockPath("test", "family");
    when(storeFileInfo.getPath()).thenReturn(path);
    String keyNamespace = KeyNamespaceUtil.constructKeyNamespace(storeFileInfo);
    assertEquals("test/family", keyNamespace);
  }

  @Test
  public void testConstructKeyNamespace_FromStoreFileInfo_LinkedFile() {
    // Test both regular files and linked files
    StoreFileInfo storeFileInfo = mock(StoreFileInfo.class);
    HFileLink link = mock(HFileLink.class);
    when(storeFileInfo.isLink()).thenReturn(true);
    Path path = KeymetaTestUtils.createMockPath("test", "family");
    when(link.getOriginPath()).thenReturn(path);
    when(storeFileInfo.getLink()).thenReturn(link);
    String keyNamespace = KeyNamespaceUtil.constructKeyNamespace(storeFileInfo);
    assertEquals("test/family", keyNamespace);
  }

  @Test
  public void testConstructKeyNamespace_FromPath() {
    // Test path parsing with different HBase directory structures
    Path path = KeymetaTestUtils.createMockPath("test", "family");
    String keyNamespace = KeyNamespaceUtil.constructKeyNamespace(path);
    assertEquals("test/family", keyNamespace);
  }

  @Test
  public void testConstructKeyNamespace_FromStrings() {
    // Test string-based construction
    String tableName = "test";
    String family = "family";
    String keyNamespace = KeyNamespaceUtil.constructKeyNamespace(tableName, family);
    assertEquals("test/family", keyNamespace);
  }

  @Test
  public void testConstructKeyNamespace_NullChecks() {
    // Test null inputs for both table name and family
    assertThrows(NullPointerException.class, () -> KeyNamespaceUtil.constructKeyNamespace(null,
      "family"));
    assertThrows(NullPointerException.class, () -> KeyNamespaceUtil.constructKeyNamespace("test",
      null));
  }
}