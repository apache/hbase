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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Collections;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Test being able to edit hbase:meta.
 */
@Category({ MiscTests.class, MediumTests.class })
public class TestHBaseMetaEdit {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHBaseMetaEdit.class);
  @Rule
  public TestName name = new TestName();
  private final static HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @Before
  public void before() throws Exception {
    UTIL.startMiniCluster();
  }

  @After
  public void after() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  // make sure that with every possible way, we get the same meta table descriptor.
  private TableDescriptor getMetaDescriptor() throws TableNotFoundException, IOException {
    Admin admin = UTIL.getAdmin();
    TableDescriptor get = admin.getDescriptor(TableName.META_TABLE_NAME);
    TableDescriptor list = admin.listTableDescriptors(null, true).stream()
      .filter(td -> td.isMetaTable()).findAny().get();
    TableDescriptor listByName =
      admin.listTableDescriptors(Collections.singletonList(TableName.META_TABLE_NAME)).get(0);
    TableDescriptor listByNs =
      admin.listTableDescriptorsByNamespace(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME).stream()
        .filter(td -> td.isMetaTable()).findAny().get();
    assertEquals(get, list);
    assertEquals(get, listByName);
    assertEquals(get, listByNs);
    return get;
  }

  /**
   * Set versions, set HBASE-16213 indexed block encoding, and add a column family.
   * Delete the column family. Then try to delete a core hbase:meta family (should fail).
   * Verify they are all in place by looking at TableDescriptor AND by checking
   * what the RegionServer sees after opening Region.
   */
  @Test
  public void testEditMeta() throws IOException {
    Admin admin = UTIL.getAdmin();
    admin.tableExists(TableName.META_TABLE_NAME);
    TableDescriptor originalDescriptor = getMetaDescriptor();
    ColumnFamilyDescriptor cfd = originalDescriptor.getColumnFamily(HConstants.CATALOG_FAMILY);
    int oldVersions = cfd.getMaxVersions();
    // Add '1' to current versions count. Set encoding too.
    cfd = ColumnFamilyDescriptorBuilder.newBuilder(cfd).setMaxVersions(oldVersions + 1).
        setConfiguration(ColumnFamilyDescriptorBuilder.DATA_BLOCK_ENCODING,
            DataBlockEncoding.ROW_INDEX_V1.toString()).build();
    admin.modifyColumnFamily(TableName.META_TABLE_NAME, cfd);
    byte [] extraColumnFamilyName = Bytes.toBytes("xtra");
    ColumnFamilyDescriptor newCfd =
      ColumnFamilyDescriptorBuilder.newBuilder(extraColumnFamilyName).build();
    admin.addColumnFamily(TableName.META_TABLE_NAME, newCfd);
    TableDescriptor descriptor = getMetaDescriptor();
    // Assert new max versions is == old versions plus 1.
    assertEquals(oldVersions + 1,
        descriptor.getColumnFamily(HConstants.CATALOG_FAMILY).getMaxVersions());
    descriptor = getMetaDescriptor();
    // Assert new max versions is == old versions plus 1.
    assertEquals(oldVersions + 1,
        descriptor.getColumnFamily(HConstants.CATALOG_FAMILY).getMaxVersions());
    assertTrue(descriptor.getColumnFamily(newCfd.getName()) != null);
    String encoding = descriptor.getColumnFamily(HConstants.CATALOG_FAMILY).getConfiguration().
        get(ColumnFamilyDescriptorBuilder.DATA_BLOCK_ENCODING);
    assertEquals(encoding, DataBlockEncoding.ROW_INDEX_V1.toString());
    Region r = UTIL.getHBaseCluster().getRegionServer(0).
        getRegion(RegionInfoBuilder.FIRST_META_REGIONINFO.getEncodedName());
    assertEquals(oldVersions + 1,
        r.getStore(HConstants.CATALOG_FAMILY).getColumnFamilyDescriptor().getMaxVersions());
    encoding = r.getStore(HConstants.CATALOG_FAMILY).getColumnFamilyDescriptor().
        getConfigurationValue(ColumnFamilyDescriptorBuilder.DATA_BLOCK_ENCODING);
    assertEquals(encoding, DataBlockEncoding.ROW_INDEX_V1.toString());
    assertTrue(r.getStore(extraColumnFamilyName) != null);
    // Assert we can't drop critical hbase:meta column family but we can drop any other.
    admin.deleteColumnFamily(TableName.META_TABLE_NAME, newCfd.getName());
    descriptor = getMetaDescriptor();
    assertTrue(descriptor.getColumnFamily(newCfd.getName()) == null);
    try {
      admin.deleteColumnFamily(TableName.META_TABLE_NAME, HConstants.CATALOG_FAMILY);
      fail("Should not reach here");
    } catch (HBaseIOException hioe) {
      assertTrue(hioe.getMessage().contains("Delete of hbase:meta"));
    }
  }

  /**
   * Validate whether meta table can be altered as READ only, shouldn't be allowed otherwise it will
   * break assignment functionalities. See HBASE-24977.
   */
  @Test
  public void testAlterMetaWithReadOnly() throws IOException {
    Admin admin = UTIL.getAdmin();
    TableDescriptor origMetaTableDesc = admin.getDescriptor(TableName.META_TABLE_NAME);
    assertFalse(origMetaTableDesc.isReadOnly());
    TableDescriptor newTD =
        TableDescriptorBuilder.newBuilder(origMetaTableDesc).setReadOnly(true).build();
    try {
      admin.modifyTable(newTD);
      fail("Meta table can't be set as read only");
    } catch (Exception e) {
      assertFalse(admin.getDescriptor(TableName.META_TABLE_NAME).isReadOnly());
    }

    // Create a table to check region assignment & meta operation
    TableName tableName = TableName.valueOf("tempTable");
    TableDescriptor td = TableDescriptorBuilder.newBuilder(tableName).setReadOnly(true)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("f1")).build())
        .build();
    UTIL.getAdmin().createTable(td);
    UTIL.deleteTable(tableName);
  }
}
