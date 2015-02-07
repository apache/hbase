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
package org.apache.hadoop.hbase.master.handler;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestDeleteTableHandler {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[] FAMILYNAME = Bytes.toBytes("fam");

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(1);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * We were only clearing rows that had a hregioninfo column in hbase:meta.  Mangled rows that
   * were missing the hregioninfo because of error were being left behind messing up any
   * subsequent table made with the same name. HBASE-12980
   * @throws IOException
   * @throws InterruptedException
   */
  @Test(timeout=60000)
  public void testDeleteForSureClearsAllTableRowsFromMeta()
  throws IOException, InterruptedException {
    final TableName tableName = TableName.valueOf("testDeleteForSureClearsAllTableRowsFromMeta");
    final HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    final HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(FAMILYNAME));
    admin.createTable(desc, HBaseTestingUtility.KEYS_FOR_HBA_CREATE_TABLE);
    // Now I have a nice table, mangle it by removing the HConstants.REGIONINFO_QUALIFIER_STR
    // content from a few of the rows.
    Scan metaScannerForMyTable = MetaReader.getScanForTableName(tableName);
    HTable metaTable = new HTable(TEST_UTIL.getConfiguration(), TableName.META_TABLE_NAME);
    try {
      ResultScanner scanner = metaTable.getScanner(metaScannerForMyTable);
      try {
        for (Result result : scanner) {
          // Just delete one row.
          Delete d = new Delete(result.getRow());
          d.deleteColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
          metaTable.delete(d);
          break;
        }
      } finally {
        scanner.close();
      }
      admin.disableTable(tableName);
      TEST_UTIL.waitTableDisabled(tableName.getName());
      admin.deleteTable(tableName);
      int rowCount = 0;
      scanner = metaTable.getScanner(metaScannerForMyTable);
      try {
        for (Result result : scanner) {
          rowCount++;
        }
      } finally {
        scanner.close();
      }
      assertEquals(0, rowCount);
    } finally {
      metaTable.close();
    }
  }
}

