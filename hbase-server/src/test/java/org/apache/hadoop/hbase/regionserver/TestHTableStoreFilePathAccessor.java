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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

@Category({ MiscTests.class, SmallTests.class })
public class TestHTableStoreFilePathAccessor extends StoreFilePathAccessorTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHTableStoreFilePathAccessor.class);

  @Override
  protected HTableStoreFilePathAccessor getStoreFilePathAccessor() {
    return new HTableStoreFilePathAccessor(TEST_UTIL.getConfiguration());
  }

  @Override
  public void cleanupTest() throws IOException {
    Admin admin = TEST_UTIL.getAdmin();
    if (admin.tableExists(TableName.STOREFILE_TABLE_NAME)
        && admin.isTableEnabled(TableName.STOREFILE_TABLE_NAME)) {
      admin.disableTable(TableName.STOREFILE_TABLE_NAME);
      admin.deleteTable(TableName.STOREFILE_TABLE_NAME);
    }
  }

  @Override
  public void verifyInitialize(MasterServices masterServices) throws Exception {
    assertFalse(TEST_UTIL.getAdmin().tableExists(TableName.STOREFILE_TABLE_NAME));
    storeFilePathAccessor.initialize(TEST_UTIL.getHBaseCluster().getMaster());
    assertNotNull(TEST_UTIL.getConnection().getTable(TableName.STOREFILE_TABLE_NAME));
    assertTrue("hbase:storefile table must be assigned and enabled.",
        ((HTableStoreFilePathAccessor) storeFilePathAccessor)
            .isStoreFileTableAssignedAndEnabled(masterServices));
  }

  @Override
  public Class getNotInitializedException() {
    return TableNotFoundException.class;
  }

}
