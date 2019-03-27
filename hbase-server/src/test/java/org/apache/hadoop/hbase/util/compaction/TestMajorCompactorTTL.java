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
package org.apache.hadoop.hbase.util.compaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ MiscTests.class, MediumTests.class })
public class TestMajorCompactorTTL extends TestMajorCompactor {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMajorCompactorTTL.class);

  @Rule
  public TestName name = new TestName();

  @Before
  @Override
  public void setUp() throws Exception {
    utility = new HBaseTestingUtility();
    utility.getConfiguration().setInt("hbase.hfile.compaction.discharger.interval", 10);
    utility.startMiniCluster();
    admin = utility.getAdmin();
  }

  @After
  @Override
  public void tearDown() throws Exception {
    utility.shutdownMiniCluster();
  }

  @Test
  public void testCompactingATable() throws Exception {
    TableName tableName = createTable(name.getMethodName());

    // Delay a bit, so we can set the table TTL to 5 seconds
    Thread.sleep(10 * 1000);

    int numberOfRegions = admin.getRegions(tableName).size();
    int numHFiles = utility.getNumHFiles(tableName, FAMILY);
    // we should have a table with more store files than we would before we major compacted.
    assertTrue(numberOfRegions < numHFiles);
    modifyTTL(tableName);

    MajorCompactorTTL compactor = new MajorCompactorTTL(utility.getConfiguration(),
        admin.getDescriptor(tableName), 1, 200);
    compactor.initializeWorkQueues();
    compactor.compactAllRegions();
    compactor.shutdown();

    // verify that the store has been completely major compacted.
    numberOfRegions = admin.getRegions(tableName).size();
    numHFiles = utility.getNumHFiles(tableName, FAMILY);
    assertEquals(numberOfRegions, numHFiles);
  }

  protected void modifyTTL(TableName tableName) throws IOException, InterruptedException {
    // Set the TTL to 5 secs, so all the files just written above will get cleaned up on compact.
    admin.disableTable(tableName);
    utility.waitTableDisabled(tableName.getName());
    TableDescriptor descriptor = admin.getDescriptor(tableName);
    ColumnFamilyDescriptor colDesc = descriptor.getColumnFamily(FAMILY);
    ColumnFamilyDescriptorBuilder cFDB = ColumnFamilyDescriptorBuilder.newBuilder(colDesc);
    cFDB.setTimeToLive(5);
    admin.modifyColumnFamily(tableName, cFDB.build());
    admin.enableTable(tableName);
    utility.waitTableEnabled(tableName);
  }

  protected TableName createTable(String name) throws IOException, InterruptedException {
    TableName tableName = TableName.valueOf(name);
    utility.createMultiRegionTable(tableName, FAMILY, 5);
    utility.waitTableAvailable(tableName);
    Connection connection = utility.getConnection();
    Table table = connection.getTable(tableName);
    // write data and flush multiple store files:
    for (int i = 0; i < 5; i++) {
      utility.loadRandomRows(table, FAMILY, 50, 100);
      utility.flush(tableName);
    }
    table.close();
    return tableName;
  }
}