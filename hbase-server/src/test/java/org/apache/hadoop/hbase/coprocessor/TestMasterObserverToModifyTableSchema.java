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
package org.apache.hadoop.hbase.coprocessor;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ CoprocessorTests.class, MediumTests.class })
public class TestMasterObserverToModifyTableSchema {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMasterObserverToModifyTableSchema.class);

  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static TableName TABLENAME = TableName.valueOf("TestTable");

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
        OnlyOneVersionAllowedMasterObserver.class.getName());
    UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testMasterObserverToModifyTableSchema() throws IOException {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(TABLENAME);
    for (int i = 1; i <= 3; i++) {
      builder.setColumnFamily(
          ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cf" + i)).setMaxVersions(i)
              .build());
    }
    try (Admin admin = UTIL.getAdmin()) {
      admin.createTable(builder.build());
      assertOneVersion(admin.getDescriptor(TABLENAME));

      builder.modifyColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cf1"))
          .setMaxVersions(Integer.MAX_VALUE).build());
      admin.modifyTable(builder.build());
      assertOneVersion(admin.getDescriptor(TABLENAME));
    }
  }

  private void assertOneVersion(TableDescriptor td) {
    for (ColumnFamilyDescriptor cfd : td.getColumnFamilies()) {
      assertEquals(1, cfd.getMaxVersions());
    }
  }

  public static class OnlyOneVersionAllowedMasterObserver
      implements MasterCoprocessor, MasterObserver {

    @Override
    public Optional<MasterObserver> getMasterObserver() {
      return Optional.of(this);
    }

    @Override
    public TableDescriptor preCreateTableRegionsInfos(
        ObserverContext<MasterCoprocessorEnvironment> ctx, TableDescriptor desc)
        throws IOException {
      TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(desc);
      for (ColumnFamilyDescriptor cfd : desc.getColumnFamilies()) {
        builder.modifyColumnFamily(
            ColumnFamilyDescriptorBuilder.newBuilder(cfd).setMaxVersions(1).build());
      }
      return builder.build();
    }

    @Override
    public TableDescriptor preModifyTable(ObserverContext<MasterCoprocessorEnvironment> env,
        TableName tableName, final TableDescriptor currentDescriptor,
        final TableDescriptor newDescriptor) throws IOException {
      TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(newDescriptor);
      for (ColumnFamilyDescriptor cfd : newDescriptor.getColumnFamilies()) {
        builder.modifyColumnFamily(
            ColumnFamilyDescriptorBuilder.newBuilder(cfd).setMaxVersions(1).build());
      }
      return builder.build();
    }
  }
}
