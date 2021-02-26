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
package org.apache.hadoop.hbase.security.visibility;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;


@Category({ SecurityTests.class, MediumTests.class })
public class TestVisibilityLabelsOnNewVersionBehaviorTable
    extends VisibilityLabelsWithDeletesTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestVisibilityLabelsOnNewVersionBehaviorTable.class);

  @Override
  protected Table createTable(byte[] fam) throws IOException {
    TableName tableName = TableName.valueOf(testName.getMethodName());
    TEST_UTIL.getAdmin()
        .createTable(TableDescriptorBuilder.newBuilder(tableName)
            .setColumnFamily(
              ColumnFamilyDescriptorBuilder.newBuilder(fam).setNewVersionBehavior(true).build())
            .build());
    return TEST_UTIL.getConnection().getTable(tableName);
  }
}
