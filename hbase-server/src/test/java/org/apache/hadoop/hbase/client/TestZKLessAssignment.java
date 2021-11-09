/*
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

package org.apache.hadoop.hbase.client;

import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;

@Category({ LargeTests.class})
public class TestZKLessAssignment {

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @Test
  public void testZkLessAssignmentRollbackAndRollForward() throws Exception {
    Configuration config = TEST_UTIL.getConfiguration();
    config.setBoolean("hbase.assignment.usezk.migrating", true);
    TEST_UTIL.startMiniCluster(2);
    TableName tableName = TableName.valueOf("testZkLessAssignmentRollbackAndRollForward");
    TEST_UTIL.createTable(tableName.getName(), new byte[][] { Bytes.toBytes("cf1") }, config)
      .close();

    try (Connection connection = ConnectionFactory.createConnection(config)) {
      List<Result> results = MetaTableAccessor.fullScanOfMeta(connection);
      boolean isSNQualifierExist = false;
      boolean isStateQualifierExist = false;
      for (Result result : results) {
        Cell cell =
          result.getColumnLatestCell(HConstants.CATALOG_FAMILY, HConstants.SERVERNAME_QUALIFIER);
        if (cell != null && cell.getValueLength() > 0) {
          isSNQualifierExist = true;
        }
        cell = result.getColumnLatestCell(HConstants.CATALOG_FAMILY, HConstants.STATE_QUALIFIER);
        if (cell != null && cell.getValueLength() > 0) {
          isStateQualifierExist = true;
        }
      }
      Assert.assertTrue(isSNQualifierExist);
      Assert.assertTrue(isStateQualifierExist);
    }

    TEST_UTIL.shutdownMiniHBaseCluster();
    config.unset("hbase.assignment.usezk.migrating");
    TEST_UTIL.restartHBaseCluster(2);

    try (Connection connection = ConnectionFactory.createConnection(config)) {
      List<Result> results = MetaTableAccessor.fullScanOfMeta(connection);
      boolean isSNQualifierExist = false;
      boolean isStateQualifierExist = false;
      for (Result result : results) {
        Cell cell =
          result.getColumnLatestCell(HConstants.CATALOG_FAMILY, HConstants.SERVERNAME_QUALIFIER);
        if (cell != null && cell.getValueLength() > 0) {
          isSNQualifierExist = true;
        }
        cell = result.getColumnLatestCell(HConstants.CATALOG_FAMILY, HConstants.STATE_QUALIFIER);
        if (cell != null && cell.getValueLength() > 0) {
          isStateQualifierExist = true;
        }
      }
      Assert.assertFalse(isSNQualifierExist);
      Assert.assertFalse(isStateQualifierExist);
    }
  }

}
