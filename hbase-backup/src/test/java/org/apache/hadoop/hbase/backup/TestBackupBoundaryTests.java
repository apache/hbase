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
package org.apache.hadoop.hbase.backup;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

@Category(LargeTests.class)
public class TestBackupBoundaryTests extends TestBackupBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestBackupBoundaryTests.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestBackupBoundaryTests.class);

  /**
   * Verify that full backup is created on a single empty table correctly.
   *
   * @throws Exception if doing the full backup fails
   */
  @Test
  public void testFullBackupSingleEmpty() throws Exception {
    LOG.info("create full backup image on single table");
    List<TableName> tables = Lists.newArrayList(table3);
    LOG.info("Finished Backup " + fullTableBackup(tables));
  }

  /**
   * Verify that full backup is created on multiple empty tables correctly.
   *
   * @throws Exception if doing the full backup fails
   */
  @Test
  public void testFullBackupMultipleEmpty() throws Exception {
    LOG.info("create full backup image on mulitple empty tables");

    List<TableName> tables = Lists.newArrayList(table3, table4);
    fullTableBackup(tables);
  }

  /**
   * Verify that full backup fails on a single table that does not exist.
   *
   * @throws Exception if doing the full backup fails
   */
  @Test(expected = IOException.class)
  public void testFullBackupSingleDNE() throws Exception {
    LOG.info("test full backup fails on a single table that does not exist");
    List<TableName> tables = toList("tabledne");
    fullTableBackup(tables);
  }

  /**
   * Verify that full backup fails on multiple tables that do not exist.
   *
   * @throws Exception if doing the full backup fails
   */
  @Test(expected = IOException.class)
  public void testFullBackupMultipleDNE() throws Exception {
    LOG.info("test full backup fails on multiple tables that do not exist");
    List<TableName> tables = toList("table1dne", "table2dne");
    fullTableBackup(tables);
  }

  /**
   * Verify that full backup fails on tableset containing real and fake tables.
   *
   * @throws Exception if doing the full backup fails
   */
  @Test(expected = IOException.class)
  public void testFullBackupMixExistAndDNE() throws Exception {
    LOG.info("create full backup fails on tableset containing real and fake table");

    List<TableName> tables = toList(table1.getNameAsString(), "tabledne");
    fullTableBackup(tables);
  }
}
