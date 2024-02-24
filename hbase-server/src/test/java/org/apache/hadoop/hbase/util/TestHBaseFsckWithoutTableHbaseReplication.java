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
package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.replication.ReplicationStorageFactory;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.hbck.HbckTestingUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MiscTests.class, MediumTests.class })
public class TestHBaseFsckWithoutTableHbaseReplication {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestHBaseFsckWithoutTableHbaseReplication.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  @Before
  public void setUp() throws Exception {
    UTIL.getConfiguration().setBoolean("hbase.write.hbck1.lock.file", false);
    UTIL.startMiniCluster(1);
  }

  @After
  public void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void test() throws Exception {
    Admin admin = UTIL.getAdmin();
    TableName tableName = TableName
      .valueOf(UTIL.getConfiguration().get(ReplicationStorageFactory.REPLICATION_QUEUE_TABLE_NAME,
        ReplicationStorageFactory.REPLICATION_QUEUE_TABLE_NAME_DEFAULT.getNameAsString()));
    assertFalse(admin.tableExists(tableName));
    HBaseFsck hBaseFsck = HbckTestingUtil.doFsck(UTIL.getConfiguration(), true);
    assertEquals(0, hBaseFsck.getRetCode());
  }
}
