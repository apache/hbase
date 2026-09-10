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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.replication.ReplicationStorageFactory;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.hbck.HbckTestingUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@Tag(MiscTests.TAG)
@Tag(MediumTests.TAG)
public class TestHBaseFsckWithoutTableHbaseReplication {

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  @BeforeEach
  public void setUp(TestInfo testInfo) throws Exception {
    UTIL.getConfiguration().setBoolean("hbase.write.hbck1.lock.file", false);
    TableName tableName =
      TableName.valueOf("replication_" + testInfo.getTestMethod().get().getName());
    UTIL.getConfiguration().set(ReplicationStorageFactory.REPLICATION_QUEUE_TABLE_NAME,
      tableName.getNameAsString());
    UTIL.startMiniCluster(1);
  }

  @AfterEach
  public void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void test(TestInfo testInfo) throws Exception {
    TableName tableName =
      TableName.valueOf("replication_" + testInfo.getTestMethod().get().getName());
    assertFalse(UTIL.getAdmin().tableExists(tableName));
    HBaseFsck hBaseFsck = HbckTestingUtil.doFsck(UTIL.getConfiguration(), true);
    assertEquals(0, hBaseFsck.getRetCode());
  }
}
