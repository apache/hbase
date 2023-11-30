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
package org.apache.hadoop.hbase.replication;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ ReplicationTests.class, LargeTests.class })
public class TestReplicationWithFSPeerStorage extends TestReplicationBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReplicationWithFSPeerStorage.class);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // enable file system based peer storage
    UTIL1.getConfiguration().set(ReplicationStorageFactory.REPLICATION_PEER_STORAGE_IMPL,
      ReplicationPeerStorageType.FILESYSTEM.name().toLowerCase());
    UTIL2.getConfiguration().set(ReplicationStorageFactory.REPLICATION_PEER_STORAGE_IMPL,
      ReplicationPeerStorageType.FILESYSTEM.name().toLowerCase());
    TestReplicationBase.setUpBeforeClass();
  }

  @Before
  public void setUp() throws Exception {
    cleanUp();
  }

  /**
   * Add a row, check it's replicated, delete it, check's gone
   */
  @Test
  public void testSimplePutDelete() throws Exception {
    runSimplePutDeleteTest();
  }

  /**
   * Try a small batch upload using the write buffer, check it's replicated
   */
  @Test
  public void testSmallBatch() throws Exception {
    runSmallBatchTest();
  }
}
