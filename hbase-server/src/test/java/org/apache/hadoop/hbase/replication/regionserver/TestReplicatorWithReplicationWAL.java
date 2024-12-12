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
package org.apache.hadoop.hbase.replication.regionserver;

import static org.apache.hadoop.hbase.HConstants.REPLICATION_WAL_FILTER_BY_SCOPE_ENABLED;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.replication.TestReplicationBase;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestReplicatorWithReplicationWAL extends TestReplicator {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReplicatorWithReplicationWAL.class);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Set RPC size limit to 10kb (will be applied to both source and sink clusters)
    CONF1.setInt(RpcServer.MAX_REQUEST_SIZE, 1024 * 10);
    CONF1.setBoolean(REPLICATION_WAL_FILTER_BY_SCOPE_ENABLED, true);
    TestReplicationBase.setUpBeforeClass();
  }

}
