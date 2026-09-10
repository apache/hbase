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

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

/**
 * Run the same test as TestReplicationKillMasterRS but with WAL compression enabled Do not add
 * other tests in this class.
 */
@Tag(ReplicationTests.TAG)
@Tag(LargeTests.TAG)
public class TestReplicationKillMasterRSCompressed extends ReplicationKillMasterRSTestBase {

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    NUM_SLAVES1 = 2;
    CONF1.setBoolean(HConstants.ENABLE_WAL_COMPRESSION, true);
    configureClusters(UTIL1, UTIL2);
    startClusters();
  }
}
