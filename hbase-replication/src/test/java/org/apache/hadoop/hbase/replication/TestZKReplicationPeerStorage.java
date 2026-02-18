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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseZKTestingUtility;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

@Tag(ReplicationTests.TAG)
@Tag(MediumTests.TAG)
public class TestZKReplicationPeerStorage extends ReplicationPeerStorageTestBase {

  private static final HBaseZKTestingUtility UTIL = new HBaseZKTestingUtility();

  @BeforeAll
  public static void setUp() throws Exception {
    UTIL.startMiniZKCluster();
    STORAGE = new ZKReplicationPeerStorage(UTIL.getZooKeeperWatcher(), UTIL.getConfiguration());
  }

  @AfterAll
  public static void tearDown() throws IOException {
    UTIL.shutdownMiniZKCluster();
  }

  @Override
  protected void assertPeerNameControlException(ReplicationException e) {
    assertThat(e.getCause(), instanceOf(KeeperException.NodeExistsException.class));
  }
}
