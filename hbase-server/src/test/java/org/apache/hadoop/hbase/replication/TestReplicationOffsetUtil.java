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

import static org.apache.hadoop.hbase.replication.ReplicationOffsetUtil.shouldReplicate;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ ReplicationTests.class, SmallTests.class })
public class TestReplicationOffsetUtil {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReplicationOffsetUtil.class);

  @Test
  public void test() {
    assertTrue(shouldReplicate(null, "whatever"));
    assertTrue(shouldReplicate(ReplicationGroupOffset.BEGIN, "whatever"));
    ServerName sn = ServerName.valueOf("host", 16010, EnvironmentEdgeManager.currentTime());
    ReplicationGroupOffset offset = new ReplicationGroupOffset(sn + ".12345", 100);
    assertTrue(shouldReplicate(offset, sn + ".12346"));
    assertFalse(shouldReplicate(offset, sn + ".12344"));
    assertTrue(shouldReplicate(offset, sn + ".12345"));
    // -1 means finish replication, so should not replicate
    assertFalse(shouldReplicate(new ReplicationGroupOffset(sn + ".12345", -1), sn + ".12345"));
  }
}
