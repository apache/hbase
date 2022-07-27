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
package org.apache.hadoop.hbase.testing;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

@Category({ MiscTests.class, LargeTests.class })
public class TestTestingHBaseClusterReplicationTwoClusters
  extends TestingHBaseClusterReplicationTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestTestingHBaseClusterReplicationTwoClusters.class);

  @Override
  protected void startClusters() throws Exception {
    sourceCluster = TestingHBaseCluster.create(TestingHBaseClusterOption.builder().build());
    sourceCluster.start();
    peerCluster = TestingHBaseCluster.create(TestingHBaseClusterOption.builder().build());
    peerCluster.start();
  }

  @Override
  protected void stopClusters() throws Exception {
  }

}
