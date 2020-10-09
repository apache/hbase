/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.replication.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

/**
 * Tests CatalogReplicaReplicationEndpoint class by setting up region replicas and verifying
 * async wal replication replays the edits to the secondary region in various scenarios.
 */
@Category({ LargeTests.class })
public class TestCatalogReplicaReplicationEndpoint extends
  TestMetaRegionReplicaReplicationEndpoint {

  @ClassRule public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCatalogReplicaReplicationEndpoint.class);

  @Override protected Configuration setupConfig() {
    Configuration conf = super.setupConfig();
    conf.set("hbase.region.replica.catalog.replication",
      CatalogReplicaReplicationEndpoint.class.getName());
    return conf;
  }

}
