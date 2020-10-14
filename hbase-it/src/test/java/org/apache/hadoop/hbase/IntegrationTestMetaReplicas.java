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
package org.apache.hadoop.hbase;

import org.apache.hadoop.hbase.client.TestMetaWithReplicasShutdownHandling;
import org.apache.hadoop.hbase.regionserver.StorefileRefresherChore;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * An integration test that starts the cluster with three replicas for the meta
 * It then creates a table, flushes the meta, kills the server holding the primary.
 * After that a client issues put/get requests on the created table - the other
 * replicas of the meta would be used to get the location of the region of the created
 * table.
 */
@Category(IntegrationTests.class)
public class IntegrationTestMetaReplicas {

  /**
   * Util to get at the cluster.
   */
  private static IntegrationTestingUtility util;

  @BeforeClass
  public static void setUp() throws Exception {
    // Set up the integration test util
    if (util == null) {
      util = new IntegrationTestingUtility();
    }
    util.getConfiguration().setInt(StorefileRefresherChore.REGIONSERVER_STOREFILE_REFRESH_PERIOD,
      1000);
    // Make sure there are three servers.
    util.initializeCluster(3);
    HBaseTestingUtility.setReplicas(util.getAdmin(), TableName.META_TABLE_NAME, 3);
    // after set replicas suceed, we can make sure that all replicas are assigned, so we do not need
    // to wait them online
  }

  @AfterClass
  public static void teardown() throws Exception {
    //Clean everything up.
    util.restoreCluster();
    util = null;
  }

  @Test
  public void testShutdownHandling() throws Exception {
    // This test creates a table, flushes the meta (with 3 replicas), kills the
    // server holding the primary meta replica. Then it does a put/get into/from
    // the test table. The put/get operations would use the replicas to locate the
    // location of the test table's region
    TestMetaWithReplicasShutdownHandling.shutdownMetaAndDoValidations(util);
  }

  public static void main(String[] args) throws Exception {
    setUp();
    new IntegrationTestMetaReplicas().testShutdownHandling();
    teardown();
  }
}
