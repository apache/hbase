/**
 * Copyright The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.rsgroup;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;

/**
 * Runs all of the units tests defined in TestGroupBase
 * as an integration test.
 * Requires TestRSGroupBase.NUM_SLAVE_BASE servers to run.
 */
@Category(IntegrationTests.class)
public class IntegrationTestRSGroup extends TestRSGroupsBase {
  //Integration specific
  private final static Log LOG = LogFactory.getLog(IntegrationTestRSGroup.class);
  private static boolean initialized = false;

  @Before
  public void beforeMethod() throws Exception {
    if(!initialized) {
      LOG.info("Setting up IntegrationTestGroup");
      LOG.info("Initializing cluster with " + NUM_SLAVES_BASE + " servers");
      TEST_UTIL = new IntegrationTestingUtility();
      ((IntegrationTestingUtility)TEST_UTIL).initializeCluster(NUM_SLAVES_BASE);
      //set shared configs
      admin = TEST_UTIL.getHBaseAdmin();
      cluster = TEST_UTIL.getHBaseClusterInterface();
      rsGroupAdmin = new VerifyingRSGroupAdminClient(rsGroupAdmin.newClient(TEST_UTIL.getConnection()),
          TEST_UTIL.getConfiguration());
      LOG.info("Done initializing cluster");
      initialized = true;
      //cluster may not be clean
      //cleanup when initializing
      afterMethod();
    }
  }

  @After
  public void afterMethod() throws Exception {
    LOG.info("Cleaning up previous test run");
    //cleanup previous artifacts
    deleteTableIfNecessary();
    deleteNamespaceIfNecessary();
    deleteGroups();
    admin.setBalancerRunning(true, true);

    LOG.info("Restoring the cluster");
    ((IntegrationTestingUtility)TEST_UTIL).restoreCluster();
    LOG.info("Done restoring the cluster");

    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        LOG.info("Waiting for cleanup to finish "+ rsGroupAdmin.listRSGroups());
        //Might be greater since moving servers back to default
        //is after starting a server
        return rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getServers().size()
            >= NUM_SLAVES_BASE;
      }
    });

    TEST_UTIL.waitFor(WAIT_TIMEOUT, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        LOG.info("Waiting for regionservers to be registered "+ rsGroupAdmin.listRSGroups());
        //Might be greater since moving servers back to default
        //is after starting a server
        return rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getServers().size()
            == getNumServers();
      }
    });

    LOG.info("Done cleaning up previous test run");
  }
}
