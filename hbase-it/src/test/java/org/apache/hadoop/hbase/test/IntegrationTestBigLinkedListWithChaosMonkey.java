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

package org.apache.hadoop.hbase.test;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.IntegrationTests;
import org.apache.hadoop.hbase.util.ChaosMonkey;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;


/**
 * This is the same integration test as {@link IntegrationTestBigLinkedList} while killing the
 * region servers and the master(s) randomly
 */
@Category(IntegrationTests.class)
public class IntegrationTestBigLinkedListWithChaosMonkey extends IntegrationTestBigLinkedList {
  private static final Log LOG = LogFactory
      .getLog(IntegrationTestBigLinkedListWithChaosMonkey.class);

  private ChaosMonkey monkey;

  public IntegrationTestBigLinkedListWithChaosMonkey() {
    super();
    Configuration conf = getConf();
    if (conf != null) {
      conf.set(TABLE_NAME_KEY, "IntegrationTestBigLinkedListWithChaosMonkey");
    } else {
      this.getTestingUtil().getConfiguration()
          .set(TABLE_NAME_KEY, "IntegrationTestBigLinkedListWithChaosMonkey");
    }
  }

  @Before
  public void setUp() throws Exception {
    if (!getTestingUtil().isDistributedCluster()) {
      this.NUM_SLAVES_BASE = 5; // only used in MiniCluster mode
    }
    super.setUp();
    monkey = new ChaosMonkey(util, ChaosMonkey.EVERY_MINUTE_RANDOM_ACTION_POLICY);
    LOG.info("Chaos Monkey Starting");
    monkey.start();
  }

  @After
  public void tearDown() throws Exception {
    if (monkey != null) {
      monkey.stop("test has finished, that's why");
      monkey.waitForStop();
    }
    super.tearDown();
  }

  @Test
  public void testContinuousIngest() throws IOException, Exception {
    // Loop <num iterations> <num mappers> <num nodes per mapper> <output dir> <num reducers>
    int ret = ToolRunner.run(
      getTestingUtil().getConfiguration(),
      new Loop(),
      new String[] { "1", "1", "1000000",
          util.getDataTestDirOnTestFS("IntegrationTestBigLinkedListWithChaosMonkey").toString(),
          "1" });
    org.junit.Assert.assertEquals(0, ret);
  }

  public static void main(String[] args) throws Exception {

    IntegrationTestBigLinkedListWithChaosMonkey test = 
        new IntegrationTestBigLinkedListWithChaosMonkey();
    IntegrationTestingUtility.setUseDistributedCluster(test.getTestingUtil().getConfiguration());
    // set minimum cluster size requirements
    test.NUM_SLAVES_BASE = 3;
    test.setUp();

    // run the test
    int ret = ToolRunner.run(test.getTestingUtil().getConfiguration(), test, args);

    test.tearDown();
    System.exit(ret);
  }
}
