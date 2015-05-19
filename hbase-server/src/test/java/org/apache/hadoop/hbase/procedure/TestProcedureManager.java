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
package org.apache.hadoop.hbase.procedure;

import static org.junit.Assert.assertArrayEquals;

import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.client.Admin;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MasterTests.class, SmallTests.class})
public class TestProcedureManager {

  private static final Log LOG = LogFactory.getLog(TestProcedureManager.class);
  private static final int NUM_RS = 2;
  private static HBaseTestingUtility util = new HBaseTestingUtility();

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // set configure to indicate which pm should be loaded
    Configuration conf = util.getConfiguration();

    conf.set(ProcedureManagerHost.MASTER_PROCEUDRE_CONF_KEY,
        SimpleMasterProcedureManager.class.getName());
    conf.set(ProcedureManagerHost.REGIONSERVER_PROCEDURE_CONF_KEY,
        SimpleRSProcedureManager.class.getName());

    util.startMiniCluster(NUM_RS);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    util.shutdownMiniCluster();
  }

  @Test
  public void testSimpleProcedureManager() throws IOException {
    Admin admin = util.getHBaseAdmin();

    byte[] result = admin.execProcedureWithRet(SimpleMasterProcedureManager.SIMPLE_SIGNATURE,
        "mytest", new HashMap<String, String>());
    assertArrayEquals("Incorrect return data from execProcedure",
      SimpleMasterProcedureManager.SIMPLE_DATA.getBytes(), result);
  }
}
