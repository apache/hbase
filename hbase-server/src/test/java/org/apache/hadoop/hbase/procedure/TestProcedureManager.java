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
package org.apache.hadoop.hbase.procedure;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MasterTests.TAG)
@Tag(MediumTests.TAG)
public class TestProcedureManager {

  private static final int NUM_RS = 2;
  private static HBaseTestingUtil util = new HBaseTestingUtil();

  @BeforeAll
  public static void setupBeforeClass() throws Exception {
    // set configure to indicate which pm should be loaded
    Configuration conf = util.getConfiguration();

    conf.set(ProcedureManagerHost.MASTER_PROCEDURE_CONF_KEY,
      SimpleMasterProcedureManager.class.getName());
    conf.set(ProcedureManagerHost.REGIONSERVER_PROCEDURE_CONF_KEY,
      SimpleRSProcedureManager.class.getName());

    util.startMiniCluster(NUM_RS);
  }

  @AfterAll
  public static void tearDownAfterClass() throws Exception {
    util.shutdownMiniCluster();
  }

  @Test
  public void testSimpleProcedureManager() throws IOException {
    Admin admin = util.getAdmin();

    byte[] result = admin.execProcedureWithReturn(SimpleMasterProcedureManager.SIMPLE_SIGNATURE,
      "mytest", new HashMap<>());
    assertArrayEquals(Bytes.toBytes(SimpleMasterProcedureManager.SIMPLE_DATA), result,
      "Incorrect return data from execProcedure");
  }
}
