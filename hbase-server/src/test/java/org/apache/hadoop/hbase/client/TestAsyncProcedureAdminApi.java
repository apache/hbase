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
package org.apache.hadoop.hbase.client;

import static org.apache.hadoop.hbase.client.AsyncConnectionConfiguration.START_LOG_ERRORS_AFTER_COUNT_KEY;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.procedure.ProcedureManagerHost;
import org.apache.hadoop.hbase.procedure.SimpleMasterProcedureManager;
import org.apache.hadoop.hbase.procedure.SimpleRSProcedureManager;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Class to test asynchronous procedure admin operations.
 */
@RunWith(Parameterized.class)
@Category({ LargeTests.class, ClientTests.class })
public class TestAsyncProcedureAdminApi extends TestAsyncAdminBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAsyncProcedureAdminApi.class);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 60000);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 120000);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);
    TEST_UTIL.getConfiguration().setInt(START_LOG_ERRORS_AFTER_COUNT_KEY, 0);
    TEST_UTIL.getConfiguration().set(ProcedureManagerHost.MASTER_PROCEDURE_CONF_KEY,
      SimpleMasterProcedureManager.class.getName());
    TEST_UTIL.getConfiguration().set(ProcedureManagerHost.REGIONSERVER_PROCEDURE_CONF_KEY,
      SimpleRSProcedureManager.class.getName());
    TEST_UTIL.getConfiguration().setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
    TEST_UTIL.startMiniCluster(2);
    ASYNC_CONN = ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration()).get();
  }

  @Test
  public void testExecProcedure() throws Exception {
    String snapshotString = "offlineTableSnapshot";
    try {
      Table table = TEST_UTIL.createTable(tableName, Bytes.toBytes("cf"));
      for (int i = 0; i < 100; i++) {
        Put put = new Put(Bytes.toBytes(i)).addColumn(Bytes.toBytes("cf"), null, Bytes.toBytes(i));
        table.put(put);
      }
      // take a snapshot of the enabled table
      Map<String, String> props = new HashMap<>();
      props.put("table", tableName.getNameAsString());
      admin.execProcedure(SnapshotManager.ONLINE_SNAPSHOT_CONTROLLER_DESCRIPTION, snapshotString,
        props).get();
      LOG.debug("Snapshot completed.");
    } finally {
      admin.deleteSnapshot(snapshotString).join();
      TEST_UTIL.deleteTable(tableName);
    }
  }

  @Test
  public void testExecProcedureWithRet() throws Exception {
    byte[] result = admin.execProcedureWithReturn(SimpleMasterProcedureManager.SIMPLE_SIGNATURE,
      "myTest2", new HashMap<>()).get();
    assertArrayEquals("Incorrect return data from execProcedure",
      Bytes.toBytes(SimpleMasterProcedureManager.SIMPLE_DATA), result);
  }

  @Test
  public void listProcedure() throws Exception {
    String procList = admin.getProcedures().get();
    assertTrue(procList.startsWith("["));
  }

  @Test
  public void isProcedureFinished() throws Exception {
    boolean failed = false;
    try {
      admin.isProcedureFinished("fake-signature", "fake-instance", new HashMap<>()).get();
    } catch (Exception e) {
      failed = true;
    }
    Assert.assertTrue(failed);
  }

  @Test
  public void abortProcedure() throws Exception {
    long procId = ThreadLocalRandom.current().nextLong();
    boolean abortResult = admin.abortProcedure(procId, true).get();
    assertFalse(abortResult);
  }
}
