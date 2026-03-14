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
package org.apache.hadoop.hbase.client;

import static org.apache.hadoop.hbase.client.AsyncProcess.START_LOG_ERRORS_AFTER_COUNT_KEY;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import org.apache.hadoop.hbase.HBaseParameterizedTestTemplate;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.procedure.ProcedureManagerHost;
import org.apache.hadoop.hbase.procedure.SimpleMasterProcedureManager;
import org.apache.hadoop.hbase.procedure.SimpleRSProcedureManager;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestTemplate;

/**
 * Class to test asynchronous procedure admin operations.
 */
@Tag(LargeTests.TAG)
@Tag(ClientTests.TAG)
@HBaseParameterizedTestTemplate(name = "{index}: policy = {0}")
public class TestAsyncProcedureAdminApi extends TestAsyncAdminBase {

  public TestAsyncProcedureAdminApi(Supplier<AsyncAdmin> admin) {
    super(admin);
  }

  @BeforeAll
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

  @AfterAll
  public static void tearDownAfterClass() throws Exception {
    TestAsyncAdminBase.tearDownAfterClass();
  }

  @TestTemplate
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

  @TestTemplate
  public void testExecProcedureWithRet() throws Exception {
    byte[] result = admin.execProcedureWithReturn(SimpleMasterProcedureManager.SIMPLE_SIGNATURE,
      "myTest2", new HashMap<>()).get();
    assertArrayEquals(Bytes.toBytes(SimpleMasterProcedureManager.SIMPLE_DATA), result,
      "Incorrect return data from execProcedure");
  }

  @TestTemplate
  public void listProcedure() throws Exception {
    String procList = admin.getProcedures().get();
    assertTrue(procList.startsWith("["));
  }

  @TestTemplate
  public void isProcedureFinished() throws Exception {
    boolean failed = false;
    try {
      admin.isProcedureFinished("fake-signature", "fake-instance", new HashMap<>()).get();
    } catch (Exception e) {
      failed = true;
    }
    assertTrue(failed);
  }

  @TestTemplate
  public void abortProcedure() throws Exception {
    long procId = ThreadLocalRandom.current().nextLong();
    boolean abortResult = admin.abortProcedure(procId, true).get();
    assertFalse(abortResult);
  }
}
