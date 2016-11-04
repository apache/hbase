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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsSnapshotDoneRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsSnapshotDoneResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SnapshotRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SnapshotResponse;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import org.apache.hadoop.hbase.shaded.com.google.protobuf.RpcController;

/**
 * Test snapshot logic from the client
 */
@Category({SmallTests.class, ClientTests.class})
public class TestSnapshotFromAdmin {

  private static final Log LOG = LogFactory.getLog(TestSnapshotFromAdmin.class);

  /**
   * Test that the logic for doing 'correct' back-off based on exponential increase and the max-time
   * passed from the server ensures the correct overall waiting for the snapshot to finish.
   * @throws Exception
   */
  @Test(timeout = 60000)
  public void testBackoffLogic() throws Exception {
    final int pauseTime = 100;
    final int maxWaitTime =
      HConstants.RETRY_BACKOFF[HConstants.RETRY_BACKOFF.length - 1] * pauseTime;
    final int numRetries = HConstants.RETRY_BACKOFF.length;
    // calculate the wait time, if we just do straight backoff (ignoring the expected time from
    // master)
    long ignoreExpectedTime = 0;
    for (int i = 0; i < HConstants.RETRY_BACKOFF.length; i++) {
      ignoreExpectedTime += HConstants.RETRY_BACKOFF[i] * pauseTime;
    }
    // the correct wait time, capping at the maxTime/tries + fudge room
    final long time = pauseTime * 3 + ((maxWaitTime / numRetries) * 3) + 300;
    assertTrue("Capped snapshot wait time isn't less that the uncapped backoff time "
        + "- further testing won't prove anything.", time < ignoreExpectedTime);

    // setup the mocks
    ConnectionImplementation mockConnection = Mockito
        .mock(ConnectionImplementation.class);
    Configuration conf = HBaseConfiguration.create();
    // setup the conf to match the expected properties
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, numRetries);
    conf.setLong("hbase.client.pause", pauseTime);

    // mock the master admin to our mock
    MasterKeepAliveConnection mockMaster = Mockito.mock(MasterKeepAliveConnection.class);
    Mockito.when(mockConnection.getConfiguration()).thenReturn(conf);
    Mockito.when(mockConnection.getKeepAliveMasterService()).thenReturn(mockMaster);
    // we need a real retrying caller
    RpcRetryingCallerFactory callerFactory = new RpcRetryingCallerFactory(conf);
    RpcControllerFactory controllerFactory = Mockito.mock(RpcControllerFactory.class);
    Mockito.when(controllerFactory.newController()).thenReturn(
      Mockito.mock(HBaseRpcController.class));
    Mockito.when(mockConnection.getRpcRetryingCallerFactory()).thenReturn(callerFactory);
    Mockito.when(mockConnection.getRpcControllerFactory()).thenReturn(controllerFactory);
    // set the max wait time for the snapshot to complete
    SnapshotResponse response = SnapshotResponse.newBuilder()
        .setExpectedTimeout(maxWaitTime)
        .build();
    Mockito
    .when(
      mockMaster.snapshot((RpcController) Mockito.any(),
        Mockito.any(SnapshotRequest.class))).thenReturn(response);
    // setup the response
    IsSnapshotDoneResponse.Builder builder = IsSnapshotDoneResponse.newBuilder();
    builder.setDone(false);
    // first five times, we return false, last we get success
    Mockito.when(
      mockMaster.isSnapshotDone((RpcController) Mockito.any(),
        Mockito.any(IsSnapshotDoneRequest.class))).thenReturn(builder.build(), builder.build(),
          builder.build(), builder.build(), builder.build(), builder.setDone(true).build());

    // setup the admin and run the test
    Admin admin = new HBaseAdmin(mockConnection);
    String snapshot = "snapshot";
    TableName table = TableName.valueOf("table");
    // get start time
    long start = System.currentTimeMillis();
    admin.snapshot(snapshot, table);
    long finish = System.currentTimeMillis();
    long elapsed = (finish - start);
    assertTrue("Elapsed time:" + elapsed + " is more than expected max:" + time, elapsed <= time);
    admin.close();
  }

  /**
   * Make sure that we validate the snapshot name and the table name before we pass anything across
   * the wire
   * @throws Exception on failure
   */
  @Test
  public void testValidateSnapshotName() throws Exception {
    ConnectionImplementation mockConnection = Mockito
        .mock(ConnectionImplementation.class);
    Configuration conf = HBaseConfiguration.create();
    Mockito.when(mockConnection.getConfiguration()).thenReturn(conf);
    // we need a real retrying caller
    RpcRetryingCallerFactory callerFactory = new RpcRetryingCallerFactory(conf);
    RpcControllerFactory controllerFactory = Mockito.mock(RpcControllerFactory.class);
    Mockito.when(controllerFactory.newController()).thenReturn(
      Mockito.mock(HBaseRpcController.class));
    Mockito.when(mockConnection.getRpcRetryingCallerFactory()).thenReturn(callerFactory);
    Mockito.when(mockConnection.getRpcControllerFactory()).thenReturn(controllerFactory);
    Admin admin = new HBaseAdmin(mockConnection);
    // check that invalid snapshot names fail
    failSnapshotStart(admin, new SnapshotDescription(HConstants.SNAPSHOT_DIR_NAME));
    failSnapshotStart(admin, new SnapshotDescription("-snapshot"));
    failSnapshotStart(admin, new SnapshotDescription("snapshot fails"));
    failSnapshotStart(admin, new SnapshotDescription("snap$hot"));
    failSnapshotStart(admin, new SnapshotDescription("snap:hot"));
    // check the table name also get verified
    failSnapshotDescriptorCreation("snapshot", ".table");
    failSnapshotDescriptorCreation("snapshot", "-table");
    failSnapshotDescriptorCreation("snapshot", "table fails");
    failSnapshotDescriptorCreation("snapshot", "tab%le");

    // mock the master connection
    MasterKeepAliveConnection master = Mockito.mock(MasterKeepAliveConnection.class);
    Mockito.when(mockConnection.getKeepAliveMasterService()).thenReturn(master);
    SnapshotResponse response = SnapshotResponse.newBuilder().setExpectedTimeout(0).build();
    Mockito.when(
      master.snapshot((RpcController) Mockito.any(), Mockito.any(SnapshotRequest.class)))
        .thenReturn(response);
    IsSnapshotDoneResponse doneResponse = IsSnapshotDoneResponse.newBuilder().setDone(true).build();
    Mockito.when(
      master.isSnapshotDone((RpcController) Mockito.any(),
          Mockito.any(IsSnapshotDoneRequest.class))).thenReturn(doneResponse);

      // make sure that we can use valid names
    admin.snapshot(new SnapshotDescription("snapshot", TableName.valueOf("table")));
  }

  private void failSnapshotStart(Admin admin, SnapshotDescription snapshot)
      throws IOException {
    try {
      admin.snapshot(snapshot);
      fail("Snapshot should not have succeed with name:" + snapshot.getName());
    } catch (IllegalArgumentException e) {
      LOG.debug("Correctly failed to start snapshot:" + e.getMessage());
    }
  }

  private void failSnapshotDescriptorCreation(final String snapshotName, final String tableName) {
    try {
      new SnapshotDescription(snapshotName, tableName);
      fail("SnapshotDescription should not have succeed with name:" + snapshotName);
    } catch (IllegalArgumentException e) {
      LOG.debug("Correctly failed to create SnapshotDescription:" + e.getMessage());
    }
  }
}
