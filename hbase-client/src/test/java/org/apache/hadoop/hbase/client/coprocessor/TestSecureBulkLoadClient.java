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
package org.apache.hadoop.hbase.client.coprocessor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.generated.SecureBulkLoadProtos.CleanupBulkLoadResponse;
import org.apache.hadoop.hbase.protobuf.generated.SecureBulkLoadProtos.PrepareBulkLoadResponse;
import org.apache.hadoop.hbase.protobuf.generated.SecureBulkLoadProtos.SecureBulkLoadHFilesResponse;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.util.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

/**
 * Test class for the SecureBulkLoadClient.
 */
@Category({ClientTests.class, SmallTests.class})
public class TestSecureBulkLoadClient {
  private static final DoNotRetryIOException DNRIOE = new DoNotRetryIOException("Go away and don't come back");

  private CoprocessorRpcChannel rpcChannel;
  private Table table;
  private SecureBulkLoadClient client;

  @Before
  public void setupMembers() {
    rpcChannel = mock(CoprocessorRpcChannel.class);
    table = mock(Table.class);
    client = new SecureBulkLoadClient(table);

    // Mock out a Table to return a custom CoprocessorRpcChannel
    when(table.coprocessorService(HConstants.EMPTY_START_ROW)).thenReturn(rpcChannel);
  }

  /**
   * Mocks an RPC to the SecureBulkLoadEndpoint.
   *
   * The provided {@code response} and {@code failureCause} are set such that the
   * {@link SecureBulkLoadClient} thinks that they were sent by a RegionServer.
   * The {@code expectedMethodName} verifies that the proper RPC method was
   * invoked as a sanity check.
   */
  @SuppressWarnings("unchecked")
  private void setupMockForMethod(
      final String expectedMethodName, final Message response, final IOException failureCause) {
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        MethodDescriptor method = invocation.getArgumentAt(0, MethodDescriptor.class);
        ServerRpcController controller = invocation.getArgumentAt(1, ServerRpcController.class);
        RpcCallback<Message> rpcCallback = invocation.getArgumentAt(4, RpcCallback.class);

        assertEquals(expectedMethodName, method.getName());
        controller.setFailedOn(failureCause);
        rpcCallback.run(response);

        return null;
      }
    }).when(rpcChannel).callMethod(
        any(MethodDescriptor.class), any(RpcController.class), any(Message.class),
        any(Message.class), any(RpcCallback.class));
  }

  @Test
  public void testPreservedExceptionOnPrepare() {
    PrepareBulkLoadResponse response = PrepareBulkLoadResponse.newBuilder()
        .setBulkToken("unused").build();
    setupMockForMethod("PrepareBulkLoad", response, DNRIOE);

    try {
      client.prepareBulkLoad(TableName.valueOf("prepare"));
    } catch (IOException e) {
      checkCaughtException(e);
    }
  }

  @Test
  public void testPreservedExceptionOnCleanup() {
    CleanupBulkLoadResponse response = CleanupBulkLoadResponse.newBuilder().build();
    setupMockForMethod("CleanupBulkLoad", response, DNRIOE);

    try {
      client.cleanupBulkLoad("unused");
    } catch (IOException e) {
      checkCaughtException(e);
    }
  }

  @Test
  public void testPreservedExceptionOnBulkLoad() {
    SecureBulkLoadHFilesResponse response = SecureBulkLoadHFilesResponse.newBuilder()
        .setLoaded(false).build();
    setupMockForMethod("SecureBulkLoadHFiles", response, DNRIOE);

    try {
      client.bulkLoadHFiles(
          Collections.<Pair<byte[],String>> emptyList(), null, "unused", new byte[0]);
    } catch (IOException e) {
      checkCaughtException(e);
    }
  }

  private void checkCaughtException(IOException e) {
    assertTrue(
        "Expected the DoNotRetryIOException to be returned without being wrapped by"
        + " another IOException. Was " + StringUtils.stringifyException(e), e == DNRIOE);
  }
}
