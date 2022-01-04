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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcCallback;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService.Interface;

/**
 * Make sure we could fallback to use replay method if replicateToReplica method is not present,
 * i.e, we are connecting an old region server.
 */
@Category({ RegionServerTests.class, SmallTests.class })
public class TestFallbackToUseReplay {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestFallbackToUseReplay.class);

  private static Configuration CONF = HBaseConfiguration.create();

  private static AsyncClusterConnectionImpl CONN;

  private static AsyncRegionReplicationRetryingCaller CALLER;

  private static RegionInfo REPLICA =
    RegionInfoBuilder.newBuilder(TableName.valueOf("test")).setReplicaId(1).build();

  private static AtomicBoolean REPLAY_CALLED = new AtomicBoolean(false);

  @BeforeClass
  public static void setUpBeforeClass() throws IOException {
    CONF.setInt(AsyncConnectionConfiguration.START_LOG_ERRORS_AFTER_COUNT_KEY, 0);
    AsyncRegionLocator locator = mock(AsyncRegionLocator.class);
    when(locator.getRegionLocation(any(), any(), anyInt(), any(), anyLong()))
      .thenReturn(CompletableFuture.completedFuture(new HRegionLocation(REPLICA,
        ServerName.valueOf("localhost", 12345, EnvironmentEdgeManager.currentTime()))));
    AdminService.Interface stub = mock(AdminService.Interface.class);
    // fail the call to replicateToReplica
    doAnswer(i -> {
      HBaseRpcController controller = i.getArgument(0, HBaseRpcController.class);
      controller.setFailed(new DoNotRetryIOException(new UnsupportedOperationException()));
      RpcCallback<?> done = i.getArgument(2, RpcCallback.class);
      done.run(null);
      return null;
    }).when(stub).replicateToReplica(any(), any(), any());
    doAnswer(i -> {
      REPLAY_CALLED.set(true);
      RpcCallback<?> done = i.getArgument(2, RpcCallback.class);
      done.run(null);
      return null;
    }).when(stub).replay(any(), any(), any());
    CONN = new AsyncClusterConnectionImpl(CONF, mock(ConnectionRegistry.class), "test", null,
      User.getCurrent()) {

      @Override
      AsyncRegionLocator getLocator() {
        return locator;
      }

      @Override
      Interface getAdminStub(ServerName serverName) throws IOException {
        return stub;
      }
    };
    CALLER = new AsyncRegionReplicationRetryingCaller(AsyncClusterConnectionImpl.RETRY_TIMER, CONN,
      10, TimeUnit.SECONDS.toNanos(1), TimeUnit.SECONDS.toNanos(10), REPLICA,
      Collections.emptyList());
  }

  @AfterClass
  public static void tearDownAfterClass() throws IOException {
    Closeables.close(CONN, true);
  }

  @Test
  public void testFallback() {
    CALLER.call().join();
    assertTrue(REPLAY_CALLED.get());
  }
}
