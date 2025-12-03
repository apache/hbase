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
package org.apache.hadoop.hbase.coprocessor;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.AsyncConnectionImpl;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.ConnectionRegistry;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterRpcServices;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests invocation of the {@link MasterObserver} interface
 * hooks at all appropriate times during normal HMaster operations.
 */
@Category({ CoprocessorTests.class, MediumTests.class })
public class TestClientMetaCoprocessor {
  private static final Logger LOG = LoggerFactory.getLogger(TestClientMetaCoprocessor.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestClientMetaCoprocessor.class);

  @Rule
  public TestName name = new TestName();

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  private static final ServerName SERVER_NAME = ServerName.valueOf("localhost", 1234, 12345);

  public static class TestCoprocessor implements ClientMetaCoprocessor {
    protected final ClientMetaObserver observer;

    public TestCoprocessor() {
      observer = mock(ClientMetaObserver.class);
      resetMock();
    }
    
    protected void resetMock() {
      reset(observer);

      try {
        doAnswer(answer -> answer.getArgument(1, String.class)).when(observer)
          .postGetClusterId(any(), any());
        doAnswer(answer -> {
          return answer.getArgument(1, ServerName.class);
        }).when(observer).postGetActiveMaster(any(), any());
        doAnswer(answer -> answer.getArgument(1, Map.class)).when(observer)
          .postGetMasters(any(), any());
        doAnswer(answer -> answer.getArgument(1, List.class)).when(observer)
          .postGetBootstrapNodes(any(), any());
        doAnswer(answer -> answer.getArgument(1, List.class)).when(observer)
          .postGetMetaLocations(any(), any());
      } catch (IOException e) {
        throw new IllegalStateException("Could not setup observer mock.", e);
      }
    }

    @Override
    public Optional<ClientMetaObserver> getClientMetaObserver() {
      return Optional.of(observer);
    }
  }

  private static TestCoprocessor getCoprocessor() {
    HMaster master = UTIL.getMiniHBaseCluster().getMaster();
    MasterRpcServices masterRpcServices = master.getMasterRpcServices();
    
    return masterRpcServices.getClientMetaCoprocessorHost().findCoprocessor(TestCoprocessor.class);
  }

  private static ClientMetaObserver getObserverMock() {
    return getCoprocessor().getClientMetaObserver().get();
  }

  private static void resetObserverMock() {
    getCoprocessor().resetMock();
  }

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    conf.set(CoprocessorHost.CLIENT_META_COPROCESSOR_CONF_KEY, TestCoprocessor.class.getName());

    UTIL.startMiniCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Before
  public void setupBefore() {
    resetObserverMock();
  }

  @Test
  public void testGetClusterId() throws Exception {
    ClientMetaObserver observer = getObserverMock();

    try (AsyncConnectionImpl asyncConnection =
      (AsyncConnectionImpl) ConnectionFactory.createAsyncConnection(UTIL.getConfiguration()).get()) {
      ConnectionRegistry connectionRegistry = asyncConnection.getConnectionRegistry();

      doReturn("cluster-id").when(observer).postGetClusterId(any(), any());
      clearInvocations(observer);

      String clusterId = connectionRegistry.getClusterId().get();
      assertEquals("cluster-id", clusterId);

      verify(observer).preGetClusterId(any());
      verify(observer).postGetClusterId(any(), any());
      verifyNoMoreInteractions(observer);
    }
  }

  @Test
  public void testGetActiveMaster() throws Exception {
    ClientMetaObserver observer = getObserverMock();

    try (AsyncConnectionImpl asyncConnection =
      (AsyncConnectionImpl) ConnectionFactory.createAsyncConnection(UTIL.getConfiguration()).get()) {
      ConnectionRegistry connectionRegistry = asyncConnection.getConnectionRegistry();

      doReturn(SERVER_NAME).when(observer).postGetActiveMaster(any(), any());
      clearInvocations(observer);

      ServerName activeMaster = connectionRegistry.getActiveMaster().get();
      assertEquals(SERVER_NAME, activeMaster);

      verify(observer).preGetActiveMaster(any());
      verify(observer).postGetActiveMaster(any(), any());
      verifyNoMoreInteractions(observer);
    }
  }

  @Test
  public void testGetMetaRegionLocations() throws Exception {
    ClientMetaObserver observer = getObserverMock();

    try (AsyncConnectionImpl asyncConnection =
      (AsyncConnectionImpl) ConnectionFactory.createAsyncConnection(UTIL.getConfiguration()).get()) {
      ConnectionRegistry connectionRegistry = asyncConnection.getConnectionRegistry();

      HRegionLocation metaRegionLocation =
        new HRegionLocation(RegionInfoBuilder.FIRST_META_REGIONINFO, SERVER_NAME);
      doReturn(List.of(metaRegionLocation)).when(observer).postGetMetaLocations(any(), any());
      clearInvocations(observer);

      RegionLocations regionLocations = connectionRegistry.getMetaRegionLocations().get();
      HRegionLocation actualMetaRegionLocation = regionLocations.getDefaultRegionLocation();

      assertEquals(metaRegionLocation, actualMetaRegionLocation);

      verify(observer).preGetMetaLocations(any());
      verify(observer).postGetMetaLocations(any(), any());
      verifyNoMoreInteractions(observer);
    }
  }
}
